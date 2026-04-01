# Lesson 7: Query Execution Flow

## Learning Goals

After this lesson you should be able to:

- Trace a query from Java construction through C++ execution to Java results
- Understand the role of splits and why they are added after task creation
- Use the UpIterator state machine to consume query results
- Use the convenience wrappers (`UpIterators.asJavaIterator`) for simple iteration
- Read and understand the query execution tests

---

## 7.1 The Execution Pipeline at a Glance

```
 1. Build         2. Execute          3. Feed data       4. Read results
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Query    │───>│ SerialTask   │───>│  addSplit()  │───>│  advance()   │
│  (plan +  │    │ (Velox task  │    │ noMoreSplits │    │  get()       │
│  config)  │    │  created)    │    │              │    │  → RowVector │
└──────────┘    └──────────────┘    └──────────────┘    └──────────────┘
```

Every query execution follows these four steps. Let's walk through each one.

---

## 7.2 Step 1: Build a Query

A `Query` bundles three things:

```java
Query query = new Query(
    planNode,          // the plan tree (what to compute)
    Config.empty(),    // query-level config (tuning knobs)
    ConnectorConfig.empty()  // per-connector config
);
```

- **Plan**: The tree of `PlanNode`s from Lesson 5 (e.g., `FilterNode → TableScanNode`)
- **Config**: Key-value pairs like `max_output_batch_rows` that tune Velox behavior
- **ConnectorConfig**: Per-connector settings (e.g., Hive-specific options)

The Query is a pure Java object at this point — no C++ objects have been created yet.

**Source:** `src/main/java/org/boostscale/velox4j/query/Query.java`

---

## 7.3 Step 2: Execute the Query

```java
SerialTask task = session.queryOps().execute(query);
```

This single line does a lot. Let's trace it:

### What happens inside `execute()`

```java
// Queries.java
public SerialTask execute(Query query) {
    try (final QueryExecutor exec = jniApi.createQueryExecutor(query)) {
        return exec.execute();
    }
}
```

1. **`createQueryExecutor(query)`**: Serializes the `Query` to JSON, sends it to C++ via JNI. C++ deserializes it and creates a `QueryExecutor` that holds the parsed plan.

2. **`exec.execute()`**: Tells C++ to create a Velox `Task` from the plan. This is where the real Velox machinery starts.

3. The `QueryExecutor` is immediately closed (via try-with-resources) — it's only needed to produce the task.

### What happens in C++ (`QueryExecutor.cc`)

```cpp
SerialTask::SerialTask(MemoryManager* memoryManager, std::shared_ptr<const Query> query) {
    // 1. Create a PlanFragment from the plan
    core::PlanFragment planFragment{query->plan(), core::ExecutionStrategy::kUngrouped, 1, {}};

    // 2. Create a QueryCtx with config and memory pool
    auto queryCtx = core::QueryCtx::create(
        nullptr,
        core::QueryConfig{query->queryConfig()->toMap()},
        query->connectorConfig()->toMap(),
        ...);

    // 3. Create the Velox Task in serial execution mode
    auto task = exec::Task::create(
        "Task - EID ...",
        std::move(planFragment),
        0,
        std::move(queryCtx),
        exec::Task::ExecutionMode::kSerial);  // ← single-threaded execution

    task_ = task;
}
```

Key point: the task is created in **serial execution mode**. See the next section for what this means.

**Source:** `src/main/cpp/main/velox4j/query/QueryExecutor.cc`

### Velox execution modes: serial vs parallel

Velox's `Task` has two execution modes (`exec::Task::ExecutionMode`). Understanding the difference helps you know what's happening under the hood.

**Parallel mode (`kParallel`)** — Velox's default mode, used by Presto and most production deployments:

- Velox creates **multiple Driver threads** internally (e.g., 4 drivers for 4 CPU cores)
- Each Driver runs a pipeline of operators (scan → filter → project → ...) independently
- Drivers pull data from a shared split queue — each driver grabs splits and processes them concurrently
- The caller starts the task via `Task::start(threadPool, numDrivers)` and collects results from an **asynchronous output buffer**
- Results arrive asynchronously — you register a callback or poll the output buffer

```
Parallel mode:

  Caller: task->start(threadPool, 4)    // fire-and-forget, results arrive asynchronously

  Internal (4 threads running concurrently):
  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
  │ Driver 0 │  │ Driver 1 │  │ Driver 2 │  │ Driver 3 │
  │ scan→flt │  │ scan→flt │  │ scan→flt │  │ scan→flt │
  │ →proj    │  │ →proj    │  │ →proj    │  │ →proj    │
  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
       └──────────────┴──────────────┴──────────────┘
                              │
                      Output Buffer (async)
                              │
                    Caller reads results
```

**Serial mode (`kSerial`)** — the mode Velox4J uses:

- Velox creates **one Driver**, and the caller drives it **synchronously** by calling `task->next()`
- Each call to `next()` processes one batch of data and returns a `RowVector`
- No internal threads — the calling thread does all the work
- Simpler programming model: call `next()` in a loop until it returns `nullptr`

```
Serial mode:

  Same caller thread does all the work:
  ┌──────────────────────────────────────────┐
  │  while (true) {                           │
  │      auto batch = task->next(&future);    │  ← caller thread runs Velox operators
  │      if (batch == nullptr) break;         │
  │      // use batch                         │
  │  }                                        │
  └──────────────────────────────────────────┘

  Internal: single Driver, no thread pool
```

**Why does Velox4J only support serial mode?** Three reasons:

1. **JNI threading is complex**. In parallel mode, Velox's internal threads would produce results asynchronously. Consuming them from Java would require thread-safe output buffers shared between C++ threads and Java, `JNI AttachCurrentThread` calls for every Velox-internal thread, and complex synchronization with Java's GC. Serial mode avoids all of this — the Java thread that calls `advance()` is the same thread that runs the Velox operators. No threading issues.

2. **Velox4J's use case doesn't need it**. Velox4J targets embedded use — a JVM application that uses Velox as a library. In this scenario, parallelism is typically managed at the **Java framework level** (e.g., Flink manages its own threads, each running one serial Velox task). The Java framework handles partition-level parallelism, so there's no need for Velox to also parallelize internally.

3. **Simpler resource management**. Serial mode means all C++ objects are accessed from one thread. The Session/ObjectStore design doesn't need concurrent access handling. Memory accounting is also simpler since one memory pool maps to one task.

---

## 7.4 Step 3: Feed Data with Splits

After creating the task, you must tell the scan nodes where to find their data. This is done via **splits**.

### What is a split?

A split is a unit of data that a `TableScanNode` should read. For the Hive connector, a split typically points to a file (or a range within a file):

```java
ConnectorSplit split = new HiveConnectorSplit(
    "connector-hive",       // connectorId — must match the scan node's connector
    0,                      // splitWeight
    false,                  // cacheable
    file.getAbsolutePath(), // filePath — the actual data file
    FileFormat.PARQUET,     // fileFormat
    0,                      // start offset in the file
    file.length(),          // length to read
    Map.of(),               // partitionKeys
    null, null, Map.of(), null, Map.of(), Map.of(), null, null  // optional fields
);
```

### Why are splits separate from the plan?

This is a **Velox design choice**. The plan describes *what* to compute (schema, filters, joins). Splits describe *where* the data lives (files, byte ranges). Separating them enables:

- Adding multiple splits to read multiple files in sequence
- Dynamic split assignment in distributed systems (a coordinator assigns splits to workers)
- Starting execution before all data locations are known

### Adding splits to a task

```java
// Tell scan node "scan-1" to read this file
task.addSplit(scanNode.getId(), split);

// Signal that no more files will be added for "scan-1"
task.noMoreSplits(scanNode.getId());
```

**Critical**: You must call `noMoreSplits()` for every scan node. Without it, the task will hang waiting for more data. The plan node ID (e.g., `"scan-1"`) links the split to the correct scan node.

### Joins need splits for both sides

```java
// For a HashJoinNode with two TableScanNodes:
task.addSplit(leftScanNode.getId(), leftSplit);
task.addSplit(rightScanNode.getId(), rightSplit);
task.noMoreSplits(leftScanNode.getId());
task.noMoreSplits(rightScanNode.getId());
```

### Multiple splits for one scan node

You can add multiple files to a single scan:

```java
for (File file : dataFiles) {
    task.addSplit(scanNode.getId(), newSplit(file));
}
task.noMoreSplits(scanNode.getId());
```

Velox reads them sequentially and produces results as if all files were a single table.

---

## 7.5 Step 4: Read Results with the UpIterator

`SerialTask` implements `UpIterator` — a state-machine based iterator that pulls columnar batches from Velox.

### The UpIterator state machine

```
            ┌──────────┐
            │  START    │
            └────┬─────┘
                 │ advance()
                 ▼
         ┌───────────────┐
    ┌───>│   advance()    │<────────────────────┐
    │    └───┬────┬────┬──┘                     │
    │        │    │    │                         │
    │  BLOCKED  AVAILABLE  FINISHED              │
    │        │    │    │                         │
    │        ▼    │    ▼                         │
    │   waitFor() │  (done)                     │
    │        │    │                              │
    │        │    ▼                              │
    │        │  get() → RowVector               │
    │        │    │                              │
    │        └────┴──────────────────────────────┘
    │             (loop back to advance)
    └─────────────────────────────────────────────
```

Three states:
- **`AVAILABLE`**: A batch of data is ready. Call `get()` to retrieve the `RowVector`.
- **`BLOCKED`**: The task is waiting (e.g., for splits, for a join build side). Call `waitFor()` to block until progress is made, then call `advance()` again.
- **`FINISHED`**: No more data. Stop iterating.

### Low-level iteration

```java
while (true) {
    UpIterator.State state = task.advance();
    switch (state) {
        case AVAILABLE:
            RowVector batch = task.get();
            // process batch...
            break;
        case BLOCKED:
            task.waitFor();   // wait for data to become available
            break;            // then loop back to advance()
        case FINISHED:
            return;           // done
    }
}
```

### Convenience wrapper: `UpIterators.asJavaIterator()`

For most use cases, you don't need the state machine. Use the convenience wrapper that converts an `UpIterator` into a standard Java `Iterator<RowVector>`:

```java
Iterator<RowVector> itr = UpIterators.asJavaIterator(task);
while (itr.hasNext()) {
    RowVector batch = itr.next();
    // process batch...
}
```

Internally, `hasNext()` loops over `advance()`/`waitFor()` until it gets `AVAILABLE` or `FINISHED`:

```java
// UpIterators.java — AsJavaIterator
public boolean hasNext() {
    while (true) {
        final UpIterator.State state = upIterator.advance();
        switch (state) {
            case BLOCKED:
                upIterator.waitFor();
                continue;        // loop and try again
            case AVAILABLE:
                return true;     // next() will call get()
            case FINISHED:
                return false;    // iteration complete
        }
    }
}
```

**Source:** `src/main/java/org/boostscale/velox4j/iterator/UpIterators.java`

### Convenience wrapper: `UpIterators.asInfiniteIterator()`

For streaming scenarios (e.g., reading from a `BlockingQueue`), use `InfiniteIterator` which never returns `FINISHED`:

```java
InfiniteIterator<RowVector> out = UpIterators.asInfiniteIterator(task);
// Non-blocking check
if (out.available()) {
    RowVector batch = out.get();
}
// Or blocking wait
out.waitFor();
RowVector batch = out.get();
```

---

## 7.6 Understanding RowVector Results

Each call to `get()` returns a `RowVector` — a columnar batch of rows. It's NOT one row; it's a batch that may contain thousands of rows.

```java
RowVector batch = task.get();
batch.getSize();    // number of rows in this batch (e.g., 1024)
batch.getType();    // RowType describing the columns
batch.toString();   // TSV-formatted string of all rows
```

The number of rows per batch is controlled by Velox's `max_output_batch_rows` config:

```java
// Force small batches (7 rows each) for testing
Query query = new Query(
    scanNode,
    Config.create(Map.of("max_output_batch_rows", "7")),
    ConnectorConfig.empty()
);
```

To collect all results into a single vector, append batches:

```java
List<RowVector> batches = Streams.fromIterator(UpIterators.asJavaIterator(task))
    .collect(Collectors.toList());

RowVector merged = session.baseVectorOps().createEmpty(batches.get(0).getType()).asRowVector();
for (RowVector batch : batches) {
    merged.append(batch);
}
```

---

## 7.7 Complete Example: Table Scan with Filter

Putting it all together — read a Parquet file, filter by region key, and print results:

```java
// 1. Initialize (once)
Velox4j.initialize();
MemoryManager memoryManager = Velox4j.newMemoryManager(AllocationListener.NOOP);

try (Session session = Velox4j.newSession(memoryManager)) {
    // 2. Define schema
    RowType schema = new RowType(
        List.of("n_nationkey", "n_name", "n_regionkey", "n_comment"),
        List.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType()));

    // 3. Build plan: Filter(TableScan)
    TableScanNode scan = new TableScanNode("scan-1", schema,
        new HiveTableHandle("connector-hive", "nation", List.of(), null, schema, Map.of()),
        toAssignments(schema));

    FilterNode filter = new FilterNode("filter-1", List.of(scan),
        new CallTypedExpr(new BooleanType(), List.of(
            FieldAccessTypedExpr.create(new BigIntType(), "n_regionkey"),
            ConstantTypedExpr.create(new BigIntType(), new BigIntValue(3L))
        ), "greaterthanorequal"));

    // 4. Create and execute query
    Query query = new Query(filter, Config.empty(), ConnectorConfig.empty());
    SerialTask task = session.queryOps().execute(query);

    // 5. Add split (tell scan where the data is)
    File file = new File("/tmp/nation.parquet");
    task.addSplit(scan.getId(), new HiveConnectorSplit(
        "connector-hive", 0, false, file.getAbsolutePath(),
        FileFormat.PARQUET, 0, file.length(),
        Map.of(), null, null, Map.of(), null, Map.of(), Map.of(), null, null));
    task.noMoreSplits(scan.getId());

    // 6. Read results
    Iterator<RowVector> results = UpIterators.asJavaIterator(task);
    while (results.hasNext()) {
        RowVector batch = results.next();
        System.out.println(batch.toString());
    }
}

memoryManager.close();
```

---

## 7.8 Task Statistics

After iterating all results, you can collect execution statistics:

```java
// Iterate all results first
Streams.fromIterator(UpIterators.asJavaIterator(task)).collect(Collectors.toList());

// Then collect stats
SerialTaskStats stats = task.collectStats();

// Query stats for a specific plan node
JsonNode scanStats = stats.planStats("scan-1");
scanStats.get("operatorType").asText();  // "TableScan"
scanStats.get("inputRows").asInt();      // 25
scanStats.get("numSplits").asInt();      // 1

JsonNode aggStats = stats.planStats("agg-1");
aggStats.get("inputRows").asInt();       // 25
aggStats.get("outputRows").asInt();      // 5
```

Stats include rows processed, time spent, splits processed, and more — useful for debugging and performance tuning.

**Source:** `src/main/java/org/boostscale/velox4j/query/SerialTaskStats.java`

---

## 7.9 The C++ Execution Loop

For deeper understanding, here's what happens inside `SerialTask::advance0()` in C++:

```cpp
UpIterator::State SerialTask::advance0(bool wait) {
    while (true) {
        auto future = ContinueFuture::makeEmpty();
        auto out = task_->next(&future);     // Ask Velox for next batch

        if (!future.valid()) {
            // Task is not blocked
            if (out == nullptr) {
                return State::FINISHED;       // No more data
            }
            pending_ = std::move(out);
            return State::AVAILABLE;          // Batch ready
        }

        if (!wait) {
            return State::BLOCKED;            // Task is blocked, return immediately
        }

        // Wait mode: block until task makes progress
        std::move(future).wait(std::chrono::seconds(1));
    }
}
```

The key function is `task_->next(&future)` — Velox's serial execution API. It processes one batch of data and returns either a `RowVector` (data ready), `nullptr` (finished), or sets a `future` (blocked).

**Source:** `src/main/cpp/main/velox4j/query/QueryExecutor.cc`

---

## 7.10 Key Takeaways

1. **Four steps**: Build Query → Execute (creates SerialTask) → Add Splits → Read Results.

2. **Splits are separate from the plan**. The plan says *what* to compute; splits say *where* the data lives. You must call `addSplit()` and `noMoreSplits()` for every scan node, or the task hangs.

3. **SerialTask is an UpIterator**. Use `advance()`/`get()` for low-level control, or `UpIterators.asJavaIterator()` for simple iteration.

4. **Results come in batches** (`RowVector`s), not individual rows. Each batch may contain many rows. Use `batch.getSize()` to check.

5. **Serial execution mode** means single-threaded. Java drives execution by calling `advance()` — each call processes one batch.

6. **Plan node IDs link splits to scans**. The ID you pass to `addSplit("scan-1", split)` must match the `TableScanNode`'s ID.

7. **Statistics are available** after iteration completes via `task.collectStats()`.

---

## 7.11 Exercises

1. **Trace the full flow**: Open `QueryTest.testTableScan1()`. List every method call from query creation to result assertion. Which calls cross the JNI boundary?

2. **Join splits**: In `QueryTest.testHashJoin()`, two scan nodes get splits. What would happen if you forgot `task.noMoreSplits(regionScanNode.getId())`?

3. **Batch sizes**: In `QueryTest.testTableScanCollectMultipleRowVectorsLoadInline()`, the config `max_output_batch_rows=7` is set. The nation table has 25 rows. How many batches do you expect? Why does the test assert `allRvs.size() > 1`?

4. **State machine**: Using only `advance()`, `waitFor()`, and `get()`, write pseudocode to consume all results from a task and count the total number of rows.

5. **External stream**: In `QueryTest.testExternalStreamFromJavaIterator()`, data flows from one query's output into another query's input via an `ExternalStream`. Trace how the data moves: which classes are involved?

---

## Next Lesson

[Lesson 8: Memory Management & Arrow Interop](lesson-08-memory-and-arrow.md) — Understand memory pools, allocation listeners, and converting between Velox and Arrow formats.
