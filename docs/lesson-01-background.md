# Lesson 1: Background & Big Picture

## Learning Goals

After this lesson you should be able to:

- Explain what Velox is and why it exists
- Explain what Velox4J is and the problem it solves
- Describe Velox4J's core design philosophy
- Identify the key dependencies and downstream users
- Read through the "Get Started" example and understand each step at a high level

---

## 1.1 What is Velox?

Velox is an **open-source C++ unified execution engine** originally funded by Meta in 2020. Its purpose is to accelerate SQL query execution. Instead of being a standalone database, Velox is designed as a **library** that other query engines embed to handle the actual computation.

Key characteristics of Velox:

- **Columnar execution**: data is processed in batches of columns (vectors), not row-by-row. This is much faster for analytical queries because it enables SIMD, better cache utilization, and reduced per-row overhead.
- **Unified engine**: provides a single execution layer that multiple query engines can share, avoiding duplicated work.
- **Extensible**: supports pluggable connectors (e.g., Hive), functions, and data types.

Major projects that depend on Velox:

| Project | Role |
|---------|------|
| [Presto](https://github.com/prestodb/presto) | Distributed SQL query engine (Meta) |
| [Apache Gluten](https://github.com/apache/incubator-gluten) | Native acceleration for Spark/Flink |

---

## 1.2 What is Velox4J?

Velox4J is a set of **Java bindings for Velox**. It lets JVM applications invoke Velox's functionalities directly — without writing or maintaining any C++ or JNI glue code themselves.

Think of it as: **Velox-as-a-Java-library**.

Without Velox4J, a Java application that wants to use Velox would need to:

1. Write C++ code that calls Velox APIs
2. Write JNI bindings to bridge Java and C++
3. Maintain both layers as Velox evolves

Velox4J eliminates all of that.

### Real-world user

The **Gluten-Flink** subproject uses Velox4J to bring Velox-powered execution to Apache Flink:
- [gluten-flink source](https://github.com/apache/incubator-gluten/tree/main/gluten-flink)

---

## 1.3 Core Design Philosophy

Velox4J's design is built on three pillars:

### Pillar 1: Seamless API Mapping via JSON Serde

This is the most important design decision. Instead of creating a translation layer, Velox4J leverages **Velox's own JSON serialization/deserialization framework**.

The flow looks like this:

```
Java objects  --(Jackson JSON)-->  JSON string  --(JNI)-->  C++ string
                                                              |
                                                     folly::parseJson()
                                                              |
                                                     ISerializable::deserialize()
                                                              |
                                                     Native Velox objects
```

Because Velox already has a JSON serde for its types, plans, expressions, and connectors, Velox4J simply implements the **same JSON schema** in Java. This means:

- Java-side objects map **1:1** to Velox C++ objects
- No re-interpreting layer in C++
- New Velox features are easy to add (just add the Java class that produces the right JSON)
- The C++ codebase stays very small

**Example**: The Java `Query` class produces JSON like:

```json
{
  "name": "Query",
  "plan": { ... },
  "queryConfig": { "values": [] },
  "connectorConfig": { "values": [] }
}
```

The C++ side deserializes this directly into a `velox4j::Query` object via `ISerializable::deserialize<Query>(...)`.

### Pillar 2: Portable

The goal is that a single Velox4J JAR can be shipped to different platforms without rebuilding. The native library is bundled inside the JAR and extracted at runtime.

### Pillar 3: Arrow Compatible

Velox4J provides utilities to convert between Velox's native columnar format (`RowVector` / `BaseVector`) and Apache Arrow Java's format (`VectorSchemaRoot` / `FieldVector`). This makes it easy to integrate with the broader Arrow ecosystem.

---

## 1.4 Key Dependencies

| Dependency | Version | Purpose |
|-----------|---------|---------|
| Velox (C++) | pinned via `velox-ref.txt` | The execution engine itself |
| Jackson | 2.18.0 | JSON serialization (Java side) |
| Apache Arrow Java | 17.0.0 | Arrow interop |
| Guava | 33.4.0 | Common utilities |
| JUnit 4 | 4.13.1 | Testing |
| JDK | 8+ | Minimum Java version |
| GCC | 11+ | C++ compiler (for building from source) |

---

## 1.5 Walk-Through: The "Get Started" Example

Let's trace through the example from the README step by step. Don't worry about understanding every detail yet — the goal is to see the big picture.

### Step 1: Initialize Velox4J

```java
Velox4j.initialize();
```

This loads the native library (a `.so` file bundled in the JAR), registers all serialization types, and initializes the Velox runtime. Must be called once before anything else.

**Source:** `src/main/java/org/boostscale/velox4j/Velox4j.java`

### Step 2: Define a schema

```java
final RowType outputType = new RowType(
    List.of("n_nationkey", "n_name", "n_regionkey", "n_comment"),
    List.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType())
);
```

A `RowType` is a struct type with named fields — like a table schema. Here we define four columns for the TPC-H `nation` table.

**Source:** `src/main/java/org/boostscale/velox4j/type/RowType.java`

### Step 3: Create a table scan plan node

```java
final TableScanNode scanNode = new TableScanNode(
    "plan-id-1", outputType,
    new HiveTableHandle("connector-hive", "table-1", false, List.of(), null, outputType, Map.of()),
    toAssignments(outputType)
);
```

A `TableScanNode` is a leaf plan node that reads data from a connector (here, the Hive connector). The `HiveTableHandle` identifies which table to read, and `Assignments` map output columns to connector column handles.

**Source:** `src/main/java/org/boostscale/velox4j/plan/TableScanNode.java`

### Step 4: Build a query

```java
final Query query = new Query(scanNode, Config.empty(), ConnectorConfig.empty());
```

A `Query` bundles a plan tree with configuration. `Config.empty()` and `ConnectorConfig.empty()` mean we use default settings.

**Source:** `src/main/java/org/boostscale/velox4j/query/Query.java`

### Step 5: Create a session

```java
final MemoryManager memoryManager = Velox4j.newMemoryManager(AllocationListener.NOOP);
final Session session = Velox4j.newSession(memoryManager);
```

A `Session` is the unit of resource ownership. All C++ objects created during a session are tracked and freed when the session is closed. The `MemoryManager` wraps Velox's memory pool and optionally notifies a listener about allocations.

**Source:** `src/main/java/org/boostscale/velox4j/session/Session.java`

### Step 6: Execute the query

```java
final SerialTask task = session.queryOps().execute(query);
```

Behind the scenes:
1. The `Query` object is serialized to JSON via Jackson
2. The JSON string is passed to C++ via JNI
3. C++ deserializes it into native Velox objects using `folly::parseJson` + `ISerializable::deserialize`
4. A Velox task is created and wrapped as a `SerialTask`

**Source:** `src/main/java/org/boostscale/velox4j/query/Queries.java`

### Step 7: Add a data split

```java
final ConnectorSplit split = new HiveConnectorSplit(
    "connector-hive", 0, false, file.getAbsolutePath(),
    FileFormat.PARQUET, 0, file.length(), ...
);
task.addSplit(scanNode.getId(), split);
task.noMoreSplits(scanNode.getId());
```

Splits tell the scan node where its data lives. Here, one split points to a local Parquet file. `noMoreSplits()` signals that no additional data sources will be added.

### Step 8-9: Iterate and convert to Arrow

```java
final Iterator<RowVector> itr = UpIterators.asJavaIterator(task);
while (itr.hasNext()) {
    final RowVector rowVector = itr.next();
    final VectorSchemaRoot vsr = Arrow.toArrowVectorSchemaRoot(new RootAllocator(), rowVector);
    System.out.println(vsr.contentToTSVString());
    vsr.close();
}
```

The task produces `RowVector` batches (Velox's columnar format). Each batch is converted to an Arrow `VectorSchemaRoot` for easy consumption.

### Step 10: Clean up

```java
session.close();
memoryManager.close();
```

Closing the session destroys all C++ objects created within it. Always close in order: session first, then memory manager.

---

## 1.6 Conceptual Architecture Diagram

```
+---------------------------------------------------------+
|                     Java Application                     |
+---------------------------------------------------------+
        |                                          ^
        | Query (JSON)                             | RowVector results
        v                                          |
+---------------------------------------------------------+
|                    Velox4J Java Layer                     |
|  Types | Plans | Expressions | Connectors | Serde        |
+---------------------------------------------------------+
        |                                          ^
        | JSON string via JNI                      | Object handles (long)
        v                                          |
+---------------------------------------------------------+
|                    Velox4J C++ Layer                      |
|  JniWrapper | ObjectStore | Session                      |
+---------------------------------------------------------+
        |                                          ^
        | Deserialized native objects              | Execution results
        v                                          |
+---------------------------------------------------------+
|                    Velox (C++ Library)                    |
|  Execution Engine | Hive Connector | Memory Pools        |
+---------------------------------------------------------+
```

---

## 1.7 Key Takeaways

1. **Velox** = C++ execution engine for SQL. Think of it as the "CPU" that runs queries.
2. **Velox4J** = Java bindings that let JVM apps use Velox without writing C++.
3. **JSON is the bridge**: Java objects serialize to JSON, cross JNI, and deserialize into native Velox objects. This keeps the codebase small and maintainable.
4. **Session-based lifecycle**: create a session, do work, close the session. All native resources are tied to a session.
5. **Arrow compatible**: results can be converted to/from Apache Arrow format.

---

## 1.8 Exercises

1. **Read the entry point**: Open `src/main/java/org/boostscale/velox4j/Velox4j.java` and trace what `initialize()` does.
2. **Explore the Query class**: Open `src/main/java/org/boostscale/velox4j/query/Query.java`. Note how it extends `ISerializable` and uses `@JsonCreator` / `@JsonGetter` annotations. Then open `src/main/cpp/main/velox4j/query/Query.h` and see how the C++ side mirrors it.
3. **Examine Config**: Open `src/main/java/org/boostscale/velox4j/config/Config.java`. This is a simple `ISerializable`. Notice the pattern: `@JsonCreator` constructor + `@JsonGetter` methods. This pattern repeats throughout the codebase.
4. **Count the plan nodes**: List all concrete `PlanNode` subclasses in `src/main/java/org/boostscale/velox4j/plan/`. How many are there? What SQL operations do they correspond to?

---

## Next Lesson

[Lesson 2: Project Structure](lesson-02-project-structure.md) — Learn to navigate every corner of the codebase.
