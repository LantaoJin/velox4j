# Lesson 8: Memory Management & Arrow Interop

## Learning Goals

After this lesson you should be able to:

- Explain how native memory is managed through the MemoryManager, allocation listeners, and Velox memory pools
- Understand how the Java AllocationListener provides a hook for external memory accounting
- Convert data between Velox's native format and Apache Arrow format
- Explain the Arrow C Data Interface and why it enables zero-copy interop
- Know when and how to use each Arrow conversion method

---

## 8.1 Why Memory Management Matters

Velox4J operates with **two separate memory worlds**:

1. **Java heap memory** — managed by the JVM garbage collector (objects, arrays, etc.)
2. **Native (off-heap) memory** — managed by C++ (Velox vectors, buffers, hash tables, etc.)

The JVM has no visibility into native memory. If Velox allocates 2 GB of native memory for a hash join, the JVM doesn't know about it — it won't trigger GC, and it won't enforce limits. This is a problem for JVM applications that need to manage total memory usage.

Velox4J's `MemoryManager` solves this by:
- Wrapping Velox's memory pool system
- Providing an `AllocationListener` callback that notifies Java of every native allocation/deallocation
- Performing leak checks when the MemoryManager is closed

---

## 8.2 The Memory Architecture

```
Java side                              C++ side
──────────                             ─────────

AllocationListener                     ListenableArbitrator
  (Java interface)                       (Velox MemoryArbitrator)
       ▲                                      │
       │ allocationChanged(diff)               │ growCapacity / shrinkCapacity
       │                                       ▼
       │                               Velox Root MemoryPool
       │                                  │
       │                            ┌─────┴──────────┐
       │                            │                 │
       │                     Leaf Pool           Aggregate Pool
       │                 (serde, vectors)      (query execution)
       │                            │                 │
       │                            └─────────────────┘
       │                                      │
       └──────────────────────────────────────┘
             JNI callback on every allocation change
```

The key insight: Velox's `MemoryArbitrator` is the point where memory capacity grows and shrinks. Velox4J plugs a custom `ListenableArbitrator` into this system that calls the Java `AllocationListener` whenever memory changes.

---

## 8.3 AllocationListener: The Java Hook

The `AllocationListener` interface is how Java learns about native memory changes:

```java
// src/main/java/org/boostscale/velox4j/memory/AllocationListener.java
public interface AllocationListener {
    AllocationListener NOOP = new NoopAllocationListener();

    @CalledFromNative
    void allocationChanged(long diff);  // diff > 0 means allocation, diff < 0 means deallocation
}
```

This single method is called by C++ (via JNI callback) whenever Velox allocates or frees memory. The `diff` is positive for allocations and negative for deallocations.

### Built-in implementations

**NoopAllocationListener** — ignores all changes (used when you don't need memory tracking):

```java
MemoryManager mm = Velox4j.newMemoryManager(AllocationListener.NOOP);
```

**BytesAllocationListener** — tracks current and peak usage:

```java
BytesAllocationListener listener = new BytesAllocationListener();
MemoryManager mm = Velox4j.newMemoryManager(listener);

// After running queries...
listener.currentBytes();  // current native memory usage
listener.peakBytes();     // highest memory usage observed
```

### Custom implementation

In a real application (e.g., Flink), you'd implement `AllocationListener` to integrate with the framework's memory management:

```java
AllocationListener listener = diff -> {
    if (diff > 0) {
        frameworkMemoryPool.reserve(diff);   // tell Flink: "Velox needs more memory"
    } else {
        frameworkMemoryPool.release(-diff);  // tell Flink: "Velox freed some memory"
    }
};
```

This is how Gluten-Flink coordinates Velox's memory usage with Flink's memory budget — if Velox tries to use more than its share, the listener can throw an exception to trigger spilling or back-pressure.

---

## 8.4 BlockAllocationListener: Reducing JNI Overhead

Every `allocationChanged()` call crosses the JNI boundary (C++ → Java), which has overhead. Velox makes many small allocations, so calling the Java listener for every one would be expensive.

The `BlockAllocationListener` wraps your listener and batches calls by rounding allocations to block-sized chunks:

```cpp
// Created automatically in StaticJniWrapper.cc
auto listener = std::make_unique<BlockAllocationListener>(
    std::make_unique<JavaAllocationListener>(env, jListener),
    8 << 10 << 10);  // block size ≈ 8MB
```

For example, if Velox allocates 100 KB, 200 KB, and 300 KB in sequence (600 KB total), the block listener might fire a single `allocationChanged(8MB)` call instead of three separate calls. When memory is freed, blocks are released in reverse.

This means the Java listener sees **rounded** values, not exact byte counts. The trade-off is accuracy vs. performance — acceptable for most use cases.

**Source:** `src/main/cpp/main/velox4j/memory/AllocationListener.h`

---

## 8.5 The C++ MemoryManager

The C++ `MemoryManager` creates and owns:

1. **A Velox `MemoryManager`** with a custom `ListenableArbitrator` that calls back to Java
2. **A root `MemoryPool`** from which child pools are derived
3. **Child pools** for different purposes (serde, query execution, vectors)
4. **Arrow memory pools** backed by the same allocation listener

```cpp
// MemoryManager.h — what it owns
class MemoryManager {
    std::unique_ptr<AllocationListener> listener_;                 // the Java listener (via JNI)
    std::unique_ptr<MemoryAllocator> arrowAllocator_;             // for Arrow allocations
    std::unique_ptr<velox::memory::MemoryManager> veloxMemoryManager_; // Velox memory system
    std::shared_ptr<velox::memory::MemoryPool> veloxRootPool_;    // root pool
    std::unordered_map<std::string, std::shared_ptr<velox::memory::MemoryPool>> veloxPoolRefs_;
    std::unordered_map<std::string, std::unique_ptr<arrow::MemoryPool>> arrowPoolRefs_;
};
```

Child pools are created on demand:

```cpp
// Get a leaf pool for deserialization work
velox::memory::MemoryPool* pool = memoryManager->getVeloxPool(
    "Query Serde Memory Pool", memory::MemoryPool::Kind::kLeaf);

// Get an Arrow pool for Arrow interop
arrow::MemoryPool* arrowPool = memoryManager->getArrowPool("Arrow Import Pool");
```

### How Arrow memory is managed through the same listener

Velox and Arrow have **completely separate** memory pool systems in C++. But Velox4J routes both through the **same `AllocationListener`**, so Java sees a unified view of all native memory.

The Arrow memory path uses a layered architecture:

```
Arrow C++ code calls:       arrow::MemoryPool::Allocate(size)
                                     │
                                     ▼
                             ArrowMemoryPool
                             (Velox4J's custom arrow::MemoryPool implementation)
                                     │
                                     ▼
                          ListenableMemoryAllocator
                          (wraps real allocator + notifies listener)
                                     │
                              ┌──────┴──────┐
                              ▼              ▼
                    StdMemoryAllocator    AllocationListener
                    (actual malloc)       (notifies Java — same instance as Velox side)
```

- **`ArrowMemoryPool`** implements Arrow's `arrow::MemoryPool` interface. When Arrow needs memory, it calls `Allocate()`, which delegates to a `MemoryAllocator`.
- **`ListenableMemoryAllocator`** wraps the real allocator. On every `allocate`/`free`, it calls `listener_->allocationChanged(diff)` to notify Java, then delegates to the actual allocator.
- **`StdMemoryAllocator`** is the real allocator that calls `std::malloc` / `std::free`.

This is set up in the MemoryManager constructor:

```cpp
// MemoryManager.cc (constructor)
MemoryManager::MemoryManager(std::unique_ptr<AllocationListener> listener)
    : listener_(std::move(listener)) {
    // Arrow side: StdMemoryAllocator wrapped with same listener
    arrowAllocator_ = std::make_unique<ListenableMemoryAllocator>(
        defaultMemoryAllocator().get(), listener_.get());

    // Velox side: MemoryManager with ListenableArbitrator (also uses same listener)
    // ...
}
```

Both the Velox path (`ListenableArbitrator`) and the Arrow path (`ListenableMemoryAllocator`) share the same `AllocationListener` instance:

```
                         Java AllocationListener
                          (single callback point)
                                  ▲     ▲
                                  │     │
                   ┌──────────────┘     └──────────────┐
                   │                                    │
          ListenableArbitrator              ListenableMemoryAllocator
          (Velox memory path)               (Arrow memory path)
                   │                                    │
                   ▼                                    ▼
          Velox MemoryPools                    StdMemoryAllocator
        (vectors, hash tables,                  (malloc/free)
         query buffers, etc.)                       │
                                                    ▼
                                              ArrowMemoryPool
                                           (Arrow allocations)
```

**Why this matters**: Without this unified approach, Arrow allocations would be invisible to Java. If you imported a large Arrow dataset, that memory wouldn't be tracked, and your Java memory limits wouldn't account for it. By routing both through the same listener, Java sees **all** native memory usage — whether it comes from Velox query execution or Arrow data import/export.

**Source:** `src/main/cpp/main/velox4j/memory/ArrowMemoryPool.cc`, `MemoryManager.cc`

### Leak detection

When the `MemoryManager` is destroyed, it checks that all pools have zero outstanding bytes:

```cpp
MemoryManager::~MemoryManager() {
    bool succeeded = tryDestruct();  // checks all pools for leaks
    if (!succeeded) {
        VELOX_FAIL("Memory leak found during destruction of MemoryManager");
    }
}
```

This is why **close order matters**: close all sessions first (freeing their objects), then close the MemoryManager. If you close the MemoryManager while objects still exist, the leak check fails.

**Source:** `src/main/cpp/main/velox4j/memory/MemoryManager.cc`

---

## 8.6 Memory Flow Summary

Here's what happens when Velox needs more memory during query execution:

```
1. Velox operator needs memory (e.g., hash table grows)
2. Velox MemoryPool requests capacity from MemoryArbitrator
3. ListenableArbitrator.growCapacity() is called
4. Arbitrator calls listener_->allocationChanged(+bytes)
5. C++ BlockAllocationListener batches the call
6. JavaAllocationListener calls Java listener via JNI
7. Java AllocationListener.allocationChanged(+bytes) fires
8. Java app can track, enforce limits, or throw if over budget
```

And when memory is freed:
```
1. C++ object is destroyed (e.g., vector goes out of scope)
2. Velox MemoryPool releases capacity
3. ListenableArbitrator.shrinkCapacity() is called
4. listener_->allocationChanged(-bytes)
5. Java listener sees negative diff
```

---

## 8.7 Apache Arrow: What and Why

### The problem Arrow solves

In the data ecosystem, every engine has its own in-memory data format:

- Velox uses `BaseVector` / `RowVector`
- Spark uses `UnsafeRow` / `ColumnarBatch`
- Flink uses `BinaryRow`
- Pandas uses NumPy arrays
- DuckDB has its own vector format

When you need to pass data between these systems, you have two bad options:

1. **Serialize/deserialize** (e.g., to CSV, JSON, Parquet) — works but extremely slow due to encoding/decoding overhead
2. **Write custom conversion code** for every pair of systems — N systems means N*(N-1) converters

```
Without Arrow (N×N problem):

  Velox ←──→ Spark
  Velox ←──→ Flink
  Velox ←──→ Pandas
  Spark ←──→ Flink
  Spark ←──→ Pandas
  Flink ←──→ Pandas
  ... every pair needs custom code
```

### What Arrow is

Apache Arrow is a **cross-language in-memory columnar data format** — a standard that everyone agrees to speak. Instead of N×N converters, each system implements one converter to/from Arrow:

```
With Arrow (N×1 problem):

  Velox  ←──→ Arrow ←──→ Spark
                │
                ├──→ Flink
                ├──→ Pandas
                ├──→ DuckDB
                ├──→ Polars
                └──→ Arrow Java (used directly in JVM apps)
```

Arrow defines:
- A **memory layout** for columnar data (how integers, strings, arrays, structs, etc. are laid out in memory)
- **Libraries** in many languages (C++, Java, Python, Rust, Go, etc.) that can read and write this layout
- The **Arrow C Data Interface** for exchanging data between libraries in the same process without copying

### Why Velox4J needs Arrow

Velox4J is a **Java library** running inside a JVM application. The application needs to read query results. But Velox's `RowVector` is a C++ object — Java can't read its internal buffers directly. There are three options:

1. **Convert to Java objects row by row** — extremely slow for large datasets, creates GC pressure
2. **Convert to a Java-friendly columnar format** — this is where Arrow comes in
3. **Stay in Velox format and only access via JNI** — works for simple cases (e.g., `toString()`), but doesn't integrate with the Java ecosystem

Arrow Java provides a rich Java API for working with columnar data (`VectorSchemaRoot`, `FieldVector`, etc.). By converting Velox results to Arrow, Java applications get:

- **Fast columnar access** without per-row JNI calls
- **Integration with the Arrow ecosystem** — write to Arrow IPC/Feather files, pass to Arrow Flight for network transfer, use with other Arrow-compatible Java libraries
- **Standard format** that other frameworks (Flink, Spark) already understand

### How Velox4J uses Arrow

Velox4J provides bidirectional conversion:

- **Export** (Velox → Arrow): After executing a query, convert `RowVector` results to Arrow `VectorSchemaRoot` for use in Java code or other Arrow-compatible tools
- **Import** (Arrow → Velox): Feed Arrow data from Java into Velox for processing (e.g., data from a Flink operator in Arrow format can be imported into a Velox plan)

The conversions use the Arrow C Data Interface (next section), which makes them near-zero-copy — only metadata is copied, not the actual data buffers.

---

## 8.8 The Arrow C Data Interface

The conversions use the **Arrow C Data Interface** — a standard for zero-copy Arrow data exchange. It defines two C structs:

- **`ArrowSchema`** — describes the data type (like a type metadata header)
- **`ArrowArray`** — points to the actual data buffers (no copy — just pointers)

```
Velox vector                    Arrow C Data Interface              Arrow Java
─────────────                   ──────────────────────              ──────────
                                ┌──────────────┐
BaseVector ──exportToArrow()──> │ ArrowSchema  │ ──importVector()──> FieldVector
                                │ ArrowArray   │
                                └──────────────┘
                                (memory addresses
                                 passed as longs
                                 across JNI)
```

The key advantage: **no data copy**. The `ArrowArray` contains pointers to the same memory buffers that the Velox vector uses. Arrow Java takes ownership of the buffers during import.

**Source:** `src/main/cpp/main/velox4j/arrow/Arrow.cc`

---

## 8.9 Arrow Conversion API

The `Arrow` class provides six conversion methods organized in three pairs:

### Type/Schema conversions

```java
Arrow arrowOps = session.arrowOps();
BufferAllocator alloc = new RootAllocator();

// Velox type → Arrow schema
Schema arrowSchema = arrowOps.toArrowSchema(alloc, rowType);

// Arrow schema → Velox type (static method — no session needed)
RowType veloxType = Arrow.fromArrowSchema(alloc, arrowSchema);

// Velox type → Arrow field (for non-row types)
Field arrowField = arrowOps.toArrowField(alloc, type);

// Arrow field → Velox type (static method)
Type veloxType = Arrow.fromArrowField(alloc, arrowField);
```

### Single vector conversions

```java
// Velox BaseVector → Arrow FieldVector (static method)
FieldVector arrowVector = Arrow.toArrowVector(alloc, baseVector);

// Arrow FieldVector → Velox BaseVector (session method — creates C++ object)
BaseVector veloxVector = arrowOps.fromArrowVector(alloc, arrowFieldVector);
```

### Row vector conversions (most common)

```java
// Velox RowVector → Arrow VectorSchemaRoot (static method)
VectorSchemaRoot vsr = Arrow.toArrowVectorSchemaRoot(alloc, rowVector);
System.out.println(vsr.contentToTSVString());  // print as table
vsr.close();  // must close!

// Arrow VectorSchemaRoot → Velox RowVector (session method)
RowVector veloxRow = arrowOps.fromArrowVectorSchemaRoot(alloc, vsr);
```

### Static vs session methods

Notice that **export** methods (Velox → Arrow) are static — they only read from existing C++ objects. But **import** methods (Arrow → Velox) require a session because they create new C++ objects that must be stored in the session's ObjectStore.

**Source:** `src/main/java/org/boostscale/velox4j/arrow/Arrow.java`

---

## 8.10 Arrow Conversion Internals

Under the hood, each conversion follows this pattern:

### Export (Velox → Arrow)

```java
// Arrow.toArrowVectorSchemaRoot()
public static VectorSchemaRoot toArrowVectorSchemaRoot(BufferAllocator alloc, RowVector vector) {
    try (ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
         ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
        // 1. C++ exports Velox vector to ArrowSchema + ArrowArray structs
        StaticJniApi.get().baseVectorToArrow(vector, cSchema, cArray);
        // 2. Arrow Java imports from the C structs
        VectorSchemaRoot vsr = Data.importVectorSchemaRoot(alloc, cArray, cSchema, null);
        return vsr;
    }
}
```

1. Java allocates empty `ArrowSchema` and `ArrowArray` structs (off-heap)
2. Passes their memory addresses to C++ via JNI
3. C++ fills the structs by calling Velox's `exportToArrow()` — this copies pointers, not data
4. Java calls Arrow's `Data.importVectorSchemaRoot()` to wrap the buffers in Arrow Java objects

### Import (Arrow → Velox)

```java
// Arrow.fromArrowVectorSchemaRoot()
public RowVector fromArrowVectorSchemaRoot(BufferAllocator alloc, VectorSchemaRoot vsr) {
    try (ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
         ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
        // 1. Arrow Java exports to ArrowSchema + ArrowArray structs
        Data.exportVectorSchemaRoot(alloc, vsr, null, cArray, cSchema);
        // 2. C++ imports from the C structs into a Velox vector
        BaseVector imported = jniApi.arrowToBaseVector(cSchema, cArray);
        return imported.asRowVector();
    }
}
```

The Arrow C Data Interface structs are just intermediaries — they're tiny metadata objects, not data copies.

---

## 8.11 Common Arrow Usage Pattern

The most common pattern is converting query results to Arrow for further processing:

```java
try (Session session = Velox4j.newSession(memoryManager)) {
    BufferAllocator alloc = new RootAllocator();

    // Execute query
    SerialTask task = session.queryOps().execute(query);
    task.addSplit(scanNode.getId(), split);
    task.noMoreSplits(scanNode.getId());

    // Convert each batch to Arrow
    Iterator<RowVector> results = UpIterators.asJavaIterator(task);
    while (results.hasNext()) {
        RowVector batch = results.next();

        // Convert to Arrow VectorSchemaRoot
        VectorSchemaRoot vsr = Arrow.toArrowVectorSchemaRoot(alloc, batch);

        // Use Arrow APIs (print, write to IPC, send over network, etc.)
        System.out.println(vsr.contentToTSVString());

        // Must close the Arrow object to free its memory
        vsr.close();
    }

    alloc.close();
}
```

---

## 8.12 Memory Ownership During Arrow Conversion

Understanding who owns the memory is important to avoid leaks or crashes:

### Velox → Arrow export

After `Arrow.toArrowVectorSchemaRoot(alloc, rowVector)`:
- The Arrow `VectorSchemaRoot` owns the buffer memory
- The Velox `RowVector` is still valid (its data was copied or ref-counted)
- You must close the `VectorSchemaRoot` when done
- The Velox `RowVector` can still be used independently

Note: C++ calls `flattenVector()` before export, which may copy data if the vector uses dictionary encoding. For flat vectors, the export is essentially zero-copy.

### Arrow → Velox import

After `arrowOps.fromArrowVectorSchemaRoot(alloc, vsr)`:
- The Velox `BaseVector` owns the buffer memory (taken from Arrow)
- The Arrow `VectorSchemaRoot` is consumed — its buffers are transferred
- The Velox vector lives in the session's ObjectStore

---

## 8.13 Key Takeaways

1. **MemoryManager bridges Java and C++ memory**. It wraps Velox's memory pool system with a Java `AllocationListener` callback so Java applications can track and control native memory usage.

2. **AllocationListener is the integration point**. Implement it to connect Velox's memory usage to your framework's memory management (e.g., Flink, Spark). For simple use, `AllocationListener.NOOP` or `BytesAllocationListener` suffice.

3. **BlockAllocationListener reduces JNI overhead** by batching small allocations into block-sized chunks before calling into Java.

4. **Leak detection on close**. The MemoryManager checks for leaks when destroyed. Always close sessions before the MemoryManager.

5. **Arrow interop uses the Arrow C Data Interface** — a cross-language standard for exchanging columnar data via `ArrowSchema` + `ArrowArray` structs. This enables near-zero-copy conversion.

6. **Six conversion methods** in the `Arrow` class, organized as three pairs: types/schemas, single vectors, and row vectors. Export is static; import requires a session.

7. **Close Arrow objects**. After exporting to Arrow, the resulting `VectorSchemaRoot` or `FieldVector` must be closed to free memory.

---

## 8.14 Exercises

1. **Track memory**: Write code that uses `BytesAllocationListener` to measure how much native memory a table scan query uses. Print `currentBytes()` and `peakBytes()` after the query completes.

2. **Custom listener**: Sketch an `AllocationListener` implementation that throws an exception if total native memory exceeds 1 GB. Where in the memory flow (section 8.6) would this exception propagate?

3. **Arrow round-trip**: Open `ArrowTest.testRowVectorRoundTrip1()`. Trace the conversion path: Velox RowVector → Arrow VectorSchemaRoot → Velox RowVector. Which methods are static and which require a session?

4. **Ownership quiz**: After this code runs, who owns the data buffers?
   ```java
   RowVector rv = task.get();
   VectorSchemaRoot vsr = Arrow.toArrowVectorSchemaRoot(alloc, rv);
   vsr.close();
   // Is rv still valid? Can you call rv.toString()?
   ```

5. **Read the arbitrator**: Open `MemoryManager.cc` and find `ListenableArbitrator::growCapacityInternal()`. What happens when `listener_->allocationChanged(neededBytes)` throws? Why does it need to handle this case?

---

## Next Lesson

[Lesson 9: External Streams & Iterators](lesson-09-external-streams.md) — Feed data from Java into Velox via external streams, down-iterators, and blocking queues.
