# Lesson 6: JNI Bridge & Session Lifecycle

## Learning Goals

After this lesson you should be able to:

- Trace the full call path from Java to C++ and back
- Explain the four-layer JNI architecture (StaticJniWrapper → StaticJniApi → JniWrapper → JniApi)
- Understand how `ObjectStore` maps integer handles to C++ shared_ptrs
- Explain the lifecycle: initialize → create MemoryManager → create Session → use APIs → close
- Know which operations are session-scoped vs. global, and why

---

## 6.1 Overview: The Four Layers

The JNI bridge has four layers, organized into two stacks (static and session-scoped):

```
┌─────────────────────────────────────────────────────────────────┐
│                        Java Application                          │
│                                                                  │
│   Velox4j.initialize()  /  Velox4j.newSession()                 │
│   session.queryOps()    /  session.arrowOps()                   │
└─────────┬────────────────────────────────┬──────────────────────┘
          │                                │
          ▼                                ▼
┌─────────────────────┐      ┌─────────────────────────┐
│   StaticJniApi       │      │   JniApi                 │
│   (high-level,       │      │   (high-level,           │
│    global)           │      │    per-session)           │
│                      │      │                          │
│  - handles serde     │      │  - handles serde         │
│  - wraps results     │      │  - wraps results         │
│    in Java objects   │      │    in Java objects        │
└─────────┬────────────┘      └────────────┬─────────────┘
          │                                │
          ▼                                ▼
┌─────────────────────┐      ┌─────────────────────────┐
│  StaticJniWrapper    │      │   JniWrapper             │
│  (raw native         │      │   (raw native            │
│   methods, global)   │      │    methods, per-session)  │
└─────────┬────────────┘      └────────────┬─────────────┘
          │                                │
          │         JNI boundary           │
          ▼                                ▼
┌─────────────────────┐      ┌─────────────────────────┐
│ StaticJniWrapper.cc  │      │  JniWrapper.cc           │
│ (C++ implementations │      │  (C++ implementations    │
│  for global methods)  │      │   for session methods)   │
└──────────────────────┘      └──────────────────────────┘
```

### Why two stacks?

**Static (global)** — Operations that don't need a session's ObjectStore:
- Initialization (`initialize`)
- Creating MemoryManagers and Sessions
- Releasing any C++ object (`releaseCppObject`)
- Reading properties from existing objects (vector type, size, encoding)
- Iterator advance/wait (stateless operations on existing objects)
- Adding splits to tasks

**Session-scoped** — Operations that **create new C++ objects** and need a session's ObjectStore to store them:
- Creating query executors, evaluators
- Deserializing vectors, converting Arrow data
- Slicing/flattening vectors
- Any operation that returns a new `long` handle

The session's `ObjectStore` tracks all C++ objects created within it. When the session closes, all those objects are destroyed.

---

## 6.2 The Static Stack

### StaticJniWrapper (Java)

Raw `native` method declarations. No serialization logic — just passes primitive types across JNI.

```java
// src/main/java/org/boostscale/velox4j/jni/StaticJniWrapper.java
public class StaticJniWrapper {
    native void initialize(String globalConfJson);
    native long createMemoryManager(AllocationListener listener);
    native long createSession(long memoryManagerId);
    native void releaseCppObject(long objectId);
    native int upIteratorAdvance(long id);
    native void serialTaskAddSplit(long id, String planNodeId, int groupId, String splitJson);
    // ... 20+ more native methods
}
```

A **singleton** — there is exactly one instance, accessed via `StaticJniWrapper.get()`.

### StaticJniApi (Java)

High-level wrapper over `StaticJniWrapper`. Handles JSON serialization and wraps return values in Java objects.

```java
// src/main/java/org/boostscale/velox4j/jni/StaticJniApi.java
public class StaticJniApi {
    public void initialize(Config globalConf) {
        jni.initialize(Serde.toPrettyJson(globalConf));    // serialize Config to JSON
    }

    public MemoryManager createMemoryManager(AllocationListener listener) {
        return new MemoryManager(jni.createMemoryManager(listener));  // wrap handle
    }

    public void serialTaskAddSplit(SerialTask task, String planNodeId, int groupId, ConnectorSplit split) {
        final String splitJson = Serde.toJson(split);       // serialize split to JSON
        jni.serialTaskAddSplit(task.id(), planNodeId, groupId, splitJson);
    }
}
```

Pattern: `StaticJniApi` converts Java objects to/from JSON strings and long handles, then delegates to `StaticJniWrapper`.

### StaticJniWrapper.cc (C++)

Implements the native methods. Each method follows this pattern:

```cpp
jlong createSession(JNIEnv* env, jobject javaThis, long memoryManagerId) {
    JNI_METHOD_START                                            // exception guard
    auto mm = ObjectStore::retrieve<MemoryManager>(memoryManagerId);  // look up by handle
    return ObjectStore::global()->save(                         // store and return handle
        std::make_shared<Session>(mm.get()));
    JNI_METHOD_END(-1L)                                        // return -1 on error
}
```

---

## 6.3 The Session Stack

### JniWrapper (Java)

Raw `native` method declarations, **scoped to a session**. Each instance holds a `sessionId`.

```java
// src/main/java/org/boostscale/velox4j/jni/JniWrapper.java
final class JniWrapper {
    private final long sessionId;

    JniWrapper(long sessionId) { this.sessionId = sessionId; }

    @CalledFromNative
    public long sessionId() { return sessionId; }   // C++ calls this to find the session

    native long createQueryExecutor(String queryJson);
    native long createEvaluator(String evalJson);
    native long arrowToBaseVector(long cSchema, long cArray);
    // ... more native methods
}
```

Key: C++ calls `sessionId()` (annotated with `@CalledFromNative`) to get the session handle and find the corresponding `ObjectStore`:

```cpp
// JniWrapper.cc
Session* sessionOf(JNIEnv* env, jobject javaThis) {
    jlong sessionId = env->CallLongMethod(javaThis, methodId);  // calls Java sessionId()
    return ObjectStore::retrieve<Session>(sessionId).get();
}
```

### JniApi (Java)

High-level wrapper over `JniWrapper`. Handles serde and object wrapping.

```java
// src/main/java/org/boostscale/velox4j/jni/JniApi.java
public final class JniApi {
    private final JniWrapper jni;

    public QueryExecutor createQueryExecutor(Query query) {
        final String queryJson = Serde.toPrettyJson(query);        // serialize
        return new QueryExecutor(this, jni.createQueryExecutor(queryJson));  // wrap handle
    }

    public BaseVector evaluatorEval(Evaluator evaluator, SelectivityVector sv, RowVector input) {
        return baseVectorWrap(jni.evaluatorEval(evaluator.id(), sv.id(), input.id()));
    }
}
```

### JniWrapper.cc (C++)

Implements session-scoped native methods. New objects are saved to the **session's** ObjectStore:

```cpp
jlong createQueryExecutor(JNIEnv* env, jobject javaThis, jstring queryJson) {
    auto session = sessionOf(env, javaThis);                        // get session
    auto queryDynamic = folly::parseJson(jQueryJson.get());         // parse JSON
    auto query = ISerializable::deserialize<Query>(queryDynamic, pool); // deserialize
    auto exec = std::make_shared<QueryExecutor>(session->memoryManager(), query);
    return session->objectStore()->save(exec);                      // save to session store
}
```

---

## 6.4 The ObjectStore: Handle Management

`ObjectStore` is the mechanism that lets Java hold references to C++ objects.

### The problem

Java and C++ live in **different memory spaces**. Java can't hold a C++ pointer — there's no `Pointer` type in Java, and C++ raw pointers are unsafe to expose anyway (if the C++ object gets freed, you'd have a dangling reference that crashes the JVM). But Velox4J needs Java to refer to C++ objects (vectors, tasks, evaluators) created by Velox.

### What is a "handle"?

A **handle** is a **long integer that acts as a lookup key** — think of it like a coat check ticket. You hand your coat (C++ object) to the attendant (ObjectStore), and they give you a ticket number (handle). Later you present the ticket to get your coat back.

```
                      ┌─────────────────────────────────────────┐
                      │  ObjectStore (the "coat check")          │
   Java says:         │                                          │
   "do something      │   Map:                                   │
    with object 42"   │     42 → shared_ptr<BaseVector>  ← real object lives here
   ─────────────────► │     43 → shared_ptr<SerialTask>          │
                      │     44 → shared_ptr<Evaluator>           │
   ◄───────────────── │                                          │
   result             └─────────────────────────────────────────┘
```

When Java wants to operate on a C++ object, it passes the handle back to C++. C++ looks it up in the map and operates on the real object:

```java
// Java: "get the size of the vector with handle 42"
int size = staticJniWrapper.baseVectorGetSize(42L);
```

```cpp
// C++: "look up handle 42, get the vector, return its size"
jint baseVectorGetSize(JNIEnv* env, jobject javaThis, jlong vid) {
    auto vector = ObjectStore::retrieve<BaseVector>(vid);  // lookup by handle
    return vector->size();
}
```

When Java calls `close()`, it tells C++ "remove handle 42 from the map":

```java
vector.close();  // → tells C++ to release handle 42
```

```cpp
ObjectStore::release(42);  // removes from map → object may be freed
```

### Why handles instead of raw pointers?

- **Safety**: A handle is an indirect reference through a map. The map owns a `shared_ptr` (see sidebar below) that keeps the C++ object alive. Even if you pass the handle around, the object won't disappear until it's explicitly released.
- **Familiar pattern**: This is the same pattern used by operating systems (file descriptors are handles to kernel objects), OpenGL (texture IDs), and databases (cursor IDs).

### Sidebar: What is shared_ptr?

If you're a Java developer, this C++ concept might be new. In Java, the garbage collector (GC) automatically frees objects when nothing references them. C++ has no GC, so you must manage memory manually. `shared_ptr` is C++'s solution — a **smart pointer with automatic reference counting**:

```cpp
auto a = std::make_shared<Vector>();  // ref count = 1 (a holds the object)
auto b = a;                            // ref count = 2 (a and b both reference it)
b = nullptr;                           // ref count = 1 (only a remains)
a = nullptr;                           // ref count = 0 → object automatically destroyed
```

In Velox4J:
- `ObjectStore.save(sharedPtr)` stores a `shared_ptr` → ref count goes up, object stays alive
- `ObjectStore.release(handle)` removes the `shared_ptr` from the map → ref count goes down
- When ref count hits 0 → C++ object is automatically destroyed (like Java GC, but explicit)

This is why `CppObject.close()` documentation says it "releases the JNI reference of the smart pointer" — it removes the ObjectStore's reference. If nothing else in C++ is using the object, it gets freed.

### Handle encoding

An `ObjectHandle` (int64) encodes two parts:

```
Bits  1-32: StoreHandle (which ObjectStore — think "which coat check counter")
Bits 33-64: ResourceHandle (which object within that store — think "ticket number")
```

This allows `ObjectStore::retrieve<T>(handle)` and `ObjectStore::release(handle)` to find both the right store and the right object from a single integer.

### Two kinds of stores

1. **Global ObjectStore** (`ObjectStore::global()`) — stores MemoryManagers and Sessions. These outlive any single session.
2. **Per-session ObjectStore** (`session->objectStore()`) — stores vectors, evaluators, tasks, etc. created within a session. Destroyed when the session closes.

```
Global ObjectStore
├── MemoryManager (handle: 100)
├── Session A (handle: 200)
│     └── Session A's ObjectStore
│           ├── QueryExecutor (handle: 200_1)
│           ├── SerialTask (handle: 200_2)
│           └── RowVector (handle: 200_3)
└── Session B (handle: 300)
      └── Session B's ObjectStore
            └── Evaluator (handle: 300_1)
```

**Source:** `src/main/cpp/main/velox4j/lifecycle/ObjectStore.h`, `ResourceMap.h`

---

## 6.5 The CppObject Interface

On the Java side, any object backed by a C++ shared_ptr implements `CppObject`:

```java
// src/main/java/org/boostscale/velox4j/jni/CppObject.java
public interface CppObject extends AutoCloseable {
    long id();          // returns the ObjectHandle

    @Override
    default void close() {
        StaticJniApi.get().releaseCppObject(this);  // calls ObjectStore::release()
    }
}
```

Implementations: `Session`, `MemoryManager`, `BaseVector`, `RowVector`, `SelectivityVector`, `Evaluator`, `QueryExecutor`, `SerialTask`, `UpIterator`, `ExternalStream`, `ISerializableCo`, `VariantCo`.

When `close()` is called, the C++ `shared_ptr` is removed from the ObjectStore. If no other C++ code holds a reference, the object is destroyed. Since `CppObject` extends `AutoCloseable`, you can use try-with-resources:

```java
try (Session session = Velox4j.newSession(memoryManager)) {
    // use session...
}  // session.close() called automatically
```

---

## 6.6 What is a Session For?

**Session is NOT a Velox concept.** It's a Velox4J invention to solve a JNI-specific problem: **how to safely manage C++ object lifetimes from Java**.

### The problem sessions solve

When Velox4J runs a query, C++ creates many objects: vectors, tasks, evaluators, intermediate results. Java holds handles to all of them. But how do we know when to destroy the C++ objects?

**Option A**: Rely on Java `close()` calls for every single object. But if the user forgets one, or an exception skips a `close()` call, C++ memory leaks.

**Option B**: Group all related objects into a scope. Destroy them all when the scope closes. **This is what Session does.**

### What a Session actually is

A Session is just **a container that owns an ObjectStore**. Every C++ object created through that session is registered in its ObjectStore. When you call `session.close()`, the ObjectStore is destroyed, which drops all the `shared_ptr`s at once:

```cpp
// Session.h — the C++ session is remarkably simple
class Session {
    MemoryManager* memoryManager_;
    std::unique_ptr<ObjectStore> objectStore_;   // THIS is the whole point
};
```

Think of it like a scratch pad. You open it, do some computation (creating many intermediate C++ objects along the way), read the results, then throw away the whole pad. You don't need to erase each line individually.

### When to create a new session

**One session per unit of work** is the general pattern. A "unit of work" could be a single query, a batch of related queries, or a request handler in a server:

```java
// Pattern 1: one session per query
try (Session session = Velox4j.newSession(memoryManager)) {
    SerialTask task = session.queryOps().execute(query);
    // iterate results...
}  // all C++ objects from this query freed

// Pattern 2: one session for multiple related operations
try (Session session = Velox4j.newSession(memoryManager)) {
    // create vectors, run evaluations, execute queries...
    // they can share intermediate C++ objects because they're in the same session
}  // everything freed at once
```

Rules of thumb:
- **Don't** use one session for the entire application lifetime — objects accumulate forever, defeating the purpose
- **Don't** create a new session for every tiny operation — unnecessary overhead
- **Do** scope sessions to logical tasks: a query, a batch, a request

### LocalSession: the implementation

`LocalSession` is the concrete `Session` implementation. It ties together the JNI layers:

```java
// src/main/java/org/boostscale/velox4j/jni/LocalSession.java
public class LocalSession implements Session {
    private final long id;          // ObjectHandle to C++ Session

    private JniApi jniApi() {
        return new JniApi(new JniWrapper(this.id));  // create JniApi bound to this session
    }

    public Queries queryOps()       { return new Queries(jniApi()); }
    public Evaluations evaluationOps() { return new Evaluations(jniApi()); }
    public Arrow arrowOps()         { return new Arrow(jniApi()); }
    // ... 7 more ops methods
}
```

Each `xxxOps()` method creates a new `JniApi` bound to this session, then wraps it in a domain-specific operations class. The ops classes (`Queries`, `Evaluations`, `Arrow`, etc.) provide user-friendly methods:

```java
// Usage
Session session = Velox4j.newSession(memoryManager);
SerialTask task = session.queryOps().execute(query);     // Queries wraps JniApi
BaseVector vector = session.arrowOps().fromArrow(schema, array);  // Arrow wraps JniApi
```

---

## 6.7 The Full Lifecycle

### Phase 1: Initialization (once per JVM)

```java
Velox4j.initialize();
```

What happens:
1. `JniLibLoader.loadAll()` extracts `.so` files from the JAR to a temp directory and calls `System.load()`
2. `JNI_OnLoad` (C++) registers `StaticJniWrapper` and `JniWrapper` native methods
3. `VariantRegistry.registerAll()` registers all Variant subclasses for serde
4. `ISerializableRegistry.registerAll()` registers all ISerializable subclasses for serde
5. `StaticJniApi.get().initialize(globalConf)` calls C++ `initialize()` which:
   - Registers Velox filesystems, functions, connectors, serde types
   - Registers Hive connector + ExternalStream connector

### Phase 2: Create MemoryManager

```java
MemoryManager memoryManager = Velox4j.newMemoryManager(AllocationListener.NOOP);
```

What happens:
1. Java calls `StaticJniApi.createMemoryManager(listener)`
2. C++ creates `MemoryManager` with a `JavaAllocationListener` that calls back into the Java listener
3. Stored in the **global** ObjectStore, handle returned to Java
4. Java wraps handle in `MemoryManager` object

### Phase 3: Create Session

```java
Session session = Velox4j.newSession(memoryManager);
```

What happens:
1. Java calls `StaticJniApi.createSession(memoryManager)`
2. C++ creates a `Session` with its own `ObjectStore`
3. Session stored in **global** ObjectStore, handle returned
4. Java wraps handle in `LocalSession`

### Phase 4: Use the Session

```java
SerialTask task = session.queryOps().execute(query);
```

What happens:
1. `session.queryOps()` creates `new Queries(new JniApi(new JniWrapper(sessionId)))`
2. `Queries.execute()` calls `JniApi.createQueryExecutor(query)` then `JniApi.queryExecutorExecute(executor)`
3. `JniApi` serializes the Query to JSON, passes to `JniWrapper.createQueryExecutor(json)`
4. C++ deserializes, creates objects, stores in **session** ObjectStore
5. Handles returned to Java, wrapped in `QueryExecutor` / `SerialTask`

### Phase 5: Close (reverse order)

```java
session.close();        // destroys session's ObjectStore → all session objects freed
memoryManager.close();  // destroys memory pools
```

**Critical**: close session before memory manager. The session may hold objects that use the memory manager's pools.

---

## 6.8 Data Flow Patterns

There are three patterns for how data crosses the JNI boundary:

### Pattern 1: JSON serialization (plans, types, configs)

```
Java object  → Serde.toJson() → JSON string → JNI → folly::parseJson → C++ object
```

Used for: `Query`, `PlanNode`, `TypedExpr`, `Type`, `ConnectorSplit`, `Config`, `Evaluation`

### Pattern 2: Object handles (vectors, tasks, iterators)

```
C++ creates object → objectStore.save() → long handle → JNI → Java CppObject wrapper
```

Used for: `BaseVector`, `RowVector`, `SerialTask`, `Evaluator`, `UpIterator`, `SelectivityVector`

### Pattern 3: Arrow C Data Interface (vector data)

```
Velox vector → ArrowSchema + ArrowArray (memory addresses) → JNI → Arrow Java
```

Used for: Velox ↔ Arrow conversions. Memory addresses are passed as `long` values.

---

## 6.9 JNI_OnLoad: The C++ Entry Point

When `System.load(libvelox4j.so)` is called, JVM invokes `JNI_OnLoad`:

```cpp
// JniLoader.cc
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* jvm, void*) {
    JNIEnv* env = jniHelpersInitialize(jvm);
    velox4j::getJniErrorState()->ensureInitialized(env);
    velox4j::jniClassRegistry()->add(env, new velox4j::StaticJniWrapper(env));
    velox4j::jniClassRegistry()->add(env, new velox4j::JniWrapper(env));
    velox4j::jniClassRegistry()->add(env, new velox4j::DownIteratorJniWrapper(env));
    velox4j::jniClassRegistry()->add(env, new velox4j::JavaAllocationListenerJniWrapper(env));
    return JAVA_VERSION;
}
```

This registers four JNI wrapper classes using [Spotify's JniHelpers](https://github.com/nickhudkins/JniHelpers) library:
1. `StaticJniWrapper` — global native methods
2. `JniWrapper` — session-scoped native methods
3. `DownIteratorJniWrapper` — C++ → Java callbacks for DownIterator
4. `JavaAllocationListenerJniWrapper` — C++ → Java callbacks for memory allocation

JniHelpers provides `addNativeMethod()` / `registerNativeMethods()` which is cleaner than writing raw JNI method signatures.

---

## 6.10 Error Handling Across JNI

C++ exceptions can't propagate through JNI. Every C++ native method uses the `JNI_METHOD_START` / `JNI_METHOD_END` macros:

```cpp
jlong someMethod(JNIEnv* env, jobject javaThis, ...) {
    JNI_METHOD_START
    // ... C++ code that may throw ...
    JNI_METHOD_END(-1L)    // return -1 if an exception occurred
}
```

`JNI_METHOD_START` is a try block. `JNI_METHOD_END` is the catch block that:
1. Catches the C++ exception
2. Calls `env->ThrowNew()` to create a Java exception
3. Returns the fallback value

On the Java side, JNI automatically raises the Java exception after the native method returns.

---

## 6.11 Key Takeaways

1. **Four layers**: `StaticJniWrapper`/`JniWrapper` (raw native methods) + `StaticJniApi`/`JniApi` (high-level, handles serde). Static stack is global; session stack is per-session.

2. **ObjectStore is the bridge**: C++ stores `shared_ptr`s and returns integer handles. Java holds handles in `CppObject` wrappers. `close()` releases the C++ reference.

3. **Two ObjectStores**: global (for MemoryManagers/Sessions) and per-session (for everything else). Session close destroys all session-scoped objects.

4. **Session-scoped JniWrapper**: C++ calls `sessionId()` back into Java to find which session's ObjectStore to use.

5. **Three data-flow patterns**: JSON strings (for plans/types), object handles (for vectors/tasks), and Arrow C Data Interface addresses (for Arrow interop).

6. **Lifecycle order matters**: initialize once → create MemoryManager → create Session → use → close Session → close MemoryManager.

7. **C++ exceptions become Java exceptions**: `JNI_METHOD_START`/`JNI_METHOD_END` macros catch and translate.

---

## 6.12 Exercises

1. **Trace a call**: Starting from `session.queryOps().execute(query)`, list every Java class and C++ function involved until the C++ `QueryExecutor` is stored in the ObjectStore.

2. **Static vs session**: For each operation, decide whether it goes through `StaticJniApi` or `JniApi`:
   - `baseVectorGetSize(vector)`
   - `createQueryExecutor(queryJson)`
   - `serialTaskAddSplit(task, nodeId, groupId, split)`
   - `arrowToBaseVector(schema, array)`

3. **ObjectHandle math**: If a session has `StoreHandle = 5` and saves an object with `ResourceHandle = 12`, what is the resulting `ObjectHandle`? (Hint: look at `toObjHandle` in `ObjectStore.h`.)

4. **Find the callback**: In `JniWrapper.cc`, the `sessionOf()` function calls back into Java. Why can't it just store the session pointer directly in the C++ `JniWrapper` class?

5. **Resource leak**: What happens if you forget to call `session.close()`? What objects leak? What about `memoryManager.close()`?

---

## Next Lesson

[Lesson 7: Query Execution Flow](lesson-07-query-execution.md) — Trace a query from construction to results, including splits and iterators.
