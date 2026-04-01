# Lesson 2: Project Structure

## Learning Goals

After this lesson you should be able to:

- Navigate the repository and quickly locate any component
- Understand how the Java and C++ codebases mirror each other
- Know where tests, build scripts, and CI workflows live
- Identify the role of every Java package and every C++ subdirectory

---

## 2.1 Top-Level Repository Layout

```
velox4j/
├── pom.xml                           # Maven project definition (Java build)
├── README.md                         # Project introduction
├── LICENSE                           # Apache 2.0
├── .github/workflows/                # CI workflows
├── src/
│   ├── main/
│   │   ├── java/                     # Java source code
│   │   ├── cpp/                      # C++ source code + CMake build
│   │   └── resources/                # Bundled native libraries (in release JARs)
│   └── test/
│       └── java/                     # Java tests
└── docs/                             # Learning materials (you are here)
```

Key observations:
- This is a **single Maven module** (`pom.xml` at root). There are no sub-modules.
- Java and C++ code live side by side under `src/main/`.
- The C++ build is triggered by Maven during `mvn install` (controlled by the `skip.cpp.build` property in `pom.xml`).

---

## 2.2 The Java Source Tree

All Java sources live under `src/main/java/org/boostscale/velox4j/`. Here is a complete package map:

```
org.boostscale.velox4j/
│
├── Velox4j.java                  # [ENTRY POINT] initialize(), newSession(), newMemoryManager()
│
├── type/                         # Velox data type definitions
│   ├── Type.java                 #   Abstract base (extends ISerializable)
│   ├── RowType.java              #   Struct type with named fields — the "schema" type
│   ├── BigIntType.java           #   64-bit integer
│   ├── IntegerType.java          #   32-bit integer
│   ├── SmallIntType.java         #   16-bit integer
│   ├── TinyIntType.java          #   8-bit integer
│   ├── HugeIntType.java          #   128-bit integer
│   ├── BooleanType.java          #   Boolean
│   ├── RealType.java             #   32-bit float
│   ├── DoubleType.java           #   64-bit float
│   ├── VarCharType.java          #   Variable-length string
│   ├── VarbinaryType.java        #   Variable-length binary
│   ├── DateType.java             #   Date
│   ├── TimestampType.java        #   Timestamp
│   ├── DecimalType.java          #   Decimal with precision and scale
│   ├── IntervalDayTimeType.java  #   Day-time interval
│   ├── IntervalYearMonthType.java#   Year-month interval
│   ├── ArrayType.java            #   Array of elements
│   ├── MapType.java              #   Key-value map
│   ├── FunctionType.java         #   Function type (for lambdas)
│   ├── OpaqueType.java           #   Opaque type
│   └── UnknownType.java          #   Unknown/unresolved type
│
├── plan/                         # Query plan nodes (tree structure)
│   ├── PlanNode.java             #   Abstract base — has id and sources
│   ├── TableScanNode.java        #   Reads from a connector (leaf node)
│   ├── FilterNode.java           #   WHERE clause
│   ├── ProjectNode.java          #   SELECT expressions / column projections
│   ├── AggregationNode.java      #   GROUP BY + aggregates
│   ├── HashJoinNode.java         #   Hash join between two inputs
│   ├── AbstractJoinNode.java     #   Shared base for join nodes
│   ├── LimitNode.java            #   LIMIT N
│   ├── OrderByNode.java          #   ORDER BY
│   ├── WindowNode.java           #   Window functions
│   ├── ValuesNode.java           #   Inline constant data (like VALUES clause)
│   └── TableWriteNode.java       #   Writes output to a connector
│
├── expression/                   # Typed expressions (used in plans)
│   ├── TypedExpr.java            #   Abstract base
│   ├── FieldAccessTypedExpr.java #   Column reference (e.g., "col_a")
│   ├── CallTypedExpr.java        #   Function call (e.g., add(a, b))
│   ├── ConstantTypedExpr.java    #   Literal constant
│   ├── CastTypedExpr.java        #   Type cast
│   ├── ConcatTypedExpr.java      #   Row construction / concat
│   ├── DereferenceTypedExpr.java #   Struct field access
│   ├── InputTypedExpr.java       #   Entire input row reference
│   └── LambdaTypedExpr.java      #   Lambda expression
│
├── connector/                    # Data source/sink connector definitions
│   ├── ConnectorTableHandle.java #   Abstract: identifies a table
│   ├── ConnectorSplit.java       #   Abstract: identifies a data chunk
│   ├── ConnectorInsertTableHandle.java # Abstract: identifies a write target
│   ├── ColumnHandle.java         #   Abstract: identifies a column
│   ├── Assignment.java           #   Maps output column name to column handle
│   ├── HiveTableHandle.java      #   Hive table identifier
│   ├── HiveConnectorSplit.java   #   Hive split (file path, format, range)
│   ├── HiveColumnHandle.java     #   Hive column identifier
│   ├── HiveInsertTableHandle.java#   Hive write target
│   ├── HiveBucketProperty.java   #   Hive bucketing configuration
│   ├── HiveBucketConversion.java #   Bucket conversion
│   ├── HiveSortingColumn.java    #   Hive sorting column spec
│   ├── HiveInsertFileNameGenerator.java # File naming for writes
│   ├── InsertTableHandle.java    #   Wraps a ConnectorInsertTableHandle
│   ├── LocationHandle.java       #   Write location
│   ├── FileFormat.java           #   Enum: PARQUET, DWRF, NIMBLE, etc.
│   ├── FileProperties.java       #   File metadata
│   ├── FileNameGenerator.java    #   Abstract: file naming strategy
│   ├── ColumnType.java           #   Enum: REGULAR, PARTITION_KEY, etc.
│   ├── CommitStrategy.java       #   Enum: NO_COMMIT, TASK_COMMIT
│   ├── CompressionKind.java      #   Enum: NONE, GZIP, SNAPPY, etc.
│   ├── RowIdProperties.java      #   Row ID configuration
│   ├── SubfieldFilter.java       #   Subfield-level filter
│   ├── ExternalStream.java       #   Interface: Java-provided data stream
│   ├── ExternalStreamTableHandle.java  # Table handle for external streams
│   ├── ExternalStreamConnectorSplit.java # Split for external streams
│   └── ExternalStreams.java      #   Factory + implementations for external streams
│
├── query/                        # Query execution
│   ├── Query.java                #   Bundles plan + config + connector config
│   ├── Queries.java              #   Session-scoped query operations
│   ├── QueryExecutor.java        #   Wraps a C++ query executor
│   ├── SerialTask.java           #   A running Velox task (add splits, iterate)
│   └── SerialTaskStats.java      #   Task execution statistics
│
├── data/                         # Columnar data vectors
│   ├── BaseVector.java           #   Abstract: any Velox vector (wraps C++ pointer)
│   ├── BaseVectors.java          #   Session-scoped vector operations
│   ├── RowVector.java            #   A vector of rows (struct vector)
│   ├── RowVectors.java           #   Session-scoped RowVector operations
│   ├── SelectivityVector.java    #   Bitmask selecting active rows
│   ├── SelectivityVectors.java   #   Session-scoped selectivity vector ops
│   └── VectorEncoding.java       #   Enum: FLAT, CONSTANT, DICTIONARY, etc.
│
├── eval/                         # Expression evaluation (outside of plans)
│   ├── Evaluation.java           #   Wraps expression + input type
│   ├── Evaluations.java          #   Session-scoped evaluation operations
│   └── Evaluator.java            #   Wraps a C++ evaluator instance
│
├── serde/                        # JSON serialization framework
│   ├── NativeBean.java           #   Marker interface for polymorphic serde
│   ├── Serde.java                #   Central ObjectMapper configuration
│   ├── SerdeRegistry.java        #   Registry: "name" -> concrete Java class
│   ├── SerdeRegistryFactory.java #   Creates registries for base classes
│   ├── PolymorphicDeserializer.java # Custom Jackson deserializer (name-based dispatch)
│   └── PolymorphicSerializer.java   # Custom Jackson serializer (adds "name" field)
│
├── serializable/                 # ISerializable base type (mirrors Velox's ISerializable)
│   ├── ISerializable.java        #   Abstract base for JSON-serializable Velox objects
│   ├── ISerializableCo.java      #   C++ companion object for an ISerializable
│   ├── ISerializableRegistry.java#   Registers all ISerializable subclasses
│   └── ISerializables.java       #   Session-scoped ISerializable operations
│
├── variant/                      # Variant type (dynamic-typed values)
│   ├── Variant.java              #   Abstract base for variant values
│   ├── VariantRegistry.java      #   Registers all variant subclasses
│   ├── VariantCo.java            #   C++ companion for a Variant
│   ├── Variants.java             #   Session-scoped variant operations
│   ├── BigIntValue.java          #   Long variant
│   ├── IntegerValue.java         #   Int variant
│   ├── SmallIntValue.java        #   Short variant
│   ├── TinyIntValue.java         #   Byte variant
│   ├── HugeIntValue.java         #   128-bit int variant
│   ├── BooleanValue.java         #   Boolean variant
│   ├── RealValue.java            #   Float variant
│   ├── DoubleValue.java          #   Double variant
│   ├── VarCharValue.java         #   String variant
│   ├── VarBinaryValue.java       #   Binary variant
│   ├── TimestampValue.java       #   Timestamp variant
│   ├── ArrayValue.java           #   Array variant
│   ├── MapValue.java             #   Map variant
│   └── RowValue.java             #   Row/struct variant
│
├── config/                       # Query and connector configuration
│   ├── Config.java               #   Key-value config (wraps List<Entry>)
│   └── ConnectorConfig.java      #   Per-connector config (connectorId -> Config)
│
├── session/                      # Session lifecycle
│   └── Session.java              #   Interface: groups all session-scoped APIs
│
├── jni/                          # JNI bridge (Java side)
│   ├── JniApi.java               #   High-level API over JniWrapper (handles serde)
│   ├── JniWrapper.java           #   Raw native method declarations (per-session)
│   ├── StaticJniApi.java         #   Global (non-session) high-level API
│   ├── StaticJniWrapper.java     #   Global native method declarations
│   ├── JniLibLoader.java         #   Extracts + loads native .so from JAR
│   ├── JniWorkspace.java         #   Manages temp directory for extracted libs
│   ├── CppObject.java            #   Interface: Java handle to a C++ object (has id())
│   ├── LocalSession.java         #   Session implementation backed by JniApi
│   └── CalledFromNative.java     #   Annotation marking methods called from C++
│
├── memory/                       # Memory management
│   ├── AllocationListener.java   #   Interface: notified on native allocations
│   ├── NoopAllocationListener.java #  No-op implementation
│   ├── BytesAllocationListener.java # Tracks allocated bytes
│   └── MemoryManager.java        #   Wraps C++ MemoryManager (CppObject)
│
├── iterator/                     # Data iterators (Java <-> C++ streaming)
│   ├── UpIterator.java           #   Pulls data from C++ to Java (CppObject)
│   ├── UpIterators.java          #   Utility: convert UpIterator to Java Iterator
│   ├── GenericUpIterator.java    #   Generic UpIterator implementation
│   ├── DownIterator.java         #   Interface: pushes data from Java to C++
│   ├── DownIterators.java        #   Utility: convert Java Iterator to DownIterator
│   ├── InfiniteIterator.java     #   DownIterator for unbounded streams
│   └── CloseableIterator.java    #   Iterator with close()
│
├── arrow/                        # Arrow interop
│   └── Arrow.java                #   Convert between Velox vectors and Arrow format
│
├── aggregate/                    # Aggregation definitions
│   ├── Aggregate.java            #   An aggregate function spec (name, args, etc.)
│   └── AggregateStep.java        #   Enum: PARTIAL, FINAL, INTERMEDIATE, SINGLE
│
├── filter/                       # Filter definitions
│   ├── Filter.java               #   Abstract base for table scan filters
│   └── AlwaysTrue.java           #   Filter that passes all rows
│
├── join/                         # Join definitions
│   └── JoinType.java             #   Enum: INNER, LEFT, RIGHT, FULL, etc.
│
├── exception/                    # Error handling
│   ├── VeloxException.java       #   Main exception type
│   └── ExceptionDescriber.java   #   Formats exception details
│
├── collection/                   # Utility
│   └── Streams.java              #   Stream utilities
│
├── resource/                     # Resource file loading (for bundled native libs)
│   ├── ResourceFile.java         #   Interface: a file from the classpath
│   ├── PlainResourceFile.java    #   Implementation backed by a classpath resource
│   └── Resources.java            #   Discovery: find resources matching a pattern
│
└── write/                        # Table write support
    ├── TableWriteTraits.java     #   Session-scoped write metadata operations
    └── ColumnStatsSpec.java      #   Column statistics specification
```

**Special file:** `src/main/java/com/fasterxml/jackson/databind/deser/BeanDeserializerUnsafe.java` — A package-private access hack to expose Jackson internals needed for the custom polymorphic deserializer. This lives outside the main package tree intentionally.

---

## 2.3 The C++ Source Tree

The C++ code lives under `src/main/cpp/` and mirrors the Java package structure:

```
src/main/cpp/
├── CMakeLists.txt                # Root CMake build (fetches Velox + JniHelpers)
├── build.sh                      # Build script: cmake, compile, install, patch RPATH
├── velox-ref.txt                 # Pinned Velox commit hash
├── velox-ref-md5.txt             # MD5 of the Velox source archive
│
├── main/                         # Production C++ source
│   ├── CMakeLists.txt            # Builds libvelox4j.so + installs dependencies
│   └── velox4j/
│       ├── jni/                  # JNI entry points
│       │   ├── JniLoader.cc      #   JNI_OnLoad: registers JniWrapper + StaticJniWrapper
│       │   ├── JniWrapper.cc     #   Per-session native methods (query, eval, vectors)
│       │   ├── JniWrapper.h
│       │   ├── StaticJniWrapper.cc # Global native methods (init, create session)
│       │   ├── StaticJniWrapper.h
│       │   ├── JniCommon.cc      #   JNI utility functions
│       │   ├── JniCommon.h
│       │   ├── JniError.cc       #   Exception handling (C++ -> Java)
│       │   └── JniError.h
│       │
│       ├── lifecycle/            # Object and session management
│       │   ├── ObjectStore.cc    #   Stores shared_ptrs, returns integer handles
│       │   ├── ObjectStore.h
│       │   ├── ResourceMap.h     #   Thread-safe map for object handles
│       │   ├── Session.cc        #   Session: owns ObjectStore + MemoryManager
│       │   └── Session.h
│       │
│       ├── query/                # Query execution
│       │   ├── Query.cc          #   Query serde (serialize/deserialize)
│       │   ├── Query.h
│       │   ├── QueryExecutor.cc  #   Creates and runs Velox tasks
│       │   └── QueryExecutor.h
│       │
│       ├── eval/                 # Expression evaluation
│       │   ├── Evaluation.cc     #   Evaluation serde
│       │   ├── Evaluation.h
│       │   ├── Evaluator.cc      #   Expression evaluator
│       │   └── Evaluator.h
│       │
│       ├── memory/               # Memory management
│       │   ├── MemoryManager.cc  #   Wraps Velox memory pools
│       │   ├── MemoryManager.h
│       │   ├── AllocationListener.cc  # Abstract listener
│       │   ├── AllocationListener.h
│       │   ├── JavaAllocationListener.cc # Calls back into Java listener
│       │   ├── JavaAllocationListener.h
│       │   ├── ArrowMemoryPool.cc # Arrow memory pool backed by Velox pool
│       │   └── ArrowMemoryPool.h
│       │
│       ├── iterator/             # Data streaming
│       │   ├── UpIterator.cc     #   Pulls data from Velox task -> Java
│       │   ├── UpIterator.h
│       │   ├── DownIterator.cc   #   Pushes data from Java -> Velox
│       │   ├── DownIterator.h
│       │   ├── BlockingQueue.cc  #   Thread-safe queue for async streaming
│       │   └── BlockingQueue.h
│       │
│       ├── connector/            # External stream connector
│       │   ├── ExternalStream.cc #   Velox DataSource backed by Java iterator
│       │   └── ExternalStream.h
│       │
│       ├── arrow/                # Arrow interop
│       │   ├── Arrow.cc          #   Velox <-> Arrow conversion
│       │   └── Arrow.h
│       │
│       ├── config/               # Query config serde
│       │   ├── Config.cc
│       │   └── Config.h
│       │
│       ├── init/                 # Initialization
│       │   ├── Init.cc           #   Registers connectors, types, serde, functions
│       │   ├── Init.h
│       │   ├── Config.cc         #   Velox4J global configuration
│       │   └── Config.h
│       │
│       └── vector/               # Vector utilities
│           ├── Vectors.cc        #   Vector serialization helpers
│           └── Vectors.h
│
└── test/                         # C++ tests
    ├── CMakeLists.txt
    └── velox4j/
        ├── test/Init.h           #   Test initialization helper
        ├── query/
        │   ├── QuerySerdeTest.cc #   Tests Query JSON round-trip
        │   └── QueryTest.cc      #   Tests query execution
        └── iterator/
            └── BlockingQueueTest.cc # Tests blocking queue
```

### How Java and C++ mirror each other

| Concern | Java | C++ |
|---------|------|-----|
| Query definition | `query/Query.java` | `query/Query.cc/.h` |
| Query execution | `query/QueryExecutor.java` | `query/QueryExecutor.cc/.h` |
| Expression eval | `eval/Evaluation.java` | `eval/Evaluation.cc/.h` |
| Memory | `memory/MemoryManager.java` | `memory/MemoryManager.cc/.h` |
| Iterators | `iterator/UpIterator.java` | `iterator/UpIterator.cc/.h` |
| Arrow | `arrow/Arrow.java` | `arrow/Arrow.cc/.h` |
| Config | `config/Config.java` | `config/Config.cc/.h` |
| Session | `session/Session.java` | `lifecycle/Session.cc/.h` |
| JNI bridge | `jni/JniWrapper.java` | `jni/JniWrapper.cc/.h` |
| JNI bridge (static) | `jni/StaticJniWrapper.java` | `jni/StaticJniWrapper.cc/.h` |

The C++ side is intentionally **thin**. Most complexity is in the Java serialization layer and in Velox itself. The C++ side mainly:
1. Receives JSON strings
2. Deserializes them into Velox objects
3. Invokes Velox APIs
4. Returns object handles (long IDs) back to Java

---

## 2.4 The Test Tree

```
src/test/java/org/boostscale/velox4j/
│
├── test/                              # Test infrastructure + sample tests
│   ├── Velox4jTests.java             #   Base test class (initializes Velox4J)
│   ├── SampleQueryTests.java         #   Helper: builds sample queries for testing
│   ├── ConfigTests.java              #   Helper: test configurations
│   ├── TypeTests.java                #   Helper: common type definitions
│   ├── ResourceTests.java            #   Helper: test resource loading
│   ├── UpIteratorTests.java          #   Helper: iterate and collect results
│   ├── TestThreads.java              #   Thread utilities for tests
│   └── dataset/                      #   Test datasets
│       ├── TestDataFile.java         #     A test data file reference
│       ├── TestDataset.java          #     Interface: a test dataset
│       └── tpch/                     #     TPC-H benchmark data
│           ├── TpchDataset.java      #       TPC-H dataset definition
│           ├── TpchDatasets.java     #       TPC-H dataset collection
│           ├── TpchTableName.java    #       TPC-H table name enum
│           └── DownloadedTpchDataset.java  # Downloads TPC-H data for tests
│
├── serde/                             # Serialization round-trip tests
│   ├── SerdeTests.java               #   Base class for serde tests
│   ├── TypeSerdeTest.java            #   Test type JSON round-trip
│   ├── PlanNodeSerdeTest.java        #   Test plan node JSON round-trip
│   ├── TypedExprSerdeTest.java       #   Test expression JSON round-trip
│   ├── ConnectorSerdeTest.java       #   Test connector JSON round-trip
│   ├── ConfigSerdeTest.java          #   Test config JSON round-trip
│   ├── FilterSerdeTest.java          #   Test filter JSON round-trip
│   ├── QuerySerdeTest.java           #   Test query JSON round-trip
│   ├── EvaluationSerdeTest.java      #   Test evaluation JSON round-trip
│   └── VariantSerdeTest.java         #   Test variant JSON round-trip
│
├── query/QueryTest.java              # End-to-end query execution test
├── eval/EvaluationTest.java          # Expression evaluation test
├── data/
│   ├── BaseVectorTest.java           # BaseVector operations test
│   ├── BaseVectorTests.java          # Base vector test helpers
│   ├── RowVectorTest.java            # RowVector operations test
│   └── SelectivityVectorTest.java    # SelectivityVector test
├── jni/
│   ├── JniApiTest.java               # JNI API test
│   └── JniApiTests.java              # JNI API test helpers
├── arrow/ArrowTest.java              # Arrow interop test
├── variant/VariantTest.java          # Variant type test
└── write/TableWriteTraitsTest.java   # Table write traits test
```

Testing strategy:
- **Serde tests** verify that Java objects round-trip through JSON correctly (Java -> JSON -> Java)
- **Execution tests** (`QueryTest`, `EvaluationTest`) verify end-to-end behavior through the JNI bridge
- **Data tests** verify vector operations (slice, flatten, partition)
- Tests use TPC-H data downloaded from the internet (see `DownloadedTpchDataset.java`)

---

## 2.5 Build System

### Java build: Maven

The `pom.xml` defines:
- **Source level**: Java 8
- **Dependencies**: Arrow, Jackson, Guava (see Lesson 1)
- **C++ build trigger**: Maven's `exec-maven-plugin` runs `build.sh` during the compile phase (can be skipped with `-Dskip.cpp.build=true`)
- **Code style**: Spotless plugin enforces Google Java Format

Build commands:
```shell
mvn clean install                    # Full build (Java + C++)
mvn clean install -Dskip.cpp.build=true  # Java only (if native lib already built)
```

### C++ build: CMake

`src/main/cpp/CMakeLists.txt` does the following:

1. **Fetches Velox** from GitHub using `FetchContent` at the exact commit pinned in `velox-ref.txt`
2. **Fetches JniHelpers** (Spotify's JNI helper library) for cleaner JNI code
3. **Builds `libvelox4j.so`** — a shared library that links Velox (as a mono library) + JniHelpers
4. **Installs** `libvelox4j.so` plus all its shared library dependencies into a flat directory

The `build.sh` script wraps the CMake build and adds post-processing:
- Uses `patchelf` to set `$ORIGIN` RPATH on all `.so` files, making the build portable (libraries find each other relative to `libvelox4j.so`)
- Verifies no symbolic links were accidentally installed
- Runs `ldd` to verify all dependencies resolve

### Native library loading at runtime

When `Velox4j.initialize()` is called:

1. `JniLibLoader` looks for bundled `.so` files under the classpath at `velox4j-lib/{os.name}/{os.arch}/`
2. All library files are extracted to a temporary work directory
3. `System.load()` loads `libvelox4j.so`, which triggers `JNI_OnLoad` in C++
4. `JNI_OnLoad` registers the `JniWrapper` and `StaticJniWrapper` native methods

**Source:** `src/main/java/org/boostscale/velox4j/jni/JniLibLoader.java`

---

## 2.6 CI Workflows

Located in `.github/workflows/`:

| Workflow | File | Purpose |
|----------|------|---------|
| Java unit tests | `ut-java.yml` | Builds C++ + runs Java tests |
| C++ unit tests | `ut-cpp.yml` | Builds and runs C++ tests (GTest) |
| Format check | `format.yml` | Checks Java (Google format) + C++ (clang-format) style |
| Snapshot publish | `pub-snapshot.yml` | Publishes SNAPSHOT JAR to Maven Central |
| Velox version bump | `bot-dep.yml` | Bot that auto-bumps the pinned Velox commit |

Supporting scripts in `.github/workflows/scripts/`:
- `common/setup-centos7.sh` — sets up CentOS 7 build environment (for release JARs)
- `common/setup-ubuntu24.sh` — sets up Ubuntu 24 build environment
- `bot-dep/bump-velox.sh` — updates `velox-ref.txt` to latest Velox commit
- `format/format.sh` — runs code format checks/fixes (Docker-based)

---

## 2.7 The CppObject Pattern

A pattern you'll see everywhere in the Java code: classes that wrap C++ objects via integer handles.

```
CppObject (interface)
├── id()     → returns the long handle to the C++ shared_ptr
└── close()  → calls StaticJniApi.releaseCppObject() to release the C++ reference

Implementations:
  Session, MemoryManager, BaseVector, RowVector, SelectivityVector,
  Evaluator, QueryExecutor, SerialTask, UpIterator, ExternalStream, ...
```

The C++ `ObjectStore` maps these long IDs to `shared_ptr<void>`. When `close()` is called from Java, the shared pointer reference is removed from the store. If no other C++ code holds a reference, the object is destroyed.

**Source:** `src/main/java/org/boostscale/velox4j/jni/CppObject.java`, `src/main/cpp/main/velox4j/lifecycle/ObjectStore.h`

---

## 2.8 Package Dependency Flow

Understanding how packages depend on each other helps you navigate:

```
                    Velox4j.java (entry point)
                         |
            +------------+------------+
            |            |            |
         session/     memory/      jni/
            |            |            |
            +-----+------+      JniApi / JniWrapper
                  |                   |
     +------------+------------+      |
     |      |     |     |      |      |
   query/ eval/ data/ arrow/ connector/
     |      |     |     |      |
     +------+-----+-----+------+
                  |
          +-------+-------+
          |       |       |
       plan/ expression/ type/
          |       |       |
          +-------+-------+
                  |
            serializable/
                  |
               serde/
```

The bottom of the stack (`serde/` and `serializable/`) is the foundation — everything serializable depends on it. The top (`Velox4j.java`) is the user-facing entry point.

---

## 2.9 Key Takeaways

1. **Single Maven module** with Java + C++ side by side. C++ is built by Maven via `build.sh`.
2. **~20 Java packages**, each focused on one concern. The structure mirrors Velox's own concepts.
3. **C++ is thin** (~25 source files). It's a JNI bridge layer, not a reimplementation of Velox.
4. **Java and C++ mirror each other** — same directory names, same concepts.
5. **CppObject pattern** — Java objects hold long IDs pointing to C++ shared_ptrs in an ObjectStore.
6. **Serde is the foundation** — the `serde/` and `serializable/` packages underpin everything.
7. **Tests are split** into serde tests (JSON round-trips) and execution tests (end-to-end through JNI).

---

## 2.10 Exercises

1. **Trace a package**: Pick any Java package (e.g., `eval/`). List all its classes, then find the matching C++ files. Confirm they share the same concepts.
2. **Read CppObject**: Open `src/main/java/org/boostscale/velox4j/jni/CppObject.java`. Then open `src/main/cpp/main/velox4j/lifecycle/ObjectStore.h`. Understand how `id()` on the Java side maps to a stored `shared_ptr` on the C++ side.
3. **Explore the build**: Read `src/main/cpp/build.sh`. What does the `patchelf` step do and why is it necessary for portability?
4. **Count the C++ source files**: How many `.cc` files are listed in `src/main/cpp/main/CMakeLists.txt`? Compare that to the number of Java files. What does this ratio tell you about the project's design?
5. **Find the serde tests**: Open any test in `src/test/java/org/boostscale/velox4j/serde/`. How does it verify that JSON serialization is correct?

---

## Next Lesson

[Lesson 3: Type System](lesson-03-type-system.md) — Deep dive into how Velox data types are modeled in Java.
