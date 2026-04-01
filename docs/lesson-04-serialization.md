# Lesson 4: Serialization Architecture

## Learning Goals

After this lesson you should be able to:

- Trace the full journey of a Java object from construction to C++ deserialization
- Explain how the `SerdeRegistry` / `SerdeRegistryFactory` tree enables polymorphic dispatch
- Understand how `PolymorphicSerializer` and `PolymorphicDeserializer` modify Jackson's behavior
- Read and write new serializable classes that follow the project's conventions
- Compare the Java and C++ serde registration and understand why they must stay in sync

---

## 4.1 Why This Lesson Matters

The serialization architecture is **the core design pattern** of Velox4J. Every interaction between Java and C++ goes through this pipeline:

```
Java object  ──Serde.toJson()──>  JSON string  ──JNI──>  C++ string
                                                            │
                                                    folly::parseJson()
                                                            │
                                                  ISerializable::deserialize()
                                                            │
                                                     Native Velox object
```

If you understand this pipeline, you can:
- Add new types, plan nodes, or expressions to Velox4J
- Debug serialization mismatches between Java and C++
- Understand why every class uses specific Jackson annotations

---

## 4.2 The Five Serde Components

The serialization system consists of five components that work together:

| Component | File | Role |
|-----------|------|------|
| `NativeBean` | `serde/NativeBean.java` | Marker interface: "this class has polymorphic serde" |
| `Serde` | `serde/Serde.java` | Central `ObjectMapper` with custom serializer/deserializer |
| `SerdeRegistryFactory` + `SerdeRegistry` | `serde/SerdeRegistryFactory.java`, `serde/SerdeRegistry.java` | Registry tree: maps JSON key-value pairs to Java classes |
| `PolymorphicSerializer` | `serde/PolymorphicSerializer.java` | Injects dispatch keys during serialization |
| `PolymorphicDeserializer` | `serde/PolymorphicDeserializer.java` | Reads dispatch keys to find the concrete class during deserialization |

Plus two registration entry points:
- `ISerializableRegistry` — registers all `ISerializable` subclasses (types, plans, expressions, connectors, etc.)
- `VariantRegistry` — registers all `Variant` subclasses

---

## 4.3 The Registry Tree

The `SerdeRegistry` forms a **tree of key-value dispatch rules**. Let's build the tree step by step.

### Step 1: Create a factory for a base class

```java
// ISerializableRegistry.java (line 67)
SerdeRegistry NAME_REGISTRY =
    SerdeRegistryFactory.createForBaseClass(ISerializable.class).key("name");
```

This creates:

```
ISerializable (base class)
  └── SerdeRegistryFactory
        └── SerdeRegistry (key: "name")
              └── (empty — registrations go here)
```

### Step 2: Register a sub-factory for types

```java
// ISerializableRegistry.java (line 84)
SerdeRegistry typeRegistry = NAME_REGISTRY.registerFactory("Type").key("type");
```

Now `"name": "Type"` routes to a second-level registry keyed by `"type"`:

```
ISerializable
  └── key: "name"
        ├── "Type" → sub-factory
        │     └── key: "type"
        │           └── (empty — type classes go here)
        └── (more registrations later)
```

### Step 3: Register concrete type classes

```java
// ISerializableRegistry.java (lines 85-102)
typeRegistry.registerClass("BOOLEAN", BooleanType.class);
typeRegistry.registerClass("BIGINT", BigIntType.class);
typeRegistry.registerClass("ROW", RowType.class);
// ...
```

```
ISerializable
  └── key: "name"
        ├── "Type" → sub-factory
        │     └── key: "type"
        │           ├── "BOOLEAN"  → BooleanType.class
        │           ├── "BIGINT"   → BigIntType.class
        │           ├── "ROW"      → RowType.class
        │           └── ...
        └── (more below)
```

### Step 4: Register other ISerializable subclasses directly

Expressions, plan nodes, connectors, etc. are registered directly under `"name"`:

```java
// ISerializableRegistry.java (lines 115-166)
NAME_REGISTRY.registerClass("CallTypedExpr", CallTypedExpr.class);
NAME_REGISTRY.registerClass("TableScanNode", TableScanNode.class);
NAME_REGISTRY.registerClass("HiveTableHandle", HiveTableHandle.class);
NAME_REGISTRY.registerClass("velox4j.Query", Query.class);
// ...
```

### The complete tree

```
ISerializable
  └── key: "name"
        │
        │ ── Types (two-level dispatch) ──
        ├── "Type"                → sub-factory → key: "type"
        │     ├── "BOOLEAN"       → BooleanType.class
        │     ├── "BIGINT"        → BigIntType.class
        │     ├── "ROW"           → RowType.class
        │     ├── "ARRAY"         → ArrayType.class
        │     ├── "MAP"           → MapType.class
        │     ├── "DECIMAL"       → DecimalType.class
        │     └── ... (14 more)
        │
        ├── "DateType"            → sub-factory → key: "type"
        │     └── "DATE"          → DateType.class
        │
        ├── "IntervalDayTimeType" → sub-factory → key: "type"
        │     └── "INTERVAL DAY TO SECOND" → IntervalDayTimeType.class
        │
        ├── "IntervalYearMonthType" → sub-factory → key: "type"
        │     └── "INTERVAL YEAR TO MONTH" → IntervalYearMonthType.class
        │
        │ ── Expressions (single-level dispatch) ──
        ├── "CallTypedExpr"       → CallTypedExpr.class
        ├── "CastTypedExpr"       → CastTypedExpr.class
        ├── "ConstantTypedExpr"   → ConstantTypedExpr.class
        ├── "FieldAccessTypedExpr"→ FieldAccessTypedExpr.class
        ├── ... (4 more)
        │
        │ ── Plan nodes ──
        ├── "TableScanNode"       → TableScanNode.class
        ├── "FilterNode"          → FilterNode.class
        ├── "ProjectNode"         → ProjectNode.class
        ├── "AggregationNode"     → AggregationNode.class
        ├── ... (6 more)
        │
        │ ── Connectors ──
        ├── "HiveTableHandle"     → HiveTableHandle.class
        ├── "HiveConnectorSplit"  → HiveConnectorSplit.class
        ├── "HiveColumnHandle"    → HiveColumnHandle.class
        ├── ... (7 more)
        │
        │ ── Filters ──
        ├── "AlwaysTrue"          → AlwaysTrue.class
        │
        │ ── Config / Query / Eval ──
        ├── "velox4j.Config"      → Config.class
        ├── "velox4j.ConnectorConfig" → ConnectorConfig.class
        ├── "velox4j.Evaluation"  → Evaluation.class
        └── "velox4j.Query"       → Query.class
```

There is a **separate** tree for `Variant`:

```
Variant
  └── key: "type"
        ├── "BOOLEAN"   → BooleanValue.class
        ├── "BIGINT"    → BigIntValue.class
        ├── "VARCHAR"   → VarCharValue.class
        ├── "ARRAY"     → ArrayValue.class
        ├── "MAP"       → MapValue.class
        ├── "ROW"       → RowValue.class
        └── ... (8 more)
```

**Source:** `ISerializableRegistry.java`, `VariantRegistry.java`

---

## 4.4 Serialization: Java Object to JSON

When you call `Serde.toJson(object)` or `Serde.toPrettyJson(object)`, Jackson's `ObjectMapper` processes the object. For `NativeBean` subclasses, the `PolymorphicSerializer` kicks in.

### How `PolymorphicSerializer` works

The serializer is installed as a `BeanSerializerModifier` on the Jackson `ObjectMapper`:

```java
// Serde.java (line 52-53)
jsonMapper.addModule(new SimpleModule().setSerializerModifier(serializerModifier));
```

For every bean being serialized, Jackson calls `modifySerializer()`. If the bean is a `NativeBean` subclass, the modifier replaces the default serializer with one that **prepends dispatch key-value pairs** before the regular fields.

There are two cases:

**Case 1: Empty bean** (no `@JsonGetter` fields, e.g., `BigIntType`)

The `EmptyBeanSerializer` writes only the dispatch keys:

```java
// PolymorphicSerializer.java (lines 33-45)
private static class EmptyBeanSerializer extends JsonSerializer<Object> {
    public void serialize(Object bean, JsonGenerator gen, ...) throws IOException {
        gen.writeStartObject();
        List<SerdeRegistry.KvPair> kvs = SerdeRegistry.findKvPairs(bean.getClass());
        for (SerdeRegistry.KvPair kv : kvs) {
            gen.writeStringField(kv.getKey(), kv.getValue());
        }
        gen.writeEndObject();
    }
}
```

For `BigIntType`, `findKvPairs` returns `[("name", "Type"), ("type", "BIGINT")]`, producing:

```json
{ "name": "Type", "type": "BIGINT" }
```

**Case 2: Non-empty bean** (has `@JsonGetter` fields, e.g., `RowType`, `Query`)

The `NonEmptyBeanSerializer` writes dispatch keys first, then delegates to Jackson's normal field serialization:

```java
// PolymorphicSerializer.java (lines 47-62)
private static final class NonEmptyBeanSerializer extends BeanSerializer {
    protected void serializeFields(Object bean, JsonGenerator gen, ...) throws IOException {
        List<SerdeRegistry.KvPair> kvs = SerdeRegistry.findKvPairs(bean.getClass());
        for (SerdeRegistry.KvPair kv : kvs) {
            gen.writeStringField(kv.getKey(), kv.getValue());
        }
        super.serializeFields(bean, gen, provider);  // writes @JsonGetter fields
    }
}
```

For `new RowType(List.of("a"), List.of(new BigIntType()))`:

```json
{
  "name": "Type",          ← dispatch key from registry
  "type": "ROW",           ← dispatch key from registry
  "names": ["a"],          ← @JsonProperty("names") field
  "cTypes": [              ← @JsonProperty("cTypes") field
    { "name": "Type", "type": "BIGINT" }
  ]
}
```

### The KvPair lookup

`SerdeRegistry.findKvPairs(clazz)` is the bridge between the registry tree and serialization. When a class is registered, its full chain of key-value pairs is stored:

```java
// SerdeRegistry.java (line 71)
CLASS_TO_KVS.put(clazz, withNewKv(new KvPair(key, value)));
```

For `BigIntType.class`, the stored pairs are: `[("name", "Type"), ("type", "BIGINT")]`.
For `Query.class`, the stored pair is: `[("name", "velox4j.Query")]`.

---

## 4.5 Deserialization: JSON to Java Object

When you call `Serde.fromJson(json, ISerializable.class)`, the `PolymorphicDeserializer` kicks in.

### How `PolymorphicDeserializer` works

The deserializer is also installed as a `BeanDeserializerModifier`:

```java
// Serde.java (line 52)
jsonMapper.addModule(new SimpleModule().setDeserializerModifier(deserializerModifier));
```

For every bean being deserialized, `modifyDeserializer()` is called. There are two cases:

**Case 1: Abstract class** (e.g., `Type`, `PlanNode`, `ISerializable`)

An `AbstractDeserializer` replaces the normal deserializer. It reads the JSON object and walks the registry tree to find the concrete class:

```java
// PolymorphicDeserializer.java (lines 86-97)
public Object deserialize(JsonParser p, DeserializationContext ctxt) {
    ObjectNode objNode = (ObjectNode) p.readValueAsTree();
    SerdeRegistry registry = findRegistry(
        SerdeRegistryFactory.getForBaseClass(baseClass), objNode);
    return deserializeWithRegistry(p, registry, objNode);
}
```

The `findRegistry` method scans the JSON object's field names to find a key that matches a registered key in the factory. For example, given:

```json
{ "name": "Type", "type": "BIGINT" }
```

1. The `ISerializable` factory has key `"name"` → finds `"name"` in JSON
2. Reads value `"Type"` → this is a sub-factory, not a class
3. Goes to the `"Type"` sub-factory, which has key `"type"` → finds `"type"` in JSON
4. Reads value `"BIGINT"` → this is a class: `BigIntType.class`
5. Calls `p.getCodec().treeToValue(objectNode, BigIntType.class)` to construct the object

**Case 2: Concrete class** (e.g., `BigIntType`, `RowType`)

When Jackson deserializes the concrete class, the dispatch keys (`"name"`, `"type"`) are still in the JSON but are not fields of the Java class. They would normally cause an error because `FAIL_ON_UNKNOWN_PROPERTIES` is enabled.

The solution: the deserializer modifier tells Jackson to **ignore** the dispatch key fields for concrete classes:

```java
// PolymorphicDeserializer.java (lines 120-128)
return bd.withByNameInclusion(
    SerdeRegistry.findKvPairs(beanClass).stream()
        .map(SerdeRegistry.KvPair::getKey)
        .collect(Collectors.toSet()),
    null);
```

This creates a `BeanDeserializer` that ignores `"name"` and `"type"` but still fails on any other unknown field.

### The BeanDeserializerUnsafe hack

To call `withByNameInclusion`, the code needs to verify that no other ignorable/includable properties are already set. The `_ignorableProps` and `_includableProps` fields on `BeanDeserializer` are package-private, so Velox4J places a helper class in Jackson's package:

```java
// com/fasterxml/jackson/databind/deser/BeanDeserializerUnsafe.java
public final class BeanDeserializerUnsafe {
    public static Set<String> getIgnorableProps(BeanDeserializer bd) {
        return bd._ignorableProps;   // package-private access
    }
}
```

This is the only file outside `org.boostscale.velox4j` in the project.

---

## 4.6 The Jackson ObjectMapper Configuration

The `Serde` class configures a strict Jackson `ObjectMapper`:

```java
// Serde.java (lines 41-54)
jsonMapper.serializationInclusion(JsonInclude.Include.NON_NULL);    // Skip null fields
jsonMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);   // Reject duplicate keys
jsonMapper.enable(JsonGenerator.Feature.STRICT_DUPLICATE_DETECTION);
jsonMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);               // No auto-detection!
jsonMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);           // Only explicit annotations
jsonMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
jsonMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
jsonMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
jsonMapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES); // Strict: no extra fields
jsonMapper.disable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES);
```

Key design choices:
- **No auto-detection**: All serialized fields must use explicit `@JsonGetter` / `@JsonProperty` annotations. This prevents accidental field exposure.
- **Strict duplicate detection**: Catches bugs where two fields produce the same JSON key.
- **Fail on unknown properties**: Catches schema mismatches early. (Dispatch keys are exempted via the ignorable props mechanism.)
- **Skip nulls**: Null fields are omitted from JSON, matching Velox's C++ behavior.

---

## 4.7 The C++ Side: How It Mirrors Java

Both Java and C++ must agree on the JSON format. Let's compare.

### Java registration

```java
NAME_REGISTRY.registerClass("velox4j.Query", Query.class);
```

### C++ registration

```cpp
// Query.cc (lines 67-70)
void Query::registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("velox4j.Query", create);
}
```

Both sides register with the same name `"velox4j.Query"`.

### Java serialization

```java
// Query.java — Jackson annotations produce:
// { "name": "velox4j.Query", "plan": {...}, "queryConfig": {...}, "connectorConfig": {...} }
```

### C++ serialization

```cpp
// Query.cc (lines 47-54)
folly::dynamic Query::serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "velox4j.Query";
    obj["plan"] = plan_->serialize();
    obj["queryConfig"] = queryConfig_->serialize();
    obj["connectorConfig"] = connectorConfig_->serialize();
    return obj;
}
```

### C++ deserialization

```cpp
// Query.cc (lines 56-65)
std::shared_ptr<Query> Query::create(const folly::dynamic& obj, void* context) {
    auto plan = ISerializable::deserialize<core::PlanNode>(obj["plan"], context);
    auto queryConfig = ISerializable::deserialize<ConfigArray>(obj["queryConfig"], context);
    auto connectorConfig = ISerializable::deserialize<ConnectorConfigArray>(
        obj["connectorConfig"], context);
    return std::make_shared<Query>(plan, queryConfig, connectorConfig);
}
```

Both sides produce and consume the exact same JSON structure. The C++ side uses `folly::dynamic` (Facebook's dynamic JSON type) while Java uses Jackson.

### C++ initialization

During `Velox4j.initialize()`, the C++ `Init.cc` registers all serde types:

```cpp
// Init.cc (lines 71-119)
ConfigArray::registerSerDe();
ConnectorConfigArray::registerSerDe();
Evaluation::registerSerDe();
Query::registerSerDe();
Type::registerSerDe();
common::Filter::registerSerDe();
connector::hive::HiveTableHandle::registerSerDe();
// ... many more
core::PlanNode::registerSerDe();
core::ITypedExpr::registerSerDe();
```

Most of these are Velox's own serde registrations (types, plan nodes, expressions are registered by Velox itself). Velox4J only adds its own types: `Query`, `ConfigArray`, `ConnectorConfigArray`, `Evaluation`, and the external stream connector.

---

## 4.8 End-to-End Example: Query Serialization

Let's trace a complete query through the pipeline.

### Step 1: Build the query in Java

```java
RowType schema = new RowType(List.of("id"), List.of(new BigIntType()));
TableScanNode scan = new TableScanNode("scan-1", schema, tableHandle, assignments);
Query query = new Query(scan, Config.empty(), ConnectorConfig.empty());
```

### Step 2: Serialize to JSON

```java
String json = Serde.toPrettyJson(query);
```

The `PolymorphicSerializer` produces:

```json
{
  "name" : "velox4j.Query",
  "plan" : {
    "name" : "TableScanNode",
    "id" : "scan-1",
    "outputType" : {
      "name" : "Type",
      "type" : "ROW",
      "names" : [ "id" ],
      "cTypes" : [ {
        "name" : "Type",
        "type" : "BIGINT"
      } ]
    },
    "tableHandle" : { ... },
    "assignments" : [ ... ]
  },
  "queryConfig" : {
    "name" : "velox4j.Config",
    "values" : [ ]
  },
  "connectorConfig" : {
    "name" : "velox4j.ConnectorConfig",
    "values" : [ ]
  }
}
```

Notice: every object has a `"name"` field. Types additionally have a `"type"` field. This is the two-level dispatch in action.

### Step 3: Send JSON to C++ via JNI

```java
// JniApi.java (line 66)
public QueryExecutor createQueryExecutor(Query query) {
    final String queryJson = Serde.toPrettyJson(query);
    return new QueryExecutor(this, jni.createQueryExecutor(queryJson));
}
```

### Step 4: C++ deserializes the JSON

```cpp
// JniWrapper.cc (lines 84-96)
jlong createQueryExecutor(JNIEnv* env, jobject javaThis, jstring queryJson) {
    auto session = sessionOf(env, javaThis);
    spotify::jni::JavaString jQueryJson{env, queryJson};
    auto queryDynamic = folly::parseJson(jQueryJson.get());     // Parse JSON string
    auto query = ISerializable::deserialize<Query>(queryDynamic, querySerdePool);  // Deserialize
    auto exec = std::make_shared<QueryExecutor>(session->memoryManager(), query);
    return session->objectStore()->save(exec);                   // Return handle
}
```

The `ISerializable::deserialize<Query>` call uses Velox's deserialization registry (where `"velox4j.Query"` was registered in `Init.cc`) to find the `Query::create` function, which then recursively deserializes the plan, config, and connector config.

---

## 4.9 The Java Class Convention

Every serializable class in Velox4J follows a consistent pattern:

```java
public class SomeNode extends ISerializable {       // 1. Extends ISerializable (or Type, PlanNode, etc.)
    private final String field1;                     // 2. Immutable fields
    private final Type field2;

    @JsonCreator                                     // 3. Annotated constructor
    public SomeNode(
            @JsonProperty("field1") String field1,   //    JSON key matches C++ key
            @JsonProperty("field2") Type field2) {
        this.field1 = field1;
        this.field2 = field2;
    }

    @JsonGetter("field1")                            // 4. Annotated getters
    public String getField1() { return field1; }

    @JsonGetter("field2")
    public Type getField2() { return field2; }
}
```

Rules:
- **`@JsonCreator`** on the constructor — Jackson uses this for deserialization
- **`@JsonProperty("key")`** on each parameter — the key must match the C++ JSON field name
- **`@JsonGetter("key")`** on each getter — the key must match the C++ JSON field name
- **No auto-detection** — if you forget an annotation, the field won't be serialized
- **Immutable** — all fields are `final`, set in the constructor

---

## 4.10 The Three-Way Round-Trip Test

The `SerdeTests.testISerializableRoundTrip()` method tests every serializable class with three checks:

```java
// SerdeTests.java (lines 70-96)
public static <T extends ISerializable> ... testISerializableRoundTrip(T inObj) {
    // 1. Java serialization round-trip (java.io.Serializable)
    byte[] serialized = serialize(inObj);
    ISerializable javaDeserialized = deserialize(serialized);
    assertJsonEquals(inJson, Serde.toPrettyJson(javaDeserialized));

    // 2. JSON round-trip (Jackson)
    ISerializable javaOutObj = Serde.fromJson(inJson, ISerializable.class);
    assertJsonEquals(inJson, Serde.toPrettyJson(javaOutObj));

    // 3. C++ round-trip (Java -> JSON -> C++ -> JSON -> Java)
    ISerializableCo inObjCo = session.iSerializableOps().asCpp(inObj);
    ISerializable cppOutObj = inObjCo.asJava();
    assertJsonEquals(inJson, Serde.toPrettyJson(cppOutObj));
}
```

**Test 1** verifies Java `Serializable` works (needed because `ISerializable` implements `java.io.Serializable`).

**Test 2** verifies Jackson JSON round-trip: object → JSON → polymorphic deserialization → JSON → compare.

**Test 3** is the most important: it sends the object to C++ through the JNI bridge, C++ deserializes it, serializes it back, and Java reads it again. This ensures the Java JSON format is **exactly compatible** with C++.

---

## 4.11 Two Serde Hierarchies: ISerializable vs Variant

The project has two **independent** serde hierarchies. This mirrors Velox's C++ design, where `ISerializable` and `variant` are completely separate base classes.

### Why two hierarchies?

In Velox's C++ code:
- `ISerializable` is the base class for **plan infrastructure** — types, plans, expressions, connectors. These describe *what to compute*.
- `variant` is a separate class for **runtime values** — a boxed scalar or complex literal. These hold *actual data*.

They are not related by inheritance in C++, so Velox4J mirrors this:

```
NativeBean (marker interface)
├── ISerializable (abstract)   ← plan infrastructure (types, plans, expressions, connectors)
│     ├── Type, PlanNode, TypedExpr, Query, Config, ...
│
└── Variant (abstract)         ← runtime values (boxed literals)
      ├── BigIntValue, VarCharValue, RowValue, ...
```

### Different JSON formats

Because they come from different C++ serialization systems, they use **different dispatch keys** and produce different JSON structures:

| Hierarchy | Dispatch key | Example JSON |
|-----------|-------------|-------------|
| `ISerializable` | `"name"` (+ optionally `"type"` for types) | `{"name": "Type", "type": "BIGINT"}` |
| `Variant` | `"type"` | `{"type": "BIGINT", "value": 42}` |

Note that a `Variant` has **no `"name"` field**. If they shared a single registry, the deserializer would look for `"name"` in a Variant JSON and fail.

### Separate registries avoid collisions

Each hierarchy has its own `SerdeRegistryFactory`, so they don't interfere with each other:

```java
// ISerializableRegistry.java — dispatches on "name"
SerdeRegistryFactory.createForBaseClass(ISerializable.class).key("name");

// VariantRegistry.java — dispatches on "type"
SerdeRegistryFactory.createForBaseClass(Variant.class).key("type");
```

Both registries happen to use `"BIGINT"` as a value, but they map to different classes:
- `ISerializable` registry: `"BIGINT"` → `BigIntType.class` (a type descriptor)
- `Variant` registry: `"BIGINT"` → `BigIntValue.class` (an actual value)

There is no conflict because Jackson selects the registry based on the **declared Java field type** (`Type` vs `Variant`), not the JSON content.

### Where they coexist: ConstantTypedExpr

`ConstantTypedExpr` is the main place where both hierarchies meet. It is an `ISerializable` that **contains** a `Variant` field:

```java
// ConstantTypedExpr.java
public class ConstantTypedExpr extends TypedExpr {   // TypedExpr extends ISerializable
    private final Variant value;                      // a Variant field
    // ...
}
```

Its JSON contains objects from both hierarchies:

```json
{
  "name": "ConstantTypedExpr",
  "type": { "name": "Type", "type": "BIGINT" },
  "value": { "type": "BIGINT", "value": 42 }
}
```

Jackson knows which deserializer to use because:
- The `"type"` field is declared as `Type` (an `ISerializable`) → uses the ISerializable registry
- The `"value"` field is declared as `Variant` → uses the Variant registry

---

## 4.12 Adding a New Serializable Class

If you need to add a new serializable type to Velox4J, follow these steps:

### 1. Create the Java class

```java
public class MyNewNode extends PlanNode {
    private final String myField;

    @JsonCreator
    public MyNewNode(
            @JsonProperty("id") String id,
            @JsonProperty("myField") String myField) {
        super(id);
        this.myField = myField;
    }

    @JsonGetter("myField")
    public String getMyField() { return myField; }

    @Override
    protected List<PlanNode> getSources() { return Collections.emptyList(); }
}
```

### 2. Register it in `ISerializableRegistry`

```java
private static void registerPlanNodes() {
    // ... existing registrations ...
    NAME_REGISTRY.registerClass("MyNewNode", MyNewNode.class);
}
```

### 3. Ensure C++ serde is also registered

On the C++ side, `MyNewNode` must be registered in Velox's serde registry with the same `"name"`. If it's a Velox built-in node, Velox handles this via `core::PlanNode::registerSerDe()`. If it's custom, you'd need a C++ `registerSerDe()` call in `Init.cc`.

### 4. Write a serde test

```java
@Test
public void testMyNewNode() {
    SerdeTests.testISerializableRoundTrip(new MyNewNode("node-1", "hello"));
}
```

---

## 4.13 Key Takeaways

1. **The registry tree** maps JSON key-value pairs to concrete Java classes. It supports both single-level dispatch (`"name": "TableScanNode"`) and two-level dispatch (`"name": "Type"` + `"type": "BIGINT"`).

2. **`PolymorphicSerializer`** injects dispatch keys before regular fields during serialization. It handles both empty beans (dispatch keys only) and non-empty beans (dispatch keys + fields).

3. **`PolymorphicDeserializer`** walks the registry tree using JSON field values to find the concrete class, then delegates to Jackson's `@JsonCreator` constructor. Dispatch keys are configured as ignorable properties on concrete deserializers.

4. **Java and C++ must stay in sync** — both sides register the same names and produce/consume the same JSON structure. The three-way round-trip test enforces this.

5. **The Jackson ObjectMapper is strict** — no auto-detection, fail on unknown properties, no duplicate keys. Every field requires explicit annotations.

6. **Two independent hierarchies** exist: `ISerializable` (for types, plans, expressions, connectors, configs) and `Variant` (for typed values).

7. **The convention is simple**: extend `ISerializable`, use `@JsonCreator` + `@JsonProperty` on constructor, `@JsonGetter` on getters, register in `ISerializableRegistry`.

---

## 4.14 Exercises

1. **Trace by hand**: Starting from `Serde.fromJson('{"name":"Type","type":"ROW","names":["a"],"cTypes":[{"name":"Type","type":"BIGINT"}]}', ISerializable.class)`, walk through the deserialization step by step: which registry is consulted first? Which key is read? What class is resolved?

2. **Find the KvPairs**: For `FilterNode.class`, what would `SerdeRegistry.findKvPairs(FilterNode.class)` return? (Hint: look at how it's registered in `ISerializableRegistry`.)

3. **Compare Java and C++**: Open `Config.java` and `Config.cc`. Verify that the Java `@JsonGetter` key names match the C++ `serialize()` field names. Do the same for `Query.java` and `Query.cc`.

4. **Understand the Unsafe hack**: Read `BeanDeserializerUnsafe.java`. Why is this class in the `com.fasterxml.jackson.databind.deser` package? What would happen if it were in `org.boostscale.velox4j`?

5. **Variant vs ISerializable**: Open `VariantRegistry.java` and `ISerializableRegistry.java`. Both register types like `"BIGINT"`, but to different classes (`BigIntValue.class` vs `BigIntType.class`). Why don't they conflict? (Hint: they use separate `SerdeRegistryFactory` instances.)

6. **Add a type mentally**: If Velox added a new type called `UUID`, what would you need to do on the Java side? List the files you'd create/modify and the exact registration call.

---

## Next Lesson

[Lesson 5: Query Plans & Expressions](lesson-05-plans-and-expressions.md) — Build query plans from plan nodes and typed expressions.
