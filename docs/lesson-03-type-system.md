# Lesson 3: Type System

## Learning Goals

After this lesson you should be able to:

- List all Velox data types available in Velox4J and categorize them
- Construct scalar, complex, and parameterized types in Java code
- Understand how types are serialized to JSON and how that JSON matches Velox's C++ type representation
- Explain the inheritance hierarchy: `NativeBean` -> `ISerializable` -> `Type` -> concrete types
- Understand how the serde registry maps type names (e.g., `"BIGINT"`) to Java classes (e.g., `BigIntType.class`)

---

## 3.1 Why Types Matter

In Velox4J, types appear everywhere:

- **Schemas**: A `RowType` defines the columns and their types for a table scan, query output, or vector
- **Expressions**: Every `TypedExpr` has a return type
- **Connectors**: Column handles specify data types and hive types
- **Vectors**: Every `BaseVector` / `RowVector` has an associated type
- **Variants**: Variant values are typed

Getting the types right is essential because they are serialized to JSON and sent to C++. If the JSON doesn't match what Velox expects, deserialization fails.

---

## 3.2 The Type Hierarchy

```
NativeBean (interface)                         ← marker for polymorphic serde
  └── ISerializable (abstract class)           ← base for all JSON-serializable Velox objects
        └── Type (abstract class)              ← base for all Velox data types
              ├── BooleanType                  ← no fields (simple scalar)
              ├── TinyIntType                  ← no fields
              ├── SmallIntType                 ← no fields
              ├── IntegerType                  ← no fields
              ├── BigIntType                   ← no fields
              ├── HugeIntType                  ← no fields
              ├── RealType                     ← no fields
              ├── DoubleType                   ← no fields
              ├── VarCharType                  ← no fields
              ├── VarbinaryType                ← no fields
              ├── TimestampType                ← no fields
              ├── DateType                     ← no fields
              ├── IntervalDayTimeType          ← no fields
              ├── IntervalYearMonthType        ← no fields
              ├── UnknownType                  ← no fields
              ├── DecimalType                  ← has precision + scale
              ├── OpaqueType                   ← has opaque string
              ├── ArrayType                    ← has 1 child type
              ├── MapType                      ← has 2 child types (key, value)
              ├── RowType                      ← has named children (struct)
              └── FunctionType                 ← has argument types + return type
```

**Source:** `src/main/java/org/boostscale/velox4j/type/Type.java` (line 18)

The base class is strikingly simple:

```java
public abstract class Type extends ISerializable {}
```

All the real work happens in the concrete subclasses and the serde framework.

---

## 3.3 Type Categories

### Category 1: Simple Scalar Types (no fields)

These types have no parameters — they are singletons by nature. Their Java classes are empty:

```java
// src/main/java/org/boostscale/velox4j/type/BigIntType.java
public class BigIntType extends Type {
  public BigIntType() {}
}
```

| Type class | Velox name | SQL equivalent | Java equivalent |
|-----------|------------|---------------|-----------------|
| `BooleanType` | `BOOLEAN` | `BOOLEAN` | `boolean` |
| `TinyIntType` | `TINYINT` | `TINYINT` | `byte` |
| `SmallIntType` | `SMALLINT` | `SMALLINT` | `short` |
| `IntegerType` | `INTEGER` | `INT` | `int` |
| `BigIntType` | `BIGINT` | `BIGINT` | `long` |
| `HugeIntType` | `HUGEINT` | — | 128-bit integer |
| `RealType` | `REAL` | `REAL` / `FLOAT` | `float` |
| `DoubleType` | `DOUBLE` | `DOUBLE` | `double` |
| `VarCharType` | `VARCHAR` | `VARCHAR` | `String` |
| `VarbinaryType` | `VARBINARY` | `VARBINARY` | `byte[]` |
| `TimestampType` | `TIMESTAMP` | `TIMESTAMP` | `Timestamp` |
| `DateType` | `DATE` | `DATE` | `LocalDate` |
| `IntervalDayTimeType` | `INTERVAL DAY TO SECOND` | `INTERVAL DAY TO SECOND` | — |
| `IntervalYearMonthType` | `INTERVAL YEAR TO MONTH` | `INTERVAL YEAR TO MONTH` | — |
| `UnknownType` | `UNKNOWN` | — | — |

Usage:

```java
Type bigint = new BigIntType();
Type varchar = new VarCharType();
Type bool = new BooleanType();
```

### Category 2: Parameterized Scalar Types

These types carry additional metadata.

#### DecimalType

Has `precision` (total digits) and `scale` (digits after decimal point):

```java
// DECIMAL(10, 5) — 10 total digits, 5 after the decimal point
Type decimal = new DecimalType(10, 5);
```

**Source:** `src/main/java/org/boostscale/velox4j/type/DecimalType.java`

#### OpaqueType

An opaque type identified by a string name. Used internally by Velox for implementation-specific types:

```java
Type opaque = new OpaqueType("some_opaque_type_id");
```

Note: OpaqueType cannot fully round-trip through C++ serde (the test expects an exception).

**Source:** `src/main/java/org/boostscale/velox4j/type/OpaqueType.java`

### Category 3: Complex Types (have child types)

Complex types contain other types. They all store children in a `List<Type>` under the JSON key `"cTypes"` — this matches Velox's C++ JSON format.

#### ArrayType

An array of elements of a single type:

```java
// ARRAY<INTEGER>
Type intArray = ArrayType.create(new IntegerType());

// ARRAY<VARCHAR>
Type stringArray = ArrayType.create(new VarCharType());

// ARRAY<ARRAY<BIGINT>> (nested)
Type nestedArray = ArrayType.create(ArrayType.create(new BigIntType()));
```

Internally stores exactly 1 child in `cTypes`:

```java
// src/main/java/org/boostscale/velox4j/type/ArrayType.java
private ArrayType(@JsonProperty("cTypes") List<Type> children) {
    Preconditions.checkArgument(children.size() == 1, ...);
    this.children = children;
}
```

**Source:** `src/main/java/org/boostscale/velox4j/type/ArrayType.java`

#### MapType

A map from key type to value type:

```java
// MAP<VARCHAR, BIGINT>
Type map = MapType.create(new VarCharType(), new BigIntType());

// MAP<INTEGER, ARRAY<VARCHAR>>
Type complexMap = MapType.create(new IntegerType(), ArrayType.create(new VarCharType()));
```

Internally stores exactly 2 children in `cTypes` (key first, value second):

```java
private MapType(@JsonProperty("cTypes") List<Type> children) {
    Preconditions.checkArgument(children.size() == 2, ...);
    this.children = children;
}
```

**Source:** `src/main/java/org/boostscale/velox4j/type/MapType.java`

#### RowType (the most important complex type)

A struct with **named** fields. This is the type used for table schemas, query output types, and row vectors:

```java
// ROW(id: BIGINT, name: VARCHAR, active: BOOLEAN)
RowType schema = new RowType(
    List.of("id", "name", "active"),
    List.of(new BigIntType(), new VarCharType(), new BooleanType())
);
```

Key properties:
- `names` — field names (JSON key: `"names"`)
- `children` — field types (JSON key: `"cTypes"`)
- Names and children must have the same size

Utility methods:
- `size()` — number of fields
- `findChild(name)` — look up a field type by name

```java
schema.size();                   // 3
schema.findChild("name");        // VarCharType
schema.getNames();               // ["id", "name", "active"]
schema.getChildren();            // [BigIntType, VarCharType, BooleanType]
```

**Source:** `src/main/java/org/boostscale/velox4j/type/RowType.java`

#### FunctionType

Represents a function signature (used with lambda expressions):

```java
// (INTEGER, VARCHAR) -> VARBINARY
Type funcType = FunctionType.create(
    List.of(new IntegerType(), new VarCharType()),  // argument types
    new VarbinaryType()                              // return type
);
```

Internally, argument types and return type are merged into a single `cTypes` list (return type is last):

```java
public static FunctionType create(List<Type> argumentTypes, Type returnType) {
    final List<Type> mergedTypes = new ArrayList<>(argumentTypes);
    mergedTypes.add(returnType);
    return new FunctionType(mergedTypes);
}
```

**Source:** `src/main/java/org/boostscale/velox4j/type/FunctionType.java`

---

## 3.4 How Types Are Serialized to JSON

The JSON format for types uses a **two-level dispatch** mechanism. Here's how it works.

### Simple scalar types

A `BigIntType` serializes to:

```json
{
  "name": "Type",
  "type": "BIGINT"
}
```

- `"name": "Type"` — tells the deserializer this is a `Type` (first-level dispatch)
- `"type": "BIGINT"` — tells the deserializer which specific type class (second-level dispatch)

Since `BigIntType` has no fields, the JSON only contains these two dispatch keys.

### Parameterized types

A `DecimalType(10, 5)` serializes to:

```json
{
  "name": "Type",
  "type": "DECIMAL",
  "precision": 10,
  "scale": 5
}
```

The dispatch keys come first, then the class-specific fields.

### Complex types

A `RowType` with two integer columns serializes to:

```json
{
  "name": "Type",
  "type": "ROW",
  "names": ["foo", "bar"],
  "cTypes": [
    {
      "name": "Type",
      "type": "INTEGER"
    },
    {
      "name": "Type",
      "type": "INTEGER"
    }
  ]
}
```

Note how `cTypes` contains recursively serialized child types — each with their own `"name"` and `"type"` keys.

### Nested complex types

An `ArrayType` containing a `MapType`:

```java
ArrayType.create(MapType.create(new VarCharType(), new BigIntType()))
```

Serializes to:

```json
{
  "name": "Type",
  "type": "ARRAY",
  "cTypes": [
    {
      "name": "Type",
      "type": "MAP",
      "cTypes": [
        { "name": "Type", "type": "VARCHAR" },
        { "name": "Type", "type": "BIGINT" }
      ]
    }
  ]
}
```

---

## 3.5 How Type Serde Registration Works

The polymorphic serde is powered by the `SerdeRegistry`. During `Velox4j.initialize()`, `ISerializableRegistry.registerAll()` is called, which registers all types:

```java
// src/main/java/org/boostscale/velox4j/serializable/ISerializableRegistry.java (line 83)
private static void registerTypes() {
    final SerdeRegistry typeRegistry = NAME_REGISTRY.registerFactory("Type").key("type");
    typeRegistry.registerClass("BOOLEAN", BooleanType.class);
    typeRegistry.registerClass("BIGINT", BigIntType.class);
    typeRegistry.registerClass("ROW", RowType.class);
    // ... etc.
}
```

This sets up a two-level lookup tree:

```
ISerializable (base class)
  └── key: "name"
        └── value: "Type"  →  key: "type"
              ├── "BOOLEAN"  →  BooleanType.class
              ├── "BIGINT"   →  BigIntType.class
              ├── "ROW"      →  RowType.class
              └── ...
```

**Serialization** (Java object -> JSON):
1. The `PolymorphicSerializer` sees a `NativeBean` subclass
2. It looks up the class in `SerdeRegistry.findKvPairs(clazz)` to get the dispatch keys
3. It writes the keys first (`"name": "Type"`, `"type": "BIGINT"`), then any class-specific fields

**Deserialization** (JSON -> Java object):
1. The `PolymorphicDeserializer` reads `"name"` from the JSON → finds `"Type"` → gets the second-level registry
2. Reads `"type"` from the JSON → finds `"BIGINT"` → gets `BigIntType.class`
3. Uses Jackson's standard `@JsonCreator` to construct the object

### Special type registrations

Most types use the standard `"name": "Type"` + `"type": "..."` pattern. But three types are registered differently:

```java
NAME_REGISTRY
    .registerFactory("IntervalDayTimeType")
    .key("type")
    .registerClass("INTERVAL DAY TO SECOND", IntervalDayTimeType.class);

NAME_REGISTRY
    .registerFactory("IntervalYearMonthType")
    .key("type")
    .registerClass("INTERVAL YEAR TO MONTH", IntervalYearMonthType.class);

NAME_REGISTRY
    .registerFactory("DateType")
    .key("type")
    .registerClass("DATE", DateType.class);
```

These use `"name": "IntervalDayTimeType"` (etc.) instead of `"name": "Type"`. This matches how Velox's C++ serde serializes these types — they have different `ISerializable` names in C++.

---

## 3.6 Types vs. Variants

A common point of confusion: **Types** describe the shape of data, while **Variants** hold actual values. Think of it as:

| Concept | Purpose | Example |
|---------|---------|---------|
| `Type` | Describes what kind of data | `BigIntType` = "this is a 64-bit integer column" |
| `Variant` | Holds an actual value | `BigIntValue(42)` = "the value 42" |

You'll encounter variants in `ConstantTypedExpr` (literal constants in expressions) and in `ValuesNode` (inline data). Variants are covered in detail in Lesson 10.

---

## 3.7 How Types Are Tested

Types are tested via **JSON round-trip tests** in `TypeSerdeTest`:

```java
// src/test/java/org/boostscale/velox4j/serde/TypeSerdeTest.java
@Test
public void testBigInt() {
    SerdeTests.testISerializableRoundTrip(new BigIntType());
}

@Test
public void testRowType() {
    SerdeTests.testISerializableRoundTrip(
        new RowType(
            ImmutableList.of("foo", "bar"),
            ImmutableList.of(new IntegerType(), new VarCharType())));
}
```

The `testISerializableRoundTrip` method performs three verifications:

1. **Java serialization round-trip**: Serialize with `java.io.Serializable`, deserialize, check JSON matches
2. **JSON round-trip**: Java object -> JSON string -> parse back to Java object -> JSON string; compare
3. **C++ round-trip**: Java object -> JSON -> send to C++ via JNI -> C++ deserializes -> C++ serializes back -> Java object; compare

This ensures that the Java type definition produces JSON that C++ can correctly understand.

**Source:** `src/test/java/org/boostscale/velox4j/serde/SerdeTests.java` (line 70)

---

## 3.8 Common Type Patterns in Practice

### Defining a table schema

```java
RowType nationSchema = new RowType(
    List.of("n_nationkey", "n_name", "n_regionkey", "n_comment"),
    List.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType())
);
```

### Nested complex types

```java
// A table with: id BIGINT, tags ARRAY<VARCHAR>, metadata MAP<VARCHAR, BIGINT>
RowType complexSchema = new RowType(
    List.of("id", "tags", "metadata"),
    List.of(
        new BigIntType(),
        ArrayType.create(new VarCharType()),
        MapType.create(new VarCharType(), new BigIntType())
    )
);
```

### Deeply nested types (from the test code)

```java
// ARRAY<MAP<VARCHAR, ROW(id: BIGINT, description: VARCHAR)>>
Type deepType = ArrayType.create(
    MapType.create(
        new VarCharType(),
        new RowType(
            ImmutableList.of("id", "description"),
            ImmutableList.of(new BigIntType(), new VarCharType())
        )
    )
);
```

**Source:** `src/test/java/org/boostscale/velox4j/serde/SerdeTests.java` (line 148)

---

## 3.9 Quick Reference: SQL to Velox4J Type Mapping

| SQL Type | Velox4J Class | Constructor |
|----------|--------------|-------------|
| `BOOLEAN` | `BooleanType` | `new BooleanType()` |
| `TINYINT` | `TinyIntType` | `new TinyIntType()` |
| `SMALLINT` | `SmallIntType` | `new SmallIntType()` |
| `INT` / `INTEGER` | `IntegerType` | `new IntegerType()` |
| `BIGINT` | `BigIntType` | `new BigIntType()` |
| `REAL` / `FLOAT` | `RealType` | `new RealType()` |
| `DOUBLE` | `DoubleType` | `new DoubleType()` |
| `VARCHAR` | `VarCharType` | `new VarCharType()` |
| `VARBINARY` | `VarbinaryType` | `new VarbinaryType()` |
| `TIMESTAMP` | `TimestampType` | `new TimestampType()` |
| `DATE` | `DateType` | `new DateType()` |
| `DECIMAL(p,s)` | `DecimalType` | `new DecimalType(p, s)` |
| `ARRAY<T>` | `ArrayType` | `ArrayType.create(elementType)` |
| `MAP<K,V>` | `MapType` | `MapType.create(keyType, valType)` |
| `ROW(...)` | `RowType` | `new RowType(names, types)` |

---

## 3.10 Key Takeaways

1. **`Type` is a thin abstract class** — all it does is extend `ISerializable`. The concrete subclasses define the actual types.
2. **Simple types have no fields** — `BigIntType`, `VarCharType`, etc. are basically marker classes. Their identity comes entirely from their serde registration name.
3. **Complex types use `cTypes`** — `ArrayType`, `MapType`, `RowType`, and `FunctionType` all store child types in a `List<Type>` under the JSON key `"cTypes"`.
4. **`RowType` is the most important type** — it defines schemas used throughout the API (table scans, query outputs, vectors, expressions).
5. **Two-level JSON dispatch** — types serialize with `"name": "Type"` + `"type": "BIGINT"` (etc.). The serde registry maps these pairs to concrete Java classes.
6. **Types are validated by three-way round-trip** — Java serialization, JSON round-trip, and C++ round-trip ensure correctness.

---

## 3.11 Exercises

1. **Build a schema**: Write Java code to create a `RowType` for a table with columns: `order_id BIGINT`, `customer_name VARCHAR`, `amount DECIMAL(18,2)`, `items ARRAY<ROW(product_id INTEGER, quantity INTEGER)>`.

2. **Predict the JSON**: Without running code, write out the JSON you'd expect from `Serde.toPrettyJson(MapType.create(new IntegerType(), new VarCharType()))`. Then verify by checking the serde test patterns.

3. **Trace the registration**: Open `ISerializableRegistry.java` (line 83-112). For each type, confirm that the registered Velox name (e.g., `"BIGINT"`) matches Velox's C++ naming convention.

4. **Find the oddball**: Three types (`DateType`, `IntervalDayTimeType`, `IntervalYearMonthType`) are registered differently from all others (lines 103-111 of `ISerializableRegistry.java`). Why do they use a different `"name"` value? Hint: look at what `registerFactory("DateType")` does vs. `registerFactory("Type")`.

5. **Read a test**: Open `TypeSerdeTest.java`. Why does `testOpaqueType()` expect a `VeloxException`?

---

## Next Lesson

[Lesson 4: Serialization Architecture](lesson-04-serialization.md) — Deep dive into the JSON serde framework that powers everything.
