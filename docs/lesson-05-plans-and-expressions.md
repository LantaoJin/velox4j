# Lesson 5: Query Plans & Expressions

## Learning Goals

After this lesson you should be able to:

- Build query plan trees from plan nodes to represent SQL queries
- Use typed expressions to define projections, filters, and aggregations
- Understand how plan nodes and expressions form a tree-of-trees structure
- Map common SQL constructs to their Velox4J plan + expression equivalents
- Read the plan node serde tests as examples

---

## 5.1 The Two Trees

A Velox query is defined by two interleaved tree structures:

1. **Plan tree** — `PlanNode` objects linked via `sources`. Defines the *execution pipeline* (scan, filter, project, join, aggregate, etc.)
2. **Expression trees** — `TypedExpr` objects linked via `inputs`. Defines *computations* within plan nodes (column references, function calls, casts, etc.)

```
Plan tree (PlanNode)                 Expression tree (TypedExpr)
─────────────────────                ───────────────────────────
AggregationNode                      CallTypedExpr("sum")
  └── FilterNode                       └── FieldAccessTypedExpr("amount")
        └── TableScanNode
                                     CallTypedExpr("gt")
                                       ├── FieldAccessTypedExpr("age")
                                       └── ConstantTypedExpr(18)
```

Plan nodes reference expressions. For example, a `FilterNode` contains a `TypedExpr` that defines its filter condition. A `ProjectNode` contains a list of `TypedExpr` that define its output columns.

---

## 5.2 Plan Node Hierarchy

```
ISerializable
  └── PlanNode (abstract)
        ├── id: String              ← unique node identifier
        └── sources: List<PlanNode> ← child nodes (0, 1, or 2)

Concrete plan nodes:
├── TableScanNode     ← leaf: reads from a connector (0 sources)
├── ValuesNode        ← leaf: inline constant data (0 sources)
├── FilterNode        ← unary: WHERE clause (1 source)
├── ProjectNode       ← unary: SELECT expressions (1 source)
├── AggregationNode   ← unary: GROUP BY + aggregates (1 source)
├── OrderByNode       ← unary: ORDER BY (1 source)
├── LimitNode         ← unary: LIMIT/OFFSET (1 source)
├── WindowNode        ← unary: window functions (1 source)
├── TableWriteNode    ← unary: writes to a connector (1 source)
└── HashJoinNode      ← binary: hash join (2 sources: left, right)
      └── extends AbstractJoinNode
```

Every node has a string `id` that must be unique within the plan. This ID is used to associate splits with scan nodes during execution.

**Source:** `src/main/java/org/boostscale/velox4j/plan/PlanNode.java`

---

## 5.3 Leaf Nodes: Data Sources

### TableScanNode

Reads data from a connector. This is the most common leaf node.

```java
RowType schema = new RowType(
    List.of("id", "name", "age"),
    List.of(new BigIntType(), new VarCharType(), new IntegerType()));

TableScanNode scan = new TableScanNode(
    "scan-1",          // node id
    schema,            // output type (schema)
    tableHandle,       // ConnectorTableHandle (e.g., HiveTableHandle)
    assignments        // List<Assignment>: maps column names to connector column handles
);
```

Key properties:
- `outputType` — the schema of the data this node produces
- `tableHandle` — identifies the table (e.g., `HiveTableHandle`)
- `assignments` — maps each output column to a `ColumnHandle` in the connector
- `getSources()` returns empty list (leaf node)

**Source:** `src/main/java/org/boostscale/velox4j/plan/TableScanNode.java`

### ValuesNode

Provides inline constant data — like SQL's `VALUES` clause.

```java
ValuesNode values = ValuesNode.create(
    "values-1",           // node id
    List.of(rowVector),   // list of RowVectors containing the data
    true,                 // parallelizable
    1                     // repeatTimes
);
```

The row vectors are serialized to a base64-encoded binary string. This is different from other nodes which serialize to structured JSON.

**Source:** `src/main/java/org/boostscale/velox4j/plan/ValuesNode.java`

---

## 5.4 Unary Nodes: Transform Operations

These nodes take exactly one source node.

### FilterNode — `WHERE`

Filters rows based on a boolean expression.

```java
// SQL: SELECT * FROM scan WHERE age > 18
TypedExpr filterExpr = new CallTypedExpr(
    new BooleanType(),
    List.of(
        FieldAccessTypedExpr.create(new IntegerType(), "age"),
        ConstantTypedExpr.create(new IntegerType(), new IntegerValue(18))
    ),
    "gt"   // greater-than function
);

FilterNode filter = new FilterNode("filter-1", List.of(scan), filterExpr);
```

The `filter` field is a `TypedExpr` that must return `BOOLEAN`.

**Source:** `src/main/java/org/boostscale/velox4j/plan/FilterNode.java`

### ProjectNode — `SELECT` expressions

Computes output columns using expressions.

```java
// SQL: SELECT id, name, age * 2 AS double_age FROM scan
ProjectNode project = new ProjectNode(
    "project-1",
    List.of(scan),
    List.of("id", "name", "double_age"),    // output column names
    List.of(
        FieldAccessTypedExpr.create(new BigIntType(), "id"),
        FieldAccessTypedExpr.create(new VarCharType(), "name"),
        new CallTypedExpr(
            new IntegerType(),
            List.of(
                FieldAccessTypedExpr.create(new IntegerType(), "age"),
                ConstantTypedExpr.create(new IntegerType(), new IntegerValue(2))
            ),
            "multiply"
        )
    )
);
```

Key: `names` and `projections` must have the same size — each name corresponds to one expression.

**Source:** `src/main/java/org/boostscale/velox4j/plan/ProjectNode.java`

### AggregationNode — `GROUP BY`

Groups rows and computes aggregates.

```java
// SQL: SELECT department, SUM(salary) FROM scan GROUP BY department
Aggregate sumAggregate = new Aggregate(
    new CallTypedExpr(
        new BigIntType(),
        List.of(FieldAccessTypedExpr.create(new BigIntType(), "salary")),
        "sum"
    ),
    List.of(new BigIntType()),    // rawInputTypes
    null,                          // mask (no filter)
    List.of(),                     // sortingKeys
    List.of(),                     // sortingOrders
    false                          // distinct
);

AggregationNode agg = new AggregationNode(
    "agg-1",
    AggregateStep.SINGLE,                                                  // step
    List.of(FieldAccessTypedExpr.create(new VarCharType(), "department")), // groupingKeys
    List.of(),                                                              // preGroupedKeys
    List.of("sum_salary"),                                                  // aggregateNames
    List.of(sumAggregate),                                                  // aggregates
    false,                                                                  // ignoreNullKeys
    false,                                                                  // noGroupsSpanBatches
    List.of(scan),                                                          // sources
    null,                                                                   // groupId
    List.of()                                                               // globalGroupingSets
);
```

`AggregateStep` controls distributed aggregation:
- `SINGLE` — complete aggregation in one step
- `PARTIAL` — first stage of two-stage aggregation (produces intermediate results)
- `FINAL` — second stage (consumes intermediate results from PARTIAL)
- `INTERMEDIATE` — re-aggregates intermediate results

**Source:** `src/main/java/org/boostscale/velox4j/plan/AggregationNode.java`, `src/main/java/org/boostscale/velox4j/aggregate/Aggregate.java`

### OrderByNode — `ORDER BY`

Sorts rows by one or more keys.

```java
// SQL: SELECT * FROM scan ORDER BY age ASC NULLS FIRST
OrderByNode orderBy = new OrderByNode(
    "order-1",
    List.of(scan),
    List.of(FieldAccessTypedExpr.create(new IntegerType(), "age")),
    List.of(new SortOrder(true, true)),   // (ascending, nullsFirst)
    false                                  // partial
);
```

`SortOrder` has two booleans: `ascending` and `nullsFirst`.

**Source:** `src/main/java/org/boostscale/velox4j/plan/OrderByNode.java`

### LimitNode — `LIMIT` / `OFFSET`

Limits the number of output rows.

```java
// SQL: SELECT * FROM scan LIMIT 10 OFFSET 5
LimitNode limit = new LimitNode(
    "limit-1",
    List.of(scan),
    5,      // offset
    10,     // count
    false   // partial
);
```

**Source:** `src/main/java/org/boostscale/velox4j/plan/LimitNode.java`

### WindowNode — Window Functions

Computes window functions over partitions of data.

```java
// SQL: SELECT *, SUM(foo) OVER (PARTITION BY bar ORDER BY foo ROWS UNBOUNDED PRECEDING TO CURRENT ROW) FROM scan
CallTypedExpr windowCall = new CallTypedExpr(
    new IntegerType(),
    List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
    "sum"
);

WindowFrame frame = new WindowFrame(
    WindowType.ROWS,
    BoundType.UNBOUNDED_PRECEDING, null,  // start bound + optional offset
    BoundType.CURRENT_ROW, null           // end bound + optional offset
);

WindowNode window = new WindowNode(
    "window-1",
    List.of(FieldAccessTypedExpr.create(new IntegerType(), "bar")),  // partitionKeys
    List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),  // sortingKeys
    List.of(new SortOrder(true, false)),                              // sortingOrders
    List.of("sum_over"),                                              // window column names
    List.of(new WindowFunction(windowCall, frame, true)),             // window functions
    false,                                                            // inputsSorted
    List.of(scan)                                                     // sources
);
```

**Source:** `src/main/java/org/boostscale/velox4j/plan/WindowNode.java`

### TableWriteNode — Writing Data

Writes data to a connector (e.g., writes Parquet files via Hive connector).

```java
TableWriteNode write = new TableWriteNode(
    "write-1",
    schema,                       // columns (RowType)
    schema.getNames(),            // columnNames
    columnStatsSpec,              // optional column statistics spec
    "connector-hive",             // connectorId
    hiveInsertTableHandle,        // ConnectorInsertTableHandle
    false,                        // hasPartitioningScheme
    outputType,                   // outputType (must match Velox's expected write output schema)
    CommitStrategy.TASK_COMMIT,   // commitStrategy
    List.of(scan)                 // sources
);
```

**Source:** `src/main/java/org/boostscale/velox4j/plan/TableWriteNode.java`

---

## 5.5 Binary Node: HashJoinNode

Takes two sources (left and right) and joins them.

```java
// SQL: SELECT * FROM left_scan INNER JOIN right_scan ON left_scan.id = right_scan.id
RowType leftSchema = new RowType(
    List.of("id", "name"), List.of(new IntegerType(), new VarCharType()));
RowType rightSchema = new RowType(
    List.of("id", "score"), List.of(new IntegerType(), new IntegerType()));
RowType outputSchema = new RowType(
    List.of("id", "name", "id", "score"),
    List.of(new IntegerType(), new VarCharType(), new IntegerType(), new IntegerType()));

HashJoinNode join = new HashJoinNode(
    "join-1",
    JoinType.INNER,
    List.of(FieldAccessTypedExpr.create(new IntegerType(), "id")),   // leftKeys
    List.of(FieldAccessTypedExpr.create(new IntegerType(), "id")),   // rightKeys
    null,                   // filter (optional post-join filter)
    leftScan,               // left source
    rightScan,              // right source
    outputSchema,           // output type
    false,                  // nullAware
    false                   // useHashTableCache
);
```

`JoinType` enum values:
- `INNER`, `LEFT`, `RIGHT`, `FULL`
- `LEFT_SEMI_FILTER`, `RIGHT_SEMI_FILTER` — semi-join with filter
- `LEFT_SEMI_PROJECT`, `RIGHT_SEMI_PROJECT` — semi-join with projection
- `ANTI` — anti-join

**Source:** `src/main/java/org/boostscale/velox4j/plan/HashJoinNode.java`, `src/main/java/org/boostscale/velox4j/join/JoinType.java`

---

## 5.6 Expression Hierarchy

```
ISerializable
  └── TypedExpr (abstract)
        ├── returnType: Type          ← the type this expression evaluates to
        └── inputs: List<TypedExpr>   ← child expressions (forms expression tree)

Concrete expressions:
├── FieldAccessTypedExpr   ← column reference ("col_a")
├── CallTypedExpr          ← function call (add, gt, sum, ...)
├── ConstantTypedExpr      ← literal constant (42, "hello", true)
├── CastTypedExpr          ← type cast (CAST / TRY_CAST)
├── ConcatTypedExpr        ← row construction (builds a ROW from fields)
├── DereferenceTypedExpr   ← struct field access by index
├── InputTypedExpr         ← reference to entire input row
└── LambdaTypedExpr        ← lambda expression
```

Every expression has:
- `returnType` — the `Type` it evaluates to (serialized as `"type"` in JSON)
- `inputs` — child expressions (serialized as `"inputs"` in JSON)

---

## 5.7 Expression Deep Dive

### FieldAccessTypedExpr — Column Reference

The most common expression. References a column by name.

```java
// Simple: reference column "age" which is an INTEGER
FieldAccessTypedExpr.create(new IntegerType(), "age")

// With input: access field "name" from a RowType input expression
FieldAccessTypedExpr.create(inputExpr, "name")
```

When used without an input (0 inputs), it references a column from the plan node's input. When used with 1 input, it accesses a named field from that input's `RowType`.

**Source:** `src/main/java/org/boostscale/velox4j/expression/FieldAccessTypedExpr.java`

### CallTypedExpr — Function Call

Calls a Velox function. The function is identified by name.

```java
// add(a, b) → INTEGER
new CallTypedExpr(
    new IntegerType(),           // return type
    List.of(exprA, exprB),       // input expressions
    "add"                        // function name
)

// gt(age, 18) → BOOLEAN
new CallTypedExpr(
    new BooleanType(),
    List.of(
        FieldAccessTypedExpr.create(new IntegerType(), "age"),
        ConstantTypedExpr.create(new IntegerType(), new IntegerValue(18))
    ),
    "gt"
)
```

Common Velox function names: `add`, `subtract`, `multiply`, `divide`, `gt`, `lt`, `eq`, `neq`, `and`, `or`, `not`, `sum`, `count`, `min`, `max`, `avg`, `upper`, `lower`, `length`, `substr`, etc.

**Source:** `src/main/java/org/boostscale/velox4j/expression/CallTypedExpr.java`

### ConstantTypedExpr — Literal Value

Holds a constant value using either a `Variant` or a serialized vector.

```java
// From a Variant (common case)
ConstantTypedExpr.create(new IntegerType(), new IntegerValue(42))
ConstantTypedExpr.create(new BooleanType(), new BooleanValue(true))
ConstantTypedExpr.create(new VarCharType(), new VarCharValue("hello"))

// From a BaseVector (for complex constants)
ConstantTypedExpr.create(someVector)
```

**Source:** `src/main/java/org/boostscale/velox4j/expression/ConstantTypedExpr.java`

### CastTypedExpr — Type Cast

Converts an expression to a different type.

```java
// CAST(age AS BIGINT)
CastTypedExpr.create(new BigIntType(), ageExpr, false)

// TRY_CAST(value AS INTEGER) — returns null instead of throwing on failure
CastTypedExpr.create(new IntegerType(), valueExpr, true)
```

**Source:** `src/main/java/org/boostscale/velox4j/expression/CastTypedExpr.java`

### ConcatTypedExpr — Row Construction

Builds a `RowType` value from individual field expressions.

```java
// Construct ROW(a, b) from two expressions
ConcatTypedExpr.create(
    List.of("a", "b"),
    List.of(exprA, exprB)
)
```

**Source:** `src/main/java/org/boostscale/velox4j/expression/ConcatTypedExpr.java`

### DereferenceTypedExpr — Struct Field Access by Index

Accesses a field from a `RowType` by its positional index (0-based).

```java
// Access field at index 0 from a struct expression
DereferenceTypedExpr.create(structExpr, 0)
```

**Source:** `src/main/java/org/boostscale/velox4j/expression/DereferenceTypedExpr.java`

### LambdaTypedExpr — Lambda Expression

Used with higher-order functions like `transform`, `filter`, `reduce`.

```java
// Lambda: (x: INTEGER) -> x * 2
RowType signature = new RowType(List.of("x"), List.of(new IntegerType()));
TypedExpr body = new CallTypedExpr(
    new IntegerType(),
    List.of(
        FieldAccessTypedExpr.create(new IntegerType(), "x"),
        ConstantTypedExpr.create(new IntegerType(), new IntegerValue(2))
    ),
    "multiply"
);
LambdaTypedExpr lambda = LambdaTypedExpr.create(signature, body);
```

The `returnType` of a `LambdaTypedExpr` is automatically set to a `FunctionType`.

**Source:** `src/main/java/org/boostscale/velox4j/expression/LambdaTypedExpr.java`

### InputTypedExpr — Entire Input Row

References the entire input row as a single expression. Rarely used directly.

```java
new InputTypedExpr(rowType)
```

**Source:** `src/main/java/org/boostscale/velox4j/expression/InputTypedExpr.java`

---

## 5.8 Composing Plans: SQL to Plan Tree

Here are common SQL patterns and their plan tree equivalents.

### Simple scan with filter and projection

```sql
SELECT id, name FROM users WHERE age > 18
```

```
ProjectNode("proj-1", names=["id","name"], projections=[id_expr, name_expr])
  └── FilterNode("filter-1", filter=gt(age, 18))
        └── TableScanNode("scan-1", outputType=ROW(id,name,age))
```

### Aggregation

```sql
SELECT department, COUNT(*), SUM(salary) FROM employees GROUP BY department
```

```
AggregationNode("agg-1", step=SINGLE, groupingKeys=[department], aggregates=[count, sum])
  └── TableScanNode("scan-1", outputType=ROW(department, salary))
```

### Join with filter

```sql
SELECT o.id, c.name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.total > 100
```

```
ProjectNode("proj-1", names=["id","name"], projections=[...])
  └── FilterNode("filter-1", filter=gt(total, 100))
        └── HashJoinNode("join-1", INNER, leftKeys=[customer_id], rightKeys=[id])
              ├── TableScanNode("scan-orders", outputType=ROW(id, customer_id, total))
              └── TableScanNode("scan-customers", outputType=ROW(id, name))
```

### Order + Limit

```sql
SELECT * FROM products ORDER BY price DESC LIMIT 10
```

```
LimitNode("limit-1", offset=0, count=10)
  └── OrderByNode("order-1", sortingKeys=[price], sortingOrders=[DESC NULLS LAST])
        └── TableScanNode("scan-1", outputType=ROW(name, price))
```

---

## 5.9 Key Takeaways

1. **Plan nodes form a tree** via `sources`. Leaf nodes (`TableScanNode`, `ValuesNode`) have no sources. Unary nodes have 1. `HashJoinNode` has 2.

2. **Expressions form nested trees** via `inputs`. `FieldAccessTypedExpr` and `ConstantTypedExpr` are leaves. `CallTypedExpr` has inputs.

3. **Plans reference expressions**. `FilterNode` has a filter expression. `ProjectNode` has a list of projection expressions. `AggregationNode` has aggregates with `CallTypedExpr` calls.

4. **Every expression has a return type**. The type system from Lesson 3 is used throughout — every `TypedExpr` declares what type it evaluates to.

5. **Functions are identified by name string**. `CallTypedExpr` uses the function name as registered in Velox (e.g., `"sum"`, `"gt"`, `"add"`).

6. **Node IDs must be unique**. They are used later to associate splits with scan nodes during query execution (covered in Lesson 7).

7. **Everything is JSON-serializable**. Plan trees and expression trees are all serialized to JSON and sent to C++ — the serialization architecture from Lesson 4 underpins all of this.

---

## 5.10 Exercises

1. **Build a plan tree**: Write the Java code for this SQL query as a plan tree:
   ```sql
   SELECT name, age * 2 AS double_age FROM people WHERE age >= 21 ORDER BY name LIMIT 5
   ```

2. **Identify the expressions**: In the `testFilterNode` test (`PlanNodeSerdeTest.java` line 140), what expression is used as the filter? Why is `ConstantTypedExpr.create(new BooleanType(), new BooleanValue(true))` used there?

3. **Read the join test**: Open `PlanNodeSerdeTest.java` line 152. Trace through the `HashJoinNode` construction. What is the output schema? Why does it have 4 columns?

4. **Expression composition**: Write a `TypedExpr` tree for this expression: `CAST(price * quantity AS BIGINT)`, where `price` is `DOUBLE` and `quantity` is `INTEGER`.

5. **Count plan node types**: How many of the 10 plan node types correspond to SQL clauses? Which ones don't have a direct SQL equivalent?

---

## Next Lesson

[Lesson 6: JNI Bridge & Session Lifecycle](lesson-06-jni-bridge.md) — How Java talks to C++ and how resources are managed.
