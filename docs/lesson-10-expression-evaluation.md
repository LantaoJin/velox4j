# Lesson 10: Expression Evaluation

## Learning Goals

After this lesson you should be able to:

- Evaluate Velox expressions directly on in-memory data without building a full query plan
- Understand the difference between query execution and expression evaluation
- Use the Evaluation/Evaluator API to apply functions, field access, and computations
- Understand SelectivityVector and how it controls which rows are evaluated
- Know when to use expression evaluation vs full query plans

---

## 10.1 Query Plans vs Expression Evaluation

In Lessons 5-7, we learned to execute queries: build a plan tree, create a task, add splits, iterate results. This is the **full pipeline** — suited for reading data from connectors, joining tables, aggregating, etc.

But sometimes you just want to **apply a computation to data you already have in memory**:

- Compute `price * quantity` on a batch of rows
- Extract a field from a struct column
- Apply a function like `upper(name)` to a vector

Building a full query plan (ValuesNode → ProjectNode → execute → iterate) for this would be overkill. Expression evaluation provides a **lighter-weight alternative**.

| | Query Execution | Expression Evaluation |
|---|---|---|
| **Input** | Data from connectors (files, external streams) | `RowVector` already in memory |
| **Overhead** | Creates Task, Drivers, splits | Creates Evaluator only (no task/driver) |
| **Capabilities** | Full SQL: scan, filter, join, aggregate, sort, write | Single expression: compute one output column |
| **Output** | Stream of `RowVector` batches via UpIterator | Single `BaseVector` result |
| **Use case** | Running queries | Applying transformations to existing data |

---

## 10.2 The Components

```
Evaluation                    Evaluator                     Result
(what to compute)             (compiled expression)         (output vector)
     │                              │                            ▲
     │ Serde.toJson()               │ eval(sv, input)            │
     ▼                              ▼                            │
   JSON ──JNI──> C++ Evaluation ──compile──> ExprSet ──evaluate──> VectorPtr
```

### Evaluation (Java)

An `Evaluation` bundles:
- **`expr`** — a `TypedExpr` tree (the expression to evaluate)
- **`queryConfig`** — optional query-level configuration
- **`connectorConfig`** — optional connector configuration

```java
Evaluation evaluation = new Evaluation(
    expr,                      // the expression tree
    Config.empty(),            // query config
    ConnectorConfig.empty()    // connector config
);
```

Like `Query`, `Evaluation` extends `ISerializable` and is serialized to JSON for C++ consumption.

**Source:** `src/main/java/org/boostscale/velox4j/eval/Evaluation.java`

### Evaluator (Java)

An `Evaluator` is a **compiled, reusable expression** that you can call repeatedly on different inputs:

```java
Evaluator evaluator = session.evaluationOps().createEvaluator(evaluation);

// Call eval() as many times as you want with different inputs
BaseVector result1 = evaluator.eval(sv, inputBatch1);
BaseVector result2 = evaluator.eval(sv, inputBatch2);
BaseVector result3 = evaluator.eval(sv, inputBatch3);
```

The key advantage: the expression is compiled once (on the C++ side) and evaluated many times. This avoids re-parsing and re-compiling the expression for each batch.

**Source:** `src/main/java/org/boostscale/velox4j/eval/Evaluator.java`

### SelectivityVector

A `SelectivityVector` is a **bitmask that controls which rows are evaluated**. Think of it as a boolean array — `true` means "evaluate this row", `false` means "skip this row".

```java
// Create a selectivity vector that selects all rows
SelectivityVector sv = session.selectivityVectorOps().create(input.getSize());

// Check if row at index 3 is selected
boolean selected = sv.isValid(3);  // true (all rows selected by default)
```

#### How it works conceptually

Imagine you have 8 rows and want to evaluate only rows 0, 2, 5, and 7:

```
Row index:       0   1   2   3   4   5   6   7
Data:           10  20  30  40  50  60  70  80
SelectivityVector: ✓   ✗   ✓   ✗   ✗   ✓   ✗   ✓

With "multiply by 2" expression:
Result:         20   ?  60   ?   ?  120  ?  160
                 ↑       ↑           ↑       ↑
           (only selected rows are computed)
```

Skipped rows (marked `✗`) are **not evaluated** — Velox doesn't waste CPU on them. The result vector still has 8 entries, but the skipped positions contain undefined values.

This is a performance optimization. For example, if a filter already determined that only 30% of rows match a condition, you can pass that information as a SelectivityVector so the subsequent expression only computes results for the matching rows.

#### Current Java API limitation

The current Velox4J Java API only exposes:
- `create(length)` — creates a vector with **all bits set to true** (all rows selected)
- `isValid(idx)` — reads a single bit

There is **no Java API to set individual bits to false** (e.g., no `setValid(idx, false)` method). In the C++ Velox API, `SelectivityVector` has full read/write support (`setValid`, `clearAll`, etc.), but these methods are not yet exposed through the JNI bridge.

This means in the current version of Velox4J, `SelectivityVector` always selects all rows. The parameter exists in the `eval()` API because:
1. It mirrors the C++ API faithfully
2. Future versions of Velox4J may expose the full SelectivityVector API
3. It documents the intent: the caller should be able to control which rows are evaluated

#### Practical usage today

For now, always create an all-selected vector:

```java
SelectivityVector sv = session.selectivityVectorOps().create(input.getSize());
BaseVector result = evaluator.eval(sv, input);  // evaluates all rows
```

If you need to evaluate only a subset of rows, the workaround is to **slice** the input RowVector first:

```java
// Evaluate only rows 10-19 (10 rows starting at offset 10)
RowVector subset = input.slice(10, 10).asRowVector();
SelectivityVector sv = session.selectivityVectorOps().create(subset.getSize());
BaseVector result = evaluator.eval(sv, subset);
```

**Source:** `src/main/java/org/boostscale/velox4j/data/SelectivityVector.java`, `SelectivityVectorTest.java`

---

## 10.3 How It Works in C++

When you call `session.evaluationOps().createEvaluator(evaluation)`, C++ does the following:

```cpp
// Evaluator.cc — constructor
Evaluator::Evaluator(MemoryManager* memoryManager, const shared_ptr<const Evaluation>& evaluation) {
    // 1. Create a QueryCtx (for memory pools and config)
    queryCtx_ = core::QueryCtx::create(...);

    // 2. Create a SimpleExpressionEvaluator
    ee_ = std::make_unique<exec::SimpleExpressionEvaluator>(queryCtx_.get(), pool);

    // 3. Compile the expression into an ExprSet
    exprSet_ = ee_->compile(evaluation->expr());
}
```

And when you call `evaluator.eval(sv, input)`:

```cpp
// Evaluator.cc — eval()
VectorPtr Evaluator::eval(const SelectivityVector& rows, const RowVector& input) {
    VectorPtr vector{};
    ee_->evaluate(exprSet_.get(), rows, input, vector);
    return vector;
}
```

The `SimpleExpressionEvaluator` is Velox's lightweight expression evaluation engine. Unlike the full query execution engine (which creates Tasks and Drivers), it directly evaluates a compiled `ExprSet` against input data. No task scheduling, no split management, no operator pipeline — just pure expression evaluation.

**Source:** `src/main/cpp/main/velox4j/eval/Evaluator.cc`

---

## 10.4 Step-by-Step Usage

### Example 1: Field Access

Extract a single column from a RowVector:

```java
// Input: a RowVector with columns (c0: BIGINT, a1: BIGINT)
RowVector input = ...;

// Expression: access column "c0"
Evaluation evaluation = new Evaluation(
    FieldAccessTypedExpr.create(new BigIntType(), "c0"),
    Config.empty(),
    ConnectorConfig.empty()
);

// Create evaluator and evaluate
SelectivityVector sv = session.selectivityVectorOps().create(input.getSize());
Evaluator evaluator = session.evaluationOps().createEvaluator(evaluation);
BaseVector result = evaluator.eval(sv, input);

// result is a BIGINT vector containing the values from column "c0"
System.out.println(result.toString());
```

**Source:** `EvaluationTest.testFieldAccess()`

### Example 2: Function Call (Multiply)

Compute `c0 * a1` for each row:

```java
RowVector input = ...;  // columns: c0 BIGINT, a1 BIGINT

// Expression: multiply(c0, a1)
Evaluation evaluation = new Evaluation(
    new CallTypedExpr(
        new BigIntType(),
        List.of(
            FieldAccessTypedExpr.create(new BigIntType(), "c0"),
            FieldAccessTypedExpr.create(new BigIntType(), "a1")
        ),
        "multiply"
    ),
    Config.empty(),
    ConnectorConfig.empty()
);

SelectivityVector sv = session.selectivityVectorOps().create(input.getSize());
Evaluator evaluator = session.evaluationOps().createEvaluator(evaluation);
BaseVector result = evaluator.eval(sv, input);

// result is a BIGINT vector containing c0 * a1 for each row
```

**Source:** `EvaluationTest.testMultiply()`

### Example 3: Reusing an Evaluator

The evaluator is compiled once and can be called many times:

```java
Evaluator evaluator = session.evaluationOps().createEvaluator(evaluation);

// Evaluate 10 times on the same input (or different inputs)
for (int i = 0; i < 10; i++) {
    BaseVector result = evaluator.eval(sv, input);
    // result is the same each time (same input)
}
```

This is efficient because the expression compilation (parsing, type checking, code generation) happens only in the constructor. The `eval()` call just runs the pre-compiled expression.

**Source:** `EvaluationTest.testMultipleEvalCalls()`

---

## 10.5 Expression Types You Can Evaluate

Any `TypedExpr` from Lesson 5 can be used in an `Evaluation`:

| Expression | Example | Description |
|-----------|---------|-------------|
| `FieldAccessTypedExpr` | `FieldAccessTypedExpr.create(new BigIntType(), "col_a")` | Extract a column |
| `CallTypedExpr` | `new CallTypedExpr(returnType, inputs, "add")` | Call a function |
| `ConstantTypedExpr` | `ConstantTypedExpr.create(new BigIntType(), new BigIntValue(42))` | Constant value |
| `CastTypedExpr` | `CastTypedExpr.create(new VarCharType(), intExpr, false)` | Type cast |
| Nested expressions | `multiply(add(a, b), c)` | Compose arbitrarily |

The expression tree can be arbitrarily deep. For example, `CAST((a + b) * c AS VARCHAR)`:

```java
TypedExpr addExpr = new CallTypedExpr(new BigIntType(),
    List.of(
        FieldAccessTypedExpr.create(new BigIntType(), "a"),
        FieldAccessTypedExpr.create(new BigIntType(), "b")),
    "add");

TypedExpr mulExpr = new CallTypedExpr(new BigIntType(),
    List.of(addExpr, FieldAccessTypedExpr.create(new BigIntType(), "c")),
    "multiply");

TypedExpr castExpr = CastTypedExpr.create(new VarCharType(), mulExpr, false);

Evaluation evaluation = new Evaluation(castExpr, Config.empty(), ConnectorConfig.empty());
```

---

## 10.6 Where Does the Input RowVector Come From?

The `RowVector` input for `eval()` can come from various sources:

### From a query result

```java
SerialTask task = session.queryOps().execute(query);
// ... add splits ...
Iterator<RowVector> results = UpIterators.asJavaIterator(task);
while (results.hasNext()) {
    RowVector batch = results.next();
    BaseVector computed = evaluator.eval(sv, batch);  // evaluate on query output
}
```

### From Arrow import

```java
VectorSchemaRoot arrowData = ...;  // from Arrow IPC, Flink, etc.
RowVector veloxData = session.arrowOps().fromArrowVectorSchemaRoot(alloc, arrowData);
BaseVector computed = evaluator.eval(sv, veloxData);
```

### From Variant (inline data)

```java
BaseVector vector = session.variantOps().toVector(
    new RowType(List.of("a", "b"), List.of(new BigIntType(), new BigIntType())),
    new RowValue(List.of(new BigIntValue(10), new BigIntValue(20)))
);
RowVector input = vector.asRowVector();
BaseVector computed = evaluator.eval(sv, input);
```

---

## 10.7 Evaluation vs ProjectNode: A Comparison

Both can compute expressions. Here's when to use which:

### Use Evaluation when:

- You already have a `RowVector` in memory
- You need to compute a single expression
- You want to reuse the compiled expression across many batches
- You don't need the full query pipeline (no scanning, joining, aggregating)

```java
// Lightweight: just compute multiply(a, b)
Evaluator evaluator = session.evaluationOps().createEvaluator(evaluation);
BaseVector result = evaluator.eval(sv, inputVector);
```

### Use ProjectNode when:

- Data needs to be scanned from a connector
- You need multiple output columns
- The computation is part of a larger query plan (filter → project → aggregate)

```java
// Full pipeline: scan → project
ProjectNode project = new ProjectNode("proj-1", List.of(scanNode),
    List.of("result"), List.of(multiplyExpr));
Query query = new Query(project, Config.empty(), ConnectorConfig.empty());
SerialTask task = session.queryOps().execute(query);
// ... add splits, iterate ...
```

---

## 10.8 The Complete Flow

Here's the complete lifecycle of an expression evaluation:

```
1. Build expression tree (Java)
   TypedExpr expr = new CallTypedExpr(..., "multiply");

2. Wrap in Evaluation (Java)
   Evaluation evaluation = new Evaluation(expr, Config.empty(), ConnectorConfig.empty());

3. Create Evaluator (crosses JNI)
   Evaluator evaluator = session.evaluationOps().createEvaluator(evaluation);
   // Java: Serde.toJson(evaluation) → JSON
   // JNI: passes JSON string to C++
   // C++: deserializes → compiles ExprSet → stores in ObjectStore → returns handle

4. Prepare input (Java)
   RowVector input = ...;
   SelectivityVector sv = session.selectivityVectorOps().create(input.getSize());

5. Evaluate (crosses JNI)
   BaseVector result = evaluator.eval(sv, input);
   // Java: passes handles (evaluator.id, sv.id, input.id) to C++
   // C++: looks up objects → calls ee_->evaluate() → stores result → returns handle

6. Use result (Java)
   System.out.println(result.toString());
   // or convert to Arrow, or pass to another evaluator
```

---

## 10.9 Key Takeaways

1. **Expression evaluation is a lightweight alternative to full query execution**. Use it when you have data in memory and want to apply a computation without the overhead of tasks, drivers, and splits.

2. **Three components**: `Evaluation` (expression + config, serializable), `Evaluator` (compiled expression, reusable), `SelectivityVector` (bitmask for which rows to evaluate).

3. **Compile once, evaluate many times**. The `Evaluator` compiles the expression in its constructor. Calling `eval()` repeatedly is efficient because it reuses the compiled form.

4. **Any TypedExpr works**. Field access, function calls, casts, nested expressions — the same expression types from Lesson 5 are used here.

5. **No tasks or drivers involved**. The `Evaluator` uses Velox's `SimpleExpressionEvaluator` directly, bypassing the full execution engine.

6. **SelectivityVector defaults to all-selected**. Creating with `create(length)` selects all rows. For most use cases, this is all you need.

---

## 10.10 Exercises

1. **Build an expression**: Write Java code to create an `Evaluation` that computes `CAST(a + b AS VARCHAR)`, where `a` and `b` are `BIGINT` columns. Then write code to evaluate it on a RowVector.

2. **Reuse pattern**: Why is it more efficient to create one `Evaluator` and call `eval()` 1000 times, rather than creating 1000 `Evaluator` instances? What C++ work happens in the constructor vs in `eval()`?

3. **Read the test**: In `EvaluationTest.testMultiply()`, what are the input column names? What type is the output? What Velox function is called?

4. **Compare approaches**: You have 100 batches of RowVectors in memory and need to compute `price * quantity` for each. Write pseudocode for both approaches: (a) using `Evaluator.eval()` and (b) using `ValuesNode → ProjectNode → execute`. Which is simpler?

5. **Trace the JNI**: Starting from `evaluator.eval(sv, input)`, list every Java method and C++ function involved until `ee_->evaluate()` is called. How many times does the call cross the JNI boundary?

---

## Congratulations!

You've completed all 10 lessons of the Velox4J curriculum. Here's a summary of what you've learned:

| Lesson | Topic | Key Concept |
|--------|-------|-------------|
| 1 | Background | Velox = C++ execution engine; Velox4J = Java bindings via JSON serde |
| 2 | Project Structure | ~20 Java packages + thin C++ JNI layer; mirrored structure |
| 3 | Type System | Types describe data shape; RowType is the schema type |
| 4 | Serialization | Registry tree + polymorphic serde = the core design pattern |
| 5 | Plans & Expressions | Plan nodes form execution tree; TypedExprs form computation tree |
| 6 | JNI Bridge | Four-layer JNI; ObjectStore handles; Session = resource scope |
| 7 | Query Execution | Query → SerialTask → addSplit → UpIterator → RowVector batches |
| 8 | Memory & Arrow | AllocationListener bridges native memory; Arrow C Data Interface for interop |
| 9 | External Streams | DownIterator and BlockingQueue feed Java data into Velox plans |
| 10 | Expression Evaluation | Lightweight alternative: compile once, evaluate many times |

For further exploration, dive into the test files — they are the best source of working examples.
