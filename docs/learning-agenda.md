# Velox4J Learning Agenda

A structured curriculum to learn the Velox4J codebase from the ground up.

## Lessons

| # | Lesson | Difficulty | File |
|---|--------|-----------|------|
| 1 | [Background & Big Picture](lesson-01-background.md) | Easy | `lesson-01-background.md` |
| 2 | [Project Structure](lesson-02-project-structure.md) | Easy | `lesson-02-project-structure.md` |
| 3 | [Type System](lesson-03-type-system.md) | Easy | `lesson-03-type-system.md` |
| 4 | [Serialization Architecture](lesson-04-serialization.md) | Medium | `lesson-04-serialization.md` |
| 5 | [Query Plans & Expressions](lesson-05-plans-and-expressions.md) | Medium | `lesson-05-plans-and-expressions.md` |
| 6 | [JNI Bridge & Session Lifecycle](lesson-06-jni-bridge.md) | Medium-Hard | `lesson-06-jni-bridge.md` |
| 7 | [Query Execution Flow](lesson-07-query-execution.md) | Medium | `lesson-07-query-execution.md` |
| 8 | [Memory Management & Arrow Interop](lesson-08-memory-and-arrow.md) | Hard | `lesson-08-memory-and-arrow.md` |
| 9 | [External Streams & Iterators](lesson-09-external-streams.md) | Hard | `lesson-09-external-streams.md` |
| 10 | [Expression Evaluation](lesson-10-expression-evaluation.md) | Medium | `lesson-10-expression-evaluation.md` |
| 11 | [Build System & CI Workflows](lesson-11-build-and-ci.md) | Medium | `lesson-11-build-and-ci.md` |
| 12 | [Joins & Distributed Execution](lesson-12-joins-and-distribution.md) | Hard | `lesson-12-joins-and-distribution.md` |

## Suggested Learning Path

1. **Foundation (Lessons 1-3):** Build vocabulary and understand the project's purpose, layout, and type system.
2. **Core Design (Lesson 4):** Master the JSON serialization bridge — this is the central architectural pattern.
3. **Main Workflow (Lessons 5-7):** Learn to build plans, understand the JNI bridge, and trace a query end-to-end.
4. **Advanced Topics (Lessons 8-10):** Dive into memory management, Arrow interop, data streaming, and expression evaluation.
5. **Build & CI (Lesson 11):** Understand how the project is built, tested, and released.
6. **Deep Dive (Lesson 12):** Understand join execution internals, Velox's distributed shuffle architecture, shuffle primitives, and how MPP frameworks integrate with Velox4J for distributed joins.
