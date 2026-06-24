# Data generator design tradeoff

Date: 2026-06-24.

## Decision

Use a Spark-native generator as the default path for the workshop data generator.

Do not introduce Polars, Faker, Mimesis, ShadowTraffic-style tooling, or `dbldatagen` as runtime dependencies for the first lab track.

## Why

The workshop objective is not realistic fake business data. The objective is to create controlled Spark problems and diagnose them with sparkMeasure.

For this goal, the important data properties are technical, not semantic:

- join-key skew;
- shuffle volume;
- task imbalance;
- small files;
- uneven file sizes;
- partition layout;
- high cardinality;
- row width;
- cache/no-cache behavior;
- broadcast vs sort-merge join behavior.

Spark-native generation gives direct control over these properties using the same engine, Delta writer, S3A path, and file layout that the workshop workloads will read later.

## Tradeoff

External generators can create more realistic records and richer schemas, but they add runtime dependencies and another abstraction layer before we have a concrete need.

| Option | Benefit | Cost |
| --- | --- | --- |
| Spark-native | Direct Spark/Delta/S3A behavior, deterministic, easy to control skew/file layout | More custom code for schemas |
| `dbldatagen` | Spark-based synthetic data, distributions, relationships | Compatibility and dependency risk with local Spark 4.1.2 |
| Polars/delta-rs | Fast local generation and Delta writes | Single-node by default; less aligned with Spark diagnostics |
| Faker/Mimesis | Realistic text/dimensions | Row-wise generation is wrong for large fact tables |
| ShadowTraffic-like model | Strong relationship/event generation model | Different problem space; our target is Delta lakehouse Spark labs |

## Current approach

The current generator uses:

- a YAML contract for scale, seed, skew, paths, partitions, and file layout;
- Spark `range` for distributed row generation;
- deterministic hash-based expressions for pseudo-random keys and dates;
- explicit PK/FK-safe formulas for joinable tables;
- Delta writes to bronze;
- Spark validation after generation.

This creates tables that are simple semantically but useful diagnostically:

```text
vendors
products
customers
sales
```

The generator validates that fact-table foreign keys join to dimensions and that the configured hot-key skew exists.

## Future rule

Only add a new generator dependency if it solves a real limitation in a lab.

Examples that could justify revisiting this decision:

- scenarios need many related schemas and the custom Spark code becomes repetitive;
- richer dimension realism becomes important for teaching;
- Spark-native generation becomes too slow for a concrete target;
- `dbldatagen` proves compatible and materially reduces code without hiding Spark behavior.

Until then, keep the generator boring, deterministic, and Spark-focused.
