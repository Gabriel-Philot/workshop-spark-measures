# Data generator verdict

Date: 2026-06-23.

## Verdict

Build a schema-first, relationship-first generator for the Spark Measures workshop. Use Spark/Delta as the primary materialization path. Test `dbldatagen` first, because it already solves much of the hard part: repeatable multi-table Spark data, consistent keys, weighted values, and distributions. If it fails compatibility or adds too much friction with Spark 4.1.2, implement a minimal internal Spark-native generator with deterministic expressions.

Do not make Polars the primary generator for v1. Polars remains useful for benchmarks and small dimension tables, but it is not the best default for join-heavy Spark diagnostic labs because it is single-node by default and does not naturally model Spark write/read/file-layout behavior.

## Why this is the right direction

The workshop goal is not generic fake data. The goal is to create controlled Spark problems and then diagnose them with sparkMeasure.

That means the generator must control:

- valid joins between facts and dimensions;
- skewed keys;
- uneven file sizes;
- number of files;
- partition layout;
- row width;
- cardinality;
- physical Delta size;
- reproducible seeds;
- generated-run manifests.

The central artifact is the data contract, not the engine.

## Recommended architecture

```text
YAML lab contract
  -> relationship planner
  -> Spark/dbldatagen materializer
  -> Delta tables in lakehouse bronze
  -> generator manifest in observability
  -> Spark validation job
  -> measured Spark lab with sparkMeasure
```

Responsibilities:

| Layer | Recommendation |
| --- | --- |
| Contract | Custom YAML in this repo |
| Relationship model | PK/FK graph, distributions, validation rules |
| Primary generation engine | `dbldatagen` POC, then Spark-native fallback if needed |
| Writer | Spark Delta to MinIO/S3A |
| Realistic text providers | Mimesis/Faker for small dimensions only |
| Optional benchmark | Polars + delta-rs |
| Validation | Spark submit job reading generated Delta |
| Observability | manifest in `observability/generator-runs/<run_id>` |

## First dataset family

Use a retail model:

```text
vendors     vendor_id PK
products    product_id PK, vendor_id FK
customers   customer_id PK
sales       sale_id PK, vendor_id/product_id/customer_id FKs
```

This supports the labs we need:

- skewed joins by hot vendor/product/customer;
- fact-to-dimension joins;
- partitioning by date/vendor/region;
- small files through high partition count or low `maxRecordsPerFile`;
- uneven files through skewed partitions;
- wide rows through optional payload columns;
- aggregation/shuffle labs over sales.

## Scale policy

Use scale as a teaching tool, not as a vanity benchmark.

The largest local preset can target 250-500 GB if disk/time allows. Generation taking 1-5 minutes is acceptable if it happens while the workshop theory begins. However, each lab should default to the smallest dataset that makes the symptom visible.

Initial scale presets:

| Preset | Target use |
| --- | --- |
| `demo` | quick sanity, sub-GB/few GB |
| `xs` | local visible Spark UI behavior |
| `s` | main workshop labs, roughly 10-50 GB when needed |
| `m` | stronger local lab, roughly 50-150 GB |
| `large-local` | optional 250-500 GB |

The manifest must record actual physical bytes because compression, column cardinality, and string width will change real table size.

## First implementation recommendation

Implement in this order:

1. Add generator package and Docker image.
2. Add `retail_sales_skew.yaml` contract.
3. POC `dbldatagen` under Spark 4.1.2 with `vendors`, `products`, `customers`, and `sales`.
4. Write Delta to `s3a://lakehouse/bronze/retail/...`.
5. Validate FK integrity and skew with Spark.
6. Emit generator manifest to `s3a://observability/generator-runs/<run_id>/manifest.json`.
7. Add `make generate LAB=retail-sales-skew SCALE=xs`.
8. Build the first measured lab: skewed join with sparkMeasure stage/task comparison.

If step 3 fails, replace `dbldatagen` with internal Spark expressions but keep the same YAML contract and validation behavior.

## Final call

Use `dbldatagen` as the first engine to test, not as an architectural dependency we blindly trust. Own the contract and validation in this repo. That gives us speed of implementation now and keeps the project safe if the library does not behave well with local Spark 4.1.2.
