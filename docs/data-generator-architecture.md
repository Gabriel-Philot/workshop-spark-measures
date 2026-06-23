# Data generator architecture

Research date: 2026-06-23.

This document records the first architecture direction for the workshop data generator. The generator is part of the Spark Measures workshop platform, but it should not be confused with the measured workloads: generating datasets is a lab setup step; diagnosing Spark behavior with sparkMeasure is the lab objective.

## Context

The workshop needs repeatable Delta datasets in MinIO so labs can demonstrate common Spark pathologies and show how sparkMeasure helps diagnose them. The generator must support:

- configurable scale, ideally by target logical rows and target physical bytes;
- Delta output, not standalone Parquet;
- bronze lakehouse datasets under `s3a://lakehouse/bronze/...`;
- skewed distributions, for example one vendor producing most sales;
- controllable file layout, including intentionally bad layouts;
- repeatable seeds and named scenarios;
- Docker execution with persisted output through the existing MinIO state.

The current platform already has MinIO, Spark, Delta, sparkMeasure, `make`, and experiment configuration. The generator should fit that shape instead of becoming a separate untracked tool.

## Critical feasibility note

The target “1 TB in 1 minute” is not a normal local-platform requirement. Writing 1 TB of physical Delta files in 60 seconds requires roughly 16.7 GB/s sustained write throughput before Delta transaction overhead, object-store protocol overhead, compression, CPU cost, and container networking. On a laptop or normal single-node Docker Compose stack with MinIO, this is not realistic.

The practical interpretation should be:

- support a 1 TB preset for capable hardware or remote clusters;
- make smaller presets first: `xs`, `s`, `m`, `l`;
- report actual generated rows, files, Delta table size, and elapsed time;
- prefer “target_bytes” calibration instead of pretending that a fixed row count equals a fixed dataset size;
- allow logical-size demos where compression can make physical bytes much smaller, but label that honestly.

## Recommendation

Use a Spark-native generator as the first production path. Keep Polars + delta-rs as a benchmarked optional path for selected tables later.

The main reason is workshop coherence. The labs are about Spark diagnostics. Spark-native generation gives us the same Delta writer, S3A behavior, partitioning behavior, and file layout that Spark workloads will consume later. It also makes skew, repartitioning, `maxRecordsPerFile`, small-file creation, wide rows, and join-key imbalance easier to express using the same primitives the lab will discuss.

Polars is still useful, but as a secondary engine:

- good for small/medium dimension tables;
- good for single-node fast generation experiments;
- good for comparing non-Spark setup speed;
- risky as the only generator for very large facts because it is single-node unless we add our own sharding/orchestration.

## Proposed repository shape

```text
generator/
  README.md
  pyproject.toml
  src/workshop_generator/
    cli.py
    config.py
    scenarios/
      retail_sales.py
      small_files.py
      skewed_join.py
    writers/
      spark_delta.py
      polars_delta.py        # optional after benchmark
  configs/
    retail_sales.yaml
    skewed_join.yaml
build/images/generator/
  Dockerfile
docs/
  data-generator-architecture.md
research/
  data-generator-research.md
```

The generator should have its own image, but it should use the same Docker network and MinIO credentials conventions as the platform. Generated data must be persisted through MinIO, not committed into Git.

## Docker execution model

The generator image should run as a one-shot container:

```bash
make generate LAB=skewed-join SCALE=s
make generate LAB=small-files SCALE=xs
make generate LAB=retail-sales SCALE=m SEED=42
```

Expected behavior:

1. Read scenario config.
2. Connect to MinIO using S3/S3A endpoint.
3. Write Delta tables under `s3a://lakehouse/bronze/<scenario>/<table>`.
4. Write a manifest under `s3a://observability/generator-runs/<run_id>/manifest.json`.
5. Print a concise summary: rows, physical bytes, file count, partitions, seed, elapsed seconds.

The manifest matters because sparkMeasure will later explain workload symptoms. We need a durable record of the dataset shape that caused those symptoms.

## Data model direction

Start with a retail/ecommerce model because it naturally supports skew and joins:

- `vendors`: small dimension table;
- `customers`: medium dimension table;
- `products`: medium dimension table;
- `sales`: large fact table;
- optional `events`: clickstream-style wide/semi-structured table.

The first useful bronze targets:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/sales
```

Labs can then read bronze and write silver/gold outputs while collecting sparkMeasure metrics.

## Skew controls

Skew should be an explicit scenario feature, not an accidental artifact.

Useful controls:

- weighted vendor distribution, e.g. vendor `V001` receives 70% of sales;
- Zipf/power-law key distribution for join-key skew;
- date skew, e.g. one day receives most events;
- region/store skew, e.g. one country dominates traffic;
- file-size skew by combining skewed partition values with partitioned writes;
- small-file pressure via too many partitions or low `maxRecordsPerFile`;
- wide-row pressure via long text, arrays, nested structs, or many columns.

For skew labs, stage-level sparkMeasure metrics may not be enough. Stage aggregates show symptoms like high shuffle, spill, or elapsed time, but task-level metrics are better for showing outliers. The platform should keep stage metrics as the default and enable task metrics per experiment when the lab is specifically about skew.

## File layout controls

File layout is part of the lab surface. The generator should expose:

- target rows;
- target approximate bytes;
- target partitions;
- partition columns;
- `maxRecordsPerFile`;
- write mode: `overwrite`, `append`;
- target rows per vendor/date partition;
- optional deliberate anti-patterns: too many tiny files, one giant hot partition, unpartitioned wide table.

For Delta writes from Spark, `maxRecordsPerFile` is a practical control for file count and size. It should be exposed as config, not hardcoded.

## Scale presets

The first version should avoid TB-by-default behavior.

| Preset | Intended use | Example target |
| --- | --- | --- |
| `xs` | local smoke/demo | 1-5 GB or less |
| `s` | normal laptop workshop | 10-50 GB |
| `m` | stronger workstation | 100-250 GB |
| `l` | large local/server run | 500 GB-1 TB |

The exact row counts should be calibrated per schema because compression and string cardinality change physical size.

## Calibration strategy

Use a two-phase generation plan:

1. Write a small sample table with the selected schema and compression.
2. Measure actual bytes per row from the Delta data files.
3. Estimate the required rows for the target size.
4. Generate the final table.
5. Write actual metrics to the generator manifest.

This avoids hardcoding bad assumptions like “100 million rows equals X GB”.

## Tooling decision matrix

| Option | Fit | Strength | Risk |
| --- | --- | --- | --- |
| Spark-native custom generator | Primary recommendation | Distributed, same writer/reader behavior as labs, direct Delta/S3A, easy skew with Spark expressions | Spark startup overhead; generation logs must be separated from measured workload logs |
| Databricks Labs `dbldatagen` | Strong candidate for POC | Spark-native, distributions, weights, schemas, relationships, Faker plugin | Databricks-oriented; Spark 4.1.2 compatibility must be validated locally |
| Polars + delta-rs | Secondary/benchmark path | Fast Rust engine, direct Delta writes, streaming sink path | Single-node by default; `sink_delta` is marked unstable; Delta feature compatibility with Spark 4.1.2 must be validated |
| DuckDB Delta extension | Research candidate | SQL generation, efficient local engine, Delta read/write support | Newer Delta write path; extension download/compatibility needs validation; not aligned with Spark diagnostics |
| PyArrow + delta-rs directly | Low-level fallback | Can stream Arrow record batches to Delta | More code to maintain; less ergonomic for workshop users |
| Faker/Mimesis row-wise | Dimension-data helper only | Realistic names/text/locales | Row-wise Python is the wrong shape for huge fact tables |

## Why not Faker-first

Faker and Mimesis are useful for realistic small dimension tables. They should not generate the large fact table row by row.

For large facts, use vectorized/distributed expressions:

- numeric IDs derived from row number;
- random columns from seeded expressions;
- categorical columns from deterministic modulo, hash, or weighted buckets;
- realistic text from a small generated dictionary joined by ID.

This gives us data that is realistic enough for Spark pathologies without paying Python per-row overhead for billions of rows.

## First implementation slice

The first implementation should be small and measurable:

1. Add a generator package and Docker image.
2. Implement one scenario: `retail_sales`.
3. Generate `vendors` and `sales` only.
4. Write Delta to bronze.
5. Support `xs` and `s` presets.
6. Support `vendor_skew` and `maxRecordsPerFile`.
7. Write a generator manifest to observability.
8. Add a `make generate` target.
9. Add one Spark dry validation that reads the generated Delta and checks skew exists.

Do not implement every lab in the first pass. The generator must prove speed, Delta compatibility, and controllable skew first.

## Open questions

- Should “1 TB” mean physical Delta bytes in MinIO or logical uncompressed source size?
- What minimum workshop hardware should we assume?
- Should generator runs overwrite a stable path per lab or create versioned run paths?
- Should generation itself be done with Spark but deliberately excluded from sparkMeasure metrics, or should we optionally measure the generation step as a separate lab?
- Should `dbldatagen` be tested before we write our own Spark expression DSL?


## Workshop-scale adjustment

The workshop does not require every lab to start with 1 TB of physical data. The practical target is to generate enough data to make Spark behavior visible on a small local cluster without making the class wait too long.

Updated scale assumptions:

- generation may run while the theory section starts, so 1-5 minutes is acceptable for larger presets;
- 500 GB can be enough for the largest local lab if the symptoms are clear;
- many labs should use much less data if the anti-pattern is strong and controlled;
- the generator must work on a machine with limited RAM by using distributed/vectorized generation and streaming-style writes, not driver-side row materialization;
- elapsed time and physical Delta size are observed outputs, not guarantees.

The first implementation should optimize for clear Spark symptoms before maximum byte volume. A 20 GB dataset with severe file-size skew or join-key skew can teach more than a 500 GB dataset that is only large.

## Relational generation contract

The generator must be schema-first and relationship-first. Engine choice is secondary. The contract should describe the data graph, and the selected engine should materialize that graph.

A table contract should include:

- table name and lakehouse target;
- primary key columns;
- foreign key relationships;
- row or target-size preset;
- nullable columns and controlled bad data, when needed;
- distributions for key columns;
- partitioning and file-layout controls;
- validation rules.

Example contract shape:

```yaml
scenario: retail_sales_skew
seed: 42
layer: bronze
base_path: s3a://lakehouse/bronze/retail

tables:
  vendors:
    rows: 1_000
    primary_key: [vendor_id]
    columns:
      vendor_id:
        type: long
        generation: sequence
      vendor_name:
        type: string
        generation: provider
      region:
        type: string
        generation: weighted_values
        values: [US, BR, EU, APAC]
        weights: [55, 20, 15, 10]

  products:
    rows: 100_000
    primary_key: [product_id]
    foreign_keys:
      vendor_id:
        references: vendors.vendor_id
        distribution: uniform

  sales:
    rows: ${scale.sales_rows}
    primary_key: [sale_id]
    foreign_keys:
      vendor_id:
        references: vendors.vendor_id
        distribution:
          type: hot_key
          hot_key: 1
          hot_key_share: 0.70
      product_id:
        references: products.product_id
        distribution: follows_vendor
      customer_id:
        references: customers.customer_id
        distribution: zipf
    write:
      format: delta
      partition_by: [sale_date]
      max_records_per_file: ${scale.max_records_per_file}
```

The important guarantee is: if a lab says `sales.vendor_id` joins to `vendors.vendor_id`, the generator must validate that relationship before the lab starts.

## Join-ready data guarantees

The generator should enforce these invariants per run:

1. Dimension tables contain unique primary keys.
2. Fact-table foreign keys only reference valid dimension keys unless a lab intentionally configures orphan keys.
3. Skewed keys are deterministic and measurable.
4. The manifest records expected and actual key distributions.
5. A Spark validation job can read the Delta tables and verify row counts, FK coverage, skew ratio, file count, and file-size distribution.

This is the piece that makes the generator similar in spirit to ShadowTraffic: not just fake rows, but repeatable related streams/tables with controlled distributions. For this workshop, however, the output should be Delta lakehouse data rather than Kafka/Postgres traffic.

## Lab case map

The generator should produce small but real Spark problems. Each problem must have a data-shape trigger and an observable sparkMeasure symptom.

| Lab | Data-shape trigger | Spark symptom | sparkMeasure focus |
| --- | --- | --- | --- |
| Skewed join | One vendor/customer/product owns most fact rows | long tail tasks, uneven shuffle, possible spill | task metrics, shuffle read/write, executor runtime |
| Small files | many tiny Delta files under the same table | high scheduling/file scan overhead | stages, task count, input metrics |
| Uneven file sizes | hot partition writes much larger files than cold partitions | imbalanced scan tasks | task duration, input bytes per task |
| Wide aggregation | high-cardinality groupBy on wide fact rows | large shuffle and high executor runtime | stage shuffle metrics, spill metrics |
| Bad partitioning | partitioned by low-value or skewed column | poor pruning or many tiny partitions | input files, elapsed time, stages |
| Cache/no-cache | repeated reads/joins over same table | repeated scans vs reduced recomputation | executor runtime, stage count, IO metrics |

A lab does not need huge data if the trigger is strong. For example, small-files and skew labs can be visible with tens of GB or less; wide-shuffle and spill labs need more careful sizing based on local memory.

## Revised engine recommendation

The architecture should have two layers:

1. Relationship contract layer: our YAML/config model for schemas, keys, distributions, lab intent, and validations.
2. Materialization layer: Spark-native writer first, with optional engines after benchmark.

Recommended materialization path:

- First POC: test `dbldatagen` locally with Spark 4.1.2 because it already supports repeatable multi-table generation, distributions, weighted values, and Spark DataFrame output.
- If `dbldatagen` works cleanly: use it for the first generator implementation and wrap it with our lab contract, manifest, and Make/Docker interface.
- If it does not work cleanly: implement the minimal internal Spark-native generator using `spark.range`, deterministic hash/seed expressions, weighted key mapping, and Delta writes.
- Keep Polars/delta-rs as an optional benchmark path for dimension tables or non-Spark generation experiments.
- Use Mimesis/Faker only for low-volume dimension attributes, such as names, addresses, product labels, and vendor descriptions.

This keeps the workshop focused on Spark behavior while still borrowing the best idea from tools like ShadowTraffic: deterministic related data with controllable distributions.

## Revised first implementation slice

The first implementation should prove relationships, not just bytes:

1. Add a generator package and Docker image.
2. Add a schema-first YAML contract for `retail_sales_skew`.
3. Generate `vendors`, `products`, `customers`, and `sales` as Delta in bronze.
4. Guarantee FK-valid joins across the generated tables.
5. Support scale presets from `demo` to `large-local`.
6. Support `vendor_skew`, `product_skew`, `date_skew`, partition count, and `maxRecordsPerFile`.
7. Write a manifest to `s3a://observability/generator-runs/<run_id>/manifest.json`.
8. Add a validation submit that checks row counts, FK integrity, skew ratio, file count, and file-size distribution.
9. Add a `make generate` target.
10. Add a measured Spark lab that joins sales to vendors/products and compares native logs/UI vs sparkMeasure.

## Current decision

Build a schema-first, relationship-first generator. Use Spark-native Delta materialization as the default path, test `dbldatagen` before writing too much custom DSL, and keep Polars/delta-rs as a secondary benchmark path. The generator must prioritize clear Spark pathologies for sparkMeasure labs over raw byte volume.
