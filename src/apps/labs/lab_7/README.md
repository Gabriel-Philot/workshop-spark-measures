# Lab 7 - Temporal Backfill Observability

Subtitle:

```text
Understanding daily volume spikes through stage-level Spark metrics
```

Lab 7 studies a common operational pattern: a daily historical backfill where
some business dates are much larger than others.

The full lab will connect temporal source volume, per-date Spark runs, persisted
StageMetrics, and a compact dashboard table. The first implemented slice is the
temporal source generator.

## Part A: Temporal source generator

The generator creates a new bronze source:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

It also writes auditable volume-plan metadata to:

```text
s3a://observability/lab7/temporal_volume_plan
```

This source is separate from the existing retail bronze tables:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

The isolation is intentional. Labs 0-6 continue to use the retail data. Lab 7
adds a new temporal entity instead of changing the earlier classroom fixtures.

## Why this entity exists

The current retail data is good for joins, shuffle, skew, runtime budgets, and
contracts. It is not ideal for temporal backfill storytelling because the daily
volume shape is not explicit enough.

Lab 7 needs a source where the volume by `event_date` is known before the Spark
job runs:

| Day type | Default multiplier |
|---|---:|
| normal day | 1x |
| medium spike | 10x |
| large spike | 100x |

That lets students compare the expected business volume against the Spark
metrics captured later by sparkMeasure StageMetrics.

## Schema

The source is deterministic and Spark-native. It is generated with
`spark.range`, not Python row loops, Faker, or Python UDFs.

Columns:

```text
event_id
event_date
event_ts
account_id
customer_id
vendor_id
product_id
region
channel
event_type
quantity
gross_amount
payload_size_bucket
created_at
```

The table is partitioned by:

```text
event_date
```

## Generation modes

`full` mode creates the configured historical range. It appends only missing
dates by default, so rerunning the command does not duplicate dates that already
exist and pass validation.

```bash
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

`append_day` mode adds one new date partition:

```bash
LAB7_GENERATE_MODE=append_day \
LAB7_APPEND_DATE=2026-01-15 \
LAB7_APPEND_VOLUME_MULTIPLIER=100 \
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

For local calibration only, this scoped reset is available:

```bash
LAB7_REPLACE_SOURCE=true \
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

That reset deletes only the Lab 7 source path, not retail data and not Lab 0-6
outputs.

## Public generation flow

Retail-only generation remains unchanged:

```bash
make generate SCALE=xs
```

That command generates only the existing retail bronze tables used by Labs 0-6.

The full workshop-source generation order is:

```bash
make generate-all SCALE=xs
```

`make generate-all` generates the existing retail bronze tables first and then
adds or validates the Lab 7 temporal source at the end.

If retail data already exists and only the Lab 7 source is needed:

```bash
make generate-lab7
```

`SCALE=s` remains an opt-in stress option for the retail data. Lab 7 should get
its visible volume from the isolated temporal source, not by forcing the shared
retail tables to be larger.

## Expected markers

The runner validates these markers:

```text
LAB7_TEMPORAL_VOLUME_PLAN_OK
LAB7_TEMPORAL_SOURCE_GENERATED_OK
LAB7_TEMPORAL_SOURCE_VALIDATION_OK
LAB7_TEMPORAL_SOURCE_GENERATOR_OK
```

## Classroom takeaway

Before analyzing a temporal backfill, create a source where the expected daily
volume is known and reproducible. Later StageMetrics are more useful when they
can be compared against a trusted business-time volume plan.
