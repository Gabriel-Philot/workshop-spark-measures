# Lab 7 - Temporal Backfill Observability

Subtitle:

```text
Understanding daily volume spikes through stage-level Spark metrics
```

Lab 7 studies a common operational pattern: a daily historical backfill where
some business dates are much larger than others.

The lab connects temporal source volume, per-date Spark runs, persisted
StageMetrics, and a compact Streamlit dashboard.

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

## Part B: Daily backfill StageMetrics by date

The daily backfill app processes one `processing_date` per Spark application.
The runner submits all configured dates one by one, so each date appears as a
separate Spark application in History Server.

Default full batch:

```bash
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

Calibration subset:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

Business outputs are written per date:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard/processing_date=<YYYY-MM-DD>/filter_strategy=early_partition_filter
```

StageMetrics rows are appended to:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

Expected per-submit markers:

```text
LAB7_DAILY_BACKFILL_CONFIG_OK
LAB7_DAILY_BACKFILL_RUN_OK
LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK
LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK
LAB7_DAILY_BACKFILL_STAGE_METRICS_OK
```

Expected batch marker:

```text
LAB7_DAILY_BACKFILL_BATCH_COMPLETED
```

This lab intentionally stores the expected source volume next to the Spark
metrics. The comparison is the teaching point: a date with `100x` rows should be
easy to compare against executor runtime, records read, shuffle, tasks, spill,
and GC time.

## Part C: Temporal backfill observability dashboard

The dashboard is a read-only presentation layer over the Lab 7B metrics table.
It does not run Spark.

It uses:

```text
Streamlit -> DuckDB -> Delta table on MinIO
```

Input:

```text
s3://observability/lab7/daily_backfill_stage_metrics
```

Start it after Lab 7B has produced metrics:

```bash
make lab7-dashboard
```

Open:

```text
http://127.0.0.1:28501
```

The dashboard shows:

- a `run_id` selector, defaulting to the latest Lab 7B batch;
- expected source rows by business date;
- `records_read` by date;
- `shuffle_bytes_written` by date;
- expected rows versus executor runtime;
- expected rows versus shuffle;
- normalized runtime, shuffle, and task metrics per million source rows;
- the raw metrics table used for the visualizations.

This is intentionally not a generic BI layer. It is a small classroom view that
helps students connect temporal volume spikes to stage-level Spark execution
counters.

The Lab 7B metrics table is append-only. If you rerun the backfill, select one
batch `run_id` in the dashboard sidebar before explaining the charts.

If the dashboard is empty, run:

```bash
make generate-lab7
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
make lab7-dashboard
```

## Classroom takeaway

Before analyzing a temporal backfill, create a source where the expected daily
volume is known and reproducible. StageMetrics become much more useful when
they are persisted and visualized with trusted business-time context.
