# Lab 7 guide: temporal backfill observability

This guide is the classroom runbook for Lab 7.

Goal:

```text
create a temporal source with known daily volume
  -> submit one Spark backfill per business date
  -> persist StageMetrics with temporal context
  -> compare normal, 10x, and 100x dates
  -> visualize the complete batch in Streamlit
```

Earlier labs diagnosed or governed one Spark execution. Lab 7 turns persisted
StageMetrics into a small historical observability product organized by
business date.

Class notes:

[Temporal backfill observability class
notes](docs/temporal_backfill_observability_class_notes.md)

Keep those notes open for metric interpretation, input-byte limitations, and
the validated local evidence.

## 0. Confirm only the required platform prerequisite

Run the classroom commands from the repository root:

```bash
cd workshop-spark-measures
```

Lab 7 assumes that the pinned dependencies and workshop images already exist.
If this is the first workshop run, or images were removed, follow [Lab 0 guide:
bootstrap and build](../lab_0/guide_lab0.md) before continuing. The Lab 7 public
runner starts Compose and creates its own isolated temporal source, so this
guide does not repeat the generic bootstrap sequence.

Teacher notes:

```text
The Lab 7 source is independent from the Bronze retail tables used by Labs 0-6.
Running Lab 7 does not require regenerating sales, vendors, products, or
customers.
```

## 1. Understand the public Lab 7 workflow

The public orchestration entry point is:

```text
src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

It performs this sequence:

```text
make compose
  -> create or validate the Lab 7 temporal source
  -> load the configured processing dates
  -> run one sequential spark-submit per date
  -> append one StageMetrics row per date
  -> print the dashboard command and metrics path
```

The two Spark entry points remain visible at the lab root:

```text
src/apps/labs/lab_7/lab_7_temporal_source_generator.py
src/apps/labs/lab_7/lab_7_daily_backfill_stage_metrics.py
```

Support runners, configuration, and transformations remain under
`lab_7_utils/`. Students should use the public runner unless they are debugging
one component.

## 2. Read the temporal volume plan first

Open:

```text
src/apps/labs/lab_7/lab_7_utils/volume_plan.yaml
```

The plan creates 14 deterministic business dates:

| date class | rows per date | multiplier |
| --- | ---: | ---: |
| normal | 10,000 | 1x |
| medium spike | 100,000 | 10x |
| large spike | 1,000,000 | 100x |

Configured spike dates:

```text
2026-01-04 -> 1,000,000 rows -> VOLUME_SPIKE
2026-01-07 ->   100,000 rows -> MEDIUM_SPIKE
2026-01-11 -> 1,000,000 rows -> VOLUME_SPIKE
```

Total planned source volume:

```text
2,210,000 rows
```

The generator writes an isolated Delta source partitioned by `event_date`:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

It also persists the expected plan:

```text
s3a://observability/lab7/temporal_volume_plan
```

Teacher notes:

```text
Start with known business volume, not Spark metrics. Students need a trusted
expectation before deciding whether records read, shuffle, runtime, or task
counts reacted to the date.
```

## 3. Optional: run the two-date smoke test

Use this only for a fast technical check:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

It compares one normal date with one `100x` date:

```text
2026-01-01 -> 10,000 rows
2026-01-04 -> 1,000,000 rows
```

In the clean local rehearsal, this command took:

```text
136.68 seconds = 2 minutes 16.68 seconds
```

That boundary included Compose readiness, generation of the complete temporal
source, and the two backfill submits.

The smoke batch is isolated by its own `run_id`, but it is not suitable for
dashboard review. It hides chronological shape and can make spike placement,
legends, and normalized charts misleading.

Teacher notes:

```text
The smoke run is optional. Do not use it as the main classroom evidence and do
not validate dashboard design from only two dates.
```

## 4. Run the complete 14-date batch

Do not run `make generate-lab7` immediately before the normal classroom flow.
The public runner already creates or validates the isolated temporal source
before starting the backfill. Use `make generate-lab7` only when source
generation is being prepared or taught separately.

Run the public workflow without `LAB7_PROCESSING_DATES`:

```bash
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

The runner submits all dates sequentially. Expected batch start:

```text
LAB7_DAILY_BACKFILL_BATCH_STARTED ... dates=14 ...
```

Expected completion:

```text
LAB7_DAILY_BACKFILL_BATCH_COMPLETED ... dates=14 ...
LAB7_TEMPORAL_BACKFILL_OBSERVABILITY_COMPLETED
```

Validated local elapsed time captured during the 2026-07-11 rehearsal:

```text
575.37 seconds = 9 minutes 35.37 seconds
```

This measurement started with empty project storage and included:

- Compose validation/readiness;
- generation and validation of the 2,210,000-row temporal source;
- 14 sequential Spark applications;
- one StageMetrics Delta append per date.

It excluded image bootstrap/build time. Reserve approximately 10 minutes in the
classroom and use the execution window to explain the volume plan and why the
lab uses one submit per date.

## 5. Read one daily terminal block before opening the dashboard

Each processing date prints:

```text
## LAB 7 DAILY BACKFILL STAGE METRICS

### Processing date
processing_date: ...
source_rows_for_date: ...
volume_multiplier: ...
spike_label: ...

### StageMetrics
executor_run_time_ms: ...
records_read: ...
input_bytes: ...
shuffle_bytes_written: ...
shuffle_bytes_read: ...
num_stages: ...
num_tasks: ...
memory_bytes_spilled: ...
disk_bytes_spilled: ...
jvm_gc_time_ms: ...

### Normalized by expected source volume
runtime_per_million_rows: ...
shuffle_per_million_rows: ...
input_bytes_per_million_rows: ...
tasks_per_million_rows: ...
```

Expected markers per date:

```text
LAB7_DAILY_BACKFILL_CONFIG_OK
LAB7_DAILY_BACKFILL_RUN_OK
LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK
LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK
LAB7_DAILY_BACKFILL_STAGE_METRICS_OK
```

Teacher notes:

```text
Use source_rows_for_date and records_read as the bridge from business volume to
Spark execution. Treat input_bytes as supporting evidence because the
StageMetrics bytesRead counter does not represent physical Delta table size
reliably on this local S3A stack.
```

## 6. Start the Lab 7 Streamlit dashboard

This is the Lab 7-specific Make command used in class:

```bash
make lab7-dashboard
```

The target:

1. confirms the core Compose stack;
2. builds the pinned lightweight dashboard image;
3. starts `wsm-lab7-dashboard`;
4. exposes Streamlit on port `28501`.

Open:

```text
http://127.0.0.1:28501
```

The dashboard uses:

```text
Streamlit -> DuckDB -> Delta table on MinIO
```

It is read-only and does not start a Spark query.

## 7. Present the dashboard from top to bottom

Select the complete batch `run_id`. The dashboard defaults to the latest batch,
but verify that `Processed dates` is `14` before presenting it.

### 7.1 Summary cards

Validated full-batch values:

```text
Processed dates:      14
Expected source rows: 2,210,000
Spike days:           3
Max expected rows:    1,000,000
Max records read:     1,000,045
Max shuffle written:  about 20.6 MB
```

### 7.2 Shuffle timeline

The dates must remain chronological. The `100x` bars should appear on days 04
and 11, with the smaller `10x` signal on day 07.

Validated shuffle ranges:

```text
normal:       487,357-522,139 B
medium spike: 2,484,042 B
large spike:  21,572,070-21,583,136 B
```

### 7.3 Memory pressure

The validated batch has zero memory and disk spill for all 14 dates. The chart
still has value: it shows that larger temporal volume increased shuffle without
crossing into spill pressure. GC ratio remains supporting evidence.

### 7.4 Volume scatter plots

Use volume versus runtime to discuss fixed per-submit cost. Use volume versus
shuffle to show the clearer relationship between source volume and data
movement.

### 7.5 Normalized execution view

The raw normalized chart keeps actual per-million values. The companion index
chart converts each metric to a ratio against the normal-day median so runtime,
shuffle, and tasks can share one readable scale.

Teacher notes:

```text
Do not promise linear runtime. The strongest story is that trusted business
volume, records read, and shuffle move together, while fixed application cost
and zero spill explain why other signals behave differently.
```

## 8. Optional: inspect the persisted evidence

Business outputs are separated by date:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard/processing_date=<YYYY-MM-DD>/filter_strategy=early_partition_filter
```

StageMetrics rows are appended to:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

All 14 date rows in one batch share the same `run_id`; each date also has its
own `date_run_id` and Spark `application_id`.

Use the MinIO Console only if the class needs to see the physical Delta layout:

```text
http://127.0.0.1:29011
```

## 9. Optional: inspect the 14 Spark applications

Open Spark History Server:

```text
http://127.0.0.1:28090
```

One date equals one Spark application. Use `processing_date` from logs and the
persisted `application_id` to correlate business time, StageMetrics, and Spark
UI detail.

This design is intentionally slower than processing all dates in one app. It
resembles a scheduled daily backfill and keeps correlation simple for students.

## 10. Optional Lab 7-specific Make controls

Generate or validate only the isolated temporal source:

```bash
make generate-lab7
```

Generate the shared retail sources first and Lab 7 source last:

```bash
make generate-all SCALE=xs
```

These targets are useful for environment preparation. The normal public Lab 7
runner already ensures its own source, so do not run `make generate-lab7`
immediately before it unless the lesson explicitly separates source generation
from backfill execution.

## 11. Classroom conclusion

End with:

```text
Persisted StageMetrics become more useful when they carry trusted business-time
context. We can now explain which dates were large, which Spark signals moved,
and which expected problems—such as spill—did not occur.
```

The progression is:

```text
known temporal volume
  -> one correlated Spark execution per date
  -> persisted stage-level evidence
  -> historical comparison
  -> lightweight dashboard
```

## 12. Optional cleanup after class

From the repository root:

```bash
make down
```

This stops the platform and preserves generated evidence. Use the project soft
cleanup drill only when the next rehearsal must start from empty storage.
