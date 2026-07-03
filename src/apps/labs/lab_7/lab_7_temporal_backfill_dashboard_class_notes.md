# Lab 7C class notes: temporal backfill observability dashboard

## Main idea

Lab 7C turns the persisted Lab 7B StageMetrics table into a visual operational
story.

The dashboard does not run Spark. It reads the Delta metrics table from MinIO
with DuckDB and uses Streamlit only as a classroom presentation layer.

## Why this exists after Lab 7B

Lab 7B creates one StageMetrics row per business date:

```text
processing_date
source_rows_for_date
spike_label
records_read
executor_run_time_ms
shuffle_bytes_written
num_tasks
num_stages
```

That table is useful, but raw rows are not the best way to teach temporal
behavior. Lab 7C asks:

```text
Can we see which business dates changed the execution profile?
```

## What to sell in the dashboard

### 0. The selected batch

Lab 7B writes metrics in append mode. If the instructor reruns the backfill,
the observability table will contain multiple batches. The dashboard therefore
defaults to the latest `run_id` and exposes a sidebar selector.

Do not mix different `run_id` values when explaining date-by-date behavior.
Each chart should represent one coherent backfill batch.

### 1. The business volume plan

Start with `source_rows_for_date` and `spike_label`.

This is the anchor. Students should first understand which dates were expected
to be larger before looking at Spark metrics.

### 2. Records read

`records_read` should track the expected volume. This is the simplest bridge
between business time and Spark execution.

If the source has `100x` rows and `records_read` also jumps, the metric is doing
its job.

### 3. Shuffle bytes written

`shuffle_bytes_written` is the strongest signal in this local lab. The business
output cardinality is small, and the local Spark application startup cost is
large, so runtime will not scale linearly with input volume.

The teaching point is:

```text
The spike day becomes visible in execution counters, especially shuffle.
```

### 4. Runtime

`executor_run_time_ms` is still useful, but it must be interpreted carefully.
This lab intentionally runs one Spark submit per date. That mirrors scheduled
backfills but adds fixed startup/stop cost around each run.

Do not promise students that runtime will grow exactly `100x` when data grows
`100x` in this local setup.

### 5. Normalized metrics

The dashboard includes metrics per million source rows. These make comparisons
fairer across dates and expose whether a large date is proportionally expensive
or simply larger.

## Why DuckDB here

DuckDB is used as a small analytical reader over persisted Delta metrics.

It is not part of the Spark workload. It is a lightweight way to show that once
StageMetrics are stored as Delta, other tools can consume them for review,
dashboards, and post-run analysis.

## Classroom takeaway

Persisted StageMetrics are not only logs. When joined with business-time
context, they become an operational view:

```text
Which dates were large?
Which Spark counters moved?
Did shuffle grow with source volume?
Is runtime a strong signal or dominated by local fixed cost?
```

This is the bridge from individual Spark troubleshooting to backfill
observability.
