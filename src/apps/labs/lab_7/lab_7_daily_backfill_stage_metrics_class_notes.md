# Lab 7B class notes: daily backfill StageMetrics by date

## Main idea

Lab 7B turns the temporal source from Lab 7A into an observable daily backfill.

Each `processing_date` is submitted as a separate Spark application. That makes
the workshop closer to a real operational pattern:

```text
One daily partition, one scheduled job run, one StageMetrics evidence row.
```

## What the app does

For one `processing_date`, the app:

1. reads the Lab 7 temporal bronze source;
2. filters early by `event_date`;
3. builds a daily activity dashboard;
4. writes the business output to a date-specific Delta path;
5. captures sparkMeasure StageMetrics;
6. writes one metrics row to the observability bucket.

Business output:

```text
event_date
region
channel
event_type
event_count
customer_count
gross_revenue
avg_ticket
created_at
```

## Why one submit per date

One Spark application per date is slower than one Python loop inside a single
Spark app, but it is easier to reason about in class:

| Design | Teaching value |
|---|---|
| one app for 14 dates | faster local execution |
| one submit per date | closer to scheduled backfills and History Server review |

For this lab, the second option is better. Students can correlate one business
date with one application and one metrics row.

## Why expected volume is stored with metrics

The metrics row includes:

```text
processing_date
source_rows_for_date
volume_multiplier
spike_label
executor_run_time_ms
records_read
input_bytes
shuffle_bytes_written
shuffle_bytes_read
num_tasks
num_stages
memory_bytes_spilled
disk_bytes_spilled
jvm_gc_time_ms
```

This is the central lesson:

```text
Spark metrics are easier to explain when they are joined to business-time
context.
```

If a `100x` date does not produce a visibly different runtime or shuffle signal,
that is itself useful calibration evidence. We then adjust `volume_plan.yaml`
instead of guessing.

## Important limitation

`input_bytes` is the sparkMeasure StageMetrics `bytesRead` counter. On the local
Delta/S3A stack, that counter can be smaller than the physical Delta file size.
Do not use it alone as the source-volume truth.

For this lab, prefer this hierarchy:

1. controlled `source_rows_for_date` from `volume_plan.yaml`;
2. `recordsRead` when emitted by StageMetrics;
3. `input_bytes` as a supporting signal with caveats;
4. shuffle/runtime/tasks as execution-shape evidence.

## How to use during class

Start with two dates:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

Then compare the terminal blocks:

```text
2026-01-01 -> 10k rows, NORMAL
2026-01-04 -> 1M rows, VOLUME_SPIKE
```

The goal is not to prove a universal runtime multiplier. The goal is to show
that persisted StageMetrics can be trended by business date.

## Local calibration evidence

During implementation, a two-date subset was run on the local WSL/Docker stack:

| processing_date | expected rows | label | executor runtime | records read | shuffle written |
|---|---:|---|---:|---:|---:|
| `2026-01-01` | 10,000 | `NORMAL` | 27,948 ms | 10,045 | 492,763 B |
| `2026-01-04` | 1,000,000 | `VOLUME_SPIKE` | 38,243 ms | 1,000,045 | 21,583,622 B |

Interpretation:

- `recordsRead` follows the volume plan almost exactly;
- shuffle is clearly larger on the spike day;
- executor runtime is higher, but not 100x higher because the local run has
  fixed Spark/Delta/MinIO costs and the output cardinality is small;
- this is acceptable for Lab 7B because the lab is about persisted evidence by
  business date, not about forcing every metric to scale linearly.

If we later need runtime to separate more dramatically, calibrate the source or
the transformation, not the retail data shared by previous labs.

## Footnote: classroom timing expectation

A full 14-date local batch was timed on the workshop WSL/Docker stack after
cleaning the Lab 7B output paths. The run took about `9 minutes`
(`531.78 seconds`).

That is acceptable for a classroom flow: start the batch, explain the temporal
design, then return to the StageMetrics output when the backfill completes.

Be explicit with students that this elapsed time is dominated by the fixed cost
of starting and stopping one Spark application per date. In this local setup,
the strongest signals are usually `recordsRead`, `shuffleBytesWritten`, and the
normalized metrics by business date, not a perfectly linear wall-clock increase.
