# Lab 7 class notes: temporal backfill observability

Classroom runbook:

[Lab 7 classroom guide](../guide_lab7.md)

## Main idea

Lab 7 shows how persisted sparkMeasure StageMetrics can explain temporal
backfill behavior.

The operational question is:

```text
I have a historical daily backfill. Some business dates are much larger than
others. Can StageMetrics show which dates changed the Spark execution profile?
```

The lab has three steps:

1. create a deterministic temporal bronze source with known daily volume;
2. process one `processing_date` per Spark application and persist StageMetrics;
3. open a dashboard that reads the metrics table and makes the temporal pattern
   visible.

## Why a new temporal source exists

The existing retail data is intentionally shared by Labs 0-6. Changing it would
make earlier labs less stable.

Lab 7 therefore adds a scoped bronze entity:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

This keeps the temporal-backfill problem isolated. Previous labs keep reading
the same retail paths.

## Why Spark-native generation

The source generator uses Spark expressions over `spark.range`.

This avoids slow and unstable synthetic-data patterns:

- Python loops;
- Faker row-by-row generation;
- Python UDFs;
- tiny append writes;
- uncontrolled cross joins.

The goal is not realistic fake identities. The goal is deterministic volume,
partitioning, and business attributes that are good enough for observability
lessons.

## Volume plan

The default plan has 14 business dates with visible spikes:

```text
normal day   -> 1x  -> 10,000 rows
medium spike -> 10x -> 100,000 rows
large spike  -> 100x -> 1,000,000 rows
```

The known volume plan is the anchor for the class. Students should first know
which dates are supposed to be larger, then inspect Spark metrics.

## Why one submit per date

Each `processing_date` is submitted as a separate Spark application.

That is slower than processing all dates inside one Spark app, but it is easier
to explain:

| Design | Teaching value |
|---|---|
| one app for 14 dates | faster local execution |
| one submit per date | closer to scheduled backfills and History Server review |

For this lab, one submit per date is the better classroom tradeoff. Students
can correlate one business date with one application and one metrics row.

## What the backfill app does

For one `processing_date`, the app:

1. reads the Lab 7 temporal bronze source;
2. filters early by `event_date`;
3. builds a daily activity dashboard table;
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

## Metrics stored by date

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

The central teaching point is:

```text
Spark metrics are easier to explain when they are joined to business-time
context.
```

## Important limitation: input bytes

`input_bytes` is the sparkMeasure StageMetrics `bytesRead` counter. On this
local Delta/S3A stack, that counter can be smaller than the physical Delta file
size.

For this lab, prefer this hierarchy:

1. controlled `source_rows_for_date` from `volume_plan.yaml`;
2. `recordsRead` when emitted by StageMetrics;
3. `input_bytes` as a supporting signal with caveats;
4. shuffle/runtime/tasks as execution-shape evidence.

## What to sell in the dashboard

### Selected batch

Lab 7 writes metrics in append mode. If the instructor reruns the backfill, the
observability table will contain multiple batches. The dashboard defaults to the
latest `run_id` and exposes a sidebar selector.

Do not mix different `run_id` values when explaining date-by-date behavior.
Each chart should represent one coherent backfill batch.

For dashboard refinement, always validate with the complete 14-date backfill.
The two-date smoke run is useful for fast checks, but it hides the temporal
shape of the lesson and can make chart ordering, legends, and spike placement
look misleading.

### Business volume plan

Start with `source_rows_for_date` and `spike_label`.

This is the anchor. Students should first understand which dates were expected
to be larger before looking at Spark metrics.

### Records read

`records_read` should track the expected volume. This is the simplest bridge
between business time and Spark execution.

If the source has `100x` rows and `records_read` also jumps, the metric is doing
its job.

### Shuffle bytes

`shuffle_bytes_written` and `shuffle_bytes_read` are the strongest signals in
this local lab. The first dashboard chart puts both shuffle counters next to the
expected source volume so students can see whether the spike date changed the
execution profile.

The teaching point is:

```text
The spike day becomes visible in execution counters, especially shuffle.
```

### Memory pressure

The memory pressure chart uses:

```text
memory_bytes_spilled
disk_bytes_spilled
jvm_gc_time_ms / executor_run_time_ms
```

Zero spill is not a missing chart. It is a useful low-pressure signal for this
workload. If a later calibration creates spills, the same chart will show memory
or disk pressure by date.

### Runtime

`executor_run_time_ms` is still useful, but it must be interpreted carefully.
This lab intentionally runs one Spark submit per date. That mirrors scheduled
backfills but adds fixed startup/stop cost around each run.

Do not promise students that runtime will grow exactly `100x` when data grows
`100x` in this local setup.

## Why DuckDB and Streamlit here

The dashboard does not run Spark. It reads the persisted Delta metrics table
from MinIO with DuckDB and uses Streamlit only as a classroom presentation
layer.

This shows a practical pattern:

```text
Spark workload -> persisted StageMetrics -> lightweight operational dashboard
```

Once StageMetrics are stored as Delta, other tools can consume them for review,
dashboards, and post-run analysis.

## Validated local evidence

The final rehearsal ran from empty project storage on the local WSL/Docker
stack with two Spark workers. The public runner created the temporal source and
processed all 14 dates under one batch `run_id`.

| day class | expected rows | records read | executor runtime | shuffle written |
|---|---:|---:|---:|---:|
| normal | 10,000 | 10,045 | 26.973-30.671s | 487,357-522,139 B |
| medium spike | 100,000 | 100,045 | 28.964s | 2,484,042 B |
| large spike | 1,000,000 | 1,000,045 | 36.455-38.866s | 21,572,070-21,583,136 B |

All 14 rows reported zero memory and disk spill. That is a useful low-pressure
result rather than missing evidence.

Interpretation:

- `recordsRead` follows the expected volume almost exactly;
- shuffle separates normal, `10x`, and `100x` days clearly;
- executor runtime rises on the large days but does not scale linearly because
  each submit has fixed Spark, Delta, MinIO, and application lifecycle cost;
- the dashboard orders all 14 dates correctly and places spike days on
  `2026-01-04`, `2026-01-07`, and `2026-01-11`.

## Footnote: classroom timing expectation

On 2026-07-11, the complete public runner took:

```text
575.37 seconds = 9 minutes 35.37 seconds
```

The measured boundary included Compose readiness, creation and validation of
the 2,210,000-row temporal source, and 14 sequential Spark applications. Image
bootstrap/build time was outside that measurement.

Budget approximately 10 minutes in class. Start the runner, explain the source
and one-submit-per-date design, then return to the StageMetrics evidence when
the batch completes. The strongest signals in this local setup are
`recordsRead`, shuffle bytes, and the normalized metrics by business date.

## Classroom takeaway

Persisted StageMetrics are not only logs. When joined with business-time
context, they become an operational view:

```text
Which dates were large?
Which Spark counters moved?
Did shuffle grow with source volume?
Was there any memory or disk spill?
Is runtime a strong signal or dominated by local fixed cost?
```

This is the bridge from individual Spark troubleshooting to temporal backfill
observability.
