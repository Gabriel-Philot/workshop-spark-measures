# Lab 2B class notes: stage metrics interpretation drill

## Source question inspiration

This lab was inspired by two certification-style questions about Spark stage
metrics.

### Question A: High GC time

A data engineer notices that a Spark job's stage shows GC time consuming 25% of
the total task duration, highlighted in red.

What is the first action they should take?

#### Alternatives

- A. Increase the number of shuffle partitions.
- B. Add more executor cores.
- C. Increase executor memory or optimize memory usage.
- D. Enable Adaptive Query Execution.

#### Answer

The expected answer is **C. Increase executor memory or optimize memory usage**.

High GC time is a memory-pressure signal. Adding cores can make the problem
worse if each task still has too little execution memory.

### Question B: Interpreting shuffle spill

A data engineer observes the following metrics in a stage:

```text
Shuffle Spill (Memory): 5 GB
Shuffle Spill (Disk): 1.2 GB
Duration: 45 minutes
```

What does this indicate?

#### Alternatives

- A. The stage is performing efficiently with minimal I/O.
- B. Spark is writing more data to disk than memory, indicating serialization
  corruption.
- C. Spark had to spill shuffle data from memory to disk.
- D. Disk storage is corrupted because disk spill is smaller than memory spill.

#### Answer

The expected answer is **C. Spark had to spill shuffle data from memory to
disk**.

`Shuffle Spill (Memory)` is the deserialized data size that could not fit in
memory. `Shuffle Spill (Disk)` is the serialized amount written to disk, so the
disk number can be smaller.

## Local workshop adaptation

The local lab uses generated retail Delta data:

- `s3a://lakehouse/bronze/retail/sales`
- `s3a://lakehouse/bronze/retail/vendors`
- `s3a://lakehouse/bronze/retail/products`

The workload creates a category/month sales summary and writes:

```text
s3a://lakehouse/gold/lab2/stage_metrics_drill/default
s3a://lakehouse/gold/lab2/stage_metrics_drill/pressure
```

The output grain is:

```text
vendor_region, category_id, sale_year_month
```

The pressure variant intentionally carries wider payload columns through a
round-robin repartition before narrowing the data. This makes shuffle and task
volume easier to see on small local data.

## Connecting the questions to sparkMeasure

The certification questions ask students to interpret metrics that also appear
in sparkMeasure StageMetrics:

| Question clue | sparkMeasure field | What it proves |
|---|---|---|
| high GC time | `jvmGCTime` | memory pressure may be present |
| shuffle spill memory | `memoryBytesSpilled` | data spilled from execution memory |
| shuffle spill disk | `diskBytesSpilled` | serialized spill was written to disk |
| expensive shuffle | `shuffleBytesWritten`, `shuffleTotalBytesRead` | data was exchanged between stages |
| expensive distributed work | `executorRunTime` | executors spent material time running tasks |
| too many or too few tasks | `numTasks` | stage shape / partitioning needs inspection |

StageMetrics answers stage-level questions. It does not prove whether one
specific task is a straggler; that belongs to TaskMetrics or Spark UI task-level
inspection.

## Lab code shape

The default variant narrows payloads before the shuffle:

```python
return (
    inputs["sales"]
    .transform(_select_sales_with_payload_width)
    .transform(_join_vendor_region_for_drill, inputs["vendors"])
    .transform(_join_product_category_for_drill, inputs["products"])
    .transform(_select_stage_metrics_fact_from_width)
    .repartition(keyed_partitions, "vendor_region", "category_id", "sale_year_month")
    .transform(_aggregate_stage_metrics_fact)
)
```

The pressure variant carries wider payload columns through a shuffle:

```python
return (
    inputs["sales"]
    .transform(_select_sales_with_payload_columns)
    .transform(_join_vendor_region_for_drill, inputs["vendors"])
    .transform(_join_product_category_for_drill, inputs["products"])
    .repartition(round_robin_partitions)
    .transform(_select_stage_metrics_fact_from_payload_columns)
    .transform(_aggregate_stage_metrics_fact)
)
```

The point is not to create fake delay. The point is to make Spark move more real
data so the StageMetrics report has useful signals.

Use the top-level classroom switch in
`lab_2b_stage_metrics_interpretation_drill.py`:

```python
CONFIG_NAME = "lab2-stage-metrics-drill-pressure"

# Useful alternative for the live demo:
# CONFIG_NAME = "lab2-stage-metrics-drill-default"
```

## How to read the output

After the submit, read the sparkMeasure aggregated report printed by the
collector:

```text
Aggregated Spark stage metrics:
numStages => ...
numTasks => ...
executorRunTime => ...
jvmGCTime => ...
diskBytesSpilled => ...
memoryBytesSpilled => ...
shuffleTotalBytesRead => ...
shuffleBytesWritten => ...
```

The lab intentionally does not emit extra derived diagnosis lines. The point is
to make students read the sparkMeasure output directly, using the class notes as
the interpretation guide.

Interpretation rules:

| Observation | Interpretation |
|---|---|
| shuffle bytes > 0 | a wide operation exchanged data between stages |
| memory/disk spill > 0 | Spark ran out of execution memory during shuffle |
| spill = 0 | this run does not show shuffle spill |
| high `jvmGCTime` relative to `executorRunTime` | memory pressure may be present |
| low `jvmGCTime` and zero spill | focus on data movement / partitioning first |
| high max task duration | not proven by StageMetrics aggregate alone |

## If spill is zero

Zero spill is not a failed lesson. It is a valid diagnosis:

```text
This stage is shuffle-heavy, but this run does not show memory spill.
Therefore, the first diagnosis is data movement / partitioning, not memory
pressure.
```

That distinction is the main point of Lab 2B. Students should not see every
slow or shuffle-heavy stage and immediately assume memory pressure.

## Validated local runs

Validated on the local WSL stack with two Spark workers and generated
`SCALE=xs` data:

```text
GENERATOR_VOLUME table=sales rows=5000000 files=114 total_bytes=762521122
```

| Variant | Stages | Tasks | Executor runtime | Shuffle written | Memory spilled | Disk spilled | GC time |
|---|---:|---:|---:|---:|---:|---:|---:|
| default | 16 | 356 | ~59.3s | ~40.0 MiB | 0 B | 0 B | ~1.7s |
| pressure | 17 | 839 | ~94.2s | ~670.9 MiB | ~800.0 MiB | ~382.2 MiB | ~3.2s |

The relevant raw sparkMeasure lines for the pressure run were:

```text
numStages => 17
numTasks => 839
executorRunTime => 94175 (1.6 min)
jvmGCTime => 3246 (3 s)
diskBytesSpilled => 400765052 (382.2 MB)
memoryBytesSpilled => 838859488 (800.0 MB)
shuffleTotalBytesRead => 703458127 (670.9 MB)
shuffleBytesWritten => 703458127 (670.9 MB)
```

Interpretation:

- spill is present because `memoryBytesSpilled` and `diskBytesSpilled` are both
  greater than zero;
- this matches the shuffle-spill source question;
- GC is low because `jvmGCTime / executorRunTime` is about `3.4%`, not close to
  the 25% source-question case.

The relevant raw sparkMeasure lines for the default run were:

```text
numStages => 16
numTasks => 356
executorRunTime => 59274 (59 s)
jvmGCTime => 1676 (2 s)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
shuffleTotalBytesRead => 41940653 (40.0 MB)
shuffleBytesWritten => 41940653 (40.0 MB)
```

Interpretation:

- shuffle is present, but much smaller than the pressure run;
- spill is absent because both spill metrics are zero;
- GC is also low, so the first diagnosis is not memory pressure.

Both variants produced the same 50-row Gold summary:

```text
default_count=50 pressure_count=50 default_minus_pressure=0 pressure_minus_default=0
```

That matters for the narrative: the two variants are semantically equivalent,
but the pressure variant makes Spark move wider rows through more shuffle tasks.
sparkMeasure exposes the execution difference without changing the business
result.

## Instructor note

This lab is intentionally about interpretation, not heroic tuning. If the local
machine does not spill, keep the narrative honest:

```text
The source question tells us how to interpret spill when it appears. Our local
run shows shuffle without spill, so we can rule out that specific symptom for
this run.
```

Also keep GC interpretation conservative on small local machines. This workshop
usually runs on WSL with a small number of Spark workers, so `jvmGCTime` can
oscillate between runs because the driver, executors, Docker, and the host
compete for limited local resources. Do not over-read small GC movements after
changing the variant. In this lab, GC only becomes the main diagnosis if the
ratio is material and repeatable, such as the source-question case where GC is
about 25% of task duration. For the validated local run above, the stronger
evidence is shuffle and spill, not GC pressure.
