# Lab 2 selected questions: Databricks Data Engineer Professional

This document keeps the instructor's selected Spark questions from Databricks
Certified Data Engineer Professional practice exams that inspired Lab 2. It is
a focused teaching selection, not a complete question bank. Read each question
before running its matching exercise, then use the answer and local mapping to
connect the exam clue to sparkMeasure evidence.

These questions are study material from practice exams. They are not presented
as official Databricks exam questions, and this workshop is not affiliated with
or endorsed by Databricks.

The teaching bridge is deliberate: certification questions often ask students
to interpret Spark UI numbers, while the local labs reproduce analogous
workload symptoms and use sparkMeasure to capture the relevant counters in a
compact, repeatable form. Understanding Spark remains the goal; sparkMeasure is
the diagnostic aid.

The questions are intentionally kept separate from the classroom runbook:

- this document asks what the metrics mean;
- [`../guide_lab2.md`](../guide_lab2.md) explains what to run and what to
  observe in the local workshop.

## Question map

| Question | Local exercise | Diagnostic lens |
|---|---|---|
| Repartitioning before aggregation | Lab 2A | StageMetrics shuffle and task volume |
| High GC time | Lab 2B | StageMetrics GC/runtime relationship |
| Shuffle spill | Lab 2B | StageMetrics memory and disk spill |
| High-end task skew | Lab 2C | TaskMetrics max versus p75 |
| Near-empty partitions | Lab 2D | TaskMetrics min versus median |

## Lab 2A: reducing shuffle during aggregation

### Question

A data engineer is attempting to execute the following PySpark code:

```python
df = spark.read.table("sales")
result = df.groupBy("region").agg(sum("revenue"))
```

However, after inspecting the execution plan and profiling the Spark job, the
engineer observes excessive data shuffling during the aggregation phase.

Which technique should be applied to reduce shuffling during the `groupBy`
aggregation operation?

### Alternatives

- A. Cache the DataFrame before aggregating.
- B. Use `.coalesce(1)` after the aggregation.
- C. Repartition by `region` before the aggregation.
- D. Use a broadcast join.

### Answer

The expected answer is **C. Repartition by `region` before the aggregation**.

```python
df = spark.read.table("sales")
result = df.repartition("region").groupBy("region").agg(sum("revenue"))
```

A grouped aggregation needs rows with the same grouping key to be co-located.
If the data is not already distributed by `region`, Spark must exchange data
across executors. Repartitioning by the grouping key makes that physical intent
explicit and can reduce unnecessary movement for the right workload shape.

The other alternatives do not address the required distribution:

- caching may help repeated reads, but does not change partitioning;
- `coalesce(1)` happens after the aggregation and can create a single-output
  bottleneck;
- broadcast is a join strategy, not an aggregation strategy.

### Local Lab 2A mapping

The local source needs a vendor join before it has a region. It then aggregates
by `vendor_region` and `sale_year_month`:

| Professional question | Local Lab 2A |
|---|---|
| `sales` table | generated Bronze `sales` Delta table |
| `region` | `vendor_region` from the `vendors` table |
| `revenue` | `sale_amount` |
| `groupBy("region")` | `groupBy("vendor_region", "sale_year_month")` |
| excessive shuffle | StageMetrics shuffle read/write and task count |

The baseline uses a round-robin repartition that is not aligned with the
grouping keys. The optimized variant repartitions by `vendor_region` and
`sale_year_month`. Shuffle remains because aggregation is still wide; the
lesson is to reduce unjustified movement, not promise zero shuffle.

## Lab 2B: interpreting high GC time

### Question

A data engineer notices that a Spark job's stage shows GC time consuming 25%
of the total task duration, highlighted in red.

What is the first action they should take?

### Alternatives

- A. Increase the number of shuffle partitions.
- B. Add more executor cores.
- C. Increase executor memory or optimize memory usage.
- D. Enable Adaptive Query Execution.

### Answer

The expected answer is **C. Increase executor memory or optimize memory
usage**.

High GC time is a memory-pressure signal. Adding cores can make the problem
worse when more concurrent tasks still have too little execution memory.

### Local Lab 2B mapping

The related sparkMeasure fields are:

```text
jvmGCTime
executorRunTime
```

Read them as a relationship rather than isolated counters:

```text
gc ratio = jvmGCTime / executorRunTime
```

The validated local workload did **not** reproduce the question's 25% GC
ratio. Its ratio remained around 3%. That is an important result: the local
evidence supports a shuffle/spill diagnosis, not a strong GC-pressure claim.

## Lab 2B: interpreting shuffle spill

### Question

A data engineer observes the following metrics in a stage:

```text
Shuffle Spill (Memory): 5 GB
Shuffle Spill (Disk): 1.2 GB
Duration: 45 minutes
```

What does this indicate?

### Alternatives

- A. The stage is performing efficiently with minimal I/O.
- B. Spark is writing more data to disk than memory, indicating serialization
  corruption.
- C. Spark had to spill shuffle data from memory to disk.
- D. Disk storage is corrupted because disk spill is smaller than memory spill.

### Answer

The expected answer is **C. Spark had to spill shuffle data from memory to
disk**.

`Shuffle Spill (Memory)` represents the deserialized size of data that could
not remain in execution memory. `Shuffle Spill (Disk)` represents the
serialized amount written to disk, so the disk value can legitimately be
smaller than the memory value.

### Local Lab 2B mapping

| Question clue | sparkMeasure field | Interpretation |
|---|---|---|
| high GC time | `jvmGCTime` | memory pressure may be present |
| shuffle spill memory | `memoryBytesSpilled` | execution memory could not retain all shuffle data |
| shuffle spill disk | `diskBytesSpilled` | serialized spill reached disk |
| expensive shuffle | `shuffleBytesWritten`, `shuffleTotalBytesRead` | data crossed stage boundaries |
| distributed work | `executorRunTime` | executors spent material time running tasks |
| partition shape | `numTasks` | task volume may need investigation |

Zero spill is also valid evidence. It means the current run does not support a
spill diagnosis, even when shuffle bytes are non-zero.

## Lab 2C: diagnosing high-end task skew

### Question

A data engineer is analyzing the Summary Metrics table in the Spark UI for a
completed stage with 27 tasks.

Observed metrics:

| Metric | 75th percentile | Max |
|---|---:|---:|
| Duration | 3 s | 30 s |
| Input Size | ~8 KiB | 803 KiB |
| Records | ~67 | 276 |

Which conclusion can the data engineer draw from the above statistics?

### Alternatives

- A. All tasks are operating over partitions with even amounts of data.
- B. A number of tasks are operating over empty or near-empty partitions.
- C. A number of tasks are operating over partitions with larger skewed
  amounts of data.

### Answer

The expected answer is **C. A number of tasks are operating over partitions
with larger skewed amounts of data**.

The key clue is the gap between a typical upper-quartile task and the maximum
task:

```text
max duration >> p75 duration
max data volume >> p75 data volume
```

This is the high-end pattern for one or a few stragglers.

### Local Lab 2C mapping

The generated retail data contains a hot vendor. Lab 2C keeps that key visible
through a shuffle join and aggregation, then uses TaskMetrics to compare task
percentiles inside the selected 27-task stage.

| Question clue | Local TaskMetrics field | Interpretation |
|---|---|---|
| `Duration max >> p75` | `duration` | one or a few tasks are stragglers |
| `Input Size max >> p75` | `shuffleTotalBytesRead` or `bytesRead` | one task processed much more data |
| `Records max >> p75` | `shuffleRecordsRead` or `recordsRead` | one task handled many more rows |
| high-end gap | `max / p75` | practical skew indicator |

The source question calls the volume field `Input Size`. In this local shuffle
stage, the clearest equivalent is normally Shuffle Read. Spark UI Summary
Metrics populate different volume columns depending on whether the stage scans
files or consumes shuffle output; the reasoning remains the same.

Validated local evidence:

```text
shuffleTotalBytesRead p75=617,822 B
shuffleTotalBytesRead max=28,946,218 B
max / p75≈46.85x
duration max / p75≈8.96x
```

## Lab 2D: diagnosing near-empty partitions

### Question

A data engineer is analyzing a Spark job through the Spark UI. They have the
following Summary Metrics for 27 completed tasks in one stage.

Observed metrics:

| Metric | Min | 25th percentile | Median | 75th percentile | Max |
|---|---:|---:|---:|---:|---:|
| Duration | 0.1 ms | 2 s | 2 s | 3 s | 3 s |
| GC Time | 0.0 ms | 0.0 ms | 44.0 ms | 51.0 ms | 0.2 s |
| Input Size / Records | 3 KiB / 16 | 179 KiB / 49 | 183.9 KiB / 55 | 186.7 KiB / 67 | 187 KiB / 276 |
| Shuffle Write Size / Records | 0.4 KiB / 16 | 6.1 KiB / 49 | 6.3 KiB / 55 | 8.1 KiB / 67 | 8.8 KiB / 276 |

Which conclusion can the data engineer draw from the above statistics?

### Alternatives

- A. A number of tasks are operating over near-empty partitions.
- B. All tasks are operating over empty or near-empty partitions.
- C. A number of tasks are operating over partitions with larger skewed
  amounts of data.
- D. All tasks are operating over partitions with even amounts of data.

### Answer

The expected answer is **A. A number of tasks are operating over near-empty
partitions**.

The key clue is at the low end:

```text
min duration << median duration
```

The high end also helps rule out Lab 2C's pattern:

```text
max duration ~= p75 duration
```

This points to one or a few tasks doing almost no useful work, rather than one
dominant high-end straggler.

### Local Lab 2D mapping

Lab 2D derives a synthetic bucket from `sale_id`, creates more shuffle
partitions than useful buckets, and disables AQE so the low-end distribution is
not coalesced away.

| Question clue | Local TaskMetrics field | Interpretation |
|---|---|---|
| `Duration min << median` | `duration` | a few tasks finish with little work |
| `Input records min << median` | `recordsRead` or `shuffleRecordsRead` | a few tasks process almost no rows |
| `Shuffle records min << median` | `shuffleRecordsRead` or `shuffleBytesWritten` | low-end empty shuffle partitions |
| `Max ~= p75` | `max / p75` near 1 | no strong high-end skew signal |

Validated local evidence from the selected 27-task stage:

```text
empty tasks=4
shuffleRecordsRead min=0
shuffleRecordsRead median=104,633
shuffleRecordsRead max/p75≈1.67x
```

Duration is noisier on the local WSL stack, so data-volume distribution is the
primary evidence for this exercise.

## Final diagnostic contrast

Keep this contrast visible when moving from Lab 2C to Lab 2D:

| Pattern | Distribution clue | Diagnosis |
|---|---|---|
| high-end outlier | `max >> p75` | skewed or straggler task |
| low-end outlier | `min << median`, with a constrained high end | empty or near-empty partition |

The direction of the outlier matters. A large aggregate stage counter alone
cannot distinguish these two patterns; task-level distribution is required.
