# Lab 2A class notes: shuffle aggregation diagnosis

## Source question inspiration

This lab was inspired by the following certification-style question.

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

### Why this answer matters for the lab

A grouped aggregation needs rows with the same grouping key to be co-located. If
the data is not already distributed by `region`, Spark must exchange data across
executors. Repartitioning by the grouping key makes that distribution explicit
and can reduce unnecessary shuffle work in the right workload shape.

The other options do not address the physical distribution required by the
aggregation:

- caching can help repeated reads, but it does not change partitioning;
- `coalesce(1)` after the aggregation happens too late and can create a single
  output bottleneck;
- broadcast join is a join optimization, not an aggregation optimization.

## Local workshop adaptation

The local lab uses generated retail Delta data:

- `s3a://lakehouse/bronze/retail/sales`
- `s3a://lakehouse/bronze/retail/vendors`

The workload joins sales to vendor metadata and writes a Gold aggregate:

```text
s3a://lakehouse/gold/lab2/shuffle_aggregation/baseline
s3a://lakehouse/gold/lab2/shuffle_aggregation/optimized
```

The aggregate grain is:

```text
vendor_region, sale_year_month
```

The output columns are:

```text
vendor_region
sale_year_month
sale_count
total_quantity
gross_sales_amount
average_sale_amount
```

## Connecting the question to the lab code

The original question is small on purpose:

```python
df = spark.read.table("sales")
result = df.groupBy("region").agg(sum("revenue"))
```

The local lab uses a slightly more realistic version of the same idea. Our
`sales` source does not already contain a region column, so the lab first joins
sales with vendor metadata:

```python
inputs["sales"]
    .alias("s")
    .transform(_join_vendor_region, inputs["vendors"].alias("v"))
    .transform(_select_regional_sales_fact)
```

This creates the equivalent of the certification question's `region` field:

```python
F.coalesce(F.col("v.region"), F.lit("UNKNOWN")).alias("vendor_region")
```

Then the lab performs the grouped aggregation:

```python
sales.groupBy("vendor_region", "sale_year_month").agg(...)
```

So the mapping is:

| Certification question | Lab 2A |
|---|---|
| `sales` table | generated Bronze `sales` Delta table |
| `region` | `vendor_region` from the `vendors` table |
| `revenue` | `sale_amount` |
| `groupBy("region")` | `groupBy("vendor_region", "sale_year_month")` |
| diagnose excessive shuffle | inspect sparkMeasure StageMetrics |

The baseline intentionally adds one bad physical decision before the
aggregation:

```python
.repartition(round_robin_partitions)
.transform(_aggregate_regional_monthly_sales)
```

That repartition is not aligned with the grouping keys. It redistributes data,
but it does not tell Spark to colocate rows by `vendor_region` and
`sale_year_month`. In class terms: it creates movement without giving the next
wide operation the layout it wants.

The optimized variant makes the physical intent explicit:

```python
.repartition(keyed_partitions, "vendor_region", "sale_year_month")
.transform(_aggregate_regional_monthly_sales)
```

This mirrors the answer from the certification-style question: repartition by
the key that the aggregation is about to use.

## How baseline sparkMeasure metrics guide the diagnosis

Before changing the code, run the baseline and read the compact
`SPARKMEASURE_METRICS` line plus the aggregated StageMetrics report.

On the latest validated `SCALE=xs` run, the baseline showed:

```text
numStages=14
numTasks=1296
executorRunTime=64829
shuffleBytesWritten=72446904
shuffleTotalBytesRead=72446904
recordsRead=5000367
recordsWritten=4
memoryBytesSpilled=0
diskBytesSpilled=0
```

The diagnosis comes from connecting those numbers to the code:

| sparkMeasure signal | What it says | Where to look in code |
|---|---|---|
| `shuffleBytesWritten≈69.1 MiB` | Spark moved a meaningful amount of data between tasks | wide operations: join, repartition, groupBy |
| `shuffleTotalBytesRead≈69.1 MiB` | downstream tasks consumed that redistributed data | aggregation stages after exchange |
| `numStages=14` | a short script became multiple physical execution stages | Spark plan / History Server stages |
| `numTasks=1296` | too many distributed tasks were needed for a 4-row result | round-robin repartition count and aggregation settings |
| `executorRunTime≈65s` | total executor work is significant even if wall-clock is shorter | expensive distributed stage work |
| `recordsRead≈5M`, `recordsWritten=4` | huge input is reduced to tiny output | aggregation workload shape |
| `memoryBytesSpilled=0`, `diskBytesSpilled=0` | memory spill is not the first suspect | focus on shuffle/distribution first |

This is the key teaching move:

```text
sparkMeasure does not magically point to one line of Python.
It gives enough evidence to decide where to inspect first.
```

For this baseline, the evidence says:

1. The job is dominated by wide distributed work, not by the final write.
2. Shuffle is present and measurable.
3. The output is tiny, so the expensive part is how Spark groups the input.
4. Spill is absent, so this is not primarily a memory-spill lesson.
5. The code contains a repartition that does not match the aggregation keys.

That makes the optimization hypothesis concrete:

```text
Align partitioning with the grouping keys before the aggregation and compare
the stage-level metrics again.
```

After switching to the optimized variant on the same `SCALE=xs` data, the
validated run showed:

| Variant | Stages | Tasks | Executor runtime | Shuffle written |
|---|---:|---:|---:|---:|
| baseline | 14 | 1,296 | ~65s | ~69.1 MiB |
| optimized | 13 | 301 | ~43s | ~50.9 MiB |

The optimized run still has shuffle because grouped aggregation is still a wide
operation. The point is not to remove shuffle entirely. The point is that
sparkMeasure made the physical cost visible before the code change and gave us
a compact before/after comparison after the change.

## Classroom flow

1. Start with `CONFIG_NAME = "lab2-shuffle-aggregation-baseline"`.
2. Run the submit command from `lab_2a_shuffle_aggregation_diagnosis.py`.
3. Read the `SPARKMEASURE_METRICS` line in the terminal.
4. Open the Spark History Server and inspect the application
   `workshop-lab2-shuffle-aggregation-baseline`.
5. Change `CONFIG_NAME` to `lab2-shuffle-aggregation-optimized`.
6. Run the same submit command again.
7. Compare the StageMetrics output and the Spark UI stages.

## What the baseline does

The baseline intentionally performs a round-robin repartition before projecting
the narrow aggregation schema and grouping:

```text
sales + vendors -> repartition(1024) -> regional sales fact -> groupBy(region, month)
```

This is a didactic anti-pattern. It creates extra shuffle work while the rows
are still wider than necessary, before the real grouped aggregation.
It also creates far too many partitions for the `SCALE=xs` data volume, which
makes the scheduler/task overhead visible in `numTasks`.

The point is not that every `repartition()` is bad. The point is that a
partitioning operation that does not match the next wide operation often creates
work that is hard to justify, especially when the partition count is much higher
than the useful parallelism available on the local cluster.

## What the optimized variant does

The optimized variant:

1. selects only columns needed by the aggregation;
2. joins vendor region;
3. repartitions by `vendor_region` and `sale_year_month`;
4. runs the same grouped aggregate.

This keeps the transformation simple enough for the workshop while matching the
certification-style idea: align partitioning with the grouping keys when the
workload benefits from it.

## StageMetrics to compare

Primary signals:

- `shuffleBytesWritten`
- `shuffleTotalBytesRead`
- `executorRunTime`
- `numStages`
- `numTasks`

Secondary signals:

- `memoryBytesSpilled`
- `diskBytesSpilled`
- `jvmGCTime`

For this first Lab 2 lesson, spill and GC are not required success criteria.
They are environment-sensitive on a small WSL/local stack. If they appear, use
them as extra evidence. If they do not appear, keep the lesson centered on
shuffle read/write and executor runtime.

## Why StageMetrics is enough here

This lab diagnoses a stage-level symptom: the workload writes and reads shuffle
data around a grouped aggregation. StageMetrics is the right level for the first
pass because the question is:

```text
Which stage-level operation is expensive?
```

TaskMetrics is not required yet because the lesson is not trying to prove task
distribution skew. Later labs should use TaskMetrics when the question depends
on min/median/75th/max task behavior.

## Expected interpretation

The baseline should show a clear shuffle signal. The optimized variant should
make the shuffle story easier to explain and, depending on data scale and local
resources, may reduce shuffle bytes, executor runtime, or stage count.

If the runtime difference is small on `SCALE=xs`, that is acceptable. The
teaching target is the diagnostic method:

```text
read the code -> inspect StageMetrics -> validate in Spark UI -> explain the
wide operation
```

For a stronger live demo, generate a larger scale before class and document the
tested size in the lab notes.

## Validated local runs

These numbers were captured on the local WSL stack with two Spark workers.

### `SCALE=xs`

Generator output:

```text
sales_rows=5,000,000
sales_files=114
sales_total_bytes≈762 MB
hot_vendor_share=0.7001
```

sparkMeasure StageMetrics comparison:

| Variant | Stages | Tasks | Executor runtime | Shuffle written | Shuffle read |
|---|---:|---:|---:|---:|---:|
| baseline | 14 | 1,296 | ~65s | ~69.1 MiB | ~69.1 MiB |
| optimized | 13 | 301 | ~43s | ~50.9 MiB | ~50.9 MiB |

`SCALE=s` was previously used to stress-test the lab, but these class notes keep
the latest validated numbers on `SCALE=xs` because the baseline was adjusted to
make the wide-row shuffle problem clearer. Re-run `SCALE=s` before using larger
scale numbers in class.

### Instructor note

The optimized variant is intentionally modest but visible. It reduces stages,
tasks, executor runtime, and shuffle bytes, but it does not eliminate shuffle
because the aggregation is still a wide operation. That is useful for the
lesson: students should see sparkMeasure as a diagnostic lens, not as proof that
a small code change magically removes the physical cost of grouping millions of
rows.
