# Lab 2D class notes: empty partitions diagnosis

## Source question inspiration

This lab was inspired by a certification-style Spark UI question about Summary
Metrics for one completed stage with 27 tasks.

### Question: Analyzing Summary Metrics for empty partitions

A data engineer is analyzing a Spark job via the Spark UI. They have the
following summary metrics for 27 completed tasks in a particular stage.

Observed metrics:

| Metric | Min | 25th percentile | Median | 75th percentile | Max |
|---|---:|---:|---:|---:|---:|
| Duration | 0.1 ms | 2 s | 2 s | 3 s | 3 s |
| GC Time | 0.0 ms | 0.0 ms | 44.0 ms | 51.0 ms | 0.2 s |
| Input Size / Records | 3 KiB / 16 | 179 KiB / 49 | 183.9 KiB / 55 | 186.7 KiB / 67 | 187 KiB / 276 |
| Shuffle Write Size / Records | 0.4 KiB / 16 | 6.1 KiB / 49 | 6.3 KiB / 55 | 8.1 KiB / 67 | 8.8 KiB / 276 |

Which conclusion can the data engineer draw from the above statistics?

#### Alternatives

- A. A number of tasks are operating over near-empty partitions.
- B. All tasks are operating over empty or near-empty partitions.
- C. A number of tasks are operating over partitions with larger skewed amounts
  of data.
- D. All tasks are operating over partitions with even amounts of data.

#### Answer

The expected answer is **A. A number of tasks are operating over near-empty
partitions**.

The key clue is the low-end gap:

```text
min duration << median duration
```

The second clue rules out the Lab 2C skew pattern:

```text
max duration ~= 75th percentile duration
```

That means the issue is not a long-running task at the high end. It is one or a
few tasks at the low end with almost no useful work.

## Local workshop adaptation

The local lab uses generated retail Delta data:

- `s3a://lakehouse/bronze/retail/sales`

The workload intentionally creates more shuffle partitions than useful
partition buckets. It uses a synthetic bucket derived from `sale_id`, not
`vendor_id`, so the lesson does not inherit the hot-vendor skew from Lab 2C.

The app writes a small Gold summary:

```text
s3a://lakehouse/gold/lab2/empty_partitions/task
```

The output grain is:

```text
partition_bucket
```

## Why this lab uses TaskMetrics

StageMetrics can show aggregate symptoms such as total task count and total
executor runtime. It cannot prove that the anomaly is at the low end of the task
distribution.

The source question is specifically about Summary Metrics distribution:

- `min`;
- `25th percentile`;
- `median`;
- `75th percentile`;
- `max`.

For that reason, this lab uses sparkMeasure TaskMetrics and creates a compact
boxed report from `collector.create_taskmetrics_DF(...)`.

## How to read the Lab 2D output

Run the task config:

```python
CONFIG_NAME = "lab2d-empty-partitions-task"
```

The task run prints the normal sparkMeasure TaskMetrics report and one boxed
classroom diagnostic report derived from the TaskMetrics DataFrame:

```text
LAB 2D TASKMETRICS EMPTY PARTITIONS REPORT
Selected stage
Metric summary
Lowest task outliers by shuffleRecordsRead
```

The boxed report is not a new measurement. It is a compact projection of the
sparkMeasure TaskMetrics DataFrame in the same shape as the Spark UI Summary
Metrics table from the source question.

## Mapping to the simulated Spark UI question

| Source question clue | Local sparkMeasure field | Interpretation |
|---|---|---|
| `Duration min << median` | `duration` | one or a few tasks finished with very little work |
| `Input records min << median` | `recordsRead` or `shuffleRecordsRead` | one or a few tasks processed almost no rows |
| `Shuffle records min << median` | `shuffleRecordsRead` or `shuffleBytesWritten` | low-end empty shuffle partitions |
| `Max ~= p75` | `max_to_p75` near 1 | not a high-end skew pattern |

The source question includes both input and shuffle-write metrics. In this local
workload, the clearest field can be `shuffleRecordsRead` because the empty
partition symptom is created around a shuffle partitioning step.

Call this out explicitly in class:

```text
The previous lab diagnosed high-end skew with max >> p75.
This lab diagnoses low-end empty partitions with min << median.
The direction of the outlier matters.
```

## Lab code shape

The workload intentionally keeps the empty-partition setup visible:

```python
return (
    inputs["sales"]
    .transform(_select_empty_partition_sales)
    .transform(_add_empty_partition_bucket, active_buckets)
    .repartition(shuffle_partitions, "partition_bucket")
    .transform(_aggregate_empty_partition_sales_summary)
)
```

The default classroom config uses 27 shuffle partitions to mirror the source
question and 48 active buckets to leave only a small number of partitions empty
on the local Spark hash distribution:

```yaml
shuffle_partitions: 27
active_buckets: 48
spark.sql.adaptive.enabled: false
```

AQE is disabled so Spark does not coalesce away the partition distribution that
the lesson is trying to show.

## Interpreting the result

Use this decision rule:

```text
If data-volume min is much smaller than median, inspect low-end task outliers.
If data-volume max is close to p75, do not diagnose high-end skew.
```

Do not confuse this with task skew:

```text
Empty partition pattern: min << median, but max ~= p75.
Task skew pattern: max >> p75.
```

## Validated local runs

Validated on the local WSL stack with two Spark workers and generated
`SCALE=xs` data:

```text
GENERATOR_VOLUME table=sales rows=5000000 files=114 total_bytes=762625975
GENERATOR_VALIDATION_OK sales_rows=5000000 hot_vendor_share=0.7001 sales_files=114
```

| Config | Selected stage | Tasks | Empty tasks | Main metric | Min | Median | p75 | Max | Max / p75 |
|---|---:|---:|---:|---|---:|---:|---:|---:|---:|
| `lab2d-empty-partitions-task` | 9 | 27 | 4 | `shuffleRecordsRead` | 0 | 104,633 | 311,706 | 521,959 | 1.67 |
| `lab2d-empty-partitions-task` | 9 | 27 | 4 | `shuffleTotalBytesRead` | 0 B | 831.9 KiB | 3.0 MiB | 5.4 MiB | 1.79 |
| `lab2d-empty-partitions-task` | 9 | 27 | 4 | `recordsWritten` | 0 | 1 | 3 | 5 | 1.67 |

The task-level run selected the 27-task stage and printed this compact
classroom report:

```text
LAB 2D TASKMETRICS EMPTY PARTITIONS REPORT
Selected stage
stageId=9 | tasks=27 | emptyTasks=4
dataMetric=shuffleRecordsRead | median/min=∞ | max/p75=1.6745x

Metric summary
metric                            min       median          p75          max   median/min    max/p75
duration                        42 ms       124 ms       153 ms      1043 ms      2.9524x    6.8170x
executorRunTime                 13 ms        85 ms       109 ms       933 ms      6.5385x    8.5596x
shuffleRecordsRead                  0       104633       311706       521959            ∞    1.6745x
shuffleTotalBytesRead             0 B    831.9 KiB      3.0 MiB      5.4 MiB            ∞    1.7880x
recordsWritten                      0            1            3            5            ∞    1.6667x

Lowest 5 task outliers by shuffleRecordsRead
 #   task  exec       dur   shufRows     shufRead recordsOut   memSpill
 1    193     0     42 ms          0          0 B          0        0 B
 2    192     0     45 ms          0          0 B          0        0 B
 3    191     1     48 ms          0          0 B          0        0 B
 4    190     0     55 ms          0          0 B          0        0 B
```

## Instructor note

Task-level percentiles can be noisy on a small local WSL stack. Focus on the
shape of the distribution:

- a zero or near-zero data-volume minimum;
- a normal data-volume median;
- a data-volume max that is not dramatically above p75.

That is the same reasoning as the simulated question. The exact milliseconds are
less important than the relationship between the low-end tasks and the typical
data-volume task. If duration shows a local outlier while records/bytes stay
tight at the high end, explain it as local execution noise rather than data
skew evidence.

## Footnote: how the TaskMetrics log block is built

The boxed report follows the same pattern introduced in Lab 2C:

1. collect native sparkMeasure TaskMetrics;
2. create a TaskMetrics DataFrame;
3. select the stage with the clearest low-end signal;
4. compute Summary Metrics-style rows;
5. emit one multiline block through the project logger.

Focused runtime flow:

```python
def log_empty_partitions_summary(spark: Any, collector: Any) -> None:
    """Create the classroom TaskMetrics report from sparkMeasure task rows."""

    task_metrics = collector.create_taskmetrics_DF(EMPTY_PARTITIONS_METRICS_VIEW)
    stage = select_empty_partitions_stage(task_metrics)
    summaries = [
        summarize_empty_partition_metric(task_metrics, int(stage["stageId"]), metric)
        for metric in EMPTY_PARTITIONS_SUMMARY_METRICS
        if metric in task_metrics.columns
    ]
    outliers = collect_empty_partition_outliers(spark, task_metrics, int(stage["stageId"]))
    logger.info(render_empty_partitions_report(stage, summaries, outliers))
```

The selected stage is the stage where the low-end data-volume gap is easiest to
teach:

```python
def select_empty_partitions_stage(task_metrics: Any) -> dict[str, Any] | None:
    """Select the stage where low-end task outliers are clearest."""

    # Condensed for class notes. The runtime also handles missing/zero values.
    stage_stats = (
        task_metrics.groupBy("stageId")
        .agg(
            F.count("*").cast("long").alias("taskCount"),
            F.min(F.col("shuffleRecordsRead")).alias("minData"),
            F.expr(
                "percentile_approx(cast(shuffleRecordsRead as double), 0.50, 10000)"
            ).alias("medianData"),
            F.expr(
                "percentile_approx(cast(shuffleRecordsRead as double), 0.75, 10000)"
            ).alias("p75Data"),
            F.max(F.col("shuffleRecordsRead")).alias("maxData"),
        )
        .withColumn(
            "dataMedianToMin",
            F.when(F.col("minData") > 0, F.col("medianData") / F.col("minData")),
        )
        .withColumn(
            "dataMaxToP75",
            F.when(F.col("p75Data") > 0, F.col("maxData") / F.col("p75Data")),
        )
    )
```

The important design choice is that the report is a teaching projection of the
same TaskMetrics data. Native sparkMeasure output remains available, but the
boxed report makes the certification-style reasoning explicit.
