# Lab 2C class notes: task duration skew diagnosis

## Source question inspiration

This lab was inspired by a certification-style Spark UI question about Summary
Metrics for one completed stage with 27 tasks.

### Question: Analyzing Summary Metrics for data skew

A data engineer is analyzing the Summary Metrics table in the Spark UI for a
completed stage with 27 tasks.

Observed metrics:

| Metric | 75th percentile | Max |
|---|---:|---:|
| Duration | 3 s | 30 s |
| Input Size | ~8 KiB | 803 KiB |
| Records | ~67 | 276 |

Which conclusion can the data engineer draw from the above statistics?

#### Alternatives

- A. All tasks are operating over partitions with even amounts of data.
- B. A number of tasks are operating over empty or near-empty partitions.
- C. A number of tasks are operating over partitions with larger skewed amounts
  of data.

#### Answer

The expected answer is **C. A number of tasks are operating over partitions with
larger skewed amounts of data**.

The key clue is the gap between the upper quartile and the maximum task:

```text
max duration >> 75th percentile duration
max input size >> 75th percentile input size
```

That is the Spark UI pattern for one or a few straggler tasks.

## Local workshop adaptation

The local lab uses generated retail Delta data:

- `s3a://lakehouse/bronze/retail/sales`
- `s3a://lakehouse/bronze/retail/vendors`

The generator already creates a hot vendor, so the lab uses a `vendor_id` join
and aggregation to create a task distribution where one task can process much
more data than the 75th percentile task.

The app writes a small Gold summary:

```text
s3a://lakehouse/gold/lab2/task_skew/task
```

The output grain is:

```text
vendor_id, vendor_region
```

## Why this lab uses TaskMetrics

StageMetrics is useful for aggregate symptoms:

- high stage duration;
- high executor runtime;
- high shuffle read/write;
- high total task count.

But the source question is about distribution inside one completed stage.
StageMetrics does not tell us whether `max >> p75`; it only gives aggregate
counters. Lab 2A and Lab 2B already cover that stage-level view, so Lab 2C keeps
the implementation task-only.

For that reason, this lab uses sparkMeasure TaskMetrics and creates a compact
Summary Metrics-style table from `collector.create_taskmetrics_DF(...)`.

## How to read the Lab 2C output

Run the task config:

```python
CONFIG_NAME = "lab2c-task-skew-task"
```

The task run prints the normal sparkMeasure TaskMetrics report and one boxed
classroom diagnostic report derived from the TaskMetrics DataFrame:

```text
LAB 2C TASKMETRICS DIAGNOSTIC REPORT
Selected stage
Metric summary
Top task outliers by shuffleTotalBytesRead
```

The boxed report is not a new measurement. It is a compact projection of
the sparkMeasure TaskMetrics DataFrame in the same shape as the Spark UI Summary
Metrics table from the source question.

## Mapping to the simulated Spark UI question

| Source question clue | Local sparkMeasure field | Interpretation |
|---|---|---|
| `Duration max >> p75` | `duration` | one or a few tasks are stragglers |
| `Input Size max >> p75` | `shuffleTotalBytesRead` or `bytesRead` | one task processed much more data |
| `Records max > p75` | `shuffleRecordsRead` or `recordsRead` | one task handled more rows |
| high-end gap | `max_to_p75` | practical skew indicator |

The source question uses Spark UI `Input Size`. In this local workload, the
stronger signal may appear in shuffle metrics because the skew is created around
a shuffle join/aggregation. Treat `shuffleTotalBytesRead` as the local
"task processed much more data" signal when that is the clearest field.

Call this out explicitly in class:

```text
The simulated question labels the data-volume column as Input Size.
This lab creates the skew inside a shuffle-heavy join/aggregation, so the
equivalent evidence appears as Shuffle Read. The reasoning is unchanged:
compare the 75th percentile task with the max task and look for a large gap.
```

This distinction is useful for students because Spark UI Summary Metrics change
which data-volume columns are populated depending on the stage type. Scan stages
usually populate input metrics; shuffle stages usually populate shuffle read or
shuffle write metrics.

## Lab code shape

The workload intentionally keeps the hot key visible:

```python
return (
    inputs["sales"]
    .transform(_select_task_skew_sales)
    .repartition(shuffle_partitions, "vendor_id")
    .transform(
        _join_task_skew_vendors,
        inputs["vendors"].transform(_select_task_skew_vendors),
        shuffle_partitions,
    )
    .transform(_select_task_skew_fact)
    .transform(_aggregate_task_skew_vendor_summary)
)
```

Broadcast joins and AQE are disabled in the Lab 2C configs so the classroom run
keeps the hot-key shuffle visible:

```yaml
spark.sql.shuffle.partitions: 27
spark.sql.adaptive.enabled: false
spark.sql.autoBroadcastJoinThreshold: -1
```

The `27` shuffle partition count intentionally mirrors the source question's 27
completed tasks.

## Interpreting the result

Use this decision rule:

```text
If max is much larger than p75 for duration and data volume, diagnose high-end
task skew.
```

For this validated run, the data-volume evidence is:

```text
shuffleTotalBytesRead p75=617,822 B
shuffleTotalBytesRead max=28,946,218 B
max / p75 ~= 46.85x
```

That is the local equivalent of the source question's `Input Size max >> p75`
clue.

Do not confuse this with empty partitions:

```text
Empty partition pattern: min << median, but max ~= p75.
Task skew pattern: max >> p75.
```

## Discussion fix

This lab is intentionally diagnostic. A fix is not required for the main lesson.

Possible remediation topics:

- salting a hot join key;
- two-step aggregation;
- processing the dominant key separately;
- enabling AQE skew handling in a production environment.

Keep the first classroom objective focused: read the task distribution and
identify why the source-question answer is skew.

## Validated local runs

Validated on the local WSL stack with two Spark workers and generated
`SCALE=xs` data:

```text
GENERATOR_VOLUME table=sales rows=5000000 files=114 total_bytes=762625975
GENERATOR_VALIDATION_OK sales_rows=5000000 hot_vendor_share=0.7001 sales_files=114
```

| Config | Selected stage | Tasks | Main skew metric | p75 | Max | Max / p75 |
|---|---:|---:|---|---:|---:|---:|
| `lab2c-task-skew-task` | 12 | 27 selected-stage tasks | `duration` | 227 ms | 2033 ms | 8.96 |
| `lab2c-task-skew-task` | 12 | 27 selected-stage tasks | `shuffleTotalBytesRead` | 617,822 B | 28,946,218 B | 46.85 |
| `lab2c-task-skew-task` | 12 | 27 selected-stage tasks | `shuffleRecordsRead` | 75,904 | 3,561,478 | 46.92 |

The task-level run selected the skewed 27-task stage and printed this compact
classroom report:

```text
LAB 2C TASKMETRICS DIAGNOSTIC REPORT
Selected stage
stageId=12 | tasks=27 | dataMetric=shuffleTotalBytesRead | dataMaxToP75=46.8520x | durationMaxToP75=8.9559x

Metric summary
metric                              p75            max    max/p75 interpretation
duration                         227 ms        2033 ms    8.9559x moderate skew signal
executorRunTime                  181 ms        1983 ms   10.9558x strong skew signal
shuffleRecordsRead                75904        3561478   46.9208x strong skew signal
shuffleTotalBytesRead         603.3 KiB       27.6 MiB   46.8520x strong skew signal
recordsWritten                        5              8    1.6000x low skew signal

Top 5 task outliers by shuffleTotalBytesRead
 #   task  exec       dur   shufRows     shufRead   memSpill  diskSpill
 1    236     0   2033 ms    3561478     27.6 MiB   80.0 MiB   16.4 MiB
```

## Instructor note

Task-level percentiles can be noisy on a small local WSL stack. Focus on the
shape of the distribution, not exact milliseconds. The classroom target is the
same reasoning as the simulated question: compare the 75th percentile to the
maximum task.

The total task count can vary across reruns because Delta overwrite work may
change once the output path already exists. The selected 27-task shuffle stage
is the stable classroom signal because it mirrors the source question's Summary
Metrics table.

## Footnote: how the TaskMetrics log block is built

The boxed report is a classroom formatting layer on top of sparkMeasure
TaskMetrics. It does not create new Spark measurements.

The required pieces are:

- an active SparkSession;
- sparkMeasure `TaskMetrics`, started with `begin()` and stopped with `end()`;
- the TaskMetrics DataFrame created by
  `collector.create_taskmetrics_DF(...)`;
- Spark SQL aggregations to compute `p75`, `max`, and `max / p75`;
- one multiline project logger call to keep the output readable in
  `spark-submit`.

The runtime keeps `collector.print_report()` enabled so students can still see
the native sparkMeasure output. The boxed report appears immediately after it
and turns the raw task rows into the Summary Metrics shape used by the source
question.

Focused runtime flow:

```python
def log_task_skew_summary(spark: Any, collector: Any) -> None:
    """Create the classroom TaskMetrics report from sparkMeasure task rows."""

    task_metrics = collector.create_taskmetrics_DF(TASK_SKEW_METRICS_VIEW)
    stage = select_task_skew_stage(task_metrics)
    if not stage:
        logger.warning("LAB2C_TASK_STAGE_SELECTED none")
        return

    summaries = []
    for metric in TASK_SKEW_SUMMARY_METRICS:
        if metric not in task_metrics.columns:
            continue
        summary = summarize_task_metric(task_metrics, int(stage["stageId"]), metric)
        if summary and float(summary.get("max", 0) or 0) > 0:
            summaries.append(summary)

    outliers = collect_task_skew_outliers(spark, task_metrics, int(stage["stageId"]))
    logger.info(render_task_skew_report(stage, summaries, outliers))
```

The selected stage is the stage with the clearest high-end data-volume skew. In
this lab, the preferred data signal is `shuffleTotalBytesRead` because the
workload is shuffle-heavy:

```python
def select_task_skew_stage(task_metrics: Any) -> dict[str, Any] | None:
    """Find the stage where task-level max-vs-p75 skew is easiest to teach."""

    # Condensed for class notes. The runtime also handles missing/zero values.
    stage_stats = (
        task_metrics.groupBy("stageId")
        .agg(
            F.count("*").cast("long").alias("taskCount"),
            F.max(F.col("duration").cast("double")).alias("maxDuration"),
            F.expr("percentile_approx(cast(duration as double), 0.75, 10000)").alias(
                "p75Duration"
            ),
            F.max(F.col("shuffleTotalBytesRead").cast("double")).alias("maxData"),
            F.expr(
                "percentile_approx(cast(shuffleTotalBytesRead as double), 0.75, 10000)"
            ).alias("p75Data"),
        )
        .withColumn(
            "durationMaxToP75",
            F.when(F.col("p75Duration") > 0, F.col("maxDuration") / F.col("p75Duration")),
        )
        .withColumn(
            "dataMaxToP75",
            F.when(F.col("p75Data") > 0, F.col("maxData") / F.col("p75Data")),
        )
    )

    return stage_stats.orderBy(F.desc("dataMaxToP75")).first().asDict()
```

Each metric row in the boxed report is computed with the same classroom rule:
compare the 75th percentile with the maximum task.

```python
def summarize_task_metric(
    task_metrics: Any,
    stage_id: int,
    metric: str,
) -> dict[str, Any] | None:
    """Summarize one TaskMetrics column using Spark UI Summary Metrics shape."""

    # Condensed for class notes. The runtime also includes min/p25/median.
    row = (
        task_metrics.filter(F.col("stageId") == stage_id)
        .agg(
            F.count("*").cast("long").alias("taskCount"),
            F.expr(f"percentile_approx(cast({metric} as double), 0.75, 10000)").alias(
                "p75"
            ),
            F.max(F.col(metric).cast("double")).alias("max"),
        )
        .first()
    )

    summary = row.asDict()
    summary["metric"] = metric
    summary["maxToP75"] = summary["max"] / summary["p75"]
    return summary
```

The outlier table is intentionally small. It orders tasks by the data-volume
metric and keeps only the top rows, so the instructor can point at one dominant
task without flooding the terminal:

```python
def collect_task_skew_outliers(
    spark: Any,
    task_metrics: Any,
    stage_id: int,
) -> list[Mapping[str, Any]]:
    """Collect the top task outliers for classroom display."""

    rows = (
        task_metrics.filter(F.col("stageId") == stage_id)
        .select("stageId", "index", "duration", "shuffleRecordsRead", "shuffleTotalBytesRead")
        .orderBy(F.desc("shuffleTotalBytesRead"), F.desc("duration"))
        .limit(5)
        .collect()
    )
    return [row.asDict() for row in rows]
```

Finally, the terminal block is pure Python formatting. The important part is
that it returns one multiline string and sends it through a single
`logger.info(...)` call:

```python
def render_task_skew_report(
    stage: Mapping[str, Any],
    summaries: list[Mapping[str, Any]],
    outliers: list[Mapping[str, Any]],
) -> str:
    """Return one classroom-friendly block with the Lab 2C TaskMetrics signal."""

    box = _ReportBox("LAB 2C TASKMETRICS DIAGNOSTIC REPORT", width=112)
    box.text("SparkMeasure collector=TaskMetrics | purpose=max-vs-p75 task skew diagnosis")
    box.rule()
    box.text("Selected stage")
    box.text("stageId=... | tasks=... | dataMetric=... | dataMaxToP75=...")
    box.rule()
    box.text("Metric summary")
    box.rule()
    box.text("Top task outliers by shuffleTotalBytesRead")
    return "\n" + box.render()
```

This is the tradeoff: native sparkMeasure output remains available, but the
lesson also gets a compact, repeatable, workshop-friendly reading of the same
TaskMetrics data.
