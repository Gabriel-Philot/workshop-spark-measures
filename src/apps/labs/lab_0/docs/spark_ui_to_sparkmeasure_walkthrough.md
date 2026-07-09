# Spark UI to sparkMeasure walkthrough

This note is the Spark UI journey for Lab 0C.

Use it after running:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/lab_0c_sparkmeasure_presentation.py
```

Lab 0C creates two comparable applications:

```text
workshop-lab0-sparkmeasure-presentation-native
workshop-lab0-sparkmeasure-presentation-observed
```

Both applications run the same `build_sales_enriched` transformation and write:

```text
s3a://lakehouse/silver/lab0/sales_enriched
```

The difference is observability:

- native mode prints Spark `explain` and runs without sparkMeasure;
- observed mode wraps the same workload with sparkMeasure StageMetrics.

## Teaching frame

Use this sequence:

```text
explain before action
  -> Spark History after action
  -> sparkMeasure summary after action
```

These are different evidence types.

`explain` is a planned view. It is emitted before the Delta write action runs.
In this lab it uses `mode=formatted`, so it focuses on the physical plan:
scans, joins, exchanges, projections, read schemas, and `AdaptiveSparkPlan`.

Spark History is an observed execution view. It exists after Spark has executed
actions and written event logs. It shows the jobs, stages, tasks, SQL/DataFrame
executions, executors, environment, timeline, and per-task details produced by
the real run.

sparkMeasure is also observed execution evidence. It does not replace the UI;
it aggregates selected Spark listener metrics into a compact terminal report
that is easier to compare across runs or reuse in automation.

## 1. Start with the terminal explain

In the Lab 0C terminal output, find:

```text
SPARK_EXPLAIN mode=formatted experiment=lab0-sparkmeasure-presentation-native
== Physical Plan ==
```

Use this to teach what Spark planned before the write action ran.

Important details to point out:

- the source scans come from Delta/Parquet paths under `lakehouse/bronze/retail`;
- the joins are planned as `BroadcastHashJoin`;
- product and vendor dimensions are broadcast;
- the plan is wrapped in `AdaptiveSparkPlan`;
- `isFinalPlan=false` means this is the planned adaptive physical plan, not a
  final post-runtime explanation of every adaptive decision.

Suggested instructor line:

```text
The explain output tells us what Spark planned. It does not tell us the full
runtime distribution of tasks, GC, executor locality, output size, or actual
job timing. For that, we need the execution UI and/or collected metrics.
```

## 2. Open Spark History Server

Open:

```text
http://127.0.0.1:28090
```

Select the native application first:

```text
workshop-lab0-sparkmeasure-presentation-native
```

The validation run for this note used application IDs like:

```text
native:   app-20260709162854-0004
observed: app-20260709162934-0005
```

Your IDs will be different. Use the application names, not the numeric suffix.

## 3. Diagnostic route: do not just browse tabs

For this lab, use the Spark UI as an investigation path:

```text
Jobs
  -> Stages sorted by duration
  -> highest-duration SPARK_WORKLOAD stages
  -> highest-duration Delta/internal stages
  -> SQL/DataFrame sub-execution linked to the workload job
  -> physical plan details
  -> executor/resource context
  -> sparkMeasure aggregate summary
```

The goal is not to click every page. The goal is to show how much evidence the
Spark UI exposes, then show why sparkMeasure is useful: it condenses many of
those runtime counters into a compact report that can be compared, logged, or
persisted.

## 4. Jobs tab: separate workload-boundary work from Delta work

Click `Jobs`.

Look for readable descriptions such as:

```text
SPARK_WORKLOAD | LAB0 | presentation | mode=observed | action=write_sales_enriched
Delta: SPARK_WORKLOAD | LAB0 | presentation | mode=observed | action=write_sales_enriched: Filtering files for query
Delta: SPARK_WORKLOAD | LAB0 | presentation | mode=observed | action=write_sales_enriched: Compute snapshot for version: ...
```

How to read this:

| Description pattern | Meaning |
| --- | --- |
| `SPARK_WORKLOAD \| LAB0 \| ... \| action=write_sales_enriched` | Jobs triggered inside the workload boundary we named. This is not a guarantee of one Spark job; one action can create multiple jobs. |
| `Delta: SPARK_WORKLOAD \| LAB0 \| ...: Filtering files for query` | Delta Lake file filtering / scan preparation around that workload boundary. |
| `Delta: ... Compute snapshot for version: ...` | Delta Lake transaction log / snapshot metadata work. |
| Java/Scala frame suffixes such as `DatabricksLogging.scala`, `CompletableFuture.java`, `NativeMethodAccessorImpl.java` | Runtime call-site details. Useful context, but not the business name. |

What this tab teaches:

- Spark has more jobs than the single business transformation suggests;
- Delta metadata work appears as separate jobs;
- each action is connected to submitted jobs and completed stages;
- job descriptions help separate workshop workload jobs from Delta internal
  maintenance jobs.

## 5. Jobs tab: identify the main materialization/write job

Do not identify the important job by its numeric ID. Job IDs change every run.

For Lab 0C, the best UI anchor for the final business output is the
materialization/write job. It is not the whole measured workload. The measured
region can also include Delta snapshot work, file filtering, broadcast
preparation, async SQL sub-executions, commit/statistics work, and the final
write.

Use the row that matches this combination:

| Signal | What to look for |
| --- | --- |
| Prefix | Starts with `SPARK_WORKLOAD \| LAB0 \| presentation`. |
| Not Delta | Does not start with `Delta:`. |
| Action | Contains `action=write_sales_enriched`. |
| Call site | Contains `save at NativeMethodAccessorImpl.java:0` or `DataFrameWriter.save`. |
| Shape | Usually has the compact final write-stage shape, for example `10/10` tasks in this local run. |
| Stage metrics | Its completed stage has both `Input` and `Output` populated. |
| SQL link | Its Job detail page has an `Associated SQL Query` link. |

This is the "main materialization/write job" for the classroom walkthrough.
Its numeric ID changes between runs; the stable identifier is the combination
of description, call site, stage shape, input/output evidence, and the
`Associated SQL Query` link.

Other rows can still start with `SPARK_WORKLOAD` because they were triggered
inside the same workload boundary. For example:

| Row shape | Interpretation |
| --- | --- |
| `SPARK_WORKLOAD ... save at NativeMethodAccessorImpl.java:0` | Main materialization/write job. Open this first to inspect the final output stage. |
| `SPARK_WORKLOAD ... DatabricksLogging.scala` | Delta commit/logging/statistics work that inherited the workload label. Not the main materialization/write job. |
| `SPARK_WORKLOAD ... CompletableFuture.java` | Async/sub-execution work inside the same boundary. Useful context, but not the final write anchor. |
| `Delta: SPARK_WORKLOAD ... Filtering files for query` | Delta file filtering / scan preparation. |
| `Delta: SPARK_WORKLOAD ... Compute snapshot for version` | Delta transaction log / snapshot work. |

This is a useful classroom moment: one workload boundary can create multiple
Spark jobs, and Delta can add its own jobs around the same boundary. The exact
longest job may change between runs. The stable lesson is how to classify the
rows before diagnosing them.

Do not over-interpret the exact job IDs or durations. They depend on Delta
state, whether the output path already existed, and local runtime details. Use
the descriptions and relative evidence.

## 6. Job detail: confirm the final materialization/write anchor

Open the main materialization/write job identified above.

The Job detail page should prove that you chose the correct row:

- when `DAG Visualization` is expanded, the graph shows the final write path
  rather than only metadata work;
- the completed stage row has an `Input` value;
- the completed stage row has an `Output` value;
- the row links to an `Associated SQL Query`;
- the stage has the same `SPARK_WORKLOAD ... save ...` description.

Validated evidence from a recent run:

```text
Description: SPARK_WORKLOAD | LAB0 | presentation | mode=observed | action=write_sales_enriched
Call site: save at NativeMethodAccessorImpl.java:0
Completed stages: 1
Tasks: 10/10
Input: 249.7 KiB
Output: 101.1 MiB
Associated SQL Query: available as a clickable link
```

This is the right job for explaining the final materialization/write path. It
is where students should start before opening stage details and the physical
plan.

Important nuance:

```text
One Spark UI Job is not the same thing as the whole measured workload.
```

If this job shows only one completed stage, that is expected for this local
run. Spark is showing one physical Spark job: the final materialization/write
path. Other work inside the same measured region can appear as separate jobs or
SQL sub-executions, especially Delta snapshot/file filtering and broadcast
preparation. To see the broader read/join/write plan, open the `Associated SQL
Query` and inspect its plan details. To see the aggregate measured region, use
the sparkMeasure output.

## 7. Stages tab: find the dominant stages first

Click `Stages`.

Sort or scan by `Duration`.

What to inspect:

- completed stages;
- skipped stages;
- stage descriptions;
- duration;
- task counts;
- input;
- output;
- shuffle read;
- shuffle write.

Validated stage examples from a recent observed run:

```text
8 s   | 10/10 tasks | SPARK_WORKLOAD | save at NativeMethodAccessorImpl.java:0 | input and output populated
6 s   | 2/2 tasks   | Delta internal | Compute snapshot for version: ...
5 s   | 50/50 tasks | Delta internal | Compute snapshot for version: ...
0.8 s | 50/50 tasks | Delta internal | Filtering files for query
```

This is the correct diagnostic move: the first question is not "which tab do I
open?", but "where did the time go?"

The observed run can have two different stories:

- `SPARK_WORKLOAD` stages show the work triggered by the lab boundary;
- `Delta:` stages show table-format work such as snapshot and file filtering;
- the top stage can be either workload work or Delta work depending on local
  state and whether the output table already existed.

That distinction is exactly why naming the workload boundary matters.

## 8. Stage detail: inspect the highest-duration Delta/internal stage

Open a high-duration stage whose description starts with `Delta:`.

Example evidence from one validation run:

```text
Duration: 8 s
Total Time Across All Tasks: 14 s
Tasks: 50
Locality Level Summary: Node local: 26; Process local: 24
Shuffle Read Size / Records: 20.2 KiB / 55
Shuffle Write Size / Records: 5.9 KiB / 50
Task duration percentiles: 49 ms, 57 ms, 63 ms, 90 ms, 3 s
GC Time max: 51 ms
```

Interpretation:

- this kind of stage is not processing the 5 million sales rows directly;
- it is Delta snapshot / metadata work;
- it has many small tasks and tiny shuffle volumes;
- the stage is useful for understanding framework overhead, not the business
  transformation itself.

Suggested instructor line:

```text
The UI tells us that not every expensive-looking stage is the business
transformation. Before blaming our Spark transformation, we need to separate
workload-boundary work from table-format work.
```

## 9. Stage detail: inspect the main materialization/write stage

From the main materialization/write job detail page, open its completed stage.
Do not look for a fixed stage number; look for the stage associated with:

```text
SPARK_WORKLOAD | LAB0 | presentation | mode=observed | action=write_sales_enriched
save at NativeMethodAccessorImpl.java:0
```

What to inspect in the stage detail page:

- status;
- submitted time;
- duration;
- associated SQL query;
- completed stages;
- stage input/output/shuffle columns;
- DAG visualization.

Validated evidence from a recent run:

```text
Duration: 8 s
Total Time Across All Tasks: about 25 s
Tasks: 10
Locality Level Summary: Process local: 10
Input Size / Records: 249.7 KiB / 5,000,000
Output Size / Records: 101.1 MiB / 5,000,000
Task duration percentiles: 0.5 s, 2 s, 3 s, 3 s, 4 s
GC Time percentiles: 4 ms, 38 ms, 44 ms, 57 ms, 90 ms
```

Interpretation:

- this is the final materialization/write stage for the business output;
- it processed 5 million rows and wrote 101.1 MiB;
- other jobs and SQL sub-executions can still be part of the measured workload
  that fed this final stage;
- there is no obvious GC pressure in this small local run;
- the stage has only 10 tasks, so the per-task table is still easy to inspect;
- sparkMeasure later compresses this type of evidence into aggregate counters
  such as `numStages`, `numTasks`, `executorRunTime`, and shuffle metrics.

This is the UI view that sparkMeasure StageMetrics intentionally does not
replace. The UI is still better for seeing task distribution, executor
assignment, logs, and per-task outliers. sparkMeasure is better for quickly
extracting aggregate evidence from the workload boundary.

## 10. SQL / DataFrame tab: map the workload job to the physical plan

From the main materialization/write Job detail page, click the `Associated SQL
Query` link. This is more reliable than memorizing query IDs from a previous
run.

The useful query page should have these signs:

| Signal | What it means |
| --- | --- |
| Description starts with `SPARK_WORKLOAD \| LAB0 ... action=write_sales_enriched` | Same named workload boundary. |
| Succeeded Jobs includes the main materialization/write job you just opened | You are looking at the SQL execution connected to that final write path. |
| `Plan Visualization` includes scan/join/write operators | This is not only a Delta metadata query. |
| `Plan Details` contains `Scan parquet`, `BroadcastHashJoin`, `Project`, and output rows | This is the physical plan for the business transformation. |

If you start from the `SQL / DataFrame` tab instead, avoid relying on a fixed
query number. First open the top-level `SPARK_WORKLOAD` query, then use its
sub-executions to find the one whose `Succeeded Jobs` contains the
main materialization/write job.

The top-level command may look like this:

```text
Execute SaveIntoDataSourceCommand
Sub Executions: ...
```

Do not stop there if it only shows the command wrapper. For this Delta write,
the useful read/join/write plan is in the sub-execution linked back to the
main materialization/write job.

Validated plan evidence from the materialization/write SQL query:

```text
AdaptiveSparkPlan
Project
BroadcastHashJoin
number of output rows: 5,000,000
BroadcastHashJoin
number of output rows: 5,000,000
Scan parquet
number of files read: 114
size of files read: 727.2 MiB
number of output rows: 5,000,000
number of partitions read: 100
BroadcastExchange
number of output rows: 100
Scan parquet
number of files read: 16
number of output rows: 100
BroadcastExchange
number of output rows: 2,000
Scan parquet
number of files read: 16
number of output rows: 2,000
```

What this teaches:

- the business plan is a scan of sales plus broadcast joins with small
  dimensions;
- the 5 million row signal comes from the sales side;
- the dimension tables are small enough to broadcast in this local demo;
- the UI plan gives context that sparkMeasure does not try to explain by
  itself.

This is where the UI complements terminal `explain`.

Terminal `explain` shows the planned physical plan before the action. SQL /
DataFrame detail shows the executed query context after the action is recorded,
including the linked jobs and the visual plan graph. These views are related,
but they are not the same artifact.

Suggested instructor line:

```text
The terminal plan helps us reason before execution. The SQL/DataFrame tab helps
us connect the executed query to jobs and stages after execution.
```

## 9. Executors tab: validate resource pressure context

Click `Executors`.

What to inspect:

- active executors;
- driver row;
- cores;
- total tasks;
- task time and GC time;
- input;
- shuffle read/write;
- executor logs.

Validated observed run:

```text
active executors including driver: 3
worker executors:                  2
total cores:                       4
total tasks:                       325
task time:                         about 1.7-1.8 min
GC time:                           about 1-2 s
failed tasks:                      0
storage memory used:               0.0 B
disk used:                         0.0 B
shuffle read/write:                about 100 KiB / 100 KiB
```

Interpretation:

- there is no strong executor pressure signal in this Lab 0 run;
- GC is small compared with total task time;
- there are no failed tasks;
- disk used is zero at executor summary level;
- this screen gives environment context, while sparkMeasure gives a compact
  workload metric summary.

This tab is useful when explaining why local results are environment-specific.
The workshop runs on a small local cluster; the same Spark plan can behave
differently with more executors, different memory, or different storage.

## 10. Environment tab: prove runtime configuration

Click `Environment`.

What to inspect:

- `spark.app.id`;
- `spark.app.name`;
- `spark.eventLog.enabled`;
- `spark.eventLog.dir`;
- Delta/S3A/Spark properties;
- driver host;
- executor settings.

This tab is the audit view for the application configuration. It helps students
connect the observed run to the platform settings used by the workshop.

## 11. Compare with sparkMeasure output

Return to the terminal and find the observed run summary:

```text
SPARKMEASURE_METRICS experiment=lab0-sparkmeasure-presentation-observed numStages=... numTasks=... executorRunTime=... shuffleBytesWritten=...
```

Validated example:

```text
numStages=15
numTasks=325
executorRunTime≈60s
shuffleBytesWritten≈100 KiB
```

Now connect the two views:

| Question | Spark UI / explain | sparkMeasure StageMetrics |
| --- | --- | --- |
| What did Spark plan? | Terminal `SPARK_EXPLAIN` and SQL/DataFrame plan views. | Not the primary tool. |
| Which jobs and stages ran? | Jobs, Job detail, Stages, SQL/DataFrame. | Aggregated `numStages`, `numTasks`. |
| Which work is the named workload boundary and which work is Delta? | Job/stage descriptions: `SPARK_WORKLOAD \| ...` versus `Delta: ...`. | Only if the workload boundary and app naming are clear. |
| Which stage was the most expensive? | Sort/scan Stages by `Duration`, then open stage detail. | StageMetrics gives aggregate totals, not a UI ranking table. |
| Where are task distributions visible? | Stage detail task table and percentiles. | Not in StageMetrics aggregate; use TaskMetrics for task-level collection. |
| What was the total executor runtime? | Visible across stage/executor pages, but requires navigation. | Direct aggregate field. |
| What was the shuffle signal? | Stage and executor shuffle columns. | Direct aggregate fields such as `shuffleBytesWritten` and `shuffleTotalBytesRead` when emitted. |
| What runtime config was used? | Environment tab. | Not the primary tool. |
| Can this feed automation? | Harder; mostly UI/event-log oriented. | Easier; compact programmatic metrics. |

## Conclusion

Spark UI and sparkMeasure are complementary.

Spark UI is the investigation surface. It is better for visual navigation,
query-plan inspection, executor context, per-task inspection, logs, environment
properties, and understanding how a workload decomposed into jobs and stages.

sparkMeasure is the compact measurement surface. It collects Spark listener
metrics around a chosen workload boundary and gives engineers a reusable summary
that can be logged, compared, persisted, or turned into later guardrails.

The correct mental model for this workshop is:

```text
explain = what Spark planned before the action
Spark UI = what Spark recorded during and after execution
sparkMeasure = selected execution metrics summarized for diagnosis and automation
```

Do not teach sparkMeasure as a replacement for the Spark UI. Teach it as the
first diagnostic layer that makes the UI journey faster and more repeatable.
