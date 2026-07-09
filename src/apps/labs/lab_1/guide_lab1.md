# Lab 1 guide: global sort diagnosis and task outlier drill-down

This guide is the classroom runbook for Lab 1.

Goal:

```text
prepare local stack
  -> generate shared bronze retail data
  -> diagnose a global sort with stage-level sparkMeasure metrics
  -> switch to task-level metrics when a stage aggregate is too broad
  -> validate the prepared task-outlier fix
```

Lab 1 comes after Lab 0. Lab 0 introduces the source inventory, the native
sparkMeasure API, and the workshop execution contract. Lab 1 uses that contract
for the first real diagnosis story.

Reference notes:

```text
docs/random_task_outlier_class_notes.md
docs/task_metrics_native_api.md
```

Use them when explaining why the first exercise starts with StageMetrics and why
the second exercise temporarily switches to TaskMetrics as a microscope.

## 0. Start from the repository root

```bash
cd workshop-spark-measures
```

Expected:

- `Makefile` exists;
- `.env.example` exists;
- `src/apps/labs/lab_1` exists.

## 1. Mini-bootstrap when starting from zero

If the local stack, images, or generated data were already prepared during Lab
0, you can skip directly to step 2.

Run the full sequence only when starting from a clean machine or when bootstrap
artifacts were removed.

```bash
make bootstrap
make build
make compose
make dry-test
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

Why this sequence exists:

- `make bootstrap` prepares pinned dependencies and local configuration;
- `make build` builds the local Spark, MinIO, and dashboard images;
- `make compose` starts MinIO, Spark Master, Spark Workers, and Spark History;
- `make dry-test` proves Spark, Delta, S3A, MinIO, and sparkMeasure work
  together;
- `make generate` creates the shared bronze retail sources used by Labs 1-6.

Bootstrap note:

```text
make bootstrap usually does not need to be repeated after make down,
make clean-data, or make removeimage.
```

Those commands stop containers, remove local generated data, or remove local
images. They do not normally remove `.env` or the pinned dependency artifacts
prepared by bootstrap. If the bootstrap cache is still present, restart from:

```bash
make build
make compose
make dry-test
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

Useful UIs:

```text
Spark Master UI:      http://127.0.0.1:28091
Spark History Server: http://127.0.0.1:28090
MinIO Console:        http://127.0.0.1:29011
```

Default MinIO credentials:

```text
user:     sparkworkshop
password: sparkworkshop123
```

Teacher notes:

```text
At this point the goal is not diagnosis yet. The goal is to prove that the
local platform exists: containers are up, Spark can talk to MinIO, Delta tables
can be read/written, and sparkMeasure can be loaded.

If something fails here, do not continue to Lab 1. Fix the platform first.
Otherwise the students will confuse environment issues with Spark workload
diagnosis.
```

Expected bronze paths:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/sales
```

## 2. Move to the Lab 1 folder

The next commands are easier to present from the Lab 1 folder, while still using
repository-relative paths for Docker Compose.

```bash
cd src/apps/labs/lab_1
```

Optional sanity check:

```bash
ls
```

Expected scripts:

```text
lab_1a_global_sort_diagnosis.py
lab_1b_random_task_outlier_diagnosis.py
```

Teacher notes:

```text
Use this folder switch to make the lesson feel app-local. The submit command
still references Docker Compose from the repository root, but the files the
students edit are now directly visible in the current folder.
```

## 3. Run 1A: global sort diagnosis

Open:

```text
lab_1a_global_sort_diagnosis.py
```

Start with the native config:

```python
CONFIG_NAME = "lab1-global-sort-diagnosis-native"
```

Then run:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_1/lab_1a_global_sort_diagnosis.py
```

Teacher notes for the native run:

```text
Watch the terminal for SPARK_EXPLAIN. The important plan operators are
Exchange and Sort. This is the native Spark evidence that the ranking operation
is wide and needs global ordering.

In the terminal, the plan is intentionally verbose. That is useful for precise
inspection, but it is not the fastest way to summarize operational pressure in
a classroom or incident review.
```

After the native run, switch the same script to:

```python
CONFIG_NAME = "lab1-global-sort-diagnosis-observed-stage"
```

Run the same submit command again.

Teacher notes for the observed-stage run:

```text
Watch the terminal for SPARKMEASURE_ENABLED=true and SPARKMEASURE_METRICS.
The useful counters are numStages, numTasks, executorRunTime,
shuffleBytesWritten, memoryBytesSpilled, and diskBytesSpilled when present in
the sparkMeasure report.

This run should feel like the same Spark workload, but with a compact
stage-level operational summary added at the end.
```

Why this comes first:

- it is a familiar Spark problem: a global `orderBy` before writing a Gold
  table;
- the native config prints Spark explain output;
- the observed-stage config enables StageMetrics through YAML config;
- students can connect the global sort to stage-level shuffle and runtime
  evidence.

Teacher notes:

```text
The goal of Lab 1A is to show a Spark problem that is visible at stage level:
a global orderBy before the write.

First, run the native config. Use the explain output and Spark UI to show that
the plan contains Exchange + Sort. The native Spark tools have the information,
but the signal is verbose.

Then run the observed-stage config. It is the same workload, but sparkMeasure
condenses the diagnosis into a few counters: numStages, numTasks,
executorRunTime, shuffleBytesWritten, and spill metrics when they appear.

The teaching point is that global sorting forces shuffle/sort work. This class
of symptom is visible enough at StageMetrics level, so the class does not need
TaskMetrics yet.
```

Expected markers:

```text
native run:
  LAB1_GLOBAL_SORT_NATIVE_OK

observed-stage run:
  LAB1_GLOBAL_SORT_SPARKMEASURE_STAGE_OK
  LAB1_GLOBAL_SORT_DIAGNOSIS_OK
```

Expected Gold output:

```text
s3a://lakehouse/gold/lab1/top_sales_global_sort
```

Classroom note:

```text
StageMetrics are enough for this first diagnosis because the expensive behavior
is visible at stage level: global sort means wide transformation, shuffle, and
sort work.
```

## 4. Review 1A in Spark History Server

Open:

```text
http://127.0.0.1:28090
```

Look for applications:

```text
workshop-lab1-global-sort-native
workshop-lab1-global-sort-observed-stage
```

What to inspect:

- `Jobs`: descriptions with the `LAB1` prefix;
- `Stages`: duration, number of tasks, and shuffle columns;
- `SQL / DataFrame`: the physical plan when available.

Teacher notes:

```text
In Jobs, use the LAB1 description to separate the workshop workload from
framework or Delta-related work.

In Stages, look for the expensive stage and inspect shuffle read/write,
duration, task count, and spill columns. The goal is to connect the global
sort from the explain plan to concrete stage-level pressure.

In SQL / DataFrame, use the plan as the bridge between code and metrics:
the query shape explains why shuffle and sort counters appear.
```

Classroom note:

```text
Spark UI gives the detailed investigation surface. sparkMeasure gives the compact
evidence line that makes the first diagnosis pass faster.
```

## 5. Prepare 1B: choose the task-outlier config

Open:

```text
lab_1b_random_task_outlier_diagnosis.py
```

Change only `CONFIG_NAME` when moving through the exercise.

Stage aggregate view:

```python
CONFIG_NAME = "lab1-random-task-outlier-stage"
```

Task diagnostic view:

```python
CONFIG_NAME = "lab1-random-task-outlier-task"
```

Fixed validation view:

```python
CONFIG_NAME = "lab1-random-task-outlier-fixed-task"
```

Why this manual switch is intentional:

- the script stays close to the workshop template;
- the classroom can pause between runs and inspect the terminal and Spark UI;
- the same source and workload story can be observed at different metric
  granularities.

Teacher notes:

```text
This is the first time the class sees StageMetrics as insufficient by itself.
Do not present TaskMetrics as the default collector. Present it as the
microscope used only after the stage aggregate says: something is slow, but the
aggregate is not precise enough.
```

## 6. Run 1B: random task outlier diagnosis

Run this command after selecting the desired `CONFIG_NAME`.

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_1/lab_1b_random_task_outlier_diagnosis.py
```

Recommended classroom sequence:

1. Run with `lab1-random-task-outlier-stage`.
2. Explain what the stage aggregate shows and what it hides.
3. Switch to `lab1-random-task-outlier-task`.
4. Inspect the boxed task-outlier report in the terminal.
5. Switch to `lab1-random-task-outlier-fixed-task`.
6. Compare the worst task and aggregate executor runtime.

Teacher notes by run:

```text
Run 1 - stage config:
  Observe the aggregate stage metrics first. The signal should say that the
  workload is expensive, but it should not clearly identify the single bad
  task. This creates the reason to zoom in.

Run 2 - task config:
  Observe the boxed LAB1_TASK_OUTLIER report. This is the microscope moment:
  task-level metrics reveal which task carried the long tail and how much
  executor runtime it consumed.

Run 3 - fixed-task config:
  Observe whether the worst task and aggregate executor runtime improve. The
  purpose is not to claim a universal tuning rule; it is to show the diagnostic
  loop: observe, isolate, change, validate.
```

Expected markers by mode:

```text
LAB1_RANDOM_TASK_OUTLIER_STAGE_OK
LAB1_RANDOM_TASK_OUTLIER_TASK_OK
LAB1_RANDOM_TASK_OUTLIER_FIXED_TASK_OK
```

Expected task diagnostic marker:

```text
LAB1_TASK_OUTLIER rank=1 stageId=... taskIndex=... executorRunTime=...
```

Expected Gold outputs:

```text
s3a://lakehouse/gold/lab1/random_task_outlier/problematic
s3a://lakehouse/gold/lab1/random_task_outlier/fixed
```

Classroom note:

```text
StageMetrics show that the measured region is expensive. TaskMetrics are used
only when the teaching question becomes: which task is the long tail?
```

## 7. Optional: review 1B in Spark History Server

Open:

```text
http://127.0.0.1:28090
```

Look for applications matching the selected configs:

```text
workshop-lab1-random-task-outlier-stage
workshop-lab1-random-task-outlier-task
workshop-lab1-random-task-outlier-fixed-task
```

What to inspect:

- stage-level symptoms in the stage run;
- task-level outliers printed by sparkMeasure in the task run;
- whether the fixed validation reduces the concentrated executor runtime.

Teacher notes:

```text
Use History Server here as supporting evidence, not the primary artifact.
The terminal task report is intentionally easier to read for this lesson.

The UI is still useful to confirm that the application names, stage shape, and
task counts match what sparkMeasure reported. If the two views disagree, pause
and reconcile the scope before teaching the conclusion.
```

Reference note:

```text
docs/random_task_outlier_class_notes.md
```

## 8. Optional: review MinIO

Open:

```text
http://127.0.0.1:29011
```

Useful paths:

```text
lakehouse/bronze/retail/vendors
lakehouse/bronze/retail/products
lakehouse/bronze/retail/customers
lakehouse/bronze/retail/sales
lakehouse/gold/lab1/top_sales_global_sort
lakehouse/gold/lab1/random_task_outlier/problematic
lakehouse/gold/lab1/random_task_outlier/fixed
observability/event-logs
```

Teacher notes:

```text
Use MinIO to close the data-platform loop: the labs are not just printing
metrics. They read Bronze Delta sources, write Gold outputs, and emit Spark
event logs that feed the History Server.

For Lab 1 specifically, sparkMeasure metric persistence is intentionally off.
The evidence is the terminal report plus Spark History, while the business
outputs are persisted under lakehouse/gold/lab1.
```

Lab 1 intentionally keeps sparkMeasure metric persistence disabled. The
terminal output and Spark History Server stay focused on the measured workload
instead of extra metrics-write jobs.

## 9. Optional cleanup after class

Return to the repository root:

```bash
cd ../../../..
```

Stop containers:

```bash
make down
```

Remove generated local Docker volumes/data only when you want a clean rerun:

```bash
make clean-data
```
