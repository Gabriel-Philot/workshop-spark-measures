# Lab 1B class notes: random task outlier diagnosis

## Lesson goal

This lesson shows why task-level sparkMeasure metrics are useful after a
stage-level diagnosis becomes too aggregated. The workload is intentionally
simple: it creates a technical audit bucket where one bucket performs a more
expensive fingerprint calculation. This is not the business-key skew lesson; it
is a controlled task straggler used to introduce `TaskMetrics`.

## What stage metrics show

Run the workload first with `CONFIG_NAME` set to the stage config:

```python
CONFIG_NAME = "lab1-random-task-outlier-stage"
```

Stage metrics answer aggregate questions:

- how many stages and tasks ran;
- total executor runtime across the measured workload;
- total shuffle written;
- whether the measured job is expensive enough to investigate.

The useful teaching line is:

```text
SPARKMEASURE_METRICS experiment=... numStages=... numTasks=... executorRunTime=... shuffleBytesWritten=...
```

This tells students that something in the measured workload is expensive, but it
does not identify the individual slow task inside the stage.

## What task metrics add

Run the same workload with `CONFIG_NAME` set to the task config:

```python
CONFIG_NAME = "lab1-random-task-outlier-task"
```

The task collector exposes one row per task. The lab prints a compact top-N view
ordered by executor runtime:

```text
LAB1_TASK_OUTLIER rank=1 stageId=24 taskIndex=277 executorRunTime=12121 recordsWritten=624697
LAB1_TASK_OUTLIER rank=2 stageId=24 taskIndex=276 executorRunTime=11814 recordsWritten=624513
```

The important columns are:

- `stageId`: the stage where the outlier happened;
- `taskIndex`: the task inside that stage;
- `executorRunTime`: executor time spent by that task;
- `duration`: end-to-end task duration;
- `recordsRead` / `recordsWritten`: data volume processed by the task;
- `shuffleTotalBytesRead` / `shuffleBytesWritten`: shuffle pressure;
- `memoryBytesSpilled` / `diskBytesSpilled`: spill signal when present.

The teaching point is direct: stage metrics show the symptom; task metrics show
the long tail.

## Validated before/after result

Problematic run:

```text
numTasks=341
executorRunTime=111402
top task executorRunTime=12121
```

Fixed run:

```text
numTasks=286
executorRunTime=91333
top task executorRunTime=6720
```

The prepared fix spreads the expensive audit bucket across more tasks. This
reduced the worst task from roughly 12.1 seconds to 6.7 seconds and reduced the
aggregate executor runtime from 111402 to 91333 in the local XS validation.

## Why task metrics are not persisted here

For this lesson, task metrics are diagnostic-only by design:

```yaml
observability:
  enabled: true
  collector: task
  persist: false
```

This follows the intended workshop use: task-level metrics are a microscope used
while diagnosing a concrete job. Persisting every task row would add extra Delta
write jobs and more artifacts, which would distract from the lesson. Stage-level
metrics remain the better fit when the goal is a compact, reusable observability
artifact.

## Instructor narrative

1. Start with stage metrics and ask: "Can we see that the job is expensive?"
2. Point out that the answer is yes, but the aggregate view hides which task is
   slow.
3. Switch to task metrics and show `LAB1_TASK_OUTLIER`.
4. Connect `stageId` and `taskIndex` back to the Spark UI if desired.
5. Apply the prepared fix or switch `CONFIG_NAME` to `lab1-random-task-outlier-fixed-task`.
6. Compare worst-task runtime and aggregate executor runtime.

The lesson should end with this mental model:

- use stage metrics to locate expensive workload regions;
- use task metrics when the question becomes "which task is the long tail?";
- avoid persisting task metrics by default unless there is a clear analytical
  reason to store them.

## Why task duration can look similar after the fix

It is possible for the fixed run to show a similar top-task `duration` while
still showing a lower `executorRunTime`. This is expected enough in a local
workshop cluster and should not be treated as a contradiction.

In sparkMeasure task metrics:

- `duration` is the task wall-clock time from start to finish;
- `executorRunTime` is the time spent executing work inside the executor.

The prepared fix reduces the concentrated compute cost of the heavy audit
bucket. That improvement is better represented by `executorRunTime` than by raw
`duration`.

A similar `duration` can still happen because the local run includes other costs
around the compute itself:

- limited local cores and task scheduling effects;
- Delta/S3A write and commit overhead;
- shuffle, serialization, or fetch overhead;
- fixed overheads that are large compared with the XS dataset size.

The instructor interpretation should be:

```text
similar duration = the wall-clock task still includes non-compute overhead
lower executorRunTime = the concentrated compute outlier was reduced
```

For this lesson, use `executorRunTime` as the primary signal for the task-level
compute outlier, and use `duration` as a complementary signal.
