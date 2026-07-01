# Native sparkMeasure TaskMetrics API

This note belongs to Lab 1 because the second exercise uses task-level metrics
inside the same workshop story. Task metrics are diagnostic-first in this lab:
we print and inspect them during the run, but do not persist them as Delta
artifacts.

## Direct API

The native sparkMeasure Python API for task-level collection mirrors the stage
API shown in Lab 0, but uses `TaskMetrics` and can expose one record per task.

```python
from sparkmeasure import TaskMetrics

task_metrics = TaskMetrics(spark)
task_metrics.begin()
# Spark action to diagnose
task_metrics.end()
task_metrics.print_report()
metrics = dict(task_metrics.aggregate_taskmetrics())
tasks_df = task_metrics.create_taskmetrics_DF("PerfTaskMetrics")
```

Useful task columns include:

- `stageId`
- `index` task index within the stage
- `duration`
- `executorRunTime`
- `recordsRead`
- `recordsWritten`
- `shuffleTotalBytesRead`
- `shuffleBytesWritten`
- `memoryBytesSpilled`
- `diskBytesSpilled`

## Workshop config equivalent

The Lab 1B script still reads YAML config for Spark settings and artifacts:

```yaml
observability:
  enabled: true
  collector: task
  persist: false
```

Unlike stage-level integration tests, this task-level lesson does not configure
or write a `metrics` output artifact. The task data is used immediately for
interactive diagnosis.

The lab logs a compact top-N task outlier summary ordered by `executorRunTime`:

```text
LAB1_TASK_OUTLIER rank=1 stageId=... taskIndex=... executorRunTime=...
```

Use stage metrics first when aggregate symptoms are enough. Switch to task
metrics when the lesson is about long tails, stragglers, or uneven work inside
one stage.
