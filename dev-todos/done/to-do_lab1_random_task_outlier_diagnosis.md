# TODO: Lab 1 random task outlier diagnosis

## Goal

Add a second Lab 1 exercise that introduces sparkMeasure task-level metrics through a simple task straggler story. The goal is not to teach vendor/product/customer skew yet. The goal is to show students that task metrics exist, when they are useful, and how they make one problematic task easier to identify than stage-level aggregates alone.

## Teaching narrative

A Spark job runs and the stage-level metrics show that something is expensive, but the aggregate view does not make the root cause obvious enough. The workload contains an audit/fingerprint rule that is much more expensive for one technical hash bucket. This is not business-key skew; it is a concentrated compute outlier. Switching from stage metrics to task metrics should make the slow task visible.

The live flow should be:

1. Run the problematic workload with `collector: stage`.
2. Show that stage metrics identify the heavy stage but remain aggregated.
3. Run the same workload with `collector: task`.
4. Show the slow task/long tail clearly.
5. Uncomment the prepared fix and comment out the problematic path.
6. Rerun and show that the outlier task and total runtime improve.

## Proposed lab files

Keep everything inside Lab 1 for workshop readability:

```text
src/apps/labs/lab_1/
  random_task_outlier_diagnosis.py
  task_metrics_native_api.md
  lab_1_utils/
    experiments.yaml
    transformations.py
```

Do not create a new Lab 2 for this exercise.

## Proposed workload

The problematic path should be conceptually similar to:

```python
problematic = (
    sales_enriched
    .withColumn("audit_bucket", F.pmod(F.xxhash64("sale_id"), F.lit(16)))
    .repartition(16, "audit_bucket")
    .withColumn(
        "audit_fingerprint",
        F.when(
            F.col("audit_bucket") == F.lit(SLOW_BUCKET),
            expensive_spark_sql_hash_expression(...),
        ).otherwise(lightweight_hash_expression(...)),
    )
)
```

Use Spark SQL/JVM expressions such as `sha2`, `xxhash64`, `concat_ws`, and repeated/nested expressions. Avoid Python UDFs for the first task-level demo because Python worker overhead can distract from the sparkMeasure task metrics story.

## Required fix snippet

Leave the fix commented in the code so it can be enabled live during the class:

```python
# FIX:
# Spread the expensive audit bucket before computing the heavy fingerprint.
#
# fixed = (
#     sales_enriched
#     .withColumn("audit_bucket", ...)
#     .withColumn("audit_salt", F.pmod(F.xxhash64("sale_id"), F.lit(8)))
#     .repartition(64, "audit_bucket", "audit_salt")
#     .withColumn("audit_fingerprint", ...)
#     .drop("audit_salt")
# )
```

The fix should keep the audit/fingerprint idea but spread the expensive bucket across more tasks. Expected outcome: lower worst-task runtime and lower total runtime.

Optionally include a second commented fix for discussion only:

```python
# Alternative fix:
# Remove this debug/audit fingerprint from the production path.
```

## Configuration requirements

Add Lab 1 YAML configs for both metric levels:

```yaml
lab1-random-task-outlier-stage:
  observability:
    enabled: true
    collector: stage
    persist: false

lab1-random-task-outlier-task:
  observability:
    enabled: true
    collector: task
    persist: false
```

The script uses a single `CONFIG_NAME` classroom switch and reads `observability.collector` plus `workload.variant` from the selected YAML config. Task metrics are diagnostic-only in this lab: they are printed and inspected in-process, not persisted as Delta artifacts.

## Native task metrics documentation

Add `src/apps/labs/lab_1/task_metrics_native_api.md` documenting the native sparkMeasure task API:

```python
from sparkmeasure import TaskMetrics

task_metrics = TaskMetrics(spark)
task_metrics.begin()
# Spark action
task_metrics.end()
task_metrics.print_report()
task_metrics.aggregate_taskmetrics()
```

The doc should also explain the Lab 1 YAML equivalent:

```yaml
observability:
  enabled: true
  collector: task
  persist: false
```

## Validation requirements

- Confirm the Lab 1 direct `TaskMetrics` path runs with Spark 4.1.2 and sparkMeasure 0.28.0.
- Validate the stage run completes and shows an aggregate symptom.
- Validate the task run completes and makes the outlier task clear.
- Because `TaskMetrics.print_report()` is aggregate-only, add a compact lab-specific top-N task outlier log such as:

```text
LAB1_TASK_OUTLIER stage_id=... task_id=... executorRunTime=... duration=...
```

- Validate the commented fix improves the worst task and total runtime before documenting it as the live-demo fix.
- Keep vendor/product/customer skew out of this lab; reserve business-key skew for a later lesson.
