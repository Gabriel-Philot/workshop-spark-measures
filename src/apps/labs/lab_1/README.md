# Lab 1: global sort diagnosis

Lab 1 uses a deliberately common problem: an apparently simple ranking job
globally sorts enriched sales data before writing a Gold Delta table. The point
is not to teach all possible fixes. The point is to show how sparkMeasure helps
isolate the expensive stage faster than reading only native Spark logs and
plans.

Classroom runbook:

```text
guide_lab1.md
```

Supporting notes:

```text
docs/random_task_outlier_class_notes.md
docs/task_metrics_native_api.md
```

## Prerequisites

Start the local stack and generate demo data first.

```bash
make compose
make generate SCALE=xs GENERATOR_RUN_ID=lab1-demo
```

Useful local UIs:

- Spark Master UI: <http://127.0.0.1:28091>
- Spark History Server: <http://127.0.0.1:28090>
- MinIO Console: <http://127.0.0.1:29011>

## Submit command

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_1/lab_1a_global_sort_diagnosis.py
```

## Required configuration

This script reads comparison metadata from `lab_1_utils/experiments.yaml`:

- `lab1-global-sort-diagnosis-native`
- `lab1-global-sort-diagnosis-observed-stage`

Both runs read the generated bronze retail Delta tables, build an enriched
sales dataset, globally sort it by `sale_amount`, and write:

```text
s3a://lakehouse/gold/lab1/top_sales_global_sort
```

The observed run enables sparkMeasure through YAML config:

```yaml
observability:
  enabled: true
  collector: stage
  persist: false
```

Metric persistence is intentionally disabled. The History Server view remains
focused on the ranking workload instead of extra Delta jobs for metrics writes.

## Teaching flow

1. Run the native job and inspect the Spark physical plan printed after
   `SPARK_EXPLAIN`.
2. Open Spark History and find the application
   `workshop-lab1-global-sort-native`.
3. Inspect the Jobs and Stages tabs. The job descriptions use the `LAB1`
   prefix, but native Spark still exposes a lot of detail.
4. Run the observed section from the same submit. Open
   `workshop-lab1-global-sort-observed-stage`.
5. Compare the terminal line `SPARKMEASURE_METRICS` with the History Server
   stages. Focus on `numStages`, `numTasks`, `executorRunTime`, and
   `shuffleBytesWritten`.
6. Explain the diagnosis: a global `orderBy` is a wide operation and introduces
   shuffle/sort work. Stage-level metrics are enough to find this class of
   issue.

## Expected markers

- `LAB1_GLOBAL_SORT_NATIVE_OK`
- `LAB1_GLOBAL_SORT_SPARKMEASURE_STAGE_OK`
- `LAB1_GLOBAL_SORT_DIAGNOSIS_OK`

## MinIO paths

- Inputs: `lakehouse/bronze/retail/sales`, `vendors`, and `products`
- Output: `lakehouse/gold/lab1/top_sales_global_sort`
- Event logs: `observability/event-logs`

## Random task outlier diagnosis

This second Lab 1 exercise introduces task-level sparkMeasure metrics without
using vendor/product/customer skew. The workload creates a technical
`audit_bucket` from a hash of `sale_id`; one of 16 buckets performs a much more
expensive fingerprint expression, which creates a task straggler.

Task metrics are diagnostic-only here. They are printed and inspected during the
run, but not persisted as Delta metrics artifacts.

Use `CONFIG_NAME` in `lab_1b_random_task_outlier_diagnosis.py` as the classroom
switch:

```python
CONFIG_NAME = "lab1-random-task-outlier-stage"       # stage aggregate view
CONFIG_NAME = "lab1-random-task-outlier-task"        # task diagnostic view
CONFIG_NAME = "lab1-random-task-outlier-fixed-task"  # fixed validation view
```

Run the same submit command after changing `CONFIG_NAME`:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_1/lab_1b_random_task_outlier_diagnosis.py
```

The selected YAML config controls both the sparkMeasure collector and the
workload variant:

```yaml
observability:
  collector: task
  persist: false
workload:
  variant: problematic
```

Expected task-level teaching marker:

```text
LAB1_TASK_OUTLIER rank=1 stageId=... taskIndex=... executorRunTime=...
```

The live-code fix is already commented in
`lab_1b_random_task_outlier_diagnosis.py`.
For repeatable validation without editing the transform call, switch
`CONFIG_NAME` to `lab1-random-task-outlier-fixed-task`.

Expected markers:

- `LAB1_RANDOM_TASK_OUTLIER_STAGE_OK`
- `LAB1_RANDOM_TASK_OUTLIER_TASK_OK`
- `LAB1_RANDOM_TASK_OUTLIER_FIXED_TASK_OK`

See `docs/task_metrics_native_api.md` for the native `TaskMetrics` API and the
YAML equivalent used by this lab. Use
`docs/random_task_outlier_class_notes.md` for the instructor narrative and
validated before/after interpretation.
