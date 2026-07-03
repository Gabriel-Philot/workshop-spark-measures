# Lab 4: stage-level workload fingerprint

Lab 4 uses sparkMeasure StageMetrics to build an operational fingerprint for a
Spark workload.

The lab answers:

```text
What does this Spark workload look like from a stage-level execution perspective?
```

Stage-level metrics are not only for reading raw counters. They can be
transformed into an operational fingerprint that helps engineers understand
whether a workload is shuffle-heavy, memory-pressure-heavy, I/O-heavy,
GC-heavy, task-heavy, or simply low-signal.

## Why this replaced the previous direction

The previous Lab 4 direction analyzed Lab 3 benchmark metadata and compared
`none` versus `stage` overhead. That was technically valid, but it repeated a
lesson the workshop already established: StageMetrics is the default low-cost
diagnostic layer, while TaskMetrics is more detailed and can cost more.

The stronger workshop sequence is:

- Lab 3: how much observability costs.
- Lab 4: what a workload looks like using StageMetrics.

## Why stage-level only

This lab is intentionally stage-level only.

It does not:

- use TaskMetrics;
- add Flight Recorder;
- parse Spark event logs;
- inspect task-level distributions.

The goal is to show how far an engineer can get with the first diagnostic
layer before deciding whether deeper tools are needed.

## How the fingerprint works

The app runs one generated-retail workload using:

- `sales`;
- `vendors`;
- `products`;
- `customers`.

The workload joins the generated bronze tables, repartitions by a deterministic
bucket, aggregates by region/category/month, and writes a small Delta summary.
sparkMeasure StageMetrics wraps that workload and returns aggregate counters.

Lab 4 maps those counters into normalized fields and ratios:

- `shuffle_total_bytes`;
- `shuffle_bytes_written`;
- `shuffle_bytes_read`;
- `input_bytes`;
- `executor_run_time_ms`;
- `jvm_gc_time_ms`;
- `memory_bytes_spilled`;
- `disk_bytes_spilled`;
- `num_stages`;
- `num_tasks`;
- `shuffle_amplification_ratio`[^input-bytes];
- `gc_time_ratio`;
- `spill_ratio`;
- `task_density_score`.

## Profiles

The classifier is intentionally small and explainable. It can assign:

- `SHUFFLE_HEAVY`;
- `MEMORY_PRESSURE`;
- `IO_HEAVY_SCAN`;
- `GC_PRESSURE`;
- `MANY_SMALL_TASKS`;
- `LOW_PARALLELISM_SIGNAL`;
- `BALANCED_OR_LOW_SIGNAL`.

Each profile includes:

- `diagnostic_flags`;
- `recommended_next_step`.

The fingerprint is not a perfect root-cause analysis. It is a lightweight
operational summary for PR reviews, incident reviews, and performance reviews.

## Configuration

Workload settings live in:

```text
src/apps/labs/lab_4/lab_4_utils/experiments.yaml
```

Fingerprint thresholds live in:

```text
src/apps/labs/lab_4/lab_4_utils/fingerprint_rules.yaml
```

The defaults are deliberately simple. They are classroom thresholds, not
universal production rules.

The threshold below protects the class from over-interpreting shuffle
amplification when the StageMetrics input byte denominator is too small:

```yaml
minimum_reliable_input_bytes: 1048576
```

## Prerequisites

Start the platform and generate data:

```bash
make compose
make generate SCALE=xs
```

## Run Lab 4

```bash
bash src/apps/labs/lab_4/run_stage_workload_fingerprint.sh
```

Manual `spark-submit` equivalent:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB4_CONFIG_NAME=lab4-stage-workload-fingerprint \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    /opt/spark/src/apps/labs/lab_4/lab_4_stage_workload_fingerprint.py
```

## Inputs

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

## Outputs

Workload output:

```text
s3a://lakehouse/gold/lab4/stage_workload_fingerprint/workload_summary
```

Normalized stage metrics:

```text
s3a://observability/lab4/stage_metrics
```

Workload fingerprints:

```text
s3a://observability/lab4/workload_fingerprints
```

## Expected markers

```text
LAB4_STAGE_METRICS_CAPTURED_OK
LAB4_WORKLOAD_FINGERPRINT_RULES_OK
LAB4_WORKLOAD_PROFILE_ASSIGNED_OK
LAB4_WORKLOAD_FINGERPRINT_WRITTEN_OK
```

## Classroom takeaway

Raw counters matter, but interpretation matters more.

StageMetrics can be transformed into a shared operational vocabulary:

- this workload is shuffle-heavy;
- this workload is spilling;
- this workload has GC pressure;
- this workload has too many small tasks;
- this workload is low-signal at the current scale.

That vocabulary helps the class discuss what to inspect next without jumping
immediately to task-level collectors.

[^input-bytes]: `input_bytes` is the StageMetrics-reported `bytesRead` counter,
    not the physical Delta table size. In local validation, the generated
    `sales` table was about `762 MB`, while StageMetrics reported `input_bytes`
    around `580-832 KB` for the Lab 4 workload. When `input_bytes` is `0` or
    below `minimum_reliable_input_bytes`, Lab 4 marks the ratio as unavailable
    or low-confidence and uses absolute shuffle volume as the safer signal.
