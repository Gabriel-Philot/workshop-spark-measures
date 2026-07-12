# TODO: Lab 3 observability overhead benchmark

## Context

Create the first Lab 3 lesson from the planning document:

```text
docs/agent_ops/dev-todos/done/lab3_observability_overhead_plan.md
```

This lesson measures the latency added by sparkMeasure observability on the
same Spark workload.

The classroom question is:

```text
How much extra wall-clock latency does StageMetrics or TaskMetrics add to the
same Spark workload when compared with the same job running without
sparkMeasure?
```

## Goal

Implement a benchmark harness that runs the same workload repeatedly in three
observability modes:

- `none`: no sparkMeasure collector;
- `stage`: sparkMeasure StageMetrics;
- `task`: sparkMeasure TaskMetrics.

The lesson should be explicit that these are local workshop-stack measurements,
not universal constants.

## Expected files

Add the Lab 3 structure:

```text
src/apps/labs/lab_3/
  README.md
  lab_3_observability_overhead_benchmark.py
  lab_3_observability_overhead_class_notes.md
  run_observability_overhead_benchmark.sh
  lab_3_utils/
    __init__.py
    experiments.yaml
    transformations.py
    overhead_runtime.py
```

Add or extend tests:

```text
tests/test_lab3_config.py
tests/test_lab3_transformations.py
```

## Implementation requirements

- Use one benchmark app controlled by YAML configs, not three duplicated apps.
- Keep the main app script short and close to the workshop contract:
  - `CONFIG_PATH`;
  - `CONFIG_NAME`;
  - `extract`;
  - `transform`;
  - `load`;
  - `validate_result`.
- Read `CONFIG_NAME` from `LAB3_CONFIG_NAME`, with a safe default.
- Keep Lab 3-specific timing and metadata behavior inside
  `lab_3_utils/overhead_runtime.py`.
- Keep transformations in `lab_3_utils/transformations.py`.
- Keep configs in `lab_3_utils/experiments.yaml`.
- Do not modify shared core runtime modules in the first slice:
  - `src/spark_workshop/runtime.py`;
  - `src/spark_workshop/jobs.py`;
  - `src/spark_workshop/metrics.py`.
- Use the project logger only; do not use raw `print` in Python code.
- Include the submit command in the app docstring.

## Workload design

Use generated retail `sales` only.

Do not join on `vendor_id`, because the generated data intentionally contains
hot-vendor skew and this lab is not about skew.

Suggested workload:

1. Read `sales`.
2. Select a narrow set of columns.
3. Derive `benchmark_bucket` from `sale_id`.
4. Repartition by `benchmark_bucket`.
5. Aggregate by `benchmark_bucket` and month.
6. Write a small Delta output.

All modes must run the same business transformation.

## Config expectations

Create these configs:

```text
lab3-overhead-none
lab3-overhead-stage
lab3-overhead-task
```

All configs should share the same workload settings:

```yaml
workload:
  shuffle_partitions: 96
  benchmark_buckets: 256
```

Only observability should differ:

- `none`: `observability.enabled: false`;
- `stage`: `observability.enabled: true`, `collector: stage`;
- `task`: `observability.enabled: true`, `collector: task`.

## Output path requirements

Each app run must write the workload result to a unique physical path:

```text
s3a://lakehouse/gold/lab3/observability_overhead/workload/
  benchmark_id=<benchmark_id>/
  mode=<mode>/
  iteration=<iteration>/
  run_id=<run_id>
```

Use Delta and avoid overwrite/append for the workload output in the first slice.
The point is to avoid path reuse side effects during the benchmark.

## Metadata persistence

Each app run must append one metadata row to:

```text
s3a://observability/lab3/overhead_runs
```

At minimum, persist:

- `benchmark_id`;
- `run_id`;
- `iteration`;
- `is_warmup`;
- `mode`;
- `config_name`;
- `app_name`;
- `application_id`;
- `workload_output_path`;
- `started_at_utc`;
- `ended_at_utc`;
- `app_wall_ms`;
- `spark_session_ms`;
- `workload_wall_ms`;
- `collector_begin_end_ms`;
- `collector_report_ms`;
- `collector_aggregate_ms`;
- `metadata_write_ms` when available;
- `row_count`;
- sparkMeasure aggregate fields when available:
  - `num_stages`;
  - `num_tasks`;
  - `executor_run_time_ms`;
  - `shuffle_bytes_written`;
- `spark_version`.

Primary future analysis should compare `workload_wall_ms`, not total
`spark-submit` process duration.

## Bash orchestrator

Create:

```text
src/apps/labs/lab_3/run_observability_overhead_benchmark.sh
```

The script should:

- run sequentially, one `spark-submit` at a time;
- wait for each workload to finish before starting the next;
- fail fast on any non-zero exit code;
- support defaults:
  - `LAB3_REPETITIONS=10`;
  - `LAB3_WARMUP_REPETITIONS=1`;
- run modes in round-robin order to reduce cache/warmup bias;
- set per-run environment variables:
  - `LAB3_BENCHMARK_ID`;
  - `LAB3_MODE`;
  - `LAB3_ITERATION`;
  - `LAB3_IS_WARMUP`;
  - `LAB3_CONFIG_NAME`;
  - `LAB3_RUN_ID`;
  - `LAB3_OUTPUT_SUFFIX`;
  - `LAB3_EMIT_SPARKMEASURE_REPORT`.

Do not run modes in parallel. Parallel execution would contaminate the
benchmark by making modes compete for CPU, memory, disk, MinIO, and Spark
workers.

## Reporting behavior

Do not print native sparkMeasure reports for every repetition by default.

Use:

```text
LAB3_EMIT_SPARKMEASURE_REPORT=true
```

for one-off demo runs when the instructor wants to show the native
sparkMeasure report.

## Class notes requirements

Create class notes that explain:

- sparkMeasure adds value but is not free;
- StageMetrics and TaskMetrics have different granularity and potential cost;
- TaskMetrics cost can grow with task count;
- repeated runs are required because local Spark timing is noisy;
- report printing is different from listener collection overhead;
- local WSL/Docker/MinIO numbers are not universal constants.

## Non-goals

- Do not implement the analysis job in this TODO.
- Do not add a tuning/fixed workload variant.
- Do not modify the data generator.
- Do not mutate shared core runtime modules.
- Do not run benchmark modes in parallel.
- Do not treat one run as a statistically meaningful result.

## Validation

Before opening the PR, validate at minimum:

```bash
make tests
make validate
make dry-test
```

Also run a small benchmark smoke test with reduced repetitions:

```bash
LAB3_REPETITIONS=1 \
LAB3_WARMUP_REPETITIONS=0 \
bash src/apps/labs/lab_3/run_observability_overhead_benchmark.sh
```

Confirm:

- all three modes finish successfully;
- each mode writes to a unique workload path;
- metadata rows are appended to `s3a://observability/lab3/overhead_runs`;
- the script waits for each mode before launching the next;
- Spark UI / History Server app names are readable.
