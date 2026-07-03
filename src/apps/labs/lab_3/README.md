# Lab 3: observability overhead benchmark post-mortem

Lab 3 measures the extra wall-clock latency introduced by sparkMeasure
observability on the same Spark workload.

The goal is not to produce universal numbers or to promise a dramatic live
demo. The goal is to show students how to benchmark the tradeoff locally,
explain why StageMetrics and TaskMetrics are useful but not free, and discuss
why measuring observability overhead is harder than it first appears.

Use this lab primarily as a post-mortem. The local workshop stack is useful for
controlled experiments, but the observed differences are small enough that a
live run can be noisy.

## Prerequisites

Start the platform and generate the retail bronze data first:

```bash
make compose
make generate SCALE=xs
```

For a more visible benchmark, use a larger generator scale after validating the
small run. This lab reads `sales`, `vendors`, `products`, and `customers`.

## Run the benchmark

Small smoke run:

```bash
LAB3_REPETITIONS=1 \
LAB3_WARMUP_REPETITIONS=0 \
bash src/apps/labs/lab_3/run_observability_overhead_benchmark.sh
```

Classroom-sized run:

```bash
LAB3_REPETITIONS=10 \
LAB3_WARMUP_REPETITIONS=1 \
bash src/apps/labs/lab_3/run_observability_overhead_benchmark.sh
```

Do not run the larger benchmark live unless the class has time to wait. On the
local WSL/Docker stack used during development, the current multi-join workload
took approximately:

- `1` repetition, `3` sequential submits: about `3m53s`;
- `3` repetitions, `9` sequential submits: about `11m51s`;
- projected `10` repetitions, `30` sequential submits: roughly `39-40m`.

The script runs these modes sequentially:

- `none`: no sparkMeasure collector;
- `stage`: sparkMeasure StageMetrics;
- `task`: sparkMeasure TaskMetrics.

It rotates mode order per repetition to reduce simple warmup/cache bias. It does
not run modes in parallel because parallel submits would make each mode compete
for CPU, memory, disk, MinIO, and Spark workers.

## Optional native sparkMeasure report

The benchmark does not print sparkMeasure native reports by default because
report rendering adds noise to repeated measurements.

For a one-off demonstration:

```bash
LAB3_REPETITIONS=1 \
LAB3_WARMUP_REPETITIONS=0 \
LAB3_EMIT_SPARKMEASURE_REPORT=true \
bash src/apps/labs/lab_3/run_observability_overhead_benchmark.sh
```

## Outputs

Each workload run writes a unique Delta output:

```text
s3a://lakehouse/gold/lab3/observability_overhead/workload/
  benchmark_id=<benchmark_id>/
  mode=<mode>/
  iteration=<iteration>/
  run_id=<run_id>
```

Each run appends one benchmark metadata row to:

```text
s3a://observability/lab3/overhead_runs
```

The metadata table includes:

- run identity: `benchmark_id`, `run_id`, `iteration`, `is_warmup`, `mode`;
- Spark identity: `config_name`, `app_name`, `application_id`, `spark_version`;
- timing fields: `app_wall_ms`, `spark_session_ms`, `workload_wall_ms`,
  `collector_begin_end_ms`, `collector_report_ms`, `collector_aggregate_ms`,
  `validation_wall_ms`;
- workload result fields: `workload_output_path`, `row_count`;
- sparkMeasure aggregate fields when present: `num_stages`, `num_tasks`,
  `executor_run_time_ms`, `shuffle_bytes_written`.

The primary comparison metric for the future analysis job is
`workload_wall_ms`. The bash script also logs `spark_submit_wall_ms`, but that
includes process startup and orchestration overhead outside the measured Spark
workload.

The bash script also logs the full benchmark wall time:

```text
LAB3_BENCHMARK_COMPLETED benchmark_id=<id> total_wall_ms=<ms> total_wall_seconds=<s>
```

## Files

```text
lab_3_observability_overhead_benchmark.py
run_observability_overhead_benchmark.sh
lab_3_observability_overhead_class_notes.md
lab_3_utils/
  experiments.yaml
  overhead_runtime.py
  transformations.py
```

The main app keeps the workshop contract visible. The benchmark-specific timing
and metadata logic stays in `lab_3_utils/overhead_runtime.py`.

## Workload shape

The workload intentionally does more than a narrow single-table aggregation:

1. read all four generated bronze retail tables;
2. join `sales` to `vendors`, `products`, and `customers`;
3. carry payload-width information through the plan;
4. repartition to a high number of shuffle partitions;
5. aggregate first by an artificial benchmark bucket;
6. aggregate again to a classroom-readable category/region/month summary;
7. rank categories by revenue inside each region pair.

The YAML disables AQE and broadcast joins for this lab so Spark does not
optimize away the task pressure that the lesson is trying to measure.

## Local benchmark post-mortem

Two benchmark designs were tested during development.

### Attempt 1: narrow sales-only workload

The first design read only `sales`, derived a bucket, aggregated by bucket and
month, and wrote a small Delta output.

Result with `10` measured repetitions:

| mode | runs | avg workload | median workload | observed signal |
|---|---:|---:|---:|---|
| none | 10 | 24.369s | 24.338s | baseline |
| stage | 10 | 24.873s | 24.704s | +504ms avg |
| task | 10 | 24.510s | 24.405s | +141ms avg |

This result is intentionally kept as a teaching point: the workload was too
light to isolate TaskMetrics overhead. The `task` run even looked cheaper than
`stage` in places because local Spark, Delta, MinIO, Docker, and WSL noise was
larger than the collector overhead.

### Attempt 2: current multi-join workload

The current workload joins four bronze tables, disables AQE/broadcast joins,
uses `384` shuffle partitions, derives `2048` benchmark buckets, and performs
two aggregation levels plus a ranking window.

Result with `3` measured repetitions:

| mode | runs | avg workload | median workload | tasks | shuffle |
|---|---:|---:|---:|---:|---:|
| none | 3 | 63.463s | 63.432s | not collected | not collected |
| stage | 3 | 64.467s | 64.600s | 2581 | ~643MB |
| task | 3 | 64.691s | 64.898s | 2581 | ~643MB |

Overhead versus `none`:

| mode | avg delta | median delta |
|---|---:|---:|
| stage | +1.004s / +1.58% | +1.168s / +1.84% |
| task | +1.228s / +1.93% | +1.466s / +2.31% |

This result is more coherent: TaskMetrics appears slightly more expensive than
StageMetrics. The difference is still modest because the job is not large enough
for task-event collection to dominate the total runtime.

## Classroom takeaway

The useful lesson is not that TaskMetrics always adds a specific number of
seconds. The useful lesson is:

- small local jobs can hide observability overhead inside runtime noise;
- increasing data size alone does not guarantee a clearer signal;
- TaskMetrics overhead is tied to task-event volume, not only bytes processed;
- report printing is a separate cost and is disabled by default in this lab;
- `none` mode records `0` for sparkMeasure fields because they were not
  collected, not because the Spark job avoided shuffle or writes;
- benchmark results should be treated as environment-specific evidence.
