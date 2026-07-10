# Lab 3 guide: observability overhead benchmark post-mortem

This guide is the classroom runbook for Lab 3.

Goal:

```text
prepare the shared retail sources
  -> run the same workload without sparkMeasure
  -> repeat with StageMetrics
  -> repeat with TaskMetrics
  -> persist timing evidence
  -> explain why repeated local benchmarks need careful interpretation
```

Lab 3 asks an operational question that the diagnosis labs leave open:

```text
What does Spark observability cost?
```

The answer is not a universal number. The lesson is how to design and interpret
a repeatable benchmark without claiming that one local result applies to every
Spark cluster.

Detailed development evidence:

[Observability overhead benchmark post-mortem](docs/observability_overhead_postmortem.md)

Use this guide for the classroom sequence and the post-mortem document for the
two benchmark attempts and their complete interpretation.

## 0. Start from the repository root

```bash
cd workshop-spark-measures
```

Expected:

- `Makefile` exists;
- `.env.example` exists;
- `src/apps/labs/lab_3` exists.

## 1. Prepare the local platform

Lab 3 uses the same Bronze retail sources as Labs 1 and 2. If the stack and
MinIO data are still available, continue to step 2 without regenerating data.

If containers were stopped but images and data remain:

```bash
make compose
```

If `make clean-data` removed MinIO data:

```bash
make compose
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

When starting from a clean machine or after images were removed:

```bash
make bootstrap
make build
make compose
make dry-test
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

The benchmark reads:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

Useful UIs:

```text
Spark Master UI:      http://127.0.0.1:28091
Spark History Server: http://127.0.0.1:28090
MinIO Console:        http://127.0.0.1:29011
```

Teacher notes:

```text
Environment readiness matters more in a benchmark than in a simple functional
demo. A failing worker, missing MinIO source, or first-time dependency download
changes the measured runtime and invalidates the comparison.
```

## 2. Move to the Lab 3 folder

```bash
cd src/apps/labs/lab_3
```

Optional sanity check:

```bash
ls
```

Expected classroom files:

```text
lab_3_observability_overhead_benchmark.py
run_observability_overhead_benchmark.sh
guide_lab3.md
docs/
lab_3_utils/
```

The Python app executes one benchmark run. The shell runner orchestrates
multiple sequential runs and changes only the observability mode and run
identity.

## 3. Understand the controlled comparison

The runner compares three modes:

| Mode | Collector | Purpose |
|---|---|---|
| `none` | disabled | baseline workload runtime |
| `stage` | StageMetrics | lower-granularity observability |
| `task` | TaskMetrics | detailed task-event observability |

All modes execute the same Spark business workload:

```text
sales + vendors + products + customers
  -> three joins
  -> deterministic benchmark bucket
  -> repartition to 384 shuffle partitions
  -> bucket-level aggregation
  -> second region/category/month aggregation
  -> ranking window
  -> unique Gold Delta output
```

The final output contains 200 rows on the validated `SCALE=xs` run. The YAML
disables AQE and broadcast joins so Spark does not remove the task pressure the
benchmark is designed to measure.

Teacher notes:

```text
The generated retail data contains vendor skew, but this lab is not a skew
diagnosis. The workload must remain identical across none, stage, and task so
the collector mode is the controlled variable.
```

## 4. Run the classroom demonstration

Run one measured repetition without warmup:

```bash
LAB3_REPETITIONS=1 \
LAB3_WARMUP_REPETITIONS=0 \
bash run_observability_overhead_benchmark.sh
```

This produces three sequential `spark-submit` executions:

```text
none -> stage -> task
```

This sequence took about `3m53s` on the validated WSL/Docker environment. Local
timing will vary.

Expected orchestration markers:

```text
LAB3_BENCHMARK_STARTED
LAB3_SUBMIT_STARTED
LAB3_SUBMIT_COMPLETED
LAB3_BENCHMARK_COMPLETED
```

Expected app markers across the three submits:

```text
LAB3_OVERHEAD_VALIDATION_OK
LAB3_METADATA_WRITTEN
LAB3_OBSERVABILITY_OVERHEAD_NONE_OK
LAB3_OBSERVABILITY_OVERHEAD_STAGE_OK
LAB3_OBSERVABILITY_OVERHEAD_TASK_OK
WORKSHOP_EXPERIMENT_COMPLETED
```

Teacher notes:

```text
This is a mechanism demonstration, not sufficient benchmark evidence. Use it
to show the three collector modes, equivalent output shapes, and appended
metadata. Then move to the validated post-mortem for the actual discussion. Do
not draw an overhead conclusion from one measured repetition.
```

### Why the classroom demonstration skips warmup

A benchmark warmup reduces cold-start bias from work such as:

- JVM class loading and JIT compilation;
- Spark executor initialization;
- first-read Delta metadata work;
- initial MinIO connections;
- operating-system and filesystem caches.

The runner persists warmup executions with:

```text
is_warmup=true
```

Measured analysis must filter those rows out. Warmup is important when
regenerating reliable benchmark evidence, but it doubles this short live
demonstration from three to six submits. The classroom flow therefore explains
the concept and uses the recorded post-mortem instead of waiting for an extra
round.

## 5. Read the timing boundaries correctly

The shell runner logs:

```text
spark_submit_wall_ms
```

This includes Docker exec, Python/JVM startup, SparkSession work, the workload,
validation, metadata persistence, and process shutdown. It is useful for
planning classroom time but is not the primary collector comparison.

The Delta metadata table contains:

```text
workload_wall_ms
```

This is the primary comparison field. It times the workload section while the
selected collector is active.

Supporting timing fields:

| Field | Meaning |
|---|---|
| `app_wall_ms` | complete Python application time before metadata persistence finishes |
| `spark_session_ms` | SparkSession setup timing recorded by the runtime |
| `workload_wall_ms` | measured Spark workload body; primary comparison |
| `collector_begin_end_ms` | complete collector window, including begin/end around the workload |
| `collector_report_ms` | optional native report rendering time |
| `collector_aggregate_ms` | aggregation of collected sparkMeasure metrics |
| `validation_wall_ms` | output validation after workload execution |

Metadata persistence happens after the collector window and writes its own
Delta row. Those extra Spark jobs do not belong to `workload_wall_ms`.

Teacher notes:

```text
Use workload_wall_ms when comparing modes. Use spark_submit_wall_ms only when
the question is how long the complete classroom command occupied the machine.
Mixing these boundaries produces a misleading benchmark.
```

## 6. Inspect the persisted evidence

Each run writes a unique business output:

```text
s3a://lakehouse/gold/lab3/observability_overhead/workload/
  benchmark_id=<benchmark_id>/
  mode=<mode>/
  iteration=<iteration>/
  run_id=<run_id>
```

The unique path prevents a later run from overwriting or reusing the previous
Delta output.

Every run appends one metadata row to:

```text
s3a://observability/lab3/overhead_runs
```

Important identity fields:

```text
benchmark_id
run_id
iteration
is_warmup
mode
config_name
app_name
application_id
```

Important sparkMeasure fields when a collector is active:

```text
num_stages
num_tasks
executor_run_time_ms
shuffle_bytes_written
```

In `none` mode those sparkMeasure fields are stored as `0` because no collector
emitted them. Do not interpret that as proof that the native Spark workload had
zero stages, tasks, executor time, or shuffle.

Warmup rows are intentionally persisted with:

```text
is_warmup=true
```

Any analysis must filter them out before comparing measured distributions.

## 7. Optional: inspect one mode manually

The shell runner is preferred because it creates comparable run identities and
rotates mode order. For teaching the Python app itself, submit one stage-mode
run manually from the Lab 3 folder:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB3_CONFIG_NAME=lab3-overhead-stage \
    LAB3_MODE=stage \
    /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_3/lab_3_observability_overhead_benchmark.py
```

Available configurations:

```text
lab3-overhead-none
lab3-overhead-stage
lab3-overhead-task
```

The config name and `LAB3_MODE` must describe the same collector. The runtime
fails fast when they disagree.

## 8. Optional: print the native sparkMeasure report

Repeated benchmark runs suppress native report printing by default:

```text
LAB3_EMIT_SPARKMEASURE_REPORT=false
```

For one demonstration:

```bash
LAB3_REPETITIONS=1 \
LAB3_WARMUP_REPETITIONS=0 \
LAB3_EMIT_SPARKMEASURE_REPORT=true \
bash run_observability_overhead_benchmark.sh
```

Teacher notes:

```text
Report rendering is a separate cost. Do not mix report-enabled runs with the
main overhead comparison unless report rendering is explicitly part of the
benchmark question.
```

## 9. Discuss the validated post-mortem

The safest classroom artifact is the recorded post-mortem, not a promise that
one live run will show a dramatic difference.

```text
Standard classroom flow:
run one short measured demonstration -> explain warmup -> discuss recorded evidence
```

Open:

[Observability overhead benchmark post-mortem](docs/observability_overhead_postmortem.md)

The first sales-only design produced only 208 measured tasks. With ten measured
repetitions, local noise was larger than the collector signal:

| Mode | Average workload | Average delta versus none |
|---|---:|---:|
| none | 24.369s | baseline |
| stage | 24.873s | +504ms |
| task | 24.510s | +141ms |

The current multi-join workload produced:

```text
20 stages
2,581 tasks
~643 MB shuffle written
200 output rows
```

With three measured repetitions:

| Mode | Average workload | Median workload | Average delta versus none |
|---|---:|---:|---:|
| none | 63.463s | 63.432s | baseline |
| stage | 64.467s | 64.600s | +1.004s / +1.58% |
| task | 64.691s | 64.898s | +1.228s / +1.93% |

Teacher notes:

```text
The second result is directionally coherent: TaskMetrics is slightly more
expensive than StageMetrics. The difference remains modest because local Spark,
Delta, MinIO, Docker, WSL, JVM startup, event logs, and filesystem cache create
noise of their own.

The conclusion is not “TaskMetrics always costs 1.228 seconds.” The conclusion
is that collector cost depends on task-event volume, workload shape, and the
environment where the measurement is made.
```

## 10. Optional only: regenerate the ten-repetition evidence

> **Classroom warning:** This is not part of the standard live flow. It runs 33
> sequential Spark applications and can occupy approximately 43-44 minutes.
> Normally, discuss the persisted post-mortem in step 9 instead.

Run this only when the schedule explicitly includes the wait or when
regenerating evidence outside the class:

```bash
LAB3_REPETITIONS=10 \
LAB3_WARMUP_REPETITIONS=1 \
bash run_observability_overhead_benchmark.sh
```

With the default three modes, this means:

```text
1 warmup round  x 3 modes =  3 warmup submits
10 measured rounds x 3 modes = 30 measured submits
total = 33 sequential spark-submit executions
```

The measured 30-submit portion projects to roughly `39-40m` on the development
machine. Including the warmup round, reserve approximately `43-44m` plus local
variation.

The runner rotates the first mode by measured iteration:

```text
iteration 1: none  -> stage -> task
iteration 2: stage -> task  -> none
iteration 3: task  -> none  -> stage
```

It remains sequential. Parallel modes would compete for the same CPU, memory,
Spark workers, disk, MinIO, and event-log destination, turning the benchmark
into a resource-contention experiment.

## 11. Optional: inspect Spark History Server

Open:

```text
http://127.0.0.1:28090
```

Application names:

```text
workshop-lab3-overhead-none
workshop-lab3-overhead-stage
workshop-lab3-overhead-task
```

What to inspect:

- confirm that the business workload shape is equivalent across modes;
- compare the `LAB3 | observability_overhead` job descriptions;
- distinguish workload jobs from output validation and metadata Delta writes;
- use the UI as a detailed companion to the persisted benchmark metadata.

## 12. Classroom conclusion

The expected conclusion is:

```text
sparkMeasure has observable overhead. The size of that overhead depends on
collector granularity, task-event volume, workload shape, and environment.
Measure the tradeoff before enabling detailed task observability everywhere.
```

StageMetrics remains the preferred first diagnostic layer because it is
coarser and normally cheaper. TaskMetrics is valuable when task-level
distribution is required, but the extra detail should have a diagnostic reason.

Lab 3 metadata remains reusable for future benchmark analysis. Lab 4 does not
depend on this table: the current Lab 4 runs its own workload and creates a
stage-level operational fingerprint.

## 13. Optional cleanup after class

Return to the repository root:

```bash
cd ../../../..
make down
```

To remove generated MinIO data:

```bash
make clean-data
```

To remove workshop images:

```bash
make removeimage
```

Do not run `make clean-data` between benchmark repetitions. It removes the
shared Bronze inputs and the persisted benchmark evidence.
