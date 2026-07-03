# Lab 3 plan: sparkMeasure observability overhead benchmark

## Purpose

Plan a Lab 3 workshop exercise that measures the latency added by
sparkMeasure observability itself.

The teaching question is:

```text
How much extra wall-clock latency does StageMetrics or TaskMetrics add to the
same Spark workload when compared with the same job running without
sparkMeasure?
```

This is a planning document only. Do not implement directly from this file.
Create specific TODO files under `dev-todos/backlog/` before implementation.

## Post-mortem update

The implemented Lab 3 should now be treated as a benchmark post-mortem, not a
guaranteed live demo with a large TaskMetrics delay.

Development ran two local benchmark designs:

- a narrow sales-only workload with `208` observed tasks, where the collector
  differences were too small and noisy to support a clean classroom claim;
- the current multi-join workload with `20` stages, `2581` observed tasks, and
  about `643 MB` of shuffle, where TaskMetrics became slightly more expensive
  than StageMetrics but still only by a modest margin.

The durable teaching point is that sparkMeasure overhead is real but
environment- and workload-dependent. Local Spark, Delta, MinIO, Docker, and WSL
noise can hide small collector differences. Use the method and post-mortem
reasoning as the lesson, not a promise that every live run will show a dramatic
gap.

## Critical assessment

The proposal is good, but the measurement can be misleading if the benchmark is
not carefully scoped.

The naive version:

```text
run job once without sparkMeasure
run job once with StageMetrics
run job once with TaskMetrics
compare times
```

is not defensible enough for a workshop because local Spark timings are noisy.
On the current stack, noise can come from:

- Docker and WSL scheduling;
- Spark application startup and class loading;
- JVM warmup;
- Delta transaction log creation;
- MinIO object-store latency;
- Spark History Server/event log writes;
- Python driver startup;
- previous run effects on OS page cache and MinIO cache;
- stdout/report rendering cost from `collector.print_report()`;
- different output paths or write modes.

The lab should present results as local benchmark evidence, not universal
sparkMeasure overhead. The class narrative should explicitly say:

```text
This measures overhead on this local workshop stack and this workload shape.
Use the method, not the absolute numbers, as the durable lesson.
```

## Recommended scope

Implement one benchmark workload and run it repeatedly in three modes:

| Mode | sparkMeasure behavior | Purpose |
|---|---|---|
| `none` | observability disabled | baseline |
| `stage` | StageMetrics enabled | stage collector overhead |
| `task` | TaskMetrics enabled | task collector overhead |

Run each mode multiple times, defaulting to 10 measured repetitions per mode.
Include optional warmup repetitions that are recorded but excluded from the
summary.

Do not build a fixed/tuned version of the workload for this lab. The variable
under test is observability mode, not Spark performance tuning.

## Main design choice

Prefer one app script with YAML-driven configs over three separate app files.

Recommended app:

```text
src/apps/labs/lab_3/lab_3_observability_overhead_benchmark.py
```

The app should read the selected config from an environment variable, with a
safe default:

```python
CONFIG_NAME = os.environ.get("LAB3_CONFIG_NAME", "lab3-overhead-none")
```

Reasoning:

- one app guarantees the business workload is identical;
- three separate files increase drift risk;
- the bash orchestrator can switch modes by setting `LAB3_CONFIG_NAME`;
- the script can still stay readable and follow the existing lab contract.

If classroom readability later requires separate files, use thin wrappers only:

```text
lab_3_overhead_none.py   -> imports the same job class and sets config_name
lab_3_overhead_stage.py  -> imports the same job class and sets config_name
lab_3_overhead_task.py   -> imports the same job class and sets config_name
```

Do not duplicate transformation logic across files.

## Expected structure

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

The analysis job should be a separate later task:

```text
src/apps/labs/lab_3/lab_3_observability_overhead_analysis.py
```

That analysis job will read the persisted benchmark records and compute
latency deltas.

## Workload recommendation

Use generated retail `sales` only for the first slice.

Avoid `vendor_id` joins because the generator intentionally creates hot-vendor
skew, and Lab 3 is not about skew. Use `sale_id`-derived buckets to keep the
workload deterministic and unrelated to Lab 2C.

Candidate transformation:

```python
return (
    inputs["sales"]
    .select("sale_id", "sale_date", "quantity", "sale_amount")
    .withColumn("benchmark_bucket", pmod(xxhash64("sale_id"), lit(bucket_count)))
    .repartition(shuffle_partitions, "benchmark_bucket")
    .groupBy("benchmark_bucket", date_format("sale_date", "yyyy-MM"))
    .agg(
        count("*").alias("sale_count"),
        sum("quantity").alias("total_quantity"),
        sum("sale_amount").alias("gross_sales_amount"),
    )
)
```

Recommended default knobs:

```yaml
shuffle_partitions: 96
benchmark_buckets: 256
```

Rationale:

- enough tasks to make TaskMetrics overhead visible;
- not so many tasks that the benchmark becomes only scheduler overhead;
- independent from generated hot-vendor skew;
- output row count remains small and stable.

Calibrate this before finalizing. If `SCALE=xs` makes overhead too noisy, tune
partition count before increasing data scale.

## Output path strategy

Each measured run must write the same logical result to a unique physical path.

Recommended workload output path:

```text
s3a://lakehouse/gold/lab3/observability_overhead/workload/
  benchmark_id=<benchmark_id>/
  mode=<none|stage|task>/
  iteration=<n>/
  run_id=<uuid>
```

Use Delta format and `mode: errorifexists` for workload outputs.

Reasoning:

- no overwrite side effects;
- no accidental reuse of the same Delta transaction log;
- each run has an auditable marker;
- later analysis can join timing metadata to the exact output path.

Do not use append for the business output in the first slice. Append would
merge all runs into one Delta table and make per-run output validation less
obvious.

## Benchmark metadata persistence

Persist benchmark metadata to the `observability` bucket, not the lakehouse
business output area.

Recommended Delta table:

```text
s3a://observability/lab3/overhead_runs
```

Write mode:

```text
append
```

Recommended partitioning:

```text
benchmark_id, mode
```

One metadata row per app run.

Minimum columns:

| Column | Meaning |
|---|---|
| `benchmark_id` | shared id for one benchmark batch |
| `run_id` | unique id for one app execution |
| `iteration` | measured iteration number |
| `is_warmup` | whether this run should be excluded from summary |
| `mode` | `none`, `stage`, or `task` |
| `config_name` | selected YAML config |
| `app_name` | Spark application name |
| `application_id` | Spark application id |
| `workload_output_path` | unique Delta output path |
| `started_at_utc` | app-level start timestamp |
| `ended_at_utc` | app-level end timestamp |
| `app_wall_ms` | app-level total measured by the Python driver |
| `spark_session_ms` | SparkSession creation time |
| `workload_wall_ms` | read/transform/write workload duration |
| `collector_begin_end_ms` | measured collector window, if separate |
| `collector_report_ms` | `print_report()` duration, if enabled |
| `collector_aggregate_ms` | `aggregate_*metrics()` duration |
| `metadata_write_ms` | time to append the metadata row |
| `row_count` | output row count or validation count |
| `num_stages` | from sparkMeasure when available |
| `num_tasks` | from sparkMeasure when available |
| `executor_run_time_ms` | from sparkMeasure when available |
| `shuffle_bytes_written` | from sparkMeasure when available |
| `spark_version` | runtime version |

Important rule:

```text
Do not include metadata_write_ms in the overhead comparison.
```

The metadata write is part of the benchmark harness, not the workload under
test.

## Timing semantics

The plan should separate these timing concepts:

| Timing | Included | Use |
|---|---|---|
| `spark_submit_wall_ms` | external process launch + Spark app + shutdown | optional orchestration-level metric |
| `app_wall_ms` | Python app from main entry to final metadata row | operational cost |
| `workload_wall_ms` | read/transform/write action only | primary comparison |
| `collector_report_ms` | native report rendering/printing | explain output overhead |
| `collector_aggregate_ms` | aggregate metrics extraction | sparkMeasure post-processing overhead |

Primary classroom comparison should use:

```text
median(workload_wall_ms by mode)
```

Then derive:

```text
stage_overhead_ms = median(stage.workload_wall_ms) - median(none.workload_wall_ms)
task_overhead_ms  = median(task.workload_wall_ms)  - median(none.workload_wall_ms)
stage_overhead_pct = stage_overhead_ms / median(none.workload_wall_ms)
task_overhead_pct  = task_overhead_ms  / median(none.workload_wall_ms)
```

Also show `p75` or `p95` because local benchmark runs can have outliers.

## Should `collector.print_report()` be included?

Be explicit. There are two valid stories:

1. **Collection overhead only**
   - Do not print native reports during the repeated benchmark.
   - Measure begin/end and aggregate metrics cost separately.
   - Best for clean overhead comparison.

2. **Operational overhead with classroom reporting**
   - Keep `collector.print_report()`.
   - This measures the cost of the way the workshop actually uses sparkMeasure.
   - Best for explaining visible user experience.

Recommendation for the first implementation:

```text
Default benchmark mode should not print the native report for every repetition.
Record collector_report_ms as zero or null.
Expose LAB3_EMIT_SPARKMEASURE_REPORT=true for a single demo run.
```

Reasoning:

- repeated terminal output will pollute class logs;
- report printing cost is not the same as listener/collection overhead;
- a single optional demo run can still show the native sparkMeasure report.

This differs from Labs 2C/2D, where the report was pedagogically useful on
every run.

## Observability modes and configs

Recommended YAML config names:

```text
lab3-overhead-none
lab3-overhead-stage
lab3-overhead-task
```

All three configs must have identical workload settings:

```yaml
workload:
  shuffle_partitions: 96
  benchmark_buckets: 256
```

Only observability should differ:

```yaml
lab3-overhead-none:
  observability:
    enabled: false

lab3-overhead-stage:
  observability:
    enabled: true
    collector: stage
    persist: false

lab3-overhead-task:
  observability:
    enabled: true
    collector: task
    persist: false
```

Do not use the existing metrics persistence in `ExperimentRunner` for this lab.
The benchmark needs a custom metadata schema and must record timings for all
modes, including `none`.

## Runtime recommendation

Create a Lab 3-specific runtime helper:

```text
src/apps/labs/lab_3/lab_3_utils/overhead_runtime.py
```

Reasoning:

- the base `ExperimentRunner` always follows the normal workshop execution path;
- Lab 3 needs precise phase timings;
- Lab 3 needs metadata rows even when observability is disabled;
- Lab 3 may need to suppress native report printing for repeated runs;
- Lab 3 should not mutate shared runtime behavior for a benchmark-specific need.

This follows the pattern already used by:

```text
src/apps/labs/lab_2/lab_2_utils/task_duration_skew_runtime.py
src/apps/labs/lab_2/lab_2_utils/empty_partitions_runtime.py
```

## Bash orchestrator

Create one bash script at the Lab 3 level:

```text
src/apps/labs/lab_3/run_observability_overhead_benchmark.sh
```

Expected behavior:

```bash
LAB3_REPETITIONS=10 \
LAB3_WARMUP_REPETITIONS=1 \
src/apps/labs/lab_3/run_observability_overhead_benchmark.sh
```

Responsibilities:

- generate a `benchmark_id`, for example `lab3-overhead-YYYYMMDD-HHMMSS`;
- loop through `none`, `stage`, and `task`;
- run warmup iterations first;
- run measured iterations in round-robin order;
- set these environment variables for each `spark-submit`:
  - `LAB3_BENCHMARK_ID`;
  - `LAB3_MODE`;
  - `LAB3_ITERATION`;
  - `LAB3_IS_WARMUP`;
  - `LAB3_CONFIG_NAME`;
  - `LAB3_RUN_ID`;
  - `LAB3_OUTPUT_SUFFIX`;
  - `LAB3_EMIT_SPARKMEASURE_REPORT`;
- execute the same Spark app every time;
- fail fast on the first failed submit.

Prefer round-robin ordering over by-mode ordering:

```text
iteration 1: none -> stage -> task
iteration 2: stage -> task -> none
iteration 3: task -> none -> stage
```

Reasoning:

- avoids always running one mode after the cache/JVM/container environment has
  warmed up;
- makes the comparison less biased on a laptop/WSL environment.

Optional later: record external `spark-submit` process duration from bash. Do
not make that the primary metric in the first slice because persisting it to
MinIO cleanly requires extra orchestration.

## Analysis job for the next task

Do not build the analysis job in the first implementation unless explicitly
requested.

The next task should create:

```text
src/apps/labs/lab_3/lab_3_observability_overhead_analysis.py
```

That job should read:

```text
s3a://observability/lab3/overhead_runs
```

and write a compact summary such as:

```text
s3a://lakehouse/gold/lab3/observability_overhead/summary
```

Expected calculations:

- count of measured runs per mode;
- median/p75/p95 `workload_wall_ms`;
- median/p75/p95 `collector_aggregate_ms`;
- `stage_overhead_ms` and `%` vs baseline;
- `task_overhead_ms` and `%` vs baseline;
- optional exclusion of warmups;
- optional outlier flagging.

## Class notes narrative

The class notes should explain:

- sparkMeasure adds value, but it is not free;
- StageMetrics usually has lower overhead than TaskMetrics because it tracks
  coarser-grained stage aggregates;
- TaskMetrics can be more expensive because it captures task-level detail and
  its cost scales with number of tasks;
- report printing and metric aggregation are separate from listener collection;
- local results are workload- and environment-specific;
- the correct way to discuss overhead is through repeated runs and summary
  statistics, not a single run.

Recommended framing:

```text
Observability has a cost. The engineering question is whether the diagnostic
value justifies that cost for the workload and environment.
```

## Risks and tradeoffs

### Risk: measuring startup instead of sparkMeasure overhead

If each repetition launches a new `spark-submit`, total process duration will
include Spark application startup. That is fine as an operational metric, but
not clean enough as the primary sparkMeasure-overhead metric.

Mitigation:

- persist app-level `workload_wall_ms`;
- use external submit duration only as optional supporting data.

### Risk: report printing dominates TaskMetrics cost

TaskMetrics reports can be verbose. If printed on every repetition, terminal
output may distort the benchmark.

Mitigation:

- suppress native reports by default in benchmark repetitions;
- allow a one-off report demo through `LAB3_EMIT_SPARKMEASURE_REPORT=true`.

### Risk: metadata write contaminates the benchmark

Writing benchmark records to MinIO is necessary, but it is not part of the job
whose overhead is being measured.

Mitigation:

- time metadata writes separately;
- exclude `metadata_write_ms` from overhead deltas.

### Risk: unique output paths change write behavior

Unique paths avoid overwrite/cache effects but force Delta to create a fresh log
every time.

Mitigation:

- use the same unique-path strategy for all modes;
- record `workload_output_path` for auditability;
- keep output small and stable.

### Risk: output validation changes timing

Counting output rows after writing is another Spark action.

Mitigation:

- either measure validation separately;
- or write deterministic output count metadata from the DataFrame before write
  only if that count is part of all modes equally.

Recommendation: validation should be outside `workload_wall_ms`.

## Acceptance criteria for the future implementation

- One benchmark app runs in `none`, `stage`, and `task` modes.
- The business workload code path is identical across modes.
- Each run writes to a unique workload output path.
- Each run appends one metadata row to `s3a://observability/lab3/overhead_runs`.
- Benchmark script can run 10 measured repetitions per mode.
- Warmup runs are supported and distinguishable.
- Metrics include enough timing fields to separate workload, collector
  aggregation, report printing, and metadata persistence.
- Class notes clearly state that local results are not universal constants.
- No shared core runtime changes are required in the first slice.

## Open questions before implementation

1. Should the first implementation default to 10 repetitions, or should it
   default to 3 and let the instructor opt into 10?
2. Should benchmark repetitions suppress native sparkMeasure reports by default?
   Recommendation: yes.
3. Should the bash orchestrator write external `spark-submit` process duration
   locally for optional inspection?
   Recommendation: yes, but do not persist that to MinIO in the first slice.
4. Should Lab 3 use `SCALE=xs` only, or should we calibrate `SCALE=s` as an
   optional stress mode?
   Recommendation: start with `xs`; only move to `s` if overhead is not visible.
