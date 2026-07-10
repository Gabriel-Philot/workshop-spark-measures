# Lab 3 post-mortem: observability overhead benchmark

Classroom runbook:

[Lab 3 classroom guide](../guide_lab3.md)

This document preserves the benchmark design history and validated local
results. Use the guide for commands and teaching order; use this post-mortem to
explain why the benchmark needed iteration.

## Teaching question

How much latency does sparkMeasure add when we run the same Spark workload with
no collector, StageMetrics, and TaskMetrics?

This lesson is about the tradeoff. sparkMeasure gives compact runtime signals,
but collecting those signals is still extra work.

This lab should be taught as a post-mortem, not as a guaranteed live
performance demo. The local benchmark produced a useful engineering lesson:
observability overhead is measurable, but small local Spark jobs can hide that
overhead inside normal runtime noise.

## Why this benchmark exists

In earlier labs, sparkMeasure helped identify slow stages, heavy shuffles,
skewed tasks, and empty partitions. Those diagnostics are valuable because they
turn a noisy Spark UI into a smaller set of metrics.

The missing question is operational:

```text
If I enable this observability in a job, what does it cost me?
```

The correct answer is not a constant. It depends on workload shape, task count,
Spark version, cluster size, listener pressure, log/report behavior, and the
machine running the benchmark.

The initial expectation was simple:

```text
TaskMetrics should be visibly more expensive than StageMetrics because it
collects finer-grained task-level data.
```

That expectation is directionally correct, but the local experiment showed that
the signal is not automatically obvious. The workload must generate enough task
events for the collector overhead to rise above Spark, Delta, MinIO, Docker, and
WSL noise.

## Benchmark design

The benchmark runs the same business transformation in three modes:

- `none`: baseline Spark job without sparkMeasure;
- `stage`: same job wrapped by sparkMeasure StageMetrics;
- `task`: same job wrapped by sparkMeasure TaskMetrics.

All three modes:

1. read the generated bronze `sales`, `vendors`, `products`, and `customers`
   Delta tables;
2. join sales to all three dimensions;
3. derive a deterministic `benchmark_bucket`;
4. carry payload-width information through the plan;
5. repartition to many shuffle partitions;
6. aggregate by bucket, region, category, and month;
7. aggregate again to a final category/region/month summary;
8. rank category revenue inside each region pair;
9. write a small Delta output to a unique physical path.

The generated retail data intentionally contains vendor skew, but this lab is
not about diagnosing skew. The workload is deliberately stable across modes so
the observability mode is the main variable.

The YAML disables AQE and broadcast joins in this lab. That is a didactic
choice: Spark should not collapse the shuffle/task pressure that we need in
order to discuss StageMetrics versus TaskMetrics overhead.

## Development post-mortem

Two workload designs were tested.

### Attempt 1: sales-only benchmark

The first design was intentionally simple:

1. read `sales`;
2. derive a deterministic bucket from `sale_id`;
3. repartition by that bucket;
4. aggregate by bucket and month;
5. write a small Delta output.

It produced only `208` measured tasks in the observed modes.

Result with `10` measured repetitions:

| mode | runs | avg workload | median workload | interpretation |
|---|---:|---:|---:|---|
| none | 10 | 24.369s | 24.338s | baseline |
| stage | 10 | 24.873s | 24.704s | +504ms avg |
| task | 10 | 24.510s | 24.405s | +141ms avg |

The result was awkward for a classroom story because `stage` looked more
expensive than `task`. That does not mean StageMetrics is generally more
expensive. It means the workload was too light for this local benchmark.

The important diagnostic point:

```text
The difference between collectors was smaller than the normal local runtime
noise.
```

The job spent time in Spark startup, Delta reads/writes, MinIO I/O, validation,
event logging, JVM warmup, and Docker/WSL scheduling. The collector overhead was
too small to dominate those effects.

### Attempt 2: current multi-join benchmark

The benchmark was then changed to create more Spark work:

1. read `sales`, `vendors`, `products`, and `customers`;
2. join sales to the three dimensions;
3. disable AQE;
4. disable broadcast joins;
5. use `384` shuffle partitions;
6. derive `2048` benchmark buckets;
7. perform a bucketed aggregation;
8. perform a second final aggregation;
9. add a ranking window;
10. write a small Delta output.

This produced:

```text
20 stages
2581 tasks
~643 MB shuffle written
200 output rows
```

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

This result is more coherent. TaskMetrics became slightly more expensive than
StageMetrics. The signal is still modest because `2581` tasks is not huge for a
task-event collection experiment, and the Spark job itself is still relatively
light.

## Why the difference is still small

The current local benchmark is not a large production workload.

It runs in a constrained local stack:

- Spark in Docker;
- MinIO as object storage;
- WSL filesystem and resource scheduling;
- two small Spark workers;
- local event log writes;
- Delta metadata and data writes;
- Python `spark-submit` process startup per run.

Those components add enough variability that a one-second collector difference
is visible only after repeated runs.

Increasing data size may help, but it is not guaranteed to isolate TaskMetrics
overhead. TaskMetrics cost is driven by task-event volume. A larger dataset that
only makes each task longer can increase total job time without increasing the
relative collector overhead much. A better pressure design creates many task
events while keeping each task reasonably bounded.

## Why the script runs sequentially

`run_observability_overhead_benchmark.sh` blocks on each `spark-submit` before
starting the next one.

This matters because concurrent benchmark modes would compete for the same
local resources:

- Docker CPU;
- WSL memory;
- Spark workers;
- local disk;
- MinIO I/O;
- Spark event logging.

Parallel execution would measure resource contention, not sparkMeasure
overhead.

## Why repeated runs are required

Local Spark timing is noisy. One run can be affected by:

- JVM startup and class loading;
- first-read Delta metadata work;
- Docker/WSL scheduling;
- MinIO latency;
- garbage collection;
- OS filesystem cache;
- History Server/event-log side effects.

The benchmark therefore supports warmup repetitions and measured repetitions.
The next analysis job should compare distributions, not a single run.

## What to compare

Use the metadata table:

```text
s3a://observability/lab3/overhead_runs
```

Primary field:

```text
workload_wall_ms
```

This field times the Spark workload section wrapped by the Lab 3 runtime.

Useful supporting fields:

- `app_wall_ms`: full Python app wall time;
- `spark_session_ms`: SparkSession creation/reuse time;
- `collector_begin_end_ms`: measured listener window around the workload;
- `collector_report_ms`: optional report rendering time;
- `collector_aggregate_ms`: time to aggregate sparkMeasure metrics after the run;
- `validation_wall_ms`: output validation time, outside the measured workload;
- `num_stages`, `num_tasks`, `executor_run_time_ms`, `shuffle_bytes_written`:
  sparkMeasure aggregate fields when the selected mode has a collector.

For the benchmark result, prefer `workload_wall_ms` over total
`spark-submit` duration. The bash script logs `spark_submit_wall_ms`, but that
includes process startup and shell/Docker orchestration overhead.

## StageMetrics versus TaskMetrics

StageMetrics is coarser. It summarizes work at the stage level and is usually
the lower-cost diagnostic entry point.

TaskMetrics is more granular. It can expose task-level outliers, but its cost
can grow with task count because it collects and aggregates more detailed task
events.

This lab should make that tradeoff visible. If the local numbers are close, the
correct teaching point is still valid: measure the overhead in your environment
before enabling detailed task observability everywhere.

In this local post-mortem, the final numbers are close. That is not a failure;
it is the point of the lesson. A benchmark must be designed and interpreted with
the same care as any other performance experiment.

## Official sparkMeasure guidance

The official sparkMeasure documentation gives a qualitative recommendation,
not a universal percentage or fixed latency benchmark. Its FAQ states:

> "Use stage metrics whenever possible as they are much more lightweight."

It then reserves task-level collection for the cases that require its finer
diagnostic detail:

> "Collect metrics at task granularity if needed for skew, long tails, and
> stragglers."

Source: [sparkMeasure official README — Frequently Asked
Questions](https://github.com/LucaCanali/sparkMeasure/blob/master/README.md#frequently-asked-questions).

The implementation model explains the direction of that tradeoff. StageMetrics
collects at stage completion, while TaskMetrics records task events. The
official documentation also warns that collected data is buffered in driver
memory, so workload shape and event volume affect the real cost. See the
[official API and configuration
reference](https://github.com/LucaCanali/sparkMeasure/blob/master/docs/Reference_SparkMeasure_API_and_Configs.md).

An earlier CERN article by sparkMeasure's author listed the following item as
future work:

> "measure the overhead of the instrumentation using Spark listeners"

Source: [CERN Database Group — On Measuring Apache Spark Workload Metrics for
Performance Troubleshooting](https://db-blog.web.cern.ch/comment/35432).

Therefore, the official sources support the expected ordering—StageMetrics is
the lightweight first diagnostic layer and TaskMetrics is the detailed
drill-down—but they do not supply an official `Stage = X%` versus `Task = Y%`
benchmark. The `+1.58%` stage and `+1.93%` task results in this document are
local Lab 3 evidence only. They must not be presented as universal sparkMeasure
overhead.

## Report printing is a separate cost

The benchmark suppresses native sparkMeasure report printing by default:

```text
LAB3_EMIT_SPARKMEASURE_REPORT=false
```

This keeps repeated measurements focused on listener collection and metric
aggregation. To demonstrate the native sparkMeasure report in class, run a
small one-off benchmark with:

```bash
LAB3_EMIT_SPARKMEASURE_REPORT=true
```

Do not mix report-printing runs into the main overhead comparison unless the
lesson explicitly wants to measure report rendering too.

## Expected conclusion

The expected conclusion is not:

```text
sparkMeasure always adds X milliseconds.
```

The expected conclusion is:

```text
sparkMeasure has observable overhead, the overhead depends on collector
granularity and workload shape, and we can measure that tradeoff with a
repeatable local harness.
```

The persisted metadata remains available for future distribution analysis, but
the current Lab 4 does not depend on this table. Lab 4 runs its own workload and
uses StageMetrics to build an operational workload fingerprint.

## Recommended classroom narrative

Use this sequence:

1. Start with the expectation:
   `TaskMetrics should cost more because it captures task-level events`.
2. Show the first sales-only result and ask why it is confusing.
3. Explain that the job was too light and local runtime noise dominated.
4. Show the revised multi-join result.
5. Point out that the signal improved, but the delta is still modest.
6. Conclude that observability overhead is real, but must be measured in the
   target environment and workload shape.

Avoid promising that the live run will always show a dramatic difference. The
safer teaching artifact is the post-mortem itself.
