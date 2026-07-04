# Lab 7 plan: Temporal Backfill Observability

## Purpose

Plan a larger stage-first workshop lab:

```text
Lab 7 - Temporal Backfill Observability
Understanding daily volume spikes through stage-level Spark metrics
```

This is a planning document only. Do not implement directly from this file.
After this design is validated, create separate TODO files under
`dev-todos/backlog/`, one task per branch and pull request.

## Teaching thesis

Previous labs showed how to collect, interpret, validate, fingerprint, and use
StageMetrics for engineering guardrails.

Lab 7 should move from a single workload run to a temporal operational pattern:

```text
I have a historical backfill with known daily volumes. Some days are 10x or
100x larger than others. Can stage-level Spark metrics make those dates visible
and explain why the backfill cost changed?
```

The durable lesson should be:

```text
Stage-level metrics become more useful when they are persisted with business
time keys. A daily Spark backfill is not just one job; it is a sequence of
comparable workload executions where volume, runtime, shuffle, records read,
task count, and spill can be trended by processing date.
```

## Scope

- Use StageMetrics only.
- Do not use TaskMetrics.
- Do not add Flight Recorder.
- Do not parse Spark event logs.
- Do not introduce a dashboard web service.
- Do not mutate the existing retail generator or the retail core tables unless
  explicitly approved later.
- Do not mix key-skew diagnosis into this lab. The primary variable should be
  temporal volume and source filtering.
- Keep the implementation classroom-friendly, even if that means accepting a
  less generic design.

## Critical assessment of the proposed lab

The proposal is strong because it simulates a real enterprise pain:

```text
My daily dashboard/backfill is expensive on some dates. Which dates explain the
cost, and what evidence proves it?
```

This is materially different from Labs 4, 5, and 6:

- Lab 4 classifies one workload profile.
- Lab 5 compares baseline versus candidate as a promotion gate.
- Lab 6 validates that metrics are trustworthy enough to feed automation.
- Lab 7 trends metrics across business dates and connects Spark behavior to
  temporal data volume.

The main risk is scope creep. The full idea contains a generator, a daily
backfill runner, a historical dashboard, and a filter-strategy comparison. Those
should not be implemented in one branch. Each slice needs its own validation and
classroom output.

After design review, the lab should use an iterative development style for the
temporal source. Start with one defensible volume plan, test the runtime locally,
and adjust the row counts before locking the classroom default. Do not introduce
`demo`/`xs`/`s` scale names unless a later implementation pass proves they add
real classroom value. The important requirement is not a specific total row
count; it is that spike days are clearly visible in the generated data and in
the stage-level metrics.

The second risk is overclaiming `input_bytes`. Lab 4 already documented that
SparkMeasure StageMetrics `bytesRead` can be lower than the physical Delta data
size observed from files. Lab 7 should capture `input_bytes`, but should avoid
treating it as the only source-read truth. Prefer this evidence stack:

1. controlled source rows from the volume plan;
2. StageMetrics `recordsRead`, when emitted;
3. StageMetrics `bytesRead`, with an explicit low-confidence note if it is zero
   or unexpectedly small;
4. optional source file inventory only as supporting metadata, not as a
   replacement for StageMetrics.

The third risk is Catalyst/Delta optimization changing the shape of the "bad"
filter. A late filter such as `to_date(event_ts) = processing_date` may still be
optimized in ways that reduce the expected contrast. The filter comparison must
be tested empirically. If the signal is weak, document the post-mortem instead
of forcing a fake result.

## Recommended data entity

Create a lab-local temporal source, separate from the current retail bronze
tables:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

Recommended schema:

| Column | Purpose |
|---|---|
| `event_id` | deterministic event identity |
| `event_date` | partition/filter key and classroom time key |
| `event_ts` | timestamp used for late-filter comparison |
| `account_id` | business dimension |
| `customer_id` | join-like/business dimension |
| `vendor_id` | business dimension |
| `product_id` | business dimension |
| `region` | dashboard dimension |
| `channel` | dashboard dimension |
| `event_type` | dashboard dimension |
| `quantity` | business measure |
| `gross_amount` | business measure |
| `payload_size_bucket` | optional deterministic payload-size signal |
| `created_at` | source generation timestamp |

Primary partition column:

```text
event_date
```

Do not make this a deeply realistic fake-data model. The purpose is controlled
Spark observability, not domain simulation.

The source should still feel like a real bronze entity: deterministic, persisted
under the lakehouse bronze zone, partitioned by `event_date`, and carrying the
business attributes needed for backfill, dashboard, and filter-strategy lessons.
It can reuse generator patterns, but it should not force the existing retail
generator contract to carry Lab 7-specific complexity unless that becomes clearly
worth it during implementation.

Non-negotiable source isolation rule:

```text
Lab 7 generation must add the temporal source without modifying any existing
retail/lab source data used by previous labs.
```

Concretely:

- never write to `s3a://lakehouse/bronze/retail/...`;
- never rewrite existing Lab 0-6 outputs or observability tables;
- write only under `s3a://lakehouse/bronze/lab7/...` and
  `s3a://observability/lab7/...`;
- default generation should behave like adding a new bronze source to the
  platform, not like resetting the whole lakehouse;
- if a reset/overwrite mode is ever needed for local cleanup, it must be
  explicit and scoped only to Lab 7 paths.

Expected generation order:

```text
1. Generate or validate the existing retail bronze sources used by Labs 0-6.
2. Generate or append the Lab 7 temporal bronze source.
3. Calibrate the Lab 7 source volume until daily spikes are visible in
   sparkMeasure StageMetrics.
```

This means the future public generation flow should keep the previous sources
available and add the new Lab 7 source at the end. If the previous retail
sources already exist and are valid, the Lab 7 flow should not need to rewrite
them. If the full bootstrap flow is run from an empty volume, retail generation
should happen first and temporal generation should happen last.

Retail scale risk assessment:

```text
Do not change the default data-generation scale for previous labs to `s`.
```

The existing retail `s` preset generates materially more data than `xs`. That is
useful as an opt-in stress option, but it can make Labs 0-6 slower, noisier, or
less reliable on the local WSL/Docker stack because those labs read the shared
retail bronze tables.

Therefore:

- keep the previous lab default on the already validated retail scale unless a
  specific lab asks for a larger opt-in run;
- document `make generate SCALE=s` as an optional stress-generation command, not
  as the safe classroom default;
- never require retail `s` for Lab 7;
- make the Lab 7 temporal source carry the extra volume needed for visibility,
  isolated under `s3a://lakehouse/bronze/lab7/...`;
- calibrate Lab 7 by changing the temporal `volume_plan.yaml`, not by making the
  shared retail tables larger.

This keeps old labs stable while still allowing the Lab 7 source to be larger
and more visually obvious in StageMetrics.

## Recommended temporal volume plan

Initial classroom candidate plan:

| Date | Multiplier | Rows |
|---|---:|---:|
| `2026-01-01` | 1x | 10,000 |
| `2026-01-02` | 1x | 10,000 |
| `2026-01-03` | 1x | 10,000 |
| `2026-01-04` | 100x | 1,000,000 |
| `2026-01-05` | 1x | 10,000 |
| `2026-01-06` | 1x | 10,000 |
| `2026-01-07` | 10x | 100,000 |
| `2026-01-08` | 1x | 10,000 |
| `2026-01-09` | 1x | 10,000 |
| `2026-01-10` | 1x | 10,000 |
| `2026-01-11` | 100x | 1,000,000 |
| `2026-01-12` | 1x | 10,000 |
| `2026-01-13` | 1x | 10,000 |
| `2026-01-14` | 1x | 10,000 |

Total default rows:

```text
2,210,000
```

This is intentionally smaller than the existing retail `xs` sales table
(`5,000,000` rows), but it has stronger temporal contrast.

This row plan is not final. Task A should test generation speed and downstream
backfill behavior before freezing the classroom default. If the local stack is
too slow, reduce `base_rows_per_day` first while preserving the 1x/10x/100x
shape. If the signal is too weak, increase the base size or number of payload
columns before adding unrelated complexity.

There should be one primary classroom volume plan. The point is to demonstrate
that stage-level metrics expose temporal volume spikes, not to teach scale
preset management.

Recommended config file:

```text
src/apps/labs/lab_7/lab_7_utils/volume_plan.yaml
```

Suggested shape:

```yaml
date_range:
  start: "2026-01-01"
  end: "2026-01-14"

base_rows_per_day: 10000

spike_days:
  "2026-01-04": 100
  "2026-01-07": 10
  "2026-01-11": 100

dimensions:
  customers: 50000
  vendors: 500
  products: 5000
  regions: [BR_SP, BR_RJ, BR_MG, BR_PR]
  channels: [APP, WEB, API]
  event_types: [ORDER_CREATED, ORDER_PAID, ORDER_CANCELLED]
```

## Generator design recommendation

Use Spark-native deterministic generation with `spark.range`, not Python loops,
Faker, Python UDFs, or row-by-row object creation.

Recommended implementation style:

```python
spark.range(0, rows_for_day, 1, numPartitions=partitions)
```

Then derive columns using Spark SQL functions:

- `pmod(xxhash64(...), N)` for deterministic IDs;
- `date_add`, `to_timestamp`, and arithmetic expressions for timestamps;
- deterministic region/channel/event-type selection;
- numeric expressions for quantity and gross amount.

For full generation, prefer one logical generation run and one Delta write,
partitioned by `event_date`. If memory/planning becomes an issue, fall back to
per-day append writes, but keep the generated output deterministic.

Append-day mode should be part of Task A, not deferred. It is directly useful for
the classroom story because it simulates a new partition arriving after a
historical source already exists.

Required generator modes:

| Mode | Purpose |
|---|---|
| `full` | create the planned 14-day source without touching other sources |
| `append_day` | append one additional `event_date` partition with a configurable multiplier |

The default classroom flow should be safe to run after the normal platform/data
bootstrap. It should add the Lab 7 temporal source and leave previously generated
retail data intact.

## Recommended workload

Backfill one daily dashboard:

```text
daily_activity_dashboard
```

Output path:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard
```

Business output columns:

| Column | Meaning |
|---|---|
| `event_date` | processed business date |
| `region` | dashboard dimension |
| `channel` | dashboard dimension |
| `event_type` | dashboard dimension |
| `event_count` | number of events |
| `customer_count` | approximate or exact distinct customer count |
| `gross_revenue` | sum of `gross_amount` |
| `avg_ticket` | `gross_revenue / event_count` |
| `created_at` | write timestamp |

Each daily run should capture one StageMetrics aggregate record.

Recommended observed metrics output:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

Minimum fields:

| Column | Meaning |
|---|---|
| `run_id` | unique id for the full backfill batch |
| `date_run_id` | unique id for one processed date |
| `app_name` | Spark app name |
| `application_id` | Spark application id |
| `lab_id` | `lab_7` |
| `workload_name` | `temporal_daily_backfill` |
| `filter_strategy` | `early_partition_filter` or `late_derived_filter` |
| `processing_date` | date being processed |
| `source_start_date` | source range start |
| `source_end_date` | source range end |
| `source_rows_for_date` | expected rows from the deterministic volume plan |
| `volume_multiplier` | expected multiplier from the volume plan |
| `spike_label` | `NORMAL`, `MEDIUM_SPIKE`, or `VOLUME_SPIKE` |
| `num_stages` | StageMetrics aggregate |
| `num_tasks` | StageMetrics aggregate |
| `executor_run_time_ms` | StageMetrics aggregate |
| `records_read` | StageMetrics aggregate when available |
| `input_bytes` | StageMetrics `bytesRead`, with caveat |
| `shuffle_bytes_written` | StageMetrics aggregate |
| `shuffle_bytes_read` | StageMetrics aggregate |
| `memory_bytes_spilled` | StageMetrics aggregate |
| `disk_bytes_spilled` | StageMetrics aggregate |
| `jvm_gc_time_ms` | StageMetrics aggregate |
| `created_at` | metric creation timestamp |

Derived fields:

| Column | Meaning |
|---|---|
| `runtime_per_million_rows` | executor runtime normalized by expected rows |
| `shuffle_per_million_rows` | total shuffle normalized by expected rows |
| `input_bytes_per_million_rows` | input bytes normalized by expected rows |
| `tasks_per_million_rows` | task count normalized by expected rows |

## Execution model recommendation

Use one `spark-submit` per processing date, coordinated by a shell runner.

Reasoning:

- matches the operational mental model of daily backfill runs;
- makes each processing date visible as a separate Spark application in History
  Server;
- produces one independently auditable metrics row per business date;
- makes per-date failure/retry behavior easier to explain;
- costs more wall-clock time because Spark starts once per date, so the runner
  must make progress and outputs very explicit.

Task B should therefore include a bash orchestrator that submits one date, waits
for completion, validates expected markers, then moves to the next date. The
app itself should process one `processing_date` per invocation.

## Filter strategies

Recommended early strategy:

```python
source = spark.read.format("delta").load(source_path)
filtered = source.where(F.col("event_date") == F.lit(processing_date))
```

Expected behavior:

- uses the partition column directly;
- should favor Delta partition pruning;
- should read only the target date partitions.

Recommended late strategy:

```python
source = spark.read.format("delta").load(source_path)
prepared = source.withColumn("event_date_derived", F.to_date("event_ts"))
filtered = prepared.where(F.col("event_date_derived") == F.lit(processing_date))
```

Expected behavior:

- avoids filtering by the partition column directly;
- should make pruning less effective;
- may scan more source data for the same business output.

Critical caveat:

Spark/Delta/Catalyst can optimize aggressively. The late strategy must be
validated locally before claiming a strong result. If the contrast is weak,
consider making the late strategy perform a legitimate wide pre-aggregation
before filtering. Do not fake metrics.

## Proposed four-task breakdown

Each task should become one TODO, one branch, one commit set, one PR, and one
validation cycle.

### Task A: Temporal source generator

Suggested branch:

```text
feature/lab7a-temporal-source-generator
```

Status:

```text
Implemented in branch feature/lab7a-temporal-source-generator.
```

Goal:

Create deterministic temporal source data with known per-day volumes.

Expected files:

```text
src/apps/labs/lab_7/
  README.md
  lab_7_temporal_source_generator.py
  lab_7_temporal_source_generator_class_notes.md
  lab_7_temporal_source_generator_class_commands.md
  run_temporal_source_generator.sh
  lab_7_utils/
    __init__.py
    volume_plan.yaml
    generator.py
    experiments.yaml
```

Expected output:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
s3a://observability/lab7/temporal_volume_plan
```

Expected markers:

```text
LAB7_TEMPORAL_VOLUME_PLAN_OK
LAB7_TEMPORAL_SOURCE_GENERATED_OK
LAB7_TEMPORAL_SOURCE_VALIDATION_OK
```

Validation:

- source table exists;
- row counts match the volume plan;
- `event_date` partitions exist for all expected dates;
- large spike days have 100x the base row count;
- medium spike day has 10x the base row count;
- source generation uses Spark-native transformations only.

Critical design choice:

Do this as a Lab 7 source generator, not as a mutation of the shared retail
generator. The existing generator is already useful for Labs 0-6 and should not
carry temporal-backfill concerns unless this pattern later proves reusable.

However, the operational generation flow should be able to run after the
existing generator without changing the retail-only command. The explicit order
is:

```bash
make generate SCALE=xs
make generate-lab7
```

The convenience command for both steps is:

```bash
make generate-all SCALE=xs
```

Optional retail stress run, only when explicitly wanted:

```bash
make generate-all SCALE=s
```

`make generate` must remain retail-only to avoid surprising previous labs and
classroom workflows.

### Task B: Daily backfill runner with StageMetrics by date

Suggested branch:

```text
feature/lab7b-daily-backfill-stage-metrics
```

Goal:

Process one `processing_date` per Spark application and provide a runner that
orchestrates the default 14-day historical backfill.

Expected files:

```text
src/apps/labs/lab_7/
  lab_7_daily_backfill_stage_metrics.py
  lab_7_daily_backfill_stage_metrics_class_notes.md
  lab_7_daily_backfill_stage_metrics_class_commands.md
  run_daily_backfill_stage_metrics.sh
  lab_7_utils/
    backfill_runtime.py
    transformations.py
```

Expected outputs:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard
s3a://observability/lab7/daily_backfill_stage_metrics
```

Expected markers:

```text
LAB7_DAILY_BACKFILL_RUN_OK
LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK
LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK
```

Validation:

- exactly 14 measured daily rows after the default runner completes;
- one Spark application per processing date;
- default strategy is `early_partition_filter`;
- spike dates appear with much larger `source_rows_for_date`;
- StageMetrics are captured for every date;
- terminal output highlights normal, medium spike, and large spike dates in a
  boxed block;
- the app does not fail if one metric such as `input_bytes` is low-confidence,
  but it must log that limitation.

Critical design choice:

Keep the main app contract readable. Put per-date collector handling, metric
normalization, and boxed terminal summary in `lab_7_utils/`. Put the 14-day loop
in the shell runner, not inside the Python app.

### Task C: Daily backfill dashboard analyzer

Suggested branch:

```text
feature/lab7c-backfill-dashboard
```

Goal:

Turn the per-date StageMetrics evidence into a compact Delta table that can be
used for charts or class discussion.

Expected files:

```text
src/apps/labs/lab_7/
  lab_7_backfill_dashboard.py
  lab_7_backfill_dashboard_class_notes.md
  lab_7_backfill_dashboard_class_commands.md
  run_backfill_dashboard.sh
  lab_7_utils/
    dashboard.py
```

Expected output:

```text
s3a://observability/lab7/daily_backfill_dashboard
```

Expected markers:

```text
LAB7_BACKFILL_DASHBOARD_WRITTEN_OK
LAB7_VOLUME_SPIKES_DETECTED_OK
```

Recommended dashboard fields:

| Column | Meaning |
|---|---|
| `processing_date` | business date |
| `source_rows` | expected source rows |
| `volume_multiplier` | 1x, 10x, or 100x |
| `executor_run_time_ms` | StageMetrics runtime |
| `records_read` | StageMetrics records read |
| `input_bytes` | StageMetrics input bytes |
| `shuffle_bytes_written` | StageMetrics shuffle written |
| `shuffle_bytes_read` | StageMetrics shuffle read |
| `num_tasks` | StageMetrics tasks |
| `memory_bytes_spilled` | StageMetrics memory spill |
| `disk_bytes_spilled` | StageMetrics disk spill |
| `jvm_gc_time_ms` | StageMetrics GC time |
| `spike_label` | normal/medium/large |
| `runtime_rank` | ranking by runtime |
| `shuffle_rank` | ranking by shuffle |
| `volume_runtime_signal` | educational label |

Validation:

- dashboard row count equals the number of processed dates for the strategy;
- spike dates are ranked near the top by source volume;
- runtime/shuffle rankings are reported but not overclaimed as universal;
- terminal block prints the top dates by volume and runtime.

Critical design choice:

This analyzer should not rerun the backfill. It should consume the persisted
metrics from Task B. That keeps it fast and reinforces that observability data
is a reusable data product.

### Task D: Filter strategy comparison

Suggested branch:

```text
feature/lab7d-filter-strategy-comparison
```

Goal:

Compare the same daily business result under two source-filter strategies:

- `early_partition_filter`;
- `late_derived_filter`.

This means:

```text
early_partition_filter -> filter directly on event_date before expensive work
late_derived_filter    -> derive/filter later, or after extra work, to show how
                          an equivalent result can create a different source
                          access/execution profile
```

This is not a separate skew lab. The goal is to teach that source filtering
strategy can change the amount of work Spark performs for the same business
date. It should only be promoted from optional to mandatory after the local
metric signal is validated.

Expected files:

```text
src/apps/labs/lab_7/
  lab_7_filter_strategy_comparison.py
  lab_7_filter_strategy_comparison_class_notes.md
  lab_7_filter_strategy_comparison_class_commands.md
  run_filter_strategy_comparison.sh
  lab_7_utils/
    filter_comparison.py
```

Expected output:

```text
s3a://observability/lab7/filter_strategy_comparison
```

Expected markers:

```text
LAB7_FILTER_STRATEGY_BASELINE_OK
LAB7_FILTER_STRATEGY_CANDIDATE_OK
LAB7_FILTER_STRATEGY_COMPARISON_OK
```

Validation:

- early and late strategies produce compatible business output;
- comparison includes `records_read`, `input_bytes`, `executor_run_time_ms`,
  `shuffle_bytes_written`, `shuffle_bytes_read`, `num_tasks`, and `num_stages`;
- terminal output shows whether the late strategy read or processed more work;
- if the difference is not strong on the local stack, the class notes explain
  why local Spark/Delta/Catalyst behavior may reduce the contrast.

Critical design choice:

Treat this as optional until Tasks A-C are stable. It is valuable, but it is the
least predictable part of the lab because optimizer behavior can compress the
expected difference.

## Expected final Lab 7 folder after all tasks

```text
src/apps/labs/lab_7/
  README.md
  lab_7_temporal_source_generator.py
  lab_7_temporal_source_generator_class_notes.md
  lab_7_temporal_source_generator_class_commands.md
  lab_7_daily_backfill_stage_metrics.py
  lab_7_daily_backfill_stage_metrics_class_notes.md
  lab_7_daily_backfill_stage_metrics_class_commands.md
  lab_7_backfill_dashboard.py
  lab_7_backfill_dashboard_class_notes.md
  lab_7_backfill_dashboard_class_commands.md
  lab_7_filter_strategy_comparison.py
  lab_7_filter_strategy_comparison_class_notes.md
  lab_7_filter_strategy_comparison_class_commands.md
  run_temporal_source_generator.sh
  run_daily_backfill_stage_metrics.sh
  run_backfill_dashboard.sh
  run_filter_strategy_comparison.sh
  lab_7_utils/
    __init__.py
    experiments.yaml
    volume_plan.yaml
    generator.py
    transformations.py
    backfill_runtime.py
    dashboard.py
    filter_comparison.py
```

Keep helper modules lab-local. Do not create shared `spark_workshop` framework
abstractions unless repeated code becomes painful across multiple future labs.

## Repository resources already available

Useful existing pieces:

- `SparkSessionSingleton` for local Spark app lifecycle;
- `SparkWorkshopJob` for readable extract/transform/load contracts;
- lab-local runtimes from Labs 3, 5, and 6 for custom multi-run logic;
- `SparkMeasureFactory.create("stage", spark)` for StageMetrics collection;
- `normalize_metrics()` for sparkMeasure aggregate dictionaries;
- artifact input/output helpers for Delta paths;
- YAML experiment loading and environment-variable interpolation;
- terminal block helpers and existing boxed summaries;
- shell runner conventions from Labs 3-6;
- deterministic Spark-native generation pattern in `generator/src/.../spark_native.py`.

Gaps to fill:

- no temporal source entity exists today;
- no generator support for per-day volume plans exists today;
- no runtime currently loops StageMetrics collection by business date;
- no dashboard/analyzer exists for per-date metrics trends;
- filter-pruning evidence must be tested and not assumed.

## Metrics reliability notes

Use these field mappings consistently:

| Lab field | sparkMeasure StageMetrics field |
|---|---|
| `num_stages` | `numStages` |
| `num_tasks` | `numTasks` |
| `executor_run_time_ms` | `executorRunTime` |
| `records_read` | `recordsRead` |
| `input_bytes` | `bytesRead` |
| `shuffle_bytes_written` | `shuffleBytesWritten` |
| `shuffle_bytes_read` | `shuffleTotalBytesRead` |
| `memory_bytes_spilled` | `memoryBytesSpilled` |
| `disk_bytes_spilled` | `diskBytesSpilled` |
| `jvm_gc_time_ms` | `jvmGCTime` |

For Lab 7, `records_read` is likely a more useful input-volume signal than
`input_bytes` on this local Delta/S3A setup. Capture both, but document that
`bytesRead` can be low-confidence.

## Open design questions for the user

Current decisions:

1. The exact default volume should be discovered during Task A by generating and
   testing one primary source locally. Preserve the 1x/10x/100x shape.
2. Append-day mode should ship with Task A.
3. The daily backfill should use one `spark-submit` per processing date.
4. Filter strategy comparison remains optional until Tasks A-C prove stable and
   the local metric signal is validated.
5. The dashboard table must be well structured. Optional Markdown/CSV/Streamlit
   presentation can be evaluated after the Delta dashboard is useful.
6. Do not add explicit scale names now. Start with one configurable volume plan
   and calibrate it during Task A.
7. Non-destructive source append/isolation is mandatory. Lab 7 generation must
   not alter data already used by previous labs.
8. `make generate` should remain retail-only. `make generate-all` should
   generate/validate existing retail sources first and then generate the Lab 7
   temporal source at the end.
9. Retail `SCALE=s` may be documented as an opt-in stress option, but it must
   not become the default prerequisite for previous labs.
10. Lab 7 visibility should come from the isolated temporal source volume, not
    from increasing the shared retail tables.

## Recommended next step

Task A is now the first implemented slice. Before starting Task B, validate the
generated source shape and decide whether the current `volume_plan.yaml` is
visually strong enough for the daily backfill StageMetrics lesson.

After Task A is reviewed and merged, create the next executable TODO:

```text
dev-todos/backlog/to-do_lab7b_daily_backfill_stage_metrics.md
```

This keeps the larger lab incremental and lets each branch prove its own output
before the next layer depends on it.
