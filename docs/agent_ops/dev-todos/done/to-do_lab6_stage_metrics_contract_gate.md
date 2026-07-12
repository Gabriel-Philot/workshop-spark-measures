# TODO: Lab 6 stage metrics contract gate

## Context

Create the next stage-first workshop lab:

```text
Lab 6 - Stage Metrics Contract Gate
```

The existing labs already cover:

- collecting Spark metrics with sparkMeasure;
- diagnosing Spark performance symptoms;
- reasoning about observability overhead;
- classifying workloads with stage-level metrics;
- turning stage-level metrics into runtime budget guardrails.

Lab 6 should introduce the trust layer for Spark observability metrics.

Main teaching message:

```text
A mature Spark observability platform does not only collect metrics. It
validates that those metrics are reliable enough to support engineering
decisions.
```

This lab should teach the flow:

```text
collect metrics -> validate contract -> use metrics with confidence
```

## Goal

Implement a small, classroom-friendly contract gate that:

1. runs a small Spark workload using existing generated retail data;
2. captures sparkMeasure StageMetrics aggregate metrics;
3. persists the raw metrics as Delta;
4. validates the metrics against a YAML-defined contract;
5. produces a `PASS` or `FAIL` contract decision;
6. writes both summary and rule-level contract results as Delta;
7. optionally injects controlled invalid records to demonstrate contract
   failures without corrupting the clean raw metrics table;
8. prints clear markers for class demos and validation.

## Scope

- Use StageMetrics only.
- Do not use TaskMetrics.
- Do not add Flight Recorder.
- Do not parse Spark event logs.
- Do not introduce task-level analysis.
- Do not require external services beyond the current local stack.
- Do not turn this into a generic data quality framework.
- Do not modify the reference repository under:

```text
/home/philot/compendium/forge/dataship/spark-plat-v0
```

## Expected files

Create a new Lab 6 folder:

```text
src/apps/labs/lab_6/
  README.md
  lab_6_stage_metrics_contract_gate.py
  lab_6_stage_metrics_contract_gate_class_notes.md
  run_stage_metrics_contract_gate.sh
  lab_6_utils/
    __init__.py
    contract_rules.yaml
    experiments.yaml
```

Likely helper files, only if they keep the main app readable:

```text
src/apps/labs/lab_6/lab_6_utils/
  contract.py
  runtime.py
  transformations.py
```

Keep Lab 6 helper behavior inside `lab_6_utils/`. Do not add shared framework
abstractions unless explicitly approved later.

## Implementation requirements

- Follow the app style used by Labs 0, 1, 2, 3, 4, and 5:
  - short main app script;
  - `CONFIG_NAME` classroom switch near the top;
  - configs in `lab_6_utils/experiments.yaml`;
  - lab-specific logic in `lab_6_utils/`;
  - logger output only, no raw `print`;
  - submit command in the app docstring;
  - readable Spark UI job descriptions;
  - Delta writes through existing repository conventions.
- Reuse existing Spark session utilities, data paths, logging style,
  sparkMeasure wrappers, and shell runner conventions.
- Prefer simple Spark DataFrame transformations and clear Python functions over
  clever abstractions.
- Do not add pandas unless the repository already uses it for similar Spark-side
  workloads.
- Keep the implementation readable enough for students to inspect during class.

## Workload design

Use generated retail Delta data already available in the platform:

- `sales`;
- `vendors`;
- `products`;
- `customers`.

Run a small business workload that produces a simple output. Suggested output:

```text
order_month
region
category
gross_revenue
order_count
customer_count
```

This workload does not need to be performance-pathological. The lab is about
trusting the collected metrics, not diagnosing the workload itself.

## StageMetrics raw output

Capture one StageMetrics aggregate row and enrich it with operational metadata.

Persist the clean raw metrics row to:

```text
s3a://observability/lab6/stage_metrics_raw
```

Required metadata fields:

- `run_id`;
- `app_name`;
- `lab_id`;
- `workload_name`;
- `workload_variant`;
- `collector_name`;
- `metric_scope`;
- `contract_version`;
- `created_at`.

Expected values:

```text
lab_id = lab_6
collector_name = sparkmeasure_stage_metrics
metric_scope = stage
```

Raw metric fields should include, when available from the existing stage-level
collector:

- `num_stages`;
- `num_tasks`;
- `executor_run_time_ms`;
- `shuffle_bytes_written`;
- `shuffle_bytes_read`;
- `jvm_gc_time_ms`;
- `memory_bytes_spilled`;
- `disk_bytes_spilled`;
- `input_bytes`.

Map actual sparkMeasure metric names carefully, following the style used in Labs
4 and 5. If a metric is unavailable in the current StageMetrics aggregate
dictionary, handle it explicitly and document the limitation.

## Contract rules

Load contract rules from:

```text
src/apps/labs/lab_6/lab_6_utils/contract_rules.yaml
```

Suggested default shape:

```yaml
contract:
  version: "1.0.0"

required_columns:
  - run_id
  - app_name
  - lab_id
  - workload_name
  - workload_variant
  - collector_name
  - metric_scope
  - contract_version
  - created_at
  - num_stages
  - num_tasks
  - executor_run_time_ms

schema_rules:
  require_columns: true

semantic_rules:
  num_stages_gt_zero: true
  num_tasks_gt_zero: true
  non_negative_metrics:
    - executor_run_time_ms
    - shuffle_bytes_written
    - shuffle_bytes_read
    - jvm_gc_time_ms
    - memory_bytes_spilled
    - disk_bytes_spilled
    - input_bytes

correlation_rules:
  required_identity_columns:
    - run_id
    - app_name
    - lab_id
    - workload_name
    - workload_variant
    - collector_name
    - metric_scope
    - created_at
  expected_values:
    collector_name: "sparkmeasure_stage_metrics"
    metric_scope: "stage"
  uniqueness_key:
    - run_id
    - workload_name
    - workload_variant
    - metric_scope

severity:
  missing_required_column: "ERROR"
  null_identity_column: "ERROR"
  invalid_expected_value: "ERROR"
  duplicate_uniqueness_key: "ERROR"
  non_negative_metric_violation: "ERROR"
  zero_stage_or_task: "ERROR"
```

Adjust names and defaults only as needed to match repository conventions.

## Contract layers

Implement three explicit contract layers.

### Layer 1: Schema Contract

Validate that required columns exist.

Examples:

- `run_id` exists;
- `app_name` exists;
- `workload_name` exists;
- `num_stages` exists;
- `num_tasks` exists;
- `executor_run_time_ms` exists;
- `created_at` exists.

Teaching message:

```text
Without a stable schema, every downstream automation is fragile.
```

### Layer 2: Semantic Contract

Validate that values make sense.

Examples:

- `num_stages > 0`;
- `num_tasks > 0`;
- `executor_run_time_ms >= 0`;
- `shuffle_bytes_written >= 0`;
- `shuffle_bytes_read >= 0`;
- `jvm_gc_time_ms >= 0`;
- `memory_bytes_spilled >= 0`;
- `disk_bytes_spilled >= 0`;
- `created_at is not null`.

Teaching message:

```text
A column existing does not mean the metric is usable.
```

### Layer 3: Correlation Contract

Validate that metrics can be joined, grouped, audited, and reused.

Examples:

- `run_id is not null`;
- `app_name is not null`;
- `workload_name is not null`;
- `workload_variant is not null`;
- `collector_name = "sparkmeasure_stage_metrics"`;
- `metric_scope = "stage"`;
- no duplicate rows by
  `run_id + workload_name + workload_variant + metric_scope`.

Teaching message:

```text
Without correlation keys, observability data becomes loose numbers that cannot
support automation.
```

## Contract outputs

Produce a rule-level result table at:

```text
s3a://observability/lab6/stage_metrics_contract_results
```

Required fields:

- `validation_run_id`;
- `source_path`;
- `contract_version`;
- `rule_id`;
- `rule_name`;
- `rule_type`;
- `severity`;
- `decision`;
- `failed_count`;
- `sample_failed_keys`;
- `recommendation`;
- `created_at`.

Allowed `rule_type` values:

- `SCHEMA`;
- `SEMANTIC`;
- `CORRELATION`.

Allowed `decision` values:

- `PASS`;
- `FAIL`.

Produce a summary table at:

```text
s3a://observability/lab6/stage_metrics_contract_summary
```

Required fields:

- `validation_run_id`;
- `source_path`;
- `contract_version`;
- `total_rules`;
- `passed_rules`;
- `failed_rules`;
- `decision`;
- `created_at`.

Allowed summary `decision` values:

- `PASS`;
- `FAIL`.

## Failure demonstration mode

Support an optional classroom failure mode with a config flag or CLI argument:

```text
--inject-invalid-records true
```

When enabled, inject controlled invalid rows into a separate validation input so
the lab can demonstrate contract failures without corrupting the clean raw
metrics table.

Write the invalid demonstration input to:

```text
s3a://observability/lab6/stage_metrics_contract_demo_input
```

Suggested invalid cases:

- missing/null `run_id`;
- `num_stages = 0`;
- `num_tasks = 0`;
- `shuffle_bytes_written = -1`;
- duplicate `run_id + workload_name + workload_variant + metric_scope`;
- null `created_at`;
- invalid `metric_scope = "task"`.

The failure demo should complete successfully as an educational flow, but the
final contract decision should be `FAIL`.

## Markers

Print these markers:

- `LAB6_STAGE_METRICS_CAPTURED_OK`;
- `LAB6_STAGE_METRICS_INPUT_OK`;
- `LAB6_CONTRACT_RULES_LOADED_OK`;
- `LAB6_SCHEMA_CONTRACT_EVALUATED`;
- `LAB6_SEMANTIC_CONTRACT_EVALUATED`;
- `LAB6_CORRELATION_CONTRACT_EVALUATED`;
- `LAB6_CONTRACT_RESULTS_WRITTEN_OK`.

Print exactly one final contract marker:

- `LAB6_STAGE_METRICS_CONTRACT_PASS`;
- `LAB6_STAGE_METRICS_CONTRACT_FAIL`.

The default classroom run should pass the contract.

Do not exit non-zero just because the contract decision is `FAIL` in the
failure demonstration mode. Fail fast only for real technical errors:

- missing input data;
- invalid YAML;
- broken Spark session;
- unsupported metric schema;
- failed Delta write.

## Documentation requirements

Create:

```text
src/apps/labs/lab_6/README.md
src/apps/labs/lab_6/lab_6_stage_metrics_contract_gate_class_notes.md
```

The README should explain:

1. lab purpose;
2. why observability metrics should be treated as a data product;
3. why collected metrics need a contract before they feed automation;
4. why this lab is stage-level only;
5. the three contract layers;
6. input data;
7. raw metrics output path;
8. contract summary output path;
9. contract rule-level output path;
10. expected markers;
11. how to run the passing scenario;
12. how to run the failure demonstration scenario;
13. how this pattern maps to dashboards, alerts, PR review, performance gates,
    and historical monitoring.

Main README message:

```text
Collecting Spark metrics is only the first step. Once those metrics become
inputs for operational decisions, they need a contract. This lab treats
StageMetrics as an observability data product and validates whether the data is
trustworthy enough to support automation.
```

The class notes should teach:

- observability metrics are data;
- operational data needs contracts;
- a metrics table without schema guarantees is fragile;
- a metrics table without semantic validation can produce wrong conclusions;
- a metrics table without correlation keys cannot support lineage, review,
  debugging, or automation;
- this is not a full data quality framework;
- this is a lightweight reliability layer before using metrics for engineering
  decisions;
- this follows the same principle behind production-grade data products:
  stable schema, valid values, traceable identity, and clear ownership.

Also add a `class_commands` Markdown file only if it materially helps classroom
execution, following the Lab 5 pattern.

## Runner requirements

Create:

```text
src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

The runner should:

1. execute the Lab 6 Spark app;
2. support the default passing scenario;
3. optionally support the invalid-record demonstration mode;
4. print or preserve expected markers;
5. make clear where raw metrics, summary results, and rule-level results were
   written;
6. follow existing shell conventions in the repository.

## Root README update

Update the root README Labs section with:

```text
Lab 6: Stage metrics contract gate that validates Spark observability metrics as
an operational data product before they feed automation.
```

## Development workflow

Follow the repository ritual:

1. create a dedicated feature branch for Lab 6 implementation;
2. inspect existing Labs 0-5 before coding;
3. implement only this lab in the first slice;
4. keep `main` clean;
5. test locally before opening the PR;
6. move this TODO from `docs/agent_ops/dev-todos/backlog/` to `docs/agent_ops/dev-todos/done/` only after the
   lab is implemented and validated;
7. open the PR and share the PR description before merging.

## Validation

Before opening the PR, validate at minimum:

```bash
make tests
make validate
make dry-test
```

Also run Lab 6 in both modes if the local stack supports it:

```bash
bash src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

```bash
LAB6_INJECT_INVALID_RECORDS=true \
bash src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

Confirm:

- the passing scenario prints `LAB6_STAGE_METRICS_CONTRACT_PASS`;
- the failure demonstration prints `LAB6_STAGE_METRICS_CONTRACT_FAIL`;
- both runs exit successfully unless there is a real technical failure;
- raw metrics are written to
  `s3a://observability/lab6/stage_metrics_raw`;
- demo invalid input is written only to
  `s3a://observability/lab6/stage_metrics_contract_demo_input`;
- rule-level results are written to
  `s3a://observability/lab6/stage_metrics_contract_results`;
- summary results are written to
  `s3a://observability/lab6/stage_metrics_contract_summary`;
- no Lab 3, Lab 4, or Lab 5 behavior regressed.

## Implementation notes and tradeoffs

- Keep the contract small and explicit. This is a workshop lab, not a Great
  Expectations replacement.
- The failure demo should validate a separate input, not mutate or poison the
  clean raw metrics table.
- Contract failure is a valid educational decision. Technical failures are the
  only cases that should produce a non-zero app exit.
- Prefer rule-level output rows over a single log blob so students can see how
  contract decisions become auditable data.
- The default contract should be strict enough to show operational discipline
  while remaining robust on a small local Spark stack.
