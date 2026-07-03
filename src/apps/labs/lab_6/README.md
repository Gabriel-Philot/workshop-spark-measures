# Lab 6 - Stage Metrics Contract Gate

Subtitle:

```text
Validating Spark observability data before using it for automation
```

Lab 6 treats sparkMeasure StageMetrics as an operational data product.

Collecting Spark metrics is only the first step. Once those metrics become
inputs for dashboards, alerts, PR review, performance gates, runtime budgets, or
historical drift analysis, they need a contract. This lab validates whether the
collected StageMetrics are trustworthy enough to support engineering decisions.

## Why this lab exists

Previous labs showed that StageMetrics can support diagnosis and automation.
Lab 6 asks a more mature platform question: can we trust the metrics before
using them for downstream decisions?

The classroom question is:

```text
Can we trust this metrics row before using it for automation?
```

The teaching flow is:

```text
collect metrics -> validate contract -> use metrics with confidence
```

## Stage-level only

This lab intentionally uses only sparkMeasure StageMetrics.

It does not use:

- TaskMetrics;
- Flight Recorder;
- Spark event-log parsing;
- task-level analysis.

The workshop narrative is stage-first: StageMetrics are the default diagnostic
and automation layer because they are usually cheaper and easier to operationalize
than task-level collectors.

## Workload

The lab runs a small retail workload over generated Delta tables:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

The business output is revenue by month, region, and category:

```text
order_month
region
category
gross_revenue
order_count
customer_count
```

The workload is not meant to be pathological. The point is not diagnosis here.
The point is validating the observability data created by the workload.

## Contract layers

Lab 6 validates three layers.

### 1. Schema Contract

The schema contract validates that required columns exist.

Examples:

- `run_id`;
- `app_name`;
- `workload_name`;
- `num_stages`;
- `num_tasks`;
- `executor_run_time_ms`;
- `created_at`.

Teaching message:

```text
Without a stable schema, every downstream automation is fragile.
```

### 2. Semantic Contract

The semantic contract validates that values make sense.

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

### 3. Correlation Contract

The correlation contract validates whether the metrics can be joined, grouped,
audited, and reused.

Examples:

- `run_id is not null`;
- `app_name is not null`;
- `workload_name is not null`;
- `workload_variant is not null`;
- `collector_name = sparkmeasure_stage_metrics`;
- `metric_scope = stage`;
- no duplicate rows by `run_id + workload_name + workload_variant + metric_scope`.

Teaching message:

```text
Without correlation keys, observability data becomes loose numbers that cannot
support automation.
```

## Metric availability

Some StageMetrics counters are required for this lab:

```text
num_stages
num_tasks
executor_run_time_ms
```

Other counters are useful but collector/environment dependent:

```text
shuffle_bytes_written
shuffle_bytes_read
jvm_gc_time_ms
memory_bytes_spilled
disk_bytes_spilled
input_bytes
```

Lab 6 keeps availability explicit. Optional counters are stored with both a
value and a boolean availability column, for example:

```text
shuffle_bytes_written
shuffle_bytes_written_available
```

This preserves the distinction between:

```text
shuffle_bytes_written = 0 and shuffle_bytes_written_available = true
```

and:

```text
shuffle_bytes_written = 0 and shuffle_bytes_written_available = false
```

The first means the collector emitted a real zero. The second means the collector
did not emit that counter and the numeric value is only a safe placeholder.

## Consumer assumptions

| Downstream consumer | Contract assumption |
| --- | --- |
| Dashboard | `created_at`, `app_name`, `workload_name`, and `metric_scope` exist so the metrics can be grouped and filtered. |
| Alerts | Runtime, shuffle, spill, and GC counters are non-negative before thresholds are evaluated. |
| Runtime budgets | `executor_run_time_ms`, `num_stages`, `num_tasks`, and optional metric availability fields are trustworthy before a gate makes a decision. |
| PR review evidence | `run_id` and `workload_variant` identify which workload run produced the evidence. |
| Historical drift monitoring | Stable keys and timestamps allow metrics to be compared across runs without double-counting duplicates. |

## Output paths

Business output:

```text
s3a://lakehouse/gold/lab6/stage_metrics_contract_gate/business_output
```

Clean raw StageMetrics:

```text
s3a://observability/lab6/stage_metrics_raw
```

Optional invalid demonstration input:

```text
s3a://observability/lab6/stage_metrics_contract_demo_input
```

Rule-level contract results:

```text
s3a://observability/lab6/stage_metrics_contract_results
```

Contract summary:

```text
s3a://observability/lab6/stage_metrics_contract_summary
```

## Expected markers

Progress markers:

```text
LAB6_STAGE_METRICS_CAPTURED_OK
LAB6_STAGE_METRICS_INPUT_OK
LAB6_CONTRACT_RULES_LOADED_OK
LAB6_SCHEMA_CONTRACT_EVALUATED
LAB6_SEMANTIC_CONTRACT_EVALUATED
LAB6_CORRELATION_CONTRACT_EVALUATED
LAB6_CONTRACT_RESULTS_WRITTEN_OK
```

Exactly one final contract marker:

```text
LAB6_STAGE_METRICS_CONTRACT_PASS
LAB6_STAGE_METRICS_CONTRACT_FAIL
```

The default classroom run should print:

```text
LAB6_STAGE_METRICS_CONTRACT_PASS
```

The invalid-record demonstration should print:

```text
LAB6_STAGE_METRICS_CONTRACT_FAIL
```

That failure is an expected contract decision, not a technical application
failure.

## Run the passing scenario

Start the platform and generate data if needed:

```bash
make compose
make generate SCALE=xs
```

Run the lab:

```bash
bash src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

## Run the failure demonstration

The demo injects invalid rows into a separate validation input. It does not
corrupt the clean raw metrics table.

```bash
LAB6_INJECT_INVALID_RECORDS=true \
bash src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

Injected examples include:

- null `run_id`;
- `num_stages = 0`;
- `num_tasks = 0`;
- `shuffle_bytes_written = -1`;
- duplicate uniqueness key;
- null `created_at`;
- invalid `metric_scope = task`.

## Production mapping

This pattern maps directly to:

- dashboard input validation;
- alert input validation;
- PR review evidence checks;
- performance promotion gates;
- historical monitoring;
- drift analysis over Spark workload metrics.

The lab is intentionally small. It is not a generic data quality framework.
It demonstrates the reliability layer a mature Spark observability platform
needs before metrics become automation inputs.
