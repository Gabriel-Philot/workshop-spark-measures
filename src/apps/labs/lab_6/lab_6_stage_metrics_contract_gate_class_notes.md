# Lab 6 class notes: Stage Metrics Contract Gate

## Core idea

Observability metrics are data.

Once Spark metrics feed runtime budgets, dashboards, alerts, performance
reviews, PR checks, or historical drift analysis, they are no longer loose logs.
They become an operational data product.

The platform should validate that data before using it for engineering
decisions.

## Narrative

Previous labs showed that StageMetrics can become more than raw logs: they can
support diagnosis, review, and automation.

Lab 6 asks a more mature platform question:

```text
What if the guardrail is reading bad observability data?
```

Examples:

- `run_id` is null, so the row cannot be traced to a workload run.
- `metric_scope` says `task`, but the downstream consumer expects `stage`.
- `num_stages = 0`, so the collector did not capture useful stage evidence.
- `shuffle_bytes_written = -1`, so the counter is not semantically valid.
- the uniqueness key is duplicated, so automation may double-count a run.

The answer is a contract gate:

```text
collect metrics -> validate contract -> use metrics with confidence
```

## Metric availability is part of the contract

The lab does not treat every missing optional metric as a real zero.

For optional counters, Lab 6 stores both the numeric value and an availability
flag:

```text
shuffle_bytes_written
shuffle_bytes_written_available
```

This distinction matters:

- `shuffle_bytes_written = 0` and `shuffle_bytes_written_available = true`
  means the collector emitted a real zero.
- `shuffle_bytes_written = 0` and `shuffle_bytes_written_available = false`
  means the collector did not emit the counter and the zero is only a safe
  placeholder.

That keeps downstream systems from confusing "no shuffle happened" with "we do
not have shuffle evidence."

## Three contract layers

### Schema Contract

A metrics table without schema guarantees is fragile.

If `executor_run_time_ms` disappears or `run_id` is renamed, downstream
automation breaks or silently makes wrong decisions.

The schema contract asks:

```text
Do the columns needed by downstream automation exist?
```

### Semantic Contract

A column existing does not mean the metric is usable.

If `num_tasks` is zero or a byte counter is negative, the row may have the right
shape but still carry invalid evidence.

The semantic contract asks:

```text
Do the metric values make sense?
```

### Correlation Contract

Observability rows need identity.

Without `run_id`, `app_name`, `workload_name`, `workload_variant`,
`collector_name`, and `metric_scope`, the row becomes a loose number. It cannot
reliably support lineage, review, debugging, grouping, or automation.

The correlation contract asks:

```text
Can this metrics row be traced, joined, grouped, and audited?
```

In a real platform, the uniqueness key may need more than the workshop fields.
Production systems often add `application_id`, attempt id, job id, pipeline id,
Git SHA, environment, or collector window id.

## Consumer assumptions

| Downstream consumer | What it assumes |
| --- | --- |
| Dashboard | `created_at`, `app_name`, `workload_name`, and `metric_scope` exist for grouping and filtering. |
| Alerts | Runtime, shuffle, spill, and GC counters are non-negative before thresholds fire. |
| Runtime budgets | Required metrics and optional availability flags are trustworthy before a promotion decision. |
| PR review evidence | `run_id` and `workload_variant` identify the workload run behind the evidence. |
| Historical drift monitoring | Stable keys and timestamps allow comparable runs without duplicate inflation. |

## What the lab writes

Clean raw StageMetrics:

```text
s3a://observability/lab6/stage_metrics_raw
```

Optional invalid classroom demo input:

```text
s3a://observability/lab6/stage_metrics_contract_demo_input
```

Rule-level results:

```text
s3a://observability/lab6/stage_metrics_contract_results
```

Summary decision:

```text
s3a://observability/lab6/stage_metrics_contract_summary
```

## Important teaching distinction

There are two different failure types:

1. Technical failure
   - missing input data;
   - invalid YAML;
   - broken Spark session;
   - failed Delta write.

2. Contract failure
   - metrics were collected;
   - the app completed;
   - but the metrics are not trustworthy enough for automation.

A contract failure is a valid educational output. In the failure demo, the app
should still exit successfully and print:

```text
LAB6_STAGE_METRICS_CONTRACT_FAIL
```

## Main takeaway

A mature Spark observability platform does not only collect metrics.

It validates that the metrics have:

- stable schema;
- valid values;
- traceable identity;
- clear ownership through contract versioning.

That is the same principle behind production-grade data products.
