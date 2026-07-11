# Lab 5 class notes: stage-level runtime budget guardrail

Classroom runbook:

[Lab 5 classroom guide](../guide_lab5.md)

## Question

Can we use sparkMeasure StageMetrics as a lightweight promotion gate for Spark
jobs?

Lab 5 says yes, with an important limitation: this is not perfect cost modeling.
It is a disciplined relative comparison between a known baseline and a candidate
implementation.

## Narrative

The candidate PR is functionally correct. That is not enough.

The lab validates that baseline and candidate produce compatible outputs:

- same schema;
- same row count;
- same total revenue within tolerance;
- same total order count.

Only after the functional comparison passes does the lab evaluate operational
evidence from StageMetrics.

This creates the teaching moment:

```text
The output is correct, but the implementation may still be too expensive to
promote.
```

## Workshop connection

Lab 4 used StageMetrics to assign a lightweight operational profile. Lab 5
keeps the same stage-first principle and asks whether aggregate evidence can
become policy: is a candidate still acceptable relative to an approved
baseline?

## Why StageMetrics are enough here

StageMetrics are aggregate counters. They do not tell us which exact task was
bad, but they are enough for many first-pass guardrails:

- candidate executor runtime grew too much;
- candidate shuffle write or read grew too much;
- candidate created too many tasks;
- candidate created too many stages;
- candidate introduced memory or disk spill.

That is enough to block or flag many expensive Spark regressions before a job
reaches production.

## What the baseline does

The baseline represents the approved implementation:

```python
inputs["sales"]
    .transform(_select_sales_for_guardrail)
    .transform(_join_vendor_region, inputs["vendors"])
    .transform(_join_product_category, inputs["products"])
    .transform(_select_business_fact)
    .repartition(keyed_partitions, "order_month", "region", "category")
    .transform(_aggregate_business_output)
```

It prunes columns early and uses a keyed repartition before the business
aggregation.

## What the candidate does

The candidate represents a PR with a controlled operational regression:

```python
inputs["sales"]
    .transform(_select_wide_sales_for_candidate)
    .transform(_join_vendor_region, inputs["vendors"])
    .transform(_join_product_category, inputs["products"])
    .transform(_join_customer_region_as_unused_context, inputs["customers"])
    .repartition(round_robin_partitions)
    .transform(_select_candidate_business_fact, guardrail_buckets)
    .repartition(round_robin_partitions, "guardrail_bucket")
    .transform(_force_candidate_cpu_burden_without_changing_revenue)
    .repartition(keyed_partitions, "order_month", "region", "category")
    .transform(_aggregate_business_output)
```

The candidate still produces the same business columns, but it carries extra
payload columns for longer, joins an unused customer dimension, computes a
discarded CPU-burden column, and shuffles by an unused bucket before the final
business aggregation.

## Metrics to read

The most important fields in the final decision block are:

`executor_run_time_ms`
: Aggregate executor runtime. This is useful for comparing relative runtime
  pressure between baseline and candidate.

`shuffle_bytes_written` and `shuffle_bytes_read`
: Data movement through shuffle. Growth here usually points to wider
  transformations, unnecessary repartitioning, or changed join/aggregation
  strategy.

`num_tasks`
: A coarse signal for scheduling and partitioning pressure.

`num_stages`
: A coarse signal that the candidate introduced extra execution phases.

`memory_bytes_spilled` and `disk_bytes_spilled`
: Evidence that memory pressure crossed into spill behavior.

`jvm_gc_time_ms`
: Supporting evidence for JVM memory/object pressure.

## Budget decision

The rules are configured in:

```text
src/apps/labs/lab_5/lab_5_utils/budget_rules.yaml
```

The lab computes candidate deltas against baseline and applies thresholds such
as:

- maximum executor runtime growth;
- maximum shuffle write growth;
- maximum shuffle read growth;
- maximum task count growth;
- maximum stage count growth;
- memory/disk spill introduced above the configured budget.

If the approved baseline already spills on the local WSL stack, Lab 5 does not
automatically call the candidate a regression for matching that existing spill.
The guardrail is comparing candidate behavior against the baseline, so the
stronger classroom signal is usually shuffle or task growth introduced by the
candidate PR.

## Why `WARNING_LOW_SIGNAL` exists

Local workshop runs are small and noisy. A tiny run can show large percentages
that are not meaningful, or small differences that disappear at a larger scale.

`WARNING_LOW_SIGNAL` exists so the lab can say:

```text
The job ran, but this local evidence is too small to treat as a promotion gate.
```

That is a better engineering habit than pretending every single benchmark run is
authoritative.

## Production mapping

The same pattern can map to:

- PR review: compare branch candidate against main baseline;
- CI/CD: run a representative sample and publish a guardrail decision;
- performance review: persist historical decisions and compare trends;
- incident review: identify when a correct code change introduced operational
  cost.

The important discipline is to persist evidence and compare against a known
baseline rather than relying on one-off terminal impressions.

## Scope reminder

Lab 5 is stage-level only.

TaskMetrics are valuable when the guardrail fails and the team needs deeper
diagnosis. They are intentionally out of scope for this lab because the lesson
is about turning coarse, cheap observability into a first engineering control.
