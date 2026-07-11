# To-do: refine the Lab 5 to Lab 6 teaching bridge

## Context

Lab 5 and Lab 6 are technically independent but conceptually connected:

```text
Lab 5 asks whether a candidate workload is operationally acceptable.
Lab 6 asks whether the observability evidence is trustworthy enough to support
that decision.
```

The distinction is subtle and should be refined only after the instructor has
prepared the final classroom narrative.

## Goal

Make the transition between the two labs explicit without adding framework
complexity or turning Lab 6 into cross-source metric reconciliation.

## Future refinement

- Explain the difference between validating a workload policy in Lab 5 and
  validating the StageMetrics data product in Lab 6.
- Document the production ordering:
  `collect -> contract gate -> runtime budget -> promotion decision`.
- Explain the source and purpose of the Lab 6 execution metadata:
  `run_id`, `application_id`, `app_name`, `lab_id`, `workload_name`,
  `workload_variant`, `collector_name`, `metric_scope`, `contract_version`, and
  `created_at`.
- Explain that `application_id` is persisted for Spark UI/History Server
  correlation but is not currently part of the required identity contract or
  uniqueness key.
- Explain optional metric availability fields and the difference between a real
  zero and an unavailable counter represented by a safe numeric placeholder.
- Explain validation metadata such as `validation_run_id`, `source_path`,
  `rule_id`, `rule_type`, `severity`, `failed_count`, `sample_failed_keys`, and
  `recommendation`.
- Connect each metadata guarantee to a concrete consumer: dashboards, alerts,
  runtime budgets, PR review evidence, and historical drift monitoring.

## Scope guardrails

- Keep both labs stage-level only.
- Do not add TaskMetrics, Flight Recorder, or Spark event-log parsing.
- Do not claim that the contract proves sparkMeasure numerical accuracy.
- Do not change runtime behavior until the teaching narrative is reviewed.
- Prefer concise additions to the existing Lab 5 and Lab 6 guides and class
  notes instead of creating another classroom document.

## Acceptance criteria

- The instructor can explain the Lab 5/Lab 6 difference in one short transition.
- Students can distinguish workload acceptance from metrics trustworthiness.
- The Lab 6 guide identifies the origin and purpose of its operational metadata.
- The production ordering and workshop ordering are both explicit.
- No Spark workload, collector, contract rule, or persisted schema changes are
  introduced by the documentation refinement.
