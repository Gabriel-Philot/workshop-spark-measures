# TODO: Lab 5 stage-level runtime budget guardrail

## Context

Create the next stage-first workshop lab:

```text
Lab 5 - Stage-Level Runtime Budget Guardrail
```

The existing labs already cover:

- collecting Spark metrics with sparkMeasure;
- diagnosing specific Spark performance symptoms;
- reasoning about observability overhead;
- classifying a workload through a stage-level operational fingerprint.

Lab 5 should turn the same stage-level evidence into an engineering control.
Instead of only using StageMetrics after a job has already behaved badly, this
lab should show how teams can compare a baseline workload against a candidate
workload and decide whether the candidate stays inside a configurable runtime
budget.

Main teaching message:

```text
Stage-level metrics are not only useful for diagnosis. They can become
lightweight promotion gates that catch expensive Spark regressions before code
reaches production.
```

## Goal

Implement a small, classroom-friendly guardrail that:

1. runs a `baseline` workload;
2. runs a semantically equivalent `candidate` workload;
3. captures sparkMeasure StageMetrics for both variants;
4. validates that both variants produce compatible business output;
5. compares candidate metrics against baseline metrics;
6. applies YAML-configured runtime budget rules;
7. writes the metrics and final decision as Delta;
8. prints clear markers for class demos and validation.

The final decision must be one of:

- `PASS`;
- `FAIL`;
- `WARNING_LOW_SIGNAL`.

The default classroom run should complete successfully even when the guardrail
decision is `FAIL` or `WARNING_LOW_SIGNAL`. A guardrail failure is expected
educational output, not a technical app failure.

## Scope

- Use StageMetrics only.
- Do not use TaskMetrics.
- Do not add Flight Recorder.
- Do not parse Spark event logs.
- Do not introduce task-level analysis.
- Do not require external services beyond the current local stack.
- Do not make Lab 5 hard-dependent on Lab 4 output.
- Do not modify the reference repository under:

```text
/home/philot/compendium/forge/dataship/spark-plat-v0
```

## Expected files

Create a new Lab 5 folder:

```text
src/apps/labs/lab_5/
  README.md
  lab_5_stage_runtime_budget_guardrail.py
  lab_5_stage_runtime_budget_guardrail_class_notes.md
  run_stage_runtime_budget_guardrail.sh
  lab_5_utils/
    __init__.py
    experiments.yaml
    budget_rules.yaml
```

Likely helper files, only if they keep the main app readable:

```text
src/apps/labs/lab_5/lab_5_utils/
  budget.py
  runtime.py
  transformations.py
```

Keep Lab 5 helper behavior inside `lab_5_utils/`. Do not add shared framework
abstractions unless explicitly approved later.

## Implementation requirements

- Follow the app style used by Labs 0, 1, 2, 3, and 4:
  - short main app script;
  - `CONFIG_NAME` classroom switch near the top;
  - configs in `lab_5_utils/experiments.yaml`;
  - lab-specific logic in `lab_5_utils/`;
  - logger output only, no raw `print`;
  - submit command in the app docstring;
  - readable Spark UI job descriptions;
  - Delta writes through existing repository conventions.
- Reuse existing Spark session utilities, path conventions, logging style,
  sparkMeasure wrappers, and shell runner conventions.
- Prefer simple Spark DataFrame transformations and clear Python functions over
  clever abstractions.
- Do not add pandas unless the repository already uses it for similar Spark-side
  workloads.
- Keep the code readable enough for students to inspect during class.

## Workload design

Use generated retail Delta data already available in the platform:

- `sales`;
- `vendors`;
- `products`;
- `customers`.

Implement two semantically equivalent variants. The classroom narrative should
be PR-review oriented: the candidate is functionally correct, but intentionally
regresses operational behavior so the guardrail has something concrete to catch.

### Baseline

The baseline should represent the approved workload:

- prune columns early;
- avoid unnecessary shuffle pressure where possible;
- keep the logic readable for a workshop;
- produce the valid business result used as the comparison baseline.

### Candidate

The candidate should represent a new PR implementation that produces the same
business output shape while introducing a controlled operational regression:

- carry unnecessary columns longer than needed;
- create more shuffle pressure;
- optionally use unnecessary repartitioning;
- keep the inefficiency easy to explain in class.

Optional later extension: add a `candidate_fixed` variant or config after the
first implementation if the class needs a second run that returns the guardrail
to `PASS`.

Suggested business output:

```text
order_month
region
category
gross_revenue
order_count
customer_count
```

Both variants should write to:

```text
s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline
s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate
```

## Output compatibility validation

Fail fast if the business outputs are incompatible.

Validate at minimum:

- same schema;
- same row count;
- same total revenue within a safe numeric tolerance;
- same total order count.

This is part of the teaching point: a candidate can be functionally correct and
still be operationally worse, but first we must prove it is functionally
comparable.

## StageMetrics fields

Capture one metrics row per variant using sparkMeasure StageMetrics aggregate
metrics only.

Persist variant metrics to:

```text
s3a://observability/lab5/stage_runtime_budget_runs
```

Required comparison fields:

- `executor_run_time_ms`;
- `shuffle_bytes_written`;
- `shuffle_bytes_read`, if available;
- `memory_bytes_spilled`, if available;
- `disk_bytes_spilled`, if available;
- `jvm_gc_time_ms`, if available;
- `num_stages`;
- `num_tasks`.

Map actual sparkMeasure metric names carefully, following the style already used
in Lab 4. If a metric is unavailable in the StageMetrics aggregate dictionary,
handle it explicitly and document the limitation.

## Comparison metrics

Compute percentage deltas from baseline to candidate, including:

- `executor_run_time_delta_pct`;
- `shuffle_written_delta_pct`;
- `shuffle_read_delta_pct`;
- `num_tasks_delta_pct`;
- `num_stages_delta_pct`;
- `gc_time_delta_pct`;
- `spill_delta_pct`.

The comparison is relative evidence from the local run, not a universal cost
model.

## Budget rules

Load budget rules from:

```text
src/apps/labs/lab_5/lab_5_utils/budget_rules.yaml
```

Suggested default shape:

```yaml
default_budget:
  max_executor_runtime_growth_pct: 20.0
  max_shuffle_written_growth_pct: 25.0
  max_shuffle_read_growth_pct: 25.0
  max_num_tasks_growth_pct: 35.0
  max_num_stages_growth_pct: 25.0
  fail_on_memory_spill_bytes_above: 0
  fail_on_disk_spill_bytes_above: 0

low_signal:
  min_executor_runtime_ms: 1000
  min_shuffle_bytes: 1048576

profiles:
  shuffle_heavy:
    max_shuffle_written_growth_pct: 15.0
    max_shuffle_read_growth_pct: 15.0
    max_executor_runtime_growth_pct: 25.0

  memory_pressure:
    fail_on_memory_spill_bytes_above: 0
    fail_on_disk_spill_bytes_above: 0
    max_executor_runtime_growth_pct: 15.0

  balanced_or_low_signal:
    max_executor_runtime_growth_pct: 30.0
    max_shuffle_written_growth_pct: 40.0
```

Keep profile-aware budgets optional and simple.

Recommended approach:

1. Recompute a lightweight stage-level profile inside Lab 5 using the same
   general style as Lab 4.
2. Use the profile to annotate the decision.
3. Apply default budget rules unless profile-specific rules are configured.

Do not require Lab 4 outputs to exist.

## Decision output

Persist the final decision to:

```text
s3a://observability/lab5/stage_runtime_budget_decisions
```

The decision row should include at least:

- `run_id`;
- `app_name`;
- `baseline_run_id`;
- `candidate_run_id`;
- `workload_name`;
- `workload_profile`;
- `decision`;
- `failed_rules`;
- `warning_flags`;
- `executor_run_time_delta_pct`;
- `shuffle_written_delta_pct`;
- `shuffle_read_delta_pct`;
- `num_tasks_delta_pct`;
- `num_stages_delta_pct`;
- `baseline_metrics`;
- `candidate_metrics`;
- `created_at`.

## Markers

Print the expected progress markers:

- `LAB5_BASELINE_STAGE_METRICS_OK`;
- `LAB5_CANDIDATE_STAGE_METRICS_OK`;
- `LAB5_OUTPUT_COMPATIBILITY_OK`;
- `LAB5_BUDGET_RULES_LOADED_OK`;
- `LAB5_RUNTIME_BUDGET_DECISION_WRITTEN_OK`.

Print exactly one final decision marker:

- `LAB5_RUNTIME_BUDGET_PASS`;
- `LAB5_RUNTIME_BUDGET_FAIL`;
- `LAB5_RUNTIME_BUDGET_WARNING_LOW_SIGNAL`.

The runner should preserve these markers in the terminal output.

## Documentation requirements

Create:

```text
src/apps/labs/lab_5/README.md
src/apps/labs/lab_5/lab_5_stage_runtime_budget_guardrail_class_notes.md
```

The README should explain:

- lab purpose;
- why stage-level metrics are enough for a first guardrail;
- what the baseline does;
- what the candidate does;
- which metrics are compared;
- what budget rules mean;
- what `PASS`, `FAIL`, and `WARNING_LOW_SIGNAL` mean;
- input paths;
- output paths;
- expected markers;
- how to run the lab;
- how this maps to production promotion gates, CI/CD, PR review, or performance
  review.

Main README message:

```text
Runtime budgets turn Spark observability into an engineering control. Instead
of only diagnosing bad jobs after they run, teams can detect cost and
performance regressions before promotion.
```

The class notes should explain:

- Spark metrics can become policy;
- a guardrail is not perfect cost modeling;
- StageMetrics are enough to catch many expensive regressions;
- the goal is relative change, not a universal cost number;
- a candidate can be functionally correct and still operationally worse;
- `WARNING_LOW_SIGNAL` exists because small local workloads may not produce
  reliable performance evidence;
- the lab is intentionally simple and classroom-friendly.

## Runner requirements

Create:

```text
src/apps/labs/lab_5/run_stage_runtime_budget_guardrail.sh
```

The runner should:

1. execute the Lab 5 Spark app;
2. print or preserve expected markers;
3. make the metrics and decision output paths clear;
4. follow existing shell conventions in the repository.

## Root README update

Update the root README Labs section with a description similar to:

```text
src/apps/labs/lab_5: stage-level runtime budget guardrail that compares baseline
and candidate workloads and produces PASS, FAIL, or WARNING_LOW_SIGNAL decisions
from sparkMeasure aggregate metrics.
```

## Validation

Before moving this TODO to `docs/agent_ops/dev-todos/done/`, validate at minimum:

```bash
make tests
```

If the local stack supports it, also run:

```bash
make compose
make generate SCALE=xs
bash src/apps/labs/lab_5/run_stage_runtime_budget_guardrail.sh
```

Confirm:

- the command succeeds for expected `PASS`, `FAIL`, or `WARNING_LOW_SIGNAL`
  decisions;
- business output compatibility validation passes;
- metrics are written to
  `s3a://observability/lab5/stage_runtime_budget_runs`;
- decisions are written to
  `s3a://observability/lab5/stage_runtime_budget_decisions`;
- business outputs are written under
  `s3a://lakehouse/gold/lab5/stage_runtime_budget/`;
- all expected markers appear exactly as documented;
- Spark UI / History Server job descriptions make the lab identifiable.

Run broader validation if infrastructure, generator, Delta, MinIO, or core
sparkMeasure integration changes are made:

```bash
make validate
make dry-test
```

## Acceptance criteria

- Lab 5 is stage-level only.
- Baseline and candidate produce compatible business outputs.
- StageMetrics rows are captured and persisted for both variants.
- Candidate-vs-baseline deltas are computed.
- YAML budget rules are loaded and applied.
- The final decision is persisted as Delta.
- The app does not exit non-zero for an expected educational guardrail `FAIL` or
  `WARNING_LOW_SIGNAL`.
- Technical failures still fail fast with helpful errors.
- README and class notes are self-contained and classroom-friendly.
- No generated data, local runtime files, JARs, wheels, image contexts, or
  secrets are committed.

## Non-goals

- Do not create a universal Spark cost model.
- Do not teach TaskMetrics in this lab.
- Do not compare task-level distributions.
- Do not parse Spark event logs or History Server files.
- Do not require Lab 4 outputs.
- Do not build a generic CI/CD service.
- Do not modify shared core modules unless explicitly approved later.
