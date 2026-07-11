# Lab 5 guide: stage-level runtime budget guardrail

This guide is the classroom runbook for Lab 5.

Goal:

```text
run an approved baseline
  -> run a functionally equivalent candidate
  -> validate the business outputs
  -> compare aggregate StageMetrics
  -> apply YAML runtime budgets
  -> persist a PASS, FAIL, or WARNING_LOW_SIGNAL decision
```

Lab 4 interpreted a workload profile. Lab 5 turns the same stage-first
observability principle into an engineering control that could support PR
review or a promotion gate.

Class notes:

[Runtime budget guardrail class
notes](docs/stage_runtime_budget_guardrail_class_notes.md)

Keep those notes open for the workload rationale, budget semantics, low-signal
behavior, and production mapping.

## 0. Confirm the shared workshop prerequisites

Start from the repository root:

```bash
cd workshop-spark-measures
```

This guide assumes that the workshop images and shared Bronze retail sources
already exist.

If this is the first workshop run, or project images and MinIO data were
removed, follow [Lab 0 guide: bootstrap through Bronze data
generation](../lab_0/guide_lab0.md). Follow
sections 1 through 5 there; the full bootstrap sequence is intentionally not
duplicated here.

If only the containers were stopped and MinIO data remains, restart the stack:

```bash
make compose
```

Expected shared sources:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

Teacher notes:

```text
Labs 1-6 share the same generated retail sources. `make down` preserves MinIO
data. Regenerate only after `make clean-data` or when the expected Bronze
tables are missing.
```

## 1. Move to the Lab 5 folder

The remaining commands assume this directory:

```bash
cd src/apps/labs/lab_5
```

Optional sanity check:

```bash
ls
```

Expected classroom entry points:

```text
lab_5_stage_runtime_budget_guardrail.py
run_stage_runtime_budget_guardrail.sh
```

## 2. Establish the promotion-gate question

Ask the class:

```text
Can a functionally correct Spark change still be too expensive to promote?
```

The lab models a PR review:

1. the baseline is the approved implementation;
2. the candidate is a proposed change;
3. both must produce compatible business outputs;
4. StageMetrics measures their operational behavior;
5. YAML rules decide whether candidate growth is acceptable.

Teacher notes:

```text
Functional correctness and operational acceptability are separate gates. Do
not discuss performance policy until the business-output compatibility check
has passed.
```

## 3. Inspect configuration and policy before execution

Open:

```text
lab_5_stage_runtime_budget_guardrail.py
lab_5_utils/experiments.yaml
lab_5_utils/budget_rules.yaml
```

The main app exposes one classroom configuration selector:

```python
CONFIG_NAME = os.environ.get(
    "LAB5_CONFIG_NAME",
    "lab5-stage-runtime-budget-guardrail",
)
```

The experiment config defines:

- baseline and candidate identities;
- partition counts used by each variant;
- the revenue compatibility tolerance;
- StageMetrics as the only collector;
- Bronze inputs and Delta outputs.

The policy file defines maximum candidate growth for:

- executor runtime;
- shuffle written and read;
- number of tasks;
- number of stages;
- memory and disk spill.

It also defines low-signal thresholds and optional profile-specific overrides.
Lab 5 recomputes its lightweight profile and does not depend on persisted Lab 4
output.

Teacher notes:

```text
The YAML values are classroom budgets, not universal Spark thresholds. A real
team would calibrate them against representative data, cluster sizes, and
historical baselines.
```

## 4. Compare the two workload implementations

Both variants produce exactly these business columns:

```text
order_month
region
category
gross_revenue
order_count
customer_count
```

### 4.1 Approved baseline

The baseline keeps the physical plan focused:

```text
sales
  -> prune fact columns
  -> join vendor region
  -> join product category
  -> select the business fact
  -> repartition by business keys
  -> aggregate the Gold result
```

### 4.2 Candidate PR

The candidate preserves the business result but introduces controlled cost:

```text
sales
  -> carry wider payload columns
  -> join vendor region
  -> join product category
  -> join unused customer context
  -> unnecessary round-robin repartition
  -> derive an unused bucket and CPU-burden expression
  -> repartition by the unused bucket
  -> repartition again by business keys
  -> aggregate the same Gold result
```

Teacher notes:

```text
The candidate is intentionally worse for didactic purposes. Ask students to
identify the unnecessary join, wide-row movement, CPU expression, and extra
repartitions before showing the StageMetrics comparison.
```

## 5. Run the runtime budget guardrail

Use the classroom runner:

```bash
bash run_stage_runtime_budget_guardrail.sh
```

The runner performs one `spark-submit`. Inside the application, baseline and
candidate run sequentially under separate StageMetrics collectors.

Expected progress markers:

```text
LAB5_BASELINE_STAGE_METRICS_OK
LAB5_CANDIDATE_STAGE_METRICS_OK
LAB5_OUTPUT_COMPATIBILITY_OK
LAB5_BUDGET_RULES_LOADED_OK
LAB5_RUNTIME_BUDGET_DECISION_WRITTEN_OK
```

Exactly one final marker must appear:

```text
LAB5_RUNTIME_BUDGET_PASS
LAB5_RUNTIME_BUDGET_FAIL
LAB5_RUNTIME_BUDGET_WARNING_LOW_SIGNAL
```

The default classroom configuration is designed to print:

```text
LAB5_RUNTIME_BUDGET_FAIL
```

This expected policy failure still exits with status `0`. A missing source,
invalid YAML, incompatible business output, unsupported metric schema, or
failed Delta write remains a technical failure and exits non-zero.

Optional expanded submit command from the Lab 5 folder:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB5_CONFIG_NAME=lab5-stage-runtime-budget-guardrail \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    /opt/spark/src/apps/labs/lab_5/lab_5_stage_runtime_budget_guardrail.py
```

## 6. Read the final decision block in order

Near the end of the submit, the app prints one boxed block:

```text
## LAB 5 RUNTIME BUDGET GUARDRAIL

### Final decision
decision: ...
workload_profile: ...
failed_rules: ...
warning_flags: ...

### Functional compatibility
status: OK
rows: baseline=... candidate=...
total_revenue: baseline=... candidate=...
total_order_count: baseline=... candidate=...

### Baseline StageMetrics
...

### Candidate StageMetrics
...

### Candidate delta versus baseline
executor runtime: baseline -> candidate | signed percentage | multiplier
shuffle written: baseline -> candidate | signed percentage | multiplier
shuffle read: baseline -> candidate | signed percentage | multiplier
tasks: baseline -> candidate | signed percentage | multiplier
stages: baseline -> candidate | signed percentage | multiplier
GC time: baseline -> candidate | signed percentage | multiplier | supporting signal
spill total: baseline -> candidate | signed percentage | multiplier | supporting signal

### Delta outputs
...
```

Read it in this order:

1. confirm functional compatibility;
2. compare baseline and candidate raw StageMetrics;
3. read each signed delta and multiplier;
4. identify which configured rules failed;
5. only then discuss the final decision.

Teacher notes:

```text
A positive delta means the candidate metric grew relative to baseline. For a
maximum-growth budget, positive growth is operationally worse. For example,
`+121.98% | 2.22x baseline` means the candidate used about 2.22 times the
baseline value, not that it became 121.98 times slower.

A negative delta means the candidate metric decreased. GC and spill are marked
as supporting signals because small local runs can make them oscillate.
```

## 7. Understand the three decisions

`PASS`

: The candidate stayed within all applicable budgets.

`FAIL`

: At least one candidate metric exceeded its configured budget.

`WARNING_LOW_SIGNAL`

: The measured workload was too small for a strong policy decision.

Low signal takes precedence over pass/fail because the platform should not
pretend that tiny local measurements are authoritative.

Teacher notes:

```text
The guardrail does not prove universal cost. It evaluates relative change
inside one controlled environment. Production use needs repeated,
representative baselines and explicit ownership of threshold changes.
```

## 8. Optional: inspect the persisted evidence

Business outputs are overwritten on each run:

```text
s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline
s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate
```

One metrics row per variant is appended to:

```text
s3a://observability/lab5/stage_runtime_budget_runs
```

One final decision row is appended to:

```text
s3a://observability/lab5/stage_runtime_budget_decisions
```

The decision row keeps:

- baseline and candidate run IDs;
- the workload profile and decision;
- failed rules and warning flags;
- percentage deltas;
- serialized baseline and candidate metrics;
- application identity and creation time.

Use the MinIO Console to show the separation between business data and
observability evidence:

```text
http://127.0.0.1:29011
```

Default credentials:

```text
user:     sparkworkshop
password: sparkworkshop123
```

## 9. Optional: correlate with Spark History Server

Open:

```text
http://127.0.0.1:28090
```

Use the printed `application_id` to locate the application. The History Server
can explain the jobs and stages behind a failed budget; the StageMetrics gate
provides the compact policy decision.

## 10. Classroom conclusion

End with:

```text
A Spark change can be functionally correct and still be operationally worse.
Stage-level runtime budgets turn observability into a lightweight engineering
control before promotion.
```

The progression is:

```text
prove equivalent output
  -> compare relative operational evidence
  -> apply explicit budgets
  -> persist the decision
  -> investigate failures with deeper tools only when needed
```

## 11. Optional cleanup after class

From the repository root:

```bash
make down
```

This stops the stack and preserves generated MinIO data. Use the project soft
cleanup drill only when the next workshop run must start from a clean state.
