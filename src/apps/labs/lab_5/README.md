# Lab 5: stage-level runtime budget guardrail

Lab 5 turns sparkMeasure StageMetrics into an engineering guardrail.

Instead of only diagnosing bad Spark jobs after they run, this lab compares an
approved baseline workload against a candidate PR workload and decides whether
the candidate stays inside a configurable runtime budget.

Main message:

```text
Runtime budgets turn Spark observability into an engineering control. Instead
of only diagnosing bad jobs after they run, teams can detect cost and
performance regressions before promotion.
```

## Why this lab exists

Earlier labs answer different questions:

- Lab 0: how sparkMeasure exposes compact Spark execution metrics.
- Lab 1 and Lab 2: how to diagnose specific Spark symptoms.
- Lab 3: how much observability costs.
- Lab 4: what a workload looks like from stage-level aggregate evidence.

Lab 5 takes the next step:

```text
Can we turn the same stage-level evidence into a lightweight promotion gate?
```

The classroom story is intentionally close to PR review:

1. The current baseline workload is approved.
2. A candidate PR changes the implementation.
3. The candidate produces the same business output.
4. StageMetrics shows whether the candidate became operationally more
   expensive.
5. YAML budget rules decide whether the candidate passes, fails, or is too
   low-signal to judge.

## Why stage-level only

This lab stays stage-level on purpose.

StageMetrics are a good first guardrail layer because they are compact, cheaper
than task-level collectors, and already expose high-value aggregate counters:

- executor runtime;
- shuffle read and write bytes;
- spill bytes;
- JVM GC time;
- number of stages;
- number of tasks.

TaskMetrics, Flight Recorder, and Spark event-log parsing are out of scope here.
Those tools are useful when deeper diagnosis is needed, but the first guardrail
should be simple enough to run frequently.

## Workloads

The lab uses generated retail Delta data:

- `sales`;
- `vendors`;
- `products`;
- `customers`.

Both workloads produce the same business output shape:

```text
order_month
region
category
gross_revenue
order_count
customer_count
```

### Baseline

The baseline represents the approved workload. It prunes columns early, joins
only the required dimensions, repartitions by the business keys, and writes the
gold output.

### Candidate

The candidate represents a functionally equivalent PR with an operational
regression. It intentionally carries wider rows for longer, joins an unused
customer dimension, adds an unnecessary round-robin repartition, computes a
discarded CPU-burden column, and shuffles by an unused bucket before writing the
same business result.

This makes the teaching point explicit:

```text
A Spark job can be functionally correct and still fail an operational budget.
```

## Budget rules

Rules live in:

```text
src/apps/labs/lab_5/lab_5_utils/budget_rules.yaml
```

The defaults compare candidate growth against baseline:

- executor runtime growth;
- shuffle write growth;
- shuffle read growth;
- task count growth;
- stage count growth;
- memory spill introduced above budget;
- disk spill introduced above budget.

For a baseline-vs-candidate guardrail, existing local baseline spill is treated
as part of the baseline evidence. The default spill rule is meant to catch a
candidate that introduces spill when the approved baseline was inside the spill
budget. This keeps the classroom decision focused on regression, not on
re-litigating every existing baseline symptom.

The app also recomputes a lightweight workload profile so profile-specific
thresholds can tighten or loosen selected rules. Lab 5 does not require Lab 4
outputs to exist.

## Decisions

The final decision is one of:

`PASS`
: The candidate stayed inside the configured runtime budget.

`FAIL`
: The candidate exceeded one or more configured budget thresholds.

`WARNING_LOW_SIGNAL`
: The local workload was too small to make a strong operational decision.

An expected educational `FAIL` or `WARNING_LOW_SIGNAL` does not make the app
exit non-zero. The app fails non-zero only for real technical problems such as
missing input data, invalid YAML, incompatible outputs, unsupported metrics, or
failed Delta writes.

## Run Lab 5

Classroom command reference:

```text
src/apps/labs/lab_5/lab_5_stage_runtime_budget_guardrail_class_commands.md
```

Start the platform and generate data first:

```bash
make compose
make generate SCALE=xs
```

Run the lab:

```bash
bash src/apps/labs/lab_5/run_stage_runtime_budget_guardrail.sh
```

Equivalent submit command:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH="/opt/spark/src:/opt/spark/generator/src" \
    LAB5_CONFIG_NAME=lab5-stage-runtime-budget-guardrail \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/src:/opt/spark/generator/src" \
    /opt/spark/src/apps/labs/lab_5/lab_5_stage_runtime_budget_guardrail.py
```

## Input paths

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

## Output paths

Business outputs:

```text
s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline
s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate
```

StageMetrics rows:

```text
s3a://observability/lab5/stage_runtime_budget_runs
```

Guardrail decisions:

```text
s3a://observability/lab5/stage_runtime_budget_decisions
```

## Expected markers

Progress markers:

```text
LAB5_BASELINE_STAGE_METRICS_OK
LAB5_CANDIDATE_STAGE_METRICS_OK
LAB5_OUTPUT_COMPATIBILITY_OK
LAB5_BUDGET_RULES_LOADED_OK
LAB5_RUNTIME_BUDGET_DECISION_WRITTEN_OK
```

Exactly one final decision marker:

```text
LAB5_RUNTIME_BUDGET_PASS
LAB5_RUNTIME_BUDGET_FAIL
LAB5_RUNTIME_BUDGET_WARNING_LOW_SIGNAL
```

## Classroom takeaway

Stage-level metrics can be used as policy. The goal is not to produce a
universal Spark cost number. The goal is to compare relative change against a
known baseline and make regression conversations concrete during PR review,
performance review, and promotion gates.
