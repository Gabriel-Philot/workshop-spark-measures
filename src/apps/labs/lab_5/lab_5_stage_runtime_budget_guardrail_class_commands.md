# Lab 5 class commands: stage-level runtime budget guardrail

This file keeps the classroom commands separate from the lab explanation.

Use the short runner command during the workshop. The expanded command is here
only to make explicit what the runner sends to `spark-submit`.

## 1. Start the local platform

Run from the repository root:

```bash
make compose
```

## 2. Generate XS retail data

Run this if the bronze retail tables are not already available:

```bash
make generate SCALE=xs
```

Expected source tables:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

## 3. Run Lab 5 with the classroom runner

This is the command to use during class:

```bash
bash src/apps/labs/lab_5/run_stage_runtime_budget_guardrail.sh
```

The runner:

1. submits the Spark app inside the `spark-master` container;
2. preserves the expected log markers;
3. confirms that exactly one final decision marker was printed;
4. prints the output paths for business outputs, metrics, and decisions.

## 4. Equivalent expanded submit command

The runner wraps this command:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH="/opt/spark/src:/opt/spark/generator/src" \
    LAB5_CONFIG_NAME=lab5-stage-runtime-budget-guardrail \
    LAB5_STAGE_RUNTIME_BUDGET_RUNS_PATH=s3a://observability/lab5/stage_runtime_budget_runs \
    LAB5_STAGE_RUNTIME_BUDGET_DECISIONS_PATH=s3a://observability/lab5/stage_runtime_budget_decisions \
    LAB5_BASELINE_OUTPUT_PATH=s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline \
    LAB5_CANDIDATE_OUTPUT_PATH=s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/src:/opt/spark/generator/src" \
    /opt/spark/src/apps/labs/lab_5/lab_5_stage_runtime_budget_guardrail.py
```

## 5. Expected markers

The runner should confirm these progress markers:

```text
LAB5_BASELINE_STAGE_METRICS_OK
LAB5_CANDIDATE_STAGE_METRICS_OK
LAB5_OUTPUT_COMPATIBILITY_OK
LAB5_BUDGET_RULES_LOADED_OK
LAB5_RUNTIME_BUDGET_DECISION_WRITTEN_OK
```

It should also confirm exactly one final decision marker:

```text
LAB5_RUNTIME_BUDGET_PASS
LAB5_RUNTIME_BUDGET_FAIL
LAB5_RUNTIME_BUDGET_WARNING_LOW_SIGNAL
```

The default classroom setup is expected to produce:

```text
LAB5_RUNTIME_BUDGET_FAIL
```

That is intentional. The business output is compatible, but the candidate PR is
operationally more expensive than the approved baseline.

## 6. Output paths

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

## 7. Optional cleanup after class

Stop the stack:

```bash
make down
```

Remove generated local Docker volumes/data:

```bash
make clean-data
```

Remove Docker images only when you intentionally want to rebuild later:

```bash
make removeimage
```
