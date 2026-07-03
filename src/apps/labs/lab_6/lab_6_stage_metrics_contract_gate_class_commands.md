# Lab 6 class commands: stage metrics contract gate

This file keeps the classroom commands separate from the lab explanation.

Use the short runner commands during the workshop. The expanded command is here
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

## 3. Run the passing scenario

This is the default classroom command:

```bash
bash src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

Expected final marker:

```text
LAB6_STAGE_METRICS_CONTRACT_PASS
```

## 4. Run the failure demonstration scenario

This command injects controlled invalid rows into a separate validation input:

```bash
LAB6_INJECT_INVALID_RECORDS=true \
bash src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

Expected final marker:

```text
LAB6_STAGE_METRICS_CONTRACT_FAIL
```

This is not a technical failure. It is the contract gate correctly rejecting
untrustworthy metrics evidence.

## 5. Equivalent expanded submit command

The runner wraps this command:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH="/opt/spark/src:/opt/spark/generator/src" \
    LAB6_CONFIG_NAME=lab6-stage-metrics-contract-gate \
    LAB6_BUSINESS_OUTPUT_PATH=s3a://lakehouse/gold/lab6/stage_metrics_contract_gate/business_output \
    LAB6_STAGE_METRICS_RAW_PATH=s3a://observability/lab6/stage_metrics_raw \
    LAB6_STAGE_METRICS_CONTRACT_DEMO_INPUT_PATH=s3a://observability/lab6/stage_metrics_contract_demo_input \
    LAB6_STAGE_METRICS_CONTRACT_RESULTS_PATH=s3a://observability/lab6/stage_metrics_contract_results \
    LAB6_STAGE_METRICS_CONTRACT_SUMMARY_PATH=s3a://observability/lab6/stage_metrics_contract_summary \
    LAB6_INJECT_INVALID_RECORDS=false \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/src:/opt/spark/generator/src" \
    /opt/spark/src/apps/labs/lab_6/lab_6_stage_metrics_contract_gate.py \
    --inject-invalid-records false
```

## 6. Expected markers

The runner should confirm these progress markers:

```text
LAB6_STAGE_METRICS_CAPTURED_OK
LAB6_STAGE_METRICS_INPUT_OK
LAB6_CONTRACT_RULES_LOADED_OK
LAB6_SCHEMA_CONTRACT_OK
LAB6_SEMANTIC_CONTRACT_OK
LAB6_CORRELATION_CONTRACT_OK
LAB6_CONTRACT_RESULTS_WRITTEN_OK
```

It should also confirm exactly one final decision marker:

```text
LAB6_STAGE_METRICS_CONTRACT_PASS
LAB6_STAGE_METRICS_CONTRACT_FAIL
```

## 7. Output paths

Business output:

```text
s3a://lakehouse/gold/lab6/stage_metrics_contract_gate/business_output
```

Clean raw StageMetrics:

```text
s3a://observability/lab6/stage_metrics_raw
```

Optional invalid demo input:

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

## 8. Optional cleanup after class

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
