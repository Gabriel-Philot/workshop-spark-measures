# Lab 0: source inventory and sparkMeasure presentation

Lab 0 is split into three scripts on purpose:

- `source_inventory.py` checks whether the generated bronze sources are ready for the labs.
- `sparkmeasure_native_api.py` shows sparkMeasure's natural Python API directly with `StageMetrics`.
- `sparkmeasure_presentation.py` runs the same Silver enrichment twice: first native, then through the workshop contract with sparkMeasure enabled.

This keeps source readiness, raw sparkMeasure usage, and our workshop abstraction separate.

## Prerequisites

Start the local stack and generate demo data before running this lab.

```bash
make compose
make generate SCALE=xs GENERATOR_RUN_ID=lab0-demo
```

Useful local UIs:

- Spark Master UI: <http://127.0.0.1:28091>
- Spark History Server: <http://127.0.0.1:28090>
- MinIO Console: <http://127.0.0.1:29011>

MinIO credentials are loaded from `.env`; the local defaults are usually `sparkworkshop` / `sparkworkshop123`.

## 1. Source inventory

Run this first to confirm the bronze tables exist, understand their physical size, and preserve the generated relationships.

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/source_inventory.py
```

Expected terminal markers:

- `LAB0_SOURCE_VOLUME`: rows, file count, total physical bytes, min/avg/max file bytes, and columns per source table.
- `LAB0_RELATIONSHIP_CHECK`: FK violation counts for generated relationships.
- `LAB0_SOURCE_CHARACTERISTIC`: final short note that the generated `sales` source has vendor imbalance for later diagnostic labs.
- `WORKSHOP_RUN_COMPLETED`: generic run summary from the shared job contract.
- `LAB0_SOURCE_INVENTORY_OK`: successful completion marker.

## 2. Natural sparkMeasure API

Run this second to show the raw library API before the workshop abstraction.

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/sparkmeasure_native_api.py
```

This script calls `StageMetrics(spark)`, `begin()`, executes a controlled `.show()` action over the Silver enrichment, then calls `end()`, `print_report()`, and `aggregate_stagemetrics()`.

Expected terminal markers:

- `SPARKMEASURE_NATURAL_API_BEGIN`
- `SPARKMEASURE_NATURAL_API_END`
- `SPARKMEASURE_NATURAL_API_METRICS`
- `LAB0_SPARKMEASURE_NATURAL_API_OK`

## 3. Workshop contract presentation

Run this script after the natural API demo. It reads bronze `sales`, `vendors`, and `products`, builds a Silver `sales_enriched` table, and writes it to MinIO.

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/sparkmeasure_presentation.py
```

The script reads comparison metadata from the local `lab_0_utils/experiments.yaml`:

- `comparison_jobs.lab0-sparkmeasure-presentation.native.config`
- `comparison_jobs.lab0-sparkmeasure-presentation.observed.config`

Both write:

```text
s3a://lakehouse/silver/lab0/sales_enriched
```

The native run prints a formatted Spark physical plan with `SPARK_EXPLAIN`. The observed run enables sparkMeasure stage metrics through YAML config and logs `SPARKMEASURE_METRICS` with `numStages`, `numTasks`, `executorRunTime`, and `shuffleBytesWritten`.

Metric persistence is disabled for this presentation experiment:

```yaml
observability:
  enabled: true
  collector: stage
  persist: false
```

That is intentional. It avoids extra Delta write jobs for the metrics table, so the Spark History view stays focused on the workload being demonstrated.

## UI walkthrough

After submitting the scripts, open the Spark History Server at <http://127.0.0.1:28090>.

Look for these applications:

- `workshop-lab0-source-inventory`
- `workshop-lab0-sparkmeasure-native-api`
- `workshop-lab0-sparkmeasure-presentation-native`
- `workshop-lab0-sparkmeasure-presentation-observed`

For each application:

1. Open the application from the History Server list.
2. Use the `Jobs` tab to compare how many jobs the application created.
3. Use the `Stages` tab to inspect stage duration, task counts, and shuffle columns.
4. Use the `SQL / DataFrame` tab when available to inspect Spark SQL execution details.
5. Use the terminal output from natural API and observed contract runs to compare direct sparkMeasure with the workshop wrapper.

## MinIO walkthrough

Open MinIO Console at <http://127.0.0.1:29011>.

Useful paths:

- `lakehouse/bronze/retail/vendors`
- `lakehouse/bronze/retail/products`
- `lakehouse/bronze/retail/customers`
- `lakehouse/bronze/retail/sales`
- `lakehouse/silver/lab0/sales_enriched`
- `observability/event-logs`

For this lab, sparkMeasure metrics are not persisted as a Delta table. Event logs still go to `observability/event-logs` because the submit command sets `spark.eventLog.dir`. The scripts use local `lab_0_utils/experiments.yaml` config and keep Spark/SRE markers separated as `SPARK_*`, `SPARKMEASURE_*`, `WORKSHOP_*`, and lab-specific `LAB0_*` lines.
