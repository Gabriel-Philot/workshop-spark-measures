# Lab 0: source inventory and sparkMeasure presentation

Lab 0 is split into two scripts on purpose:

- `source_inventory.py` checks whether the generated bronze sources are ready for the labs.
- `sparkmeasure_presentation.py` runs a focused Bronze-to-Silver refinement twice: first with native Spark output, then with sparkMeasure enabled.

This keeps the source-readiness checks separate from the sparkMeasure demonstration. The inventory script intentionally creates several Spark actions because it profiles multiple tables and relationships. The presentation script is the cleaner comparison point because both runs execute the same small refinement workload.

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
- `LAB0_SOURCE_INVENTORY_OK`: successful completion marker.

This script uses `lab0-source-inventory` from `src/config/experiments.yaml` and keeps sparkMeasure disabled.

## 2. sparkMeasure presentation

Run this script after the source inventory. It uses only the bronze `sales` table and writes one Silver Delta table.

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

The script runs two named experiments:

- `lab0-sparkmeasure-presentation-native`
- `lab0-sparkmeasure-presentation-observed`

Both write:

```text
s3a://lakehouse/silver/lab0/vendor_sales_summary
```

The native run prints a formatted Spark physical plan with `explain(mode="formatted")`. The observed run enables sparkMeasure stage metrics and logs a compact line with `numStages`, `numTasks`, `executorRunTime`, and `shuffleBytesWritten`.

Metric persistence is disabled for this presentation experiment:

```yaml
observability:
  enabled: true
  collector: stage
  persist: false
```

That is intentional. It avoids extra Delta write jobs for the metrics table, so the Spark History view stays focused on the workload being demonstrated. sparkMeasure still collects metrics around `workload()` and exposes them through the terminal logs.

## UI walkthrough

After submitting the scripts, open the Spark History Server at <http://127.0.0.1:28090>.

Look for these applications:

- `workshop-lab0-source-inventory`
- `workshop-lab0-sparkmeasure-presentation-native`
- `workshop-lab0-sparkmeasure-presentation-observed`

For each application:

1. Open the application from the History Server list.
2. Use the `Jobs` tab to compare how many jobs the application created.
3. Use the `Stages` tab to inspect stage duration, task counts, and shuffle columns.
4. Use the `SQL / DataFrame` tab when available to inspect Spark SQL execution details.
5. Use the terminal output from the observed run to compare native Spark UI detail with sparkMeasure's compact stage summary.

The useful contrast for the workshop is:

- Spark native UI is detailed and useful, but students need to navigate jobs, stages, SQL plans, and logs.
- sparkMeasure gives a compact workload-level summary directly in the submit output, which is easier to use while diagnosing a specific experiment.

Playwright validation against the local UI confirmed these navigation details:

- The History Server home table shows the app name, but the clickable application link is the App ID.
- Application pages expose `Jobs`, `Stages`, `Storage`, `Environment`, `Executors`, and `SQL / DataFrame` tabs.
- The source inventory app is intentionally noisier because it profiles several tables, physical file sizes, and FK checks; do not use it as the sparkMeasure comparison point.
- The presentation app is the comparison point. It has one intentional refinement write, but Delta metadata and commit internals still appear as Spark jobs and stages. Exact UI counts can change depending on prior table state and overwrite behavior.

## MinIO walkthrough

Open MinIO Console at <http://127.0.0.1:29011>.

Useful paths:

- `lakehouse/bronze/retail/vendors`
- `lakehouse/bronze/retail/products`
- `lakehouse/bronze/retail/customers`
- `lakehouse/bronze/retail/sales`
- `lakehouse/silver/lab0/vendor_sales_summary`
- `observability/event-logs`

For this lab, sparkMeasure metrics are not persisted as a Delta table. Event logs still go to `observability/event-logs` because the submit command sets `spark.eventLog.dir`.
