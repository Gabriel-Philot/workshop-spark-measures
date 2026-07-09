# Lab 0 guide: source inventory and sparkMeasure introduction

This guide is the classroom runbook for Lab 0.

Goal:

```text
prepare local stack
  -> generate bronze retail data
  -> inspect source readiness
  -> show native sparkMeasure API
  -> show the workshop contract with sparkMeasure enabled
```

Lab 0 should be run before the diagnosis labs. It gives students the baseline
context: what data exists, what Spark exposes natively, what sparkMeasure adds,
and why the workshop uses a small execution contract instead of raw scripts
everywhere.

Reference note:

```text
docs/contract_rationale.md
```

Use it when explaining why Lab 0 shows the native sparkMeasure API first and
then introduces the workshop contract.

## 0. Start from the repository root

```bash
cd workshop-spark-measures
```

Expected:

- `Makefile` exists;
- `.env.example` exists;
- `src/apps/labs/lab_0` exists.

## 1. Bootstrap local dependencies

Run this once per machine, or whenever pinned dependencies change.

```bash
make bootstrap
```

Why:

- creates or updates `.env`;
- syncs the local Python environment;
- pulls pinned base images;
- downloads Spark/Delta/S3A/sparkMeasure artifacts;
- prepares the Python wheel cache used by Spark jobs.

Expected final line:

```text
Bootstrap completed
```

## 2. Build local images

```bash
make build
```

Why:

- builds the local Spark runtime image;
- builds the Spark History image;
- builds MinIO images;
- builds the Lab 7 dashboard image, even though Lab 0 will not use it.

Expected:

- command exits with status `0`;
- no missing bootstrap artifact errors.

## 3. Start the platform

```bash
make compose
```

Why:

- starts MinIO;
- creates the required buckets;
- starts Spark Master;
- starts two Spark workers;
- starts Spark History Server.

Expected readiness lines:

```text
Validation passed
MinIO is ready
Spark Master is ready
Spark Workers (2) is ready
Spark History is ready
```

Useful UIs:

```text
Spark Master UI:     http://127.0.0.1:28091
Spark History Server: http://127.0.0.1:28090
MinIO Console:       http://127.0.0.1:29011
```

Default MinIO credentials:

```text
user:     sparkworkshop
password: sparkworkshop123
```

## 4. Run the sparkMeasure dry test

```bash
make dry-test
```

Why:

- proves Spark can submit jobs;
- proves Delta and S3A can read/write through MinIO;
- proves the sparkMeasure JAR and Python package are usable together.

Expected:

- command exits with status `0`;
- local log exists at:

```text
build/var/dry-test.log
```

Do not start Lab 0 if this step fails. Fix the platform first.

## 5. Generate the bronze retail data

```bash
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

Why this run id is explicit:

- `GENERATOR_RUN_ID` does not change the generated schema or scale;
- it labels the generator execution for manifests, logs, and classroom
  troubleshooting;
- `workshop-sparkMeasures-lab1-6` means this is the shared retail source family
  used before the Lab 7 temporal source;
- Lab 7 uses a separate temporal source generated later.

Expected bronze paths:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/sales
```

Expected local log:

```text
build/var/generate-xs.log
```

## 6. Move to the Lab 0 folder

The next commands are easier to present from the Lab 0 folder, while still
using repository-relative paths for Docker Compose.

```bash
cd src/apps/labs/lab_0
```

Optional sanity check:

```bash
ls
```

Expected scripts:

```text
lab_0a_source_inventory.py
lab_0b_sparkmeasure_native_api.py
lab_0c_sparkmeasure_presentation.py
```

## 7. Run 0A: source inventory

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/lab_0a_source_inventory.py
```

Why this comes first:

- confirms the generated sources exist before teaching sparkMeasure;
- shows table row counts and physical file sizes;
- validates fact-to-dimension relationships;
- gives students context for later Spark behavior.

Expected markers:

```text
LAB0_SOURCE_VOLUME
LAB0_RELATIONSHIP_CHECK
LAB0_SOURCE_CHARACTERISTIC
WORKSHOP_RUN_COMPLETED
LAB0_SOURCE_INVENTORY_OK
```

Classroom note:

```text
This is not a sparkMeasure demo yet. It is the source readiness check.
We first prove what data exists and whether it is safe to use in the labs.
```

## 8. Run 0B: native sparkMeasure API

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/lab_0b_sparkmeasure_native_api.py
```

Why this comes second:

- shows the natural sparkMeasure API before the workshop wrapper;
- makes `StageMetrics(spark)`, `begin()`, `end()`, `print_report()`, and
  `aggregate_stagemetrics()` visible;
- gives students a direct mental model of what the library does.

Expected markers:

```text
SPARKMEASURE_NATURAL_API_BEGIN
SPARKMEASURE_NATURAL_API_END
SPARKMEASURE_NATURAL_API_METRICS
LAB0_SPARKMEASURE_NATURAL_API_OK
```

Classroom note:

```text
This is the "raw library" moment. The workshop abstraction is not hiding magic;
it is wrapping this pattern so later labs can focus on diagnosis instead of
boilerplate.
```

## 9. Run 0C: workshop contract presentation

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/lab_0c_sparkmeasure_presentation.py
```

Why this comes third:

- runs the same Bronze-to-Silver enrichment in two modes;
- native mode prints Spark explain output;
- observed mode enables sparkMeasure through YAML config;
- students can compare Spark's verbose native evidence with compact
  sparkMeasure summary metrics.

Expected markers:

```text
LAB0_PRESENTATION_NATIVE_OK
LAB0_PRESENTATION_SPARKMEASURE_OK
LAB0_SPARKMEASURE_PRESENTATION_OK
SPARKMEASURE_METRICS
WORKSHOP_RUN_COMPLETED
```

Expected Silver output:

```text
s3a://lakehouse/silver/lab0/sales_enriched
```

Classroom note:

```text
This is the transition from "how the library is called" to "how a data platform
would operationalize it". The measured workload stays readable, while
observability settings live in configuration.
```

If students ask why the script is structured this way, use:

```text
docs/contract_rationale.md
```

For the detailed Spark UI journey after Lab 0C, use:

[`docs/spark_ui_to_sparkmeasure_walkthrough.md`](docs/spark_ui_to_sparkmeasure_walkthrough.md)

Use that walkthrough when switching from terminal output to Spark History. It
shows how to classify `SPARK_WORKLOAD | ...` rows versus `Delta: ...` rows,
then how to reach the relevant stage and physical plan.

## 10. Review Spark History Server

Open:

```text
http://127.0.0.1:28090
```

Look for applications:

```text
workshop-lab0-source-inventory
workshop-lab0-sparkmeasure-native-api
workshop-lab0-sparkmeasure-presentation-native
workshop-lab0-sparkmeasure-presentation-observed
```

What to inspect:

- `Jobs`: readable job descriptions such as `SPARK_WORKLOAD | ...` and
  `Delta: SPARK_WORKLOAD | ...`;
- the main materialization/write job: the `SPARK_WORKLOAD | ...` row that
  contains `save at NativeMethodAccessorImpl.java:0`; its detail page should
  show an `Associated SQL Query` and a completed stage with both `Input` and
  `Output`;
- `Stages`: stage duration, task counts, shuffle columns;
- `SQL / DataFrame`: physical execution details when available.

Do not read that single Spark UI Job as the whole workload. In this lab it is
the best anchor for the final write path. Delta snapshot/file filtering,
broadcast preparation, async sub-executions, and commit/statistics work can
appear as separate jobs under the same `SPARK_WORKLOAD` boundary. Use the
`Associated SQL Query` to inspect the broader physical plan, and use
sparkMeasure to compare the aggregate measured region.

Classroom note:

```text
History Server is still useful. sparkMeasure does not replace Spark UI; it
summarizes useful evidence so the first diagnostic pass is faster.
```

Detailed walkthrough:

[`docs/spark_ui_to_sparkmeasure_walkthrough.md`](docs/spark_ui_to_sparkmeasure_walkthrough.md)

## 11. Review MinIO

Open:

```text
http://127.0.0.1:29011
```

Useful paths:

```text
lakehouse/bronze/retail/vendors
lakehouse/bronze/retail/products
lakehouse/bronze/retail/customers
lakehouse/bronze/retail/sales
lakehouse/silver/lab0/sales_enriched
observability/event-logs
```

Lab 0 intentionally does not persist sparkMeasure metrics as Delta tables.
Metric persistence is disabled for the presentation experiment so the History
Server stays focused on the workload jobs instead of metrics-write jobs.

## 12. Optional cleanup after class

Return to the repository root:

```bash
cd ../../../..
```

Stop containers:

```bash
make down
```

Remove generated local Docker volumes/data only when you want a clean rerun:

```bash
make clean-data
```
