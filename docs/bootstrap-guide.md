# Bootstrap guide

This guide prepares the local workshop environment from a clean checkout until
the generated Delta datasets required by the labs are available in MinIO.

The workshop does not require separate `make` targets per lesson. The public
bootstrap flow is intentionally small:

```text
bootstrap dependencies
  -> build local images
  -> start the local Spark/MinIO stack
  -> run a sparkMeasure dry test
  -> generate the shared retail data
  -> generate the Lab 7 temporal source
```

## 1. Start from the repository root

```bash
cd workshop-spark-measures
```

Expected state:

- you are inside the cloned workshop repository;
- `Makefile` is present;
- `.env.example` is present.

## 2. Bootstrap local dependencies

```bash
make bootstrap
```

Expected behavior:

- creates `.env` from `.env.example` when missing;
- adds missing `.env` keys when `.env.example` changes;
- runs `uv sync`;
- pulls pinned base images;
- resolves Spark, Delta Lake, Hadoop AWS, and sparkMeasure artifacts;
- downloads pinned Python wheels;
- ends with:

```text
Bootstrap completed
```

If this step fails, fix missing local tools first. See `docs/requirements.md`.

## 3. Build local workshop images

```bash
make build
```

Expected behavior:

- validates the bootstrap cache;
- prepares local Docker build contexts;
- builds the MinIO server image;
- builds the MinIO client image;
- builds the Spark runtime image;
- builds the Spark History image;
- builds the Lab 7 dashboard image.

This must complete before `make compose`, `make dry-test`, or data generation
can run reliably.

## 4. Start the local platform

Default two-worker stack:

```bash
make compose
```

Expected behavior:

- validates local images and port availability;
- starts MinIO;
- creates the expected MinIO buckets;
- starts Spark Master;
- starts two Spark workers;
- starts Spark History Server;
- waits until services are ready.

Expected readiness lines:

```text
Validation passed
MinIO is ready
Spark Master is ready
Spark Workers (2) is ready
Spark History is ready
```

Optional three-worker demo mode:

```bash
make compose-three-workers
```

Expected worker readiness line:

```text
Spark Workers (3) is ready
```

## 5. Run the sparkMeasure dry test

```bash
make dry-test
```

Expected behavior:

- submits `src/apps/sparkmeasure_dry_test.py` to Spark;
- validates Spark, Delta, S3A, MinIO, and sparkMeasure integration;
- writes the dry-test log to:

```text
build/var/dry-test.log
```

Use this step before generating workshop data. It proves that the local stack
can run a Spark job with the pinned sparkMeasure setup.

## 6. Generate the shared retail datasets

For a fast local classroom setup:

```bash
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

`GENERATOR_RUN_ID` labels the generator execution for manifests, logs, and
classroom troubleshooting. It does not change the generated schema or scale.
Here it makes explicit that this is the shared retail source family used before
the separate Lab 7 temporal source.

For the smallest sanity setup:

```bash
make generate SCALE=demo
```

Expected behavior:

- starts the platform if it is not already running;
- submits the generator through Spark;
- writes Delta bronze tables under:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/sales
```

- writes generator run metadata under the observability bucket;
- writes a local generation log such as:

```text
build/var/generate-xs.log
```

These retail datasets are the common source data for Labs 0 through 6.
Lab 7 uses a separate temporal source generated in the next step.

## 7. Generate the Lab 7 temporal source

```bash
make generate-lab7
```

Expected behavior:

- starts the platform if it is not already running;
- validates the Lab 7 volume plan;
- creates or validates the deterministic temporal source;
- writes the Lab 7 source Delta table to:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

- writes the volume plan to:

```text
s3a://observability/lab7/temporal_volume_plan
```

Expected markers include:

```text
LAB7_TEMPORAL_VOLUME_PLAN_OK
LAB7_TEMPORAL_SOURCE_GENERATED_OK
LAB7_TEMPORAL_SOURCE_VALIDATION_OK
LAB7_TEMPORAL_SOURCE_GENERATOR_OK
```

## 8. One-command data setup for all labs

Instead of running steps 6 and 7 separately, use:

```bash
make generate-all SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

Expected behavior:

- generates the shared retail datasets for Labs 0 through 6;
- generates or validates the Lab 7 temporal source;
- leaves MinIO with the data required to run the full lab sequence.

Use this command when preparing the workshop before class.

## 9. Optional: run the full Lab 7 backfill evidence flow

Lab 7 can go beyond source-data generation and produce the daily backfill
StageMetrics table used by the dashboard.

```bash
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

Expected behavior:

- validates the local stack;
- ensures the Lab 7 temporal source exists;
- runs the 14-day daily backfill flow;
- writes business output to:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard
```

- writes StageMetrics by processing date to:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

Expected final marker:

```text
LAB7_TEMPORAL_BACKFILL_OBSERVABILITY_COMPLETED
```

On the reference local setup, the full 14-day flow took about 9.5 minutes once
the temporal source already existed.

## 10. Optional: open the Lab 7 dashboard

```bash
make lab7-dashboard
```

Expected output:

```text
Lab 7 dashboard: http://127.0.0.1:28501
```

Open:

```text
http://127.0.0.1:28501
```

## 11. Useful service URLs

After `make compose`, use:

```bash
make services
```

Expected behavior:

- prints the configured local service URLs for MinIO, Spark Master, Spark
  History Server, and the Lab 7 dashboard.

## 12. Clean local state

Stop containers:

```bash
make down
```

Remove local Docker volumes and generated MinIO data:

```bash
make clean-data
```

Remove workshop-built images:

```bash
make removeimage
```

Use cleanup before re-running a full workshop setup from scratch.
