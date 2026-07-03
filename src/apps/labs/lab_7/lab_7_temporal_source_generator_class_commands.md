# Lab 7A class commands: temporal source generator

This file keeps classroom commands separate from the lab explanation.

## 1. Start the local platform

Run from the repository root:

```bash
make compose
```

## 2. Generate retail-only sources

This keeps the historical generator behavior and creates only the retail bronze
sources used by Labs 0-6:

```bash
make generate SCALE=xs
```

## 3. Generate all classroom sources

This command runs retail first and then adds or validates the Lab 7 temporal
bronze source:

```bash
make generate-all SCALE=xs
```

## 4. Run only the Lab 7 temporal source generator

Use this when retail data already exists:

```bash
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

Expected output paths:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
s3a://observability/lab7/temporal_volume_plan
```

## 5. Append one new business date

Use this to simulate a new daily partition arriving after the historical source
already exists:

```bash
LAB7_GENERATE_MODE=append_day \
LAB7_APPEND_DATE=2026-01-15 \
LAB7_APPEND_VOLUME_MULTIPLIER=100 \
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

## 6. Scoped Lab 7 calibration reset

Use only when calibrating the Lab 7 source volume:

```bash
LAB7_REPLACE_SOURCE=true \
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

This deletes only:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

It does not delete retail data.

## 7. Equivalent expanded submit command

The runner wraps this command:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH="/opt/spark/src:/opt/spark/generator/src" \
    LAB7_CONFIG_NAME=lab7-temporal-source-generator \
    LAB7_TEMPORAL_SOURCE_PATH=s3a://lakehouse/bronze/lab7/source_events_temporal \
    LAB7_TEMPORAL_VOLUME_PLAN_PATH=s3a://observability/lab7/temporal_volume_plan \
    LAB7_GENERATE_MODE=full \
    LAB7_APPEND_DATE="" \
    LAB7_APPEND_VOLUME_MULTIPLIER=1 \
    LAB7_REPLACE_SOURCE=false \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/src:/opt/spark/generator/src" \
    /opt/spark/src/apps/labs/lab_7/lab_7_temporal_source_generator.py \
    --mode full \
    --append-date "" \
    --append-volume-multiplier 1 \
    --replace-lab7-source false
```

## 8. Expected markers

```text
LAB7_TEMPORAL_VOLUME_PLAN_OK
LAB7_TEMPORAL_SOURCE_GENERATED_OK
LAB7_TEMPORAL_SOURCE_VALIDATION_OK
LAB7_TEMPORAL_SOURCE_GENERATOR_OK
```
