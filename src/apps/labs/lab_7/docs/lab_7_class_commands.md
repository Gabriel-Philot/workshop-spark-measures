# Lab 7 class commands: temporal backfill observability

This file is the classroom command sequence for Lab 7.

Lab 7 has three moments:

1. create or validate the isolated temporal source;
2. run a daily backfill and persist StageMetrics by `processing_date`;
3. open the dashboard that reads the persisted metrics table.

## 1. Clean start, when needed

Use this before a full rehearsal if you want to rebuild all local runtime data:

```bash
make clean-data
```

`make clean-data` removes Docker volumes through Compose and clears the local
MinIO data directory under `build/var/minio-data`.

If Docker images were also removed, rebuild them before running the lab:

```bash
make build
```

## 2. Complete Lab 7 run

This is the main classroom runner.

It starts the local stack, ensures the Lab 7 temporal source exists, and then
runs the daily backfill for every date in `volume_plan.yaml`.

```bash
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

The full local run took about 9 minutes during calibration because it starts one
Spark application per processing date.

## 3. Short calibration run

Use this for a quick smoke test or class preview:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

This compares:

```text
2026-01-01 -> normal day -> 10,000 rows
2026-01-04 -> large spike -> 1,000,000 rows
```

Do not use the two-date run to validate dashboard layout. Visual refinement
should use the complete 14-date run so spike days appear in their real temporal
position.

## 4. Open the dashboard

After the backfill has produced metrics:

```bash
make lab7-dashboard
```

Open:

```text
http://127.0.0.1:28501
```

## 5. Output paths

Temporal bronze source:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

Temporal volume plan:

```text
s3a://observability/lab7/temporal_volume_plan
```

Business output by processing date:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard/processing_date=<YYYY-MM-DD>/filter_strategy=early_partition_filter
```

StageMetrics table consumed by the dashboard:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

## 6. Useful environment controls

Skip Compose when the stack is already running:

```bash
LAB7_SKIP_COMPOSE=true \
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

Skip source generation when the Lab 7 temporal source is already valid:

```bash
LAB7_SKIP_SOURCE_GENERATION=true \
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

Reset only the scoped Lab 7 temporal source before regenerating it:

```bash
LAB7_REPLACE_SOURCE=true \
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

Append one new business date to the temporal source:

```bash
LAB7_GENERATE_MODE=append_day \
LAB7_APPEND_DATE=2026-01-15 \
LAB7_APPEND_VOLUME_MULTIPLIER=100 \
bash src/apps/labs/lab_7/lab_7_utils/runners/run_temporal_source_generator.sh
```

## 7. Expected markers

Source generation markers:

```text
LAB7_TEMPORAL_VOLUME_PLAN_OK
LAB7_TEMPORAL_SOURCE_GENERATED_OK
LAB7_TEMPORAL_SOURCE_VALIDATION_OK
LAB7_TEMPORAL_SOURCE_GENERATOR_OK
```

Backfill markers per processing date:

```text
LAB7_DAILY_BACKFILL_CONFIG_OK
LAB7_DAILY_BACKFILL_RUN_OK
LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK
LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK
LAB7_DAILY_BACKFILL_STAGE_METRICS_OK
```

Batch markers:

```text
LAB7_DAILY_BACKFILL_BATCH_COMPLETED
LAB7_TEMPORAL_BACKFILL_OBSERVABILITY_COMPLETED
```

## 8. Internal runners

The public classroom runner is:

```bash
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

The source-only and backfill-only runners are kept under
`lab_7_utils/runners/` as implementation support. Use them only for calibration
or debugging.
