# TODO: Lab 7B daily backfill StageMetrics by date

## Context

Implement the second slice of:

```text
Lab 7 - Temporal Backfill Observability
```

Task A created the deterministic temporal bronze source and persisted the
expected volume plan. Task B should run daily backfills and capture sparkMeasure
StageMetrics per business date.

## Goal

Create a Lab 7 daily backfill app that:

1. processes one `processing_date` per Spark application;
2. reads `s3a://lakehouse/bronze/lab7/source_events_temporal`;
3. defaults to `early_partition_filter`;
4. writes one daily dashboard business output for that date;
5. captures StageMetrics only;
6. persists one metrics row per `processing_date`;
7. includes the expected source volume from `volume_plan.yaml`;
8. provides a bash runner that submits the default 14 dates one by one.

## Expected files

```text
src/apps/labs/lab_7/
  lab_7_daily_backfill_stage_metrics.py
  lab_7_daily_backfill_stage_metrics_class_notes.md
  lab_7_daily_backfill_stage_metrics_class_commands.md
  run_daily_backfill_stage_metrics.sh
  lab_7_utils/
    backfill.py
    backfill_runtime.py
    transformations.py
```

## Outputs

Business output, one physical Delta path per processed date:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard/processing_date=<YYYY-MM-DD>/filter_strategy=<strategy>
```

Metrics table:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

## Required markers

Per submit:

```text
LAB7_DAILY_BACKFILL_CONFIG_OK
LAB7_DAILY_BACKFILL_RUN_OK
LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK
LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK
LAB7_DAILY_BACKFILL_STAGE_METRICS_OK
```

Runner batch marker:

```text
LAB7_DAILY_BACKFILL_BATCH_COMPLETED
```

## Runner behavior

Default:

```bash
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

Runs all dates from `volume_plan.yaml`.

Calibration subset:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

## Validation

Run at least:

```bash
python3 -m py_compile src/apps/labs/lab_7/lab_7_daily_backfill_stage_metrics.py src/apps/labs/lab_7/lab_7_utils/backfill.py src/apps/labs/lab_7/lab_7_utils/backfill_runtime.py src/apps/labs/lab_7/lab_7_utils/transformations.py
bash -n src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
make tests
```

If the local stack supports it, run a small calibration subset before running
all 14 dates.
