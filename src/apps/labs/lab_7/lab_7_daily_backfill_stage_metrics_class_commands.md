# Lab 7B class commands: daily backfill StageMetrics by date

This file keeps classroom commands separate from the lab explanation.

## 1. Start the local platform

Run from the repository root:

```bash
make compose
```

## 2. Generate sources

Retail only:

```bash
make generate SCALE=xs
```

All sources, retail first and Lab 7 temporal source last:

```bash
make generate-all SCALE=xs
```

Lab 7 source only:

```bash
make generate-lab7
```

## 3. Run a calibration subset

Use this first during development or class rehearsal:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

This compares:

```text
2026-01-01 -> normal day -> 10,000 rows
2026-01-04 -> large spike -> 1,000,000 rows
```

## 4. Run the full 14-date batch

```bash
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

The runner submits one Spark application per processing date.

## 5. Output paths

Business output per date:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard/processing_date=<YYYY-MM-DD>/filter_strategy=early_partition_filter
```

Metrics table:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

## 6. Expected markers

Per submit:

```text
LAB7_DAILY_BACKFILL_CONFIG_OK
LAB7_DAILY_BACKFILL_RUN_OK
LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK
LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK
LAB7_DAILY_BACKFILL_STAGE_METRICS_OK
```

Batch:

```text
LAB7_DAILY_BACKFILL_BATCH_COMPLETED
```
