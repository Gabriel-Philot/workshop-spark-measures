# Lab 7C class commands: temporal backfill dashboard

## 1. Start the platform

After pulling this lab for the first time, refresh the local image cache:

```bash
make bootstrap
make build
```

Then start the platform:

```bash
make compose
```

## 2. Ensure the Lab 7 temporal source exists

```bash
make generate-lab7
```

## 3. Produce the StageMetrics table consumed by the dashboard

For a short classroom preview:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

For the complete 14-date backfill:

```bash
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

The complete local run took about 9 minutes during calibration.

## 4. Start the dashboard

```bash
make lab7-dashboard
```

Open:

```text
http://127.0.0.1:28501
```

## 5. Dashboard input

```text
s3://observability/lab7/daily_backfill_stage_metrics
```

## 6. If the dashboard is empty

Run Lab 7A and Lab 7B first:

```bash
make generate-lab7
bash src/apps/labs/lab_7/run_daily_backfill_stage_metrics.sh
```

Then refresh the Streamlit page.
