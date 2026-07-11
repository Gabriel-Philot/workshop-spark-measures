# Lab 7: temporal backfill observability

Lab 7 connects deterministic daily source volume, one Spark backfill per
business date, persisted sparkMeasure StageMetrics, and a read-only Streamlit
dashboard.

The lab asks:

```text
Which business dates changed the Spark execution profile, and which stage-level
signals make that change visible?
```

## Classroom material

- [Lab 7 classroom guide](guide_lab7.md): temporal source, optional smoke run,
  full 14-date batch, measured duration, expected markers, and dashboard flow.
- [Temporal backfill observability class
  notes](docs/temporal_backfill_observability_class_notes.md): source design,
  StageMetrics interpretation, limitations, and validated local evidence.

## Workshop connection

```text
Earlier labs: diagnose or govern one Spark execution
  -> Lab 7: persist StageMetrics across business time and compare a backfill history
```

The lab remains stage-level only. Its focus is temporal context, persistence,
and visualization—not task-level drill-down.

## Public entry points

```text
lab_7_temporal_source_generator.py
lab_7_daily_backfill_stage_metrics.py
run_temporal_backfill_observability.sh
```

The public runner ensures the isolated Lab 7 source and executes one sequential
Spark application for each configured date.

## Classroom flow

From the repository root:

```bash
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
make lab7-dashboard
```

Open:

```text
http://127.0.0.1:28501
```

The validated 14-date flow took approximately `9 min 35 s` on the local
WSL/Docker workshop stack. The guide records the exact measurement boundary.

## Main outputs

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
s3a://observability/lab7/temporal_volume_plan
s3a://lakehouse/gold/lab7/daily_activity_dashboard
s3a://observability/lab7/daily_backfill_stage_metrics
```
