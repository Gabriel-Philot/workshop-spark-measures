# TODO: Lab 7A temporal source generator

## Context

Implement the first slice of:

```text
Lab 7 - Temporal Backfill Observability
```

This slice creates the deterministic temporal bronze source used by the later
daily backfill, dashboard, and filter-strategy lessons.

## Goal

Create a Lab 7 temporal source generator that:

1. preserves all existing retail and Lab 0-6 data;
2. adds a new bronze source under `s3a://lakehouse/bronze/lab7/...`;
3. uses a configurable but single classroom volume plan;
4. generates visibly different daily volumes with normal, 10x, and 100x days;
5. supports both full generation and append-day generation;
6. validates generated row counts against the volume plan;
7. persists the generated volume plan metadata under `s3a://observability/lab7/...`;
8. prints clear classroom markers.

## Non-negotiable constraints

- Do not write to `s3a://lakehouse/bronze/retail/...`.
- Do not rewrite Lab 0-6 outputs or observability tables.
- Do not make retail `SCALE=s` a required prerequisite.
- Do not use Python row-by-row generation, Faker, or Python UDFs.
- Do not introduce TaskMetrics or Flight Recorder.

## Expected files

```text
src/apps/labs/lab_7/
  README.md
  lab_7_temporal_source_generator.py
  lab_7_temporal_source_generator_class_notes.md
  lab_7_temporal_source_generator_class_commands.md
  run_temporal_source_generator.sh
  lab_7_utils/
    __init__.py
    experiments.yaml
    volume_plan.yaml
    generator.py
```

## Required outputs

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
s3a://observability/lab7/temporal_volume_plan
```

## Required markers

```text
LAB7_TEMPORAL_VOLUME_PLAN_OK
LAB7_TEMPORAL_SOURCE_GENERATED_OK
LAB7_TEMPORAL_SOURCE_VALIDATION_OK
```

Final success marker:

```text
LAB7_TEMPORAL_SOURCE_GENERATOR_OK
```

## Public generation flow

Retail-only generation should keep the existing command:

```bash
make generate SCALE=xs
```

The full classroom source generation command should generate retail data first
and then add the Lab 7 temporal source at the end:

```bash
make generate-all SCALE=xs
```

The Lab 7-only command should also work:

```bash
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

Append-day mode should work with:

```bash
LAB7_GENERATE_MODE=append_day \
LAB7_APPEND_DATE=2026-01-15 \
LAB7_APPEND_VOLUME_MULTIPLIER=100 \
bash src/apps/labs/lab_7/run_temporal_source_generator.sh
```

## Validation

Run at least:

```bash
python3 -m py_compile src/apps/labs/lab_7/lab_7_temporal_source_generator.py src/apps/labs/lab_7/lab_7_utils/generator.py
bash -n src/apps/labs/lab_7/run_temporal_source_generator.sh
make tests
make validate
```

If the local stack supports it, run the Lab 7 generator command and validate the
markers plus generated row counts.
