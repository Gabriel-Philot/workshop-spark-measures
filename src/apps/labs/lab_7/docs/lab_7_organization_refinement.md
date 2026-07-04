# Lab 7 organization refinement notes

## Purpose

Lab 7 is now functionally useful, but the folder is too noisy for classroom use.
This note exists so the next refinement pass can reorganize the lab without
changing behavior.

This note records the organization decision used during the Lab 7 cleanup.

## Current issue

The root of `src/apps/labs/lab_7/` currently mixes different kinds of files:

- primary Spark app entry points;
- shell runners;
- class commands;
- class notes;
- dashboard implementation;
- lab utilities;
- volume/config files.

That made sense during incremental development, but it is harder for students
to scan. The root should show the public lab interface first, not every support
artifact.

## Current target organization

Keep the root focused on files an instructor or student is likely to run or
read first:

```text
src/apps/labs/lab_7/
├── README.md
├── lab_7_temporal_source_generator.py
├── lab_7_daily_backfill_stage_metrics.py
├── run_temporal_backfill_observability.sh
├── dashboard/
├── docs/
└── lab_7_utils/
```

Classroom material now lives under:

```text
src/apps/labs/lab_7/docs/
├── lab_7_class_commands.md
├── lab_7_class_notes.md
└── lab_7_organization_refinement.md
```

Source-only and backfill-only shell runners are implementation support:

```text
src/apps/labs/lab_7/lab_7_utils/runners/
├── run_temporal_source_generator.sh
└── run_daily_backfill_stage_metrics.sh
```

The public classroom runner remains at the lab root.

## Files that should probably stay at root

- `README.md`
- `lab_7_temporal_source_generator.py`
- `lab_7_daily_backfill_stage_metrics.py`
- `run_temporal_backfill_observability.sh`

These are the visible public interface of the lab.

## Dashboard folder

Keep the dashboard implementation inside:

```text
src/apps/labs/lab_7/dashboard/
```

That folder already has a clear boundary:

- `app.py`;
- `config.py`;
- `data.py`.

Do not merge dashboard files into `lab_7_utils/`. The dashboard is a separate
presentation layer, not a Spark runtime utility.

## Validation rule for tomorrow

Always validate Lab 7 dashboard refinements with the complete 14-date backfill.

The two-date smoke run is useful for fast technical checks, but it is not enough
for visual review. It can hide chart-ordering issues and make spike placement
look wrong.

Minimum validation after folder cleanup:

```bash
python3 -m py_compile src/apps/labs/lab_7/lab_7_temporal_source_generator.py
python3 -m py_compile src/apps/labs/lab_7/lab_7_daily_backfill_stage_metrics.py
python3 -m py_compile src/apps/labs/lab_7/dashboard/app.py
bash -n src/apps/labs/lab_7/run_temporal_backfill_observability.sh
bash -n src/apps/labs/lab_7/lab_7_utils/runners/run_temporal_source_generator.sh
bash -n src/apps/labs/lab_7/lab_7_utils/runners/run_daily_backfill_stage_metrics.sh
uv run pytest tests/test_lab7_temporal_generator.py
make tests
make lab7-dashboard
```

For visual validation, use a `run_id` with all 14 dates from:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

## Non-goals

- Do not change Spark workload semantics.
- Do not change the generated temporal source schema.
- Do not change output paths.
- Do not change Make targets unless the current naming blocks clarity.
- Do not rewrite the dashboard logic unless there is a concrete visual issue.

## Acceptance criteria

- Lab 7 root folder is easier to scan.
- Public submit commands remain obvious.
- Support notes are still easy to find.
- Existing Make targets still work.
- Existing class commands still work after path updates.
- Dashboard still reads the same Delta metrics table.
- Visual review uses the full 14-date run.
