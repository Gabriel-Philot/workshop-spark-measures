# TODO: Lab 7 folder structure refinement

## Context

Lab 7 was intentionally developed in slices:

- temporal source generation;
- daily backfill StageMetrics;
- Streamlit dashboard over persisted Delta metrics.

That kept the implementation safe, but the lab folder now has too many
top-level files. Before treating Lab 7 as classroom-ready, refine the folder
layout so the root of `src/apps/labs/lab_7/` is easier to navigate.

## Goal

Reorganize Lab 7 files without changing behavior.

Suggested direction:

- keep public runner scripts and primary app entry points easy to find;
- move helper-only notes, commands, or implementation details into a clearer
  support folder if that improves readability;
- preserve the current submit commands and dashboard workflow;
- avoid breaking existing docs or Make targets.

## Validation note

Always validate dashboard refinements with the complete 14-date backfill.

The two-date smoke run is useful for quick checks, but it is not enough for
visual review. The dashboard needs the full date range so spike days remain in
the correct temporal position and chart legends/order are tested against the
real classroom story.

## Acceptance criteria

- Lab 7 root folder is easier to scan.
- Existing Lab 7A, 7B, and 7C commands still work.
- Dashboard still reads `s3a://observability/lab7/daily_backfill_stage_metrics`.
- Visual validation uses a 14-date `run_id`, not only a smoke run.
