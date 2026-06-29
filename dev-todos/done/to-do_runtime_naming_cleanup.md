# TODO: rename experiment runtime concepts

Date: 2026-06-29.
Status: done.

## Context

The first runtime implementation lived under `spark_workshop.experiments`. That package name was too broad and made the platform harder to explain, because the code owns Spark session lifecycle, workload execution, validation hooks, optional sparkMeasure collection, and optional metric persistence.

## Completed scope

Moved the runtime package to:

```text
src/spark_workshop/runtime/
  __init__.py
  base.py
  runner.py
```

Updated imports across:

- runtime package internals;
- app scripts;
- workshop job contracts;
- tests;
- public package exports;
- docs.

Renamed the architecture note from `docs/experiment-pattern.md` to `docs/runtime-pattern.md`.

## Deferred scope

Class names were intentionally kept for compatibility in this slice:

- `ExperimentRunner`
- `SparkExperiment`
- `ExperimentContext`
- `ExperimentRun`
- `ExperimentConfig`

A future cleanup may rename these to `WorkshopRunner`, `SparkWorkload`, or similar if the benefit is worth the broader diff.

## Acceptance criteria

- No code/docs references to `spark_workshop.experiments` remain.
- Lab 0 scripts keep the same submit commands.
- `make tests` passes.
- `make validate` passes.

## Notes

`sre_sparkmeasures` was intentionally not used for this package because the runtime also executes native workloads without sparkMeasure. SparkMeasure-specific code remains in `metrics/`.
