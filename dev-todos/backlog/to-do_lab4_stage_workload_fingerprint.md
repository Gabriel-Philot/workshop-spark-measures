# TODO: Lab 4 stage-level workload fingerprint

## Context

The previous Lab 4 direction analyzed Lab 3 `none` versus `stage` benchmark
overhead. That direction is no longer useful enough for the workshop.

The new Lab 4 should teach how to interpret StageMetrics as an operational
workload fingerprint.

Workshop narrative:

- Lab 3: how much observability costs.
- Lab 4: what a workload looks like using StageMetrics.

## Goal

Refactor the existing Lab 4 implementation into:

```text
Lab 4 - Stage-Level Workload Fingerprint
```

The lab should answer:

```text
What does this Spark workload look like from a stage-level execution perspective?
```

## Scope

- Use StageMetrics only.
- Do not use TaskMetrics.
- Do not add Flight Recorder.
- Do not parse Spark event logs.
- Do not introduce task-level analysis.
- Do not remove or rewrite Lab 3.

## Expected files

```text
src/apps/labs/lab_4/
  README.md
  lab_4_stage_workload_fingerprint.py
  lab_4_stage_workload_fingerprint_class_notes.md
  run_stage_workload_fingerprint.sh
  lab_4_utils/
    __init__.py
    experiments.yaml
    fingerprint.py
    fingerprint_rules.yaml
    runtime.py
    transformations.py
```

## Functional requirements

- Run a small Spark workload using generated retail data:
  - `sales`;
  - `vendors`;
  - `products`;
  - `customers`.
- Collect stage-level aggregate metrics only.
- Derive normalized diagnostic ratios:
  - `shuffle_amplification_ratio`;
  - `gc_time_ratio`;
  - `spill_ratio`;
  - `task_density_score`.
- Treat `bytesRead` as the StageMetrics-reported input byte counter, not as the
  physical Delta table size. Do not trust `shuffle_amplification_ratio` when the
  denominator is below `minimum_reliable_input_bytes`; use absolute shuffle
  volume as the safer fallback signal.
- Persist fingerprint rows to:

```text
s3a://observability/lab4/workload_fingerprints
```

- Persist raw normalized stage aggregate rows to:

```text
s3a://observability/lab4/stage_metrics
```

- Print markers:
  - `LAB4_STAGE_METRICS_CAPTURED_OK`;
  - `LAB4_WORKLOAD_FINGERPRINT_RULES_OK`;
  - `LAB4_WORKLOAD_PROFILE_ASSIGNED_OK`;
  - `LAB4_WORKLOAD_FINGERPRINT_WRITTEN_OK`.

## Profiles

The classifier should be simple and explainable:

- `SHUFFLE_HEAVY`;
- `MEMORY_PRESSURE`;
- `IO_HEAVY_SCAN`;
- `GC_PRESSURE`;
- `MANY_SMALL_TASKS`;
- `LOW_PARALLELISM_SIGNAL`;
- `BALANCED_OR_LOW_SIGNAL`.

## Validation

Run:

```bash
make tests
make validate
make dry-test
```

If the local stack and generated data are available, also run:

```bash
bash src/apps/labs/lab_4/run_stage_workload_fingerprint.sh
```

Confirm the fingerprint Delta output path and expected markers.
