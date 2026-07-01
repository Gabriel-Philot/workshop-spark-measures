# TODO: Lab 2B stage metrics interpretation drill

## Context

Create the second Lab 2 lesson from the planning document:

```text
dev-todos/plan/lab2_lesson_mapping.md
```

This lesson should turn sparkMeasure StageMetrics into a practical classroom
reading exercise. The goal is not only to run a workload, but to teach students
how to interpret the metric names and connect them to likely Spark symptoms.

Primary reference: see the private vault mapping in
`dev-todos/plan/lab2_lesson_mapping.md`.

Relevant source themes:

- high GC time can indicate memory pressure;
- shuffle spill memory/disk means data spilled during shuffle;
- max task duration much greater than median suggests skew, but that specific
  distribution diagnosis belongs to a later TaskMetrics lesson.

## Goal

Implement a Lab 2 lesson that uses sparkMeasure StageMetrics to teach how to
read stage-level symptoms.

The classroom story should be:

1. Run one controlled workload that produces visible stage metrics.
2. Inspect compact sparkMeasure output.
3. Explain which stage metrics are direct evidence and which are only symptoms.
4. Make clear when StageMetrics is enough and when the next step should be
   TaskMetrics or Spark UI task inspection.

This lesson should remain stage-level. Use it to prepare students for later
task-level lessons, not to solve those lessons upfront.

## Expected files

Add or extend the Lab 2 structure:

```text
src/apps/labs/lab_2/
  README.md
  stage_metrics_interpretation_drill.py
  stage_metrics_interpretation_drill_class_notes.md
  lab_2_utils/
    __init__.py
    experiments.yaml
    transformations.py
```

## Implementation requirements

- Follow the same app style used by Lab 0 and Lab 1.
- Keep the main app script short and classroom-readable.
- Include the `spark-submit` command in the app docstring.
- Use the project logger only; do not use raw `print`.
- Keep transformations in `lab_2_utils/transformations.py`.
- Keep configs in `lab_2_utils/experiments.yaml`.
- If additional runtime/helper logic is needed, place it in `lab_2_utils/`,
  following the Lab 1 pattern:

```text
src/apps/labs/lab_1/lab_1_utils/random_task_outlier_runtime.py
```

- Do not change shared core modules unless explicitly approved later:
  - `src/spark_workshop/runtime.py`
  - `src/spark_workshop/jobs.py`
  - `src/spark_workshop/metrics.py`

## Suggested lesson design

Use generated retail Delta data and build a workload that can produce a
noticeable combination of shuffle, executor runtime, and possibly spill/GC.

Candidate workload:

1. Read `sales`, `vendors`, and optionally `products`.
2. Join and aggregate by a medium-cardinality key.
3. Use a config switch to make the workload slightly heavier when needed.
4. Collect StageMetrics and log a short interpretation block.

The lesson should not depend on a guaranteed spill event. On a small WSL/local
cluster, spill and GC can be sensitive to memory, partition counts, and data
volume. Treat spill/GC as metrics to inspect if they appear, not as mandatory
success criteria for the first implementation.

## Config expectations

Suggested config names:

```text
lab2-stage-metrics-drill-default
lab2-stage-metrics-drill-heavy
```

Each config should expose:

- source table/path names;
- stage collector mode;
- Spark UI app/job description fields;
- workload mode;
- partition/shuffle settings only if needed for stable classroom output.

Keep the instructor switch simple: ideally only `CONFIG_NAME` changes in the
main script.

## Class notes requirements

Create:

```text
src/apps/labs/lab_2/stage_metrics_interpretation_drill_class_notes.md
```

The notes must explain:

- which certification-vault notes inspired the lab;
- what each selected StageMetrics field means in class terms;
- which metrics are strong evidence:
  - `shuffleBytesWritten`
  - `shuffleTotalBytesRead`
  - `executorRunTime`
  - `memoryBytesSpilled`
  - `diskBytesSpilled`
  - `jvmGCTime`
- which questions StageMetrics cannot answer alone;
- when to move to TaskMetrics or Spark UI task-level pages.

## Constraints and expected changes

- This lesson may need careful calibration because GC/spill are not guaranteed
  on every local machine.
- Prefer a stable shuffle-oriented workload over a fragile memory-pressure demo.
- Do not make the generator larger just to force spill unless explicitly
  approved later.
- If the current generated XS data is too small to show useful signals, document
  the required generator size in the class notes and validation notes.
- Avoid adding a generic metrics framework. Keep any lesson-specific helper in
  `lab_2_utils/`.

## Acceptance criteria

- The lab runs with the standard local stack and generated retail data.
- StageMetrics are collected and logged clearly.
- The output gives enough information for a classroom metric-reading exercise.
- The class notes include the certification-vault reference path.
- The notes distinguish StageMetrics evidence from TaskMetrics-only evidence.
- No generated data, local runtime files, JARs, or secrets are committed.

## Validation

Before moving this TODO to `dev-todos/done/`, validate at minimum:

```bash
make tests
make compose
make generate XS
```

Then run the lesson with `spark-submit` and confirm:

- Spark UI / History Server uses readable job descriptions;
- sparkMeasure StageMetrics are emitted in logs;
- the selected metrics are visible enough to explain;
- the lesson remains readable from the app script.

Run broader validation if infrastructure, generator, Delta, MinIO, or core
sparkMeasure integration changes are made:

```bash
make validate
make dry-test
```

## Non-goals

- Do not implement the skew diagnosis here.
- Do not implement the empty partition diagnosis here.
- Do not require TaskMetrics in this lesson.
- Do not intentionally destabilize the local WSL environment to force memory
  pressure.
- Do not modify the reference repository under:

```text
/home/philot/compendium/forge/dataship/spark-plat-v0
```
