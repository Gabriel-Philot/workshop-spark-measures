# TODO: Lab 2A shuffle aggregation diagnosis

## Context

Create the first Lab 2 lesson from the planning document:

```text
docs/agent_ops/dev-todos/done/lab2_lesson_mapping.md
```

This lesson should connect a certification-style Spark UI / Spark performance
question to a local sparkMeasure workshop example.

Primary reference: see the private vault mapping in
`docs/agent_ops/dev-todos/done/lab2_lesson_mapping.md`.

The source question is about a `sales` aggregation by `region` that creates
shuffle, and the expected reasoning is that partitioning by the grouping key can
reduce unnecessary shuffle work.

## Goal

Implement a Lab 2 lesson that shows how sparkMeasure StageMetrics helps diagnose
a shuffle-heavy aggregation.

The classroom story should be:

1. Run a simple aggregation that looks normal from code alone.
2. Use sparkMeasure StageMetrics to show the job is shuffle-heavy.
3. Compare it with an improved variant.
4. Explain which metrics helped isolate the issue:
   - `shuffleBytesWritten`
   - `shuffleTotalBytesRead`
   - `executorRunTime`
   - `numStages`
   - `numTasks`
   - spill metrics when present

This first Lab 2 lesson should stay at stage level. Do not use TaskMetrics here.

## Expected files

Add the Lab 2 structure if it does not exist:

```text
src/apps/labs/lab_2/
  README.md
  lab_2a_shuffle_aggregation_diagnosis.py
  lab_2a_shuffle_aggregation_diagnosis_class_notes.md
  lab_2_utils/
    __init__.py
    experiments.yaml
    transformations.py
```

## Implementation requirements

- Follow the same script shape used by:
  - `src/apps/labs/lab_0/lab_0c_sparkmeasure_presentation.py`
  - `src/apps/labs/lab_1/global_sort_diagnosis.py`
  - `src/apps/labs/lab_1/random_task_outlier_diagnosis.py`
- Keep the main app script short and classroom-readable.
- Include the `spark-submit` command in the app docstring.
- Use the project logger only; do not use raw `print`.
- Keep transformations in `lab_2_utils/transformations.py`.
- Keep lesson configs in `lab_2_utils/experiments.yaml`.
- If the lesson needs runtime/helper behavior beyond the base workshop contract,
  keep that behavior inside `lab_2_utils/`, following the Lab 1 pattern from:

```text
src/apps/labs/lab_1/lab_1_utils/random_task_outlier_runtime.py
```

- Do not change shared core modules such as:
  - `src/spark_workshop/runtime.py`
  - `src/spark_workshop/jobs.py`
  - `src/spark_workshop/metrics.py`

unless explicitly approved later.

## Suggested lesson design

Use the generated retail Delta data already produced by the workshop generator.

Candidate sources:

- `bronze.sales`
- `bronze.vendors`

Suggested transformation:

1. Read sales and vendors.
2. Join sales to vendor attributes.
3. Build a revenue aggregation by a vendor/location/grouping key.
4. Keep a problematic version that carries unnecessary columns or uses a weak
   partitioning shape before aggregation.
5. Keep an improved version that narrows columns earlier and/or repartitions by
   the grouping key before the aggregation.

The exact grouping key should be chosen during implementation based on the
current generated schema. Prefer a key that is easy to explain in class.

## Config expectations

Use explicit config names in YAML so the instructor can switch variants by
changing only one config name in the main script, following the Lab 1 pattern.

Suggested config names:

```text
lab2-shuffle-aggregation-baseline
lab2-shuffle-aggregation-optimized
```

Each config should make the classroom mode obvious:

- source tables / paths
- output mode, if any
- sparkMeasure collector mode: `stage`
- whether the transformation uses the baseline or optimized path
- app/job naming fields for Spark UI readability

## Class notes requirements

Create:

```text
src/apps/labs/lab_2/lab_2a_shuffle_aggregation_diagnosis_class_notes.md
```

The notes must explain:

- which certification-vault question inspired the lab;
- the relevant source question theme;
- what Spark shuffle is in this specific lab;
- why StageMetrics is sufficient for this lesson;
- what to compare between baseline and optimized runs;
- how to read the sparkMeasure metrics;
- how this differs from later TaskMetrics lessons.

Keep the class notes practical and workshop-oriented. Avoid turning them into a
generic Spark tuning guide.

## Acceptance criteria

- The lab runs with the standard local stack and generated retail data.
- The app follows the existing Lab 0 / Lab 1 contract style.
- StageMetrics are collected and logged for both baseline and optimized variants.
- Spark UI job descriptions make the lab identifiable.
- The observed metrics show a meaningful shuffle signal.
- The class notes include the certification-vault reference path.
- No broad reusable framework abstractions are introduced.
- No generated data, JARs, local runtime files, or secrets are committed.

## Validation

Before moving this TODO to `docs/agent_ops/dev-todos/done/`, validate at minimum:

```bash
make tests
make compose
make generate XS
```

Then run both Lab 2A variants with `spark-submit` and confirm:

- the jobs appear in Spark UI / History Server with readable names;
- sparkMeasure StageMetrics are emitted in logs;
- the baseline and optimized variants produce comparable business results;
- the metric comparison is useful enough for a classroom explanation.

Run broader validation if the implementation touches infrastructure, generator,
Spark runtime, Delta, MinIO, or sparkMeasure integration:

```bash
make validate
make dry-test
```

## Non-goals

- Do not implement skew diagnosis in this lesson.
- Do not implement empty partition diagnosis in this lesson.
- Do not use TaskMetrics in this lesson.
- Do not alter the data generator unless the existing generated tables cannot
  support the lesson.
- Do not modify the reference repository under:

```text
/home/philot/compendium/forge/dataship/spark-plat-v0
```
