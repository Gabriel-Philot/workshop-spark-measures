# TODO: Lab 2D empty partitions diagnosis

## Context

Create a later Lab 2 lesson focused on identifying empty or near-empty
partitions through task-level distribution reasoning.

Planning source:

```text
dev-todos/plan/lab2_lesson_mapping.md
```

Primary certification-vault reference: see the private vault mapping in
`dev-todos/plan/lab2_lesson_mapping.md`.

Source question numeric pattern:

```text
min duration: 0.1 ms
median duration: 2s
75th percentile: 3s
max: 3s
```

The teaching point is:

```text
min much lower than the median suggests empty or near-empty partitions, while
max close to the 75th percentile argues against high-end skew.
```

## Goal

Implement a Lab 2 lesson that teaches students to distinguish empty partition
symptoms from skew symptoms.

The classroom story should be:

1. Run a workload with too many partitions for the data volume.
2. Show that stage-level metrics may reveal too many tasks, but not the full
   distribution story.
3. Use TaskMetrics or Spark UI task pages to show many tiny/empty tasks.
4. Apply a simple fix by reducing or controlling partition count.
5. Compare task distribution and runtime before and after the fix.

This lesson should be task-level and should come after the task skew lesson, so
students can contrast low-end outliers with high-end outliers.

## Expected files

Add or extend the Lab 2 structure:

```text
src/apps/labs/lab_2/
  empty_partitions_diagnosis.py
  empty_partitions_diagnosis_class_notes.md
  lab_2_utils/
    __init__.py
    experiments.yaml
    transformations.py
    empty_partitions_runtime.py
```

The runtime helper file is optional. If helper behavior is needed, keep it inside
`lab_2_utils/`.

## Implementation requirements

- Follow the same app style used by Lab 0 and Lab 1.
- Keep the main app script short and classroom-readable.
- Include the `spark-submit` command in the app docstring.
- Use the project logger only; do not use raw `print`.
- Keep transformations in `lab_2_utils/transformations.py`.
- Keep configs in `lab_2_utils/experiments.yaml`.
- Keep task-specific runtime/helper behavior inside `lab_2_utils/`, following
  the Lab 1 pattern:

```text
src/apps/labs/lab_1/lab_1_utils/random_task_outlier_runtime.py
```

- Do not persist TaskMetrics by default. Treat TaskMetrics as diagnostic output
  unless explicitly approved otherwise.
- Do not change shared core modules unless explicitly approved later:
  - `src/spark_workshop/runtime.py`
  - `src/spark_workshop/jobs.py`
  - `src/spark_workshop/metrics.py`

## Suggested lesson design

Use generated retail Delta data and intentionally create too many partitions in
the lab transformation.

Candidate workload:

1. Read a moderate-size retail table such as `sales`.
2. Repartition to an intentionally excessive partition count.
3. Apply a simple aggregation or projection that triggers tasks across those
   partitions.
4. Use TaskMetrics to show many low-duration or low-record tasks.
5. Compare with a corrected partition count.

The problem should be created in the lab code/config, not in the data generator.
This keeps the generator reusable and makes the lesson easier to explain.

## Config expectations

Suggested config names:

```text
lab2-empty-partitions-stage
lab2-empty-partitions-task
lab2-empty-partitions-task-fixed
```

Expected behavior:

- `stage`: shows the aggregate symptom, especially high task count relative to
  useful work;
- `task`: shows low-end outliers through TaskMetrics;
- `task-fixed`: uses a smaller/controlled partition count and compares output.

The excessive partition count should be configurable from YAML so the instructor
can tune it for the local machine without editing transformation logic.

## Class notes requirements

Create:

```text
src/apps/labs/lab_2/empty_partitions_diagnosis_class_notes.md
```

The notes must explain:

- which certification-vault question inspired the lab;
- why `min << median` points to empty or near-empty partitions;
- why `max ~= 75th percentile` argues against high-end skew;
- how to compare this lesson with the task skew lesson;
- which TaskMetrics fields are useful:
  - task `duration`
  - task `executorRunTime`
  - task records read/written when available
  - task shuffle read/write when present
- why StageMetrics alone is insufficient for the full conclusion.

## Constraints and expected changes

- This lesson is intentionally artificial: the problem is created through
  excessive partitioning so students can see the pattern.
- On a small local WSL stack, too many partitions can create scheduler overhead
  and noisy timings. Keep the default partition count safe.
- The fix should be obvious and reversible: reduce partition count or avoid the
  unnecessary repartition.
- The generated data does not need to change.
- Keep TaskMetrics output compact; do not dump a full task DataFrame.

## Acceptance criteria

- The lab runs with the standard local stack and generated retail data.
- The stage-only run shows a high task count or weak useful-work signal.
- The task-level run shows low-end outlier tasks clearly.
- The fixed variant reduces empty/near-empty task symptoms.
- Spark UI job descriptions make the lab identifiable.
- Class notes include the certification-vault reference path.
- No generated data, local runtime files, JARs, or secrets are committed.

## Validation

Before moving this TODO to `dev-todos/done/`, validate at minimum:

```bash
make tests
make compose
make generate XS
```

Then run all configured variants with `spark-submit` and confirm:

- Spark UI / History Server uses readable names;
- StageMetrics and TaskMetrics behavior match the selected config;
- the task-level output exposes empty/near-empty task symptoms clearly;
- the fixed variant produces a useful comparison.

If XS data is not enough, document the tested generator size and why it is
needed.

Run broader validation if infrastructure, generator, Delta, MinIO, or core
sparkMeasure integration changes are made:

```bash
make validate
make dry-test
```

## Non-goals

- Do not teach high-end skew in this lesson except as a comparison point.
- Do not modify the data generator.
- Do not persist TaskMetrics by default.
- Do not turn this into a general partition tuning framework.
- Do not modify the reference repository under:

```text
/home/philot/compendium/forge/dataship/spark-plat-v0
```
