# TODO: Lab 2C task duration skew diagnosis

## Context

Create a later Lab 2 lesson focused on task-level skew diagnosis.

Planning source:

```text
dev-todos/plan/lab2_lesson_mapping.md
```

Primary certification-vault reference: see the private vault mapping in
`dev-todos/plan/lab2_lesson_mapping.md`.

Source question numeric pattern:

```text
75th percentile duration: 3s
max duration: 30s
typical input: ~8 KiB
max input: 803 KiB
```

The teaching point is:

```text
max task duration much greater than the 75th percentile indicates a straggler,
often caused by skew.
```

## Goal

Implement a Lab 2 lesson that shows why stage-level metrics can reveal a symptom,
but task-level metrics are needed to prove task skew.

The classroom story should be:

1. Run a workload that looks slow or uneven at stage level.
2. Use StageMetrics to identify the suspicious stage-level symptom.
3. Enable TaskMetrics or inspect Spark UI task-level data.
4. Show that one or a few tasks dominate runtime/input/shuffle.
5. Apply a small commented fix in class and compare the task-level distribution.

This lesson should be explicitly task-level. It should come after students have
already seen StageMetrics in Lab 2A and Lab 2B.

## Expected files

Add or extend the Lab 2 structure:

```text
src/apps/labs/lab_2/
  task_duration_skew_diagnosis.py
  task_duration_skew_diagnosis_class_notes.md
  lab_2_utils/
    __init__.py
    experiments.yaml
    transformations.py
    task_duration_skew_runtime.py
```

The runtime helper file is optional, but if helper behavior is needed, keep it
inside `lab_2_utils/`.

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

Use the generated retail skew already available in the data generator, especially
the hot vendor behavior.

Candidate workload:

1. Read `sales` and `vendors`.
2. Join or aggregate by a key affected by hot vendor skew.
3. Make the problematic path produce one or a few large partitions/tasks.
4. Keep the fix visible but commented, so the instructor can switch it in class.
5. Use TaskMetrics to log a compact outlier table or summary.

Potential fixes to evaluate during implementation:

- salting the hot key;
- changing repartition strategy;
- narrowing data before the expensive operation;
- using a controlled two-step aggregation.

Pick the smallest fix that produces a measurable task-level improvement without
making the script hard to read.

## Config expectations

Suggested config names:

```text
lab2-task-skew-stage
lab2-task-skew-task
lab2-task-skew-task-fixed
```

Expected behavior:

- `stage`: collects stage-level symptoms only;
- `task`: collects task-level diagnostic output for the skewed path;
- `task-fixed`: uses the corrected transformation and collects task-level output.

Keep the instructor switch simple. Ideally only `CONFIG_NAME` changes in the
main script, and any code-level classroom fix remains clearly commented in the
transformation file.

## Class notes requirements

Create:

```text
src/apps/labs/lab_2/task_duration_skew_diagnosis_class_notes.md
```

The notes must explain:

- which certification-vault question inspired the lab;
- why `max >> 75th percentile` is the key clue;
- why StageMetrics alone cannot prove task skew;
- which TaskMetrics fields are useful:
  - task `duration`
  - task `executorRunTime`
  - task input records/bytes when available
  - task shuffle read/write
  - task spill when present
- how to compare the broken and fixed paths;
- how to use Spark UI task pages as a visual cross-check.

## Constraints and expected changes

- This lesson is more sensitive than StageMetrics-only labs because it depends
  on task distribution, not just aggregate stage counters.
- XS data may be too small to make the skew visually convincing. If so, document
  the minimum generator size needed for a good demo.
- The generated hot vendor skew should be reused before changing the generator.
- If additional synthetic skew is needed, implement it in the lab transformation,
  not in the generator, unless explicitly approved later.
- TaskMetrics output should be compact. Avoid dumping large task DataFrames into
  the terminal.

## Acceptance criteria

- The lab runs with the standard local stack and generated retail data.
- The stage-only run shows a plausible symptom.
- The task-level run identifies at least one outlier task clearly.
- The fixed run improves the task-level signal or total runtime enough to teach.
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
- the task-level output exposes the skew signal clearly;
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

- Do not teach empty partitions in this lesson.
- Do not turn this into a general skew tuning framework.
- Do not persist TaskMetrics by default.
- Do not modify the data generator unless explicitly approved later.
- Do not modify the reference repository under:

```text
/home/philot/compendium/forge/dataship/spark-plat-v0
```
