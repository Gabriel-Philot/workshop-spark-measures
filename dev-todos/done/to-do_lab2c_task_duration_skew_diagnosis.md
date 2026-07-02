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

Implement a Lab 2 lesson that shows why task-level metrics are required to
prove task skew.

The classroom story should be:

1. Run a workload with a hot-key distribution.
2. Use TaskMetrics or inspect Spark UI task-level data.
3. Show that one or a few tasks dominate runtime/input/shuffle.
4. Connect the observed task distribution to the source question's Summary
   Metrics pattern.

This lesson should be explicitly task-level. It should come after students have
already seen StageMetrics in Lab 2A and Lab 2B.

## Expected files

Add or extend the Lab 2 structure:

```text
src/apps/labs/lab_2/
  lab_2c_task_duration_skew_diagnosis.py
  lab_2c_task_duration_skew_diagnosis_class_notes.md
  lab_2_utils/
    __init__.py
    experiments.yaml
    transformations.py
    task_duration_skew_runtime.py
```

The runtime helper is expected for this lesson because the main script should
stay clean while the lab still needs a compact TaskMetrics percentile summary.
Keep this helper inside `lab_2_utils/`.

## Implementation requirements

- Follow the same app style used by Lab 0 and Lab 1.
- Keep the main app script short and classroom-readable.
- Keep the main app script close to the workshop contract:
  - define `CONFIG_PATH` and `CONFIG_NAME`;
  - import transformations and lab-local runtime helpers from `lab_2_utils`;
  - implement `extract`, `transform`, `load`, and `validate_result`;
  - do not put TaskMetrics DataFrame summarization logic directly in the main
    app file.
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
- Use the lab-local runtime helper to derive a compact boxed Summary Metrics
  style report from `collector.create_taskmetrics_DF(...)`. The point is to
  teach students to read sparkMeasure task-level output in the same shape as
  the simulated Spark UI question without burying the signal in submit logs.
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
4. Keep any remediation visible but commented/documented, so the instructor can
   discuss it without making the lesson depend on a second fixed run.
5. Use TaskMetrics to log a compact percentile table and, if useful, a small
   top-outlier table.

Preferred TaskMetrics fields:

- `duration`
- `executorRunTime`
- `recordsRead` or `shuffleRecordsRead`
- `bytesRead` or `shuffleTotalBytesRead`
- `recordsWritten`, when the selected action writes output
- `shuffleBytesWritten`, when the relevant stage writes shuffle data

If the workload symptom appears in shuffle metrics rather than input metrics,
the class notes must make that adaptation explicit. The source question uses
Spark UI `Input Size`; the local lab may use `shuffleTotalBytesRead` as the
equivalent "task processed much more data" signal.

When validating the lesson, explicitly confirm that the class notes explain this
adaptation. Students should understand that scan stages usually expose input
metrics, while shuffle-heavy stages expose the same data-volume reasoning
through shuffle read/write metrics.

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
lab2c-task-skew-task
```

Expected behavior:

- `task`: collects task-level diagnostic output for the skewed path.

A fixed variant is optional and should not be required for acceptance. This lab
is a diagnosis lesson aligned to a simulated Summary Metrics question, not a
general skew-tuning lesson.

Keep the instructor switch simple. Ideally only `CONFIG_NAME` changes in the
main script, and any code-level classroom fix remains clearly commented in the
transformation file.

## Class notes requirements

Create:

```text
src/apps/labs/lab_2/lab_2c_task_duration_skew_diagnosis_class_notes.md
```

The notes must explain:

- the self-contained source question summary and expected answer;
- why `max >> 75th percentile` is the key clue;
- why this lab is task-only, while earlier Lab 2 lessons cover stage-level
  aggregate symptoms;
- how the TaskMetrics percentile table maps to the Spark UI Summary Metrics
  table from the source question;
- which TaskMetrics fields are useful:
  - task `duration`
  - task `executorRunTime`
  - task input records/bytes when available
  - task shuffle read/write
  - task spill when present
- how to discuss a possible fix without making the lesson depend on a fixed run;
- how to use Spark UI task pages as a visual cross-check.

Do not include private vault paths in the class-facing notes. Keep private
reference paths in this TODO or in `dev-todos/plan/lab2_lesson_mapping.md`.

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
- The task-level run identifies at least one outlier task clearly.
- The TaskMetrics helper logs a boxed percentile-style report that supports the
  source question's `max >> 75th percentile` reasoning.
- Spark UI job descriptions make the lab identifiable.
- Class notes include a self-contained source-question summary and answer.
- No generated data, local runtime files, JARs, or secrets are committed.

## Validation

Before moving this TODO to `dev-todos/done/`, validate at minimum:

```bash
make tests
make compose
make generate SCALE=xs
```

Then run all configured variants with `spark-submit` and confirm:

- Spark UI / History Server uses readable names;
- TaskMetrics behavior matches the selected config;
- the task-level output exposes the skew signal clearly;
- class notes match the validated local TaskMetrics percentile output.

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
- Do not require a fixed run unless explicitly approved during implementation.
- Do not modify the data generator unless explicitly approved later.
- Do not modify the reference repository under:

```text
/home/philot/compendium/forge/dataship/spark-plat-v0
```
