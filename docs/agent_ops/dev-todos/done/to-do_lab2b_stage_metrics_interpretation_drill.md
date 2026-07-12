# TODO: Lab 2B stage metrics interpretation: shuffle spill and GC

## Context

Create the second Lab 2 lesson from the planning document:

```text
docs/agent_ops/dev-todos/done/lab2_lesson_mapping.md
```

This lesson should not be a generic metric catalog. It should be a
question-driven workshop exercise that teaches students how to interpret
sparkMeasure StageMetrics and decide what the metrics can and cannot prove.

Primary reference: see the private vault mapping in
`docs/agent_ops/dev-todos/done/lab2_lesson_mapping.md`.

The class-facing materials must be self-contained. Do not include private vault
paths in the app README or class notes.

## Source question inspiration

This lesson should be inspired mainly by two certification-style questions.

### Question A: High GC time

A data engineer notices that a Spark job's stage shows GC time consuming 25% of
the total task duration, highlighted in red. What is the first action they
should take?

Conceptual alternatives:

- increase the number of shuffle partitions;
- add more executor cores;
- increase executor memory or optimize memory usage;
- enable Adaptive Query Execution.

Expected answer:

```text
Increase executor memory or optimize memory usage.
```

Teaching point:

```text
High GC time is a memory-pressure signal. More cores can make memory pressure
worse if memory per task remains insufficient.
```

### Question B: Interpreting shuffle spill

A data engineer observes the following metrics in a stage:

```text
Shuffle Spill (Memory): 5 GB
Shuffle Spill (Disk): 1.2 GB
Duration: 45 minutes
```

What does this indicate?

Conceptual alternatives:

- the stage is efficient with minimal I/O;
- Spark is writing more data to disk than memory, indicating serialization
  corruption;
- Spark had to spill shuffle data from memory to disk;
- disk storage is corrupted because disk spill is smaller than memory spill.

Expected answer:

```text
Spark had to spill shuffle data from memory to disk during shuffle operations.
```

Teaching point:

```text
Memory spill is the deserialized in-memory amount; disk spill is the serialized
amount written to disk, so disk spill can be smaller.
```

## Goal

Implement a Lab 2 lesson that teaches students how to read StageMetrics around
shuffle, spill, GC, and executor runtime.

The classroom story should be:

1. Start from the two source questions above.
2. Run one controlled stage-level workload.
3. Use sparkMeasure StageMetrics to inspect:
   - `shuffleBytesWritten`
   - `shuffleTotalBytesRead`
   - `memoryBytesSpilled`
   - `diskBytesSpilled`
   - `jvmGCTime`
   - `executorRunTime`
   - `numStages`
   - `numTasks`
4. Explain which metrics are direct evidence and which are only supporting
   symptoms.
5. Explicitly show when StageMetrics is enough and when the next step should be
   TaskMetrics or Spark UI task-level inspection.

This lesson should remain stage-level. It should prepare students for later
task-level skew and empty-partition lessons, not solve those lessons upfront.

## Expected files

Add or extend the Lab 2 structure:

```text
src/apps/labs/lab_2/
  README.md
  lab_2b_stage_metrics_interpretation_drill.py
  lab_2b_stage_metrics_interpretation_drill_class_notes.md
  lab_2_utils/
    __init__.py
    experiments.yaml
    transformations.py
```

Optional only if needed:

```text
src/apps/labs/lab_2/lab_2_utils/stage_metrics_runtime.py
```

Keep any Lab 2B-specific runtime/helper behavior in `lab_2_utils/`, following
the Lab 1 pattern from:

```text
src/apps/labs/lab_1/lab_1_utils/random_task_outlier_runtime.py
```

## Implementation requirements

- Follow the same app style used by Lab 2A:
  - short main app script;
  - `CONFIG_NAME` classroom switch near the top;
  - configs in `lab_2_utils/experiments.yaml`;
  - transformations in `lab_2_utils/transformations.py`;
  - logger output only, no raw `print`;
  - submit command in the app docstring;
  - readable Spark UI job descriptions.
- Do not change shared core modules unless explicitly approved later:
  - `src/spark_workshop/runtime`
  - `src/spark_workshop/jobs`
  - `src/spark_workshop/metrics`
- Keep the app script readable enough that students can inspect it during class.
- Do not persist sparkMeasure metrics by default unless explicitly approved.

## Suggested lesson design

Use generated retail Delta data and build one stable shuffle-heavy workload that
can be interpreted even when spill does not occur.

Candidate workload:

1. Read `sales`, `vendors`, and optionally `products`.
2. Build a wider intermediate row than Lab 2A, using existing payload columns
   from `sales` when available.
3. Run a shuffle-heavy aggregation or join with intentionally constrained
   shuffle partition settings.
4. Collect StageMetrics.
5. Do not add derived diagnosis lines to the app output. Students should read
   the raw sparkMeasure aggregated report directly, using the class notes to
   interpret:
   - shuffle present or absent;
   - spill present or absent;
   - GC time present and whether it is material;
   - whether the evidence is enough for a stage-level conclusion.

Important calibration rule:

```text
Do not require actual spill as a success condition.
```

On the local WSL stack, spill and GC are environment-sensitive. The lesson is
still valid if the run shows:

```text
high shuffle + zero spill + low GC
```

because that lets the instructor say:

```text
This stage is shuffle-heavy, but this run does not show memory spill or high GC.
Therefore, the first diagnosis is data movement / partitioning, not memory
pressure.
```

If spill or high GC does appear, the class notes should explain how to interpret
it using the source questions.

## Config expectations

Suggested config names:

```text
lab2-stage-metrics-drill-default
lab2-stage-metrics-drill-pressure
```

Each config should expose:

- source table/path names;
- `observability.collector: stage`;
- `observability.persist: false`;
- Spark UI app/job description fields;
- workload mode;
- shuffle partition settings only if needed for stable classroom output.

Keep the instructor switch simple: ideally only `CONFIG_NAME` changes in the
main script.

## Class notes requirements

Create:

```text
src/apps/labs/lab_2/lab_2b_stage_metrics_interpretation_drill_class_notes.md
```

The notes must include:

- the full self-contained High GC question and expected answer;
- the full self-contained Shuffle Spill question and expected answer;
- a clear mapping between those questions and sparkMeasure StageMetrics fields;
- the lab code snippets that produce the relevant physical behavior;
- a metrics interpretation table, following the style used in Lab 2A;
- validated local numbers from at least `SCALE=xs`;
- a section explaining what it means if spill is zero;
- a section explaining why task-duration skew is not proven by StageMetrics
  alone and belongs to a later TaskMetrics lesson.

Do not include private vault paths in the class notes.

## Constraints and expected changes

- This lesson may need careful calibration because GC/spill are not guaranteed
  on every local machine.
- Prefer a stable shuffle interpretation lesson over a fragile memory-pressure
  demo.
- Do not make the generator larger just to force spill unless explicitly
  approved later.
- Do not introduce fake `sleep`, arbitrary UDF delay, or synthetic CPU burn
  unrelated to the source questions.
- If the current generated XS data is too small to show useful shuffle signals,
  document the required generator size in the class notes and validation notes.
- Avoid adding a generic metrics framework. Keep lesson-specific helpers in
  `lab_2_utils/`.

## Acceptance criteria

- The lab runs with the standard local stack and generated retail data.
- StageMetrics are collected and logged clearly.
- The output gives enough information for a classroom metric-reading exercise.
- The class notes include self-contained source questions and answers.
- The notes distinguish:
  - direct shuffle evidence;
  - direct spill evidence;
  - GC/memory-pressure evidence;
  - TaskMetrics-only distribution evidence.
- No private vault paths are included in class-facing docs.
- No generated data, local runtime files, JARs, or secrets are committed.

## Validation

Before moving this TODO to `docs/agent_ops/dev-todos/done/`, validate at minimum:

```bash
make tests
make compose
make generate SCALE=xs
```

Then run the lesson with `spark-submit` and confirm:

- Spark UI / History Server uses readable job descriptions;
- sparkMeasure StageMetrics are emitted in logs;
- the selected metrics are visible enough to explain;
- the lesson remains readable from the app script;
- class notes match the validated local numbers.

Run broader validation if infrastructure, generator, Delta, MinIO, or core
sparkMeasure integration changes are made:

```bash
make validate
make dry-test
```

## Non-goals

- Do not implement task-level skew diagnosis here.
- Do not implement empty partition diagnosis here.
- Do not require TaskMetrics in this lesson.
- Do not intentionally destabilize the local WSL environment to force memory
  pressure.
- Do not modify the data generator for this lesson unless explicitly approved.
- Do not modify the reference repository under:

```text
/home/philot/compendium/forge/dataship/spark-plat-v0
```
