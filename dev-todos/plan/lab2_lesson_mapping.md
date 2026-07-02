# Lab 2 lesson mapping: certification questions to sparkMeasure workshops

## Purpose

This is a planning document, not an implementation TODO. Use it to preserve the
mapping between certification-style questions from the local certifications vault
and future Lab 2 workshop lessons.

The goal for Lab 2 is to keep the workshop close to the kind of numeric reasoning
students see in practice questions: stage duration, task duration distribution,
shuffle read/write, spill, GC time, input/output bytes, and the difference between
wall-clock time and aggregated task time.

Do not implement directly from this file. For each accepted lesson, create a
separate TODO under `dev-todos/backlog/` first.

## Local references

The external vault was cloned locally for research:

```text
/home/philot/compendium/forge/certifications-vault
```

Primary reference files:

- `/home/philot/compendium/forge/certifications-vault/databricks-professional/derar/01/20260103_databricks-spark-ui-data-skew_q01_7d4e.md`
- `/home/philot/compendium/forge/certifications-vault/databricks-professional/derar/02/20260104_databricks-spark-ui-empty-partitions_q02.md`
- `/home/philot/compendium/forge/certifications-vault/databricks-professional/certiQ/240-300/20260112_databricks-spark-shuffle-optimization_q02_a254.md`
- `/home/philot/compendium/forge/certifications-vault/databricks-professional/resumes-topics/spark_ui_metrics.md`
- `/home/philot/compendium/forge/certifications-vault/databricks-professional/certiQ/180-240/20260111_databricks-spark-ui-predicate-pushdown_q01_220a.md`
- `/home/philot/compendium/forge/certifications-vault/databricks-professional/certiQ/60-120/20260109_databricks-parquet-file-sizing_q70_a4d2.md`
- `/home/philot/compendium/forge/certifications-vault/databricks-professional/derar/03/20260105_delta-partitioning-performance_q01_p3x2.md`
- `/home/philot/compendium/forge/certifications-vault/databricks-professional/certiQ/0-60/20260108_databricks-ganglia-driver-bottleneck_q06_f8a2.md`

## Required Lab 2 conventions

Lab 2 must follow the same workshop structure already used by Lab 0 and Lab 1.

Expected structure:

```text
src/apps/labs/lab_2/
  README.md
  <lesson_name>.py
  <lesson_name>_class_notes.md
  lab_2_utils/
    __init__.py
    experiments.yaml
    transformations.py
```

For each lesson:

- include one `.py` app script;
- include one `.md` class-notes file explaining which vault question inspired
  the lesson and how the sparkMeasure metrics connect to that question;
- keep the app script template-like, using the same style as:
  - `src/apps/labs/lab_0/sparkmeasure_presentation.py`
  - `src/apps/labs/lab_1/global_sort_diagnosis.py`
  - `src/apps/labs/lab_1/random_task_outlier_diagnosis.py`
- keep transformations in `lab_2_utils/transformations.py`;
- keep configs in `lab_2_utils/experiments.yaml`;
- use logger output, not raw `print`;
- include submit command in the app docstring, following the Lab 0/1 pattern;
- keep native-vs-observed or config-switch behavior explicit and classroom-friendly;
- if a lesson needs runtime/helper behavior beyond the base workshop contract, keep
  it inside `lab_2_utils/`, following the Lab 1 pattern from
  `src/apps/labs/lab_1/lab_1_utils/random_task_outlier_runtime.py`;
- do not change the shared `spark_workshop.runtime`, `spark_workshop.jobs`, or
  `spark_workshop.metrics` core for Lab 2-specific needs unless explicitly
  approved later;
- do not add broad reusable platform abstractions unless the lab truly needs them.

## Current platform capabilities

Already available:

- Spark 4.1.2, Delta Lake, MinIO, Spark History Server;
- sparkMeasure stage and task collectors;
- retail generator with related `vendors`, `products`, `customers`, and `sales`;
- generated hot vendor skew;
- stage-level metrics:
  - `numStages`
  - `numTasks`
  - `executorRunTime`
  - `jvmGCTime`
  - `shuffleTotalBytesRead`
  - `shuffleBytesWritten`
  - `memoryBytesSpilled`
  - `diskBytesSpilled`
  - `recordsRead`
  - `recordsWritten`
- task-level metrics for later lessons when distribution matters:
  - task `duration`
  - task `executorRunTime`
  - task `recordsWritten`
  - task shuffle read/write
  - task spill

Current caution:

- StageMetrics is strong for aggregate symptoms.
- Questions based on min/median/75th/max task distribution require Spark UI
  task summaries or sparkMeasure TaskMetrics. StageMetrics alone is not enough
  to prove those distributions.

## Lesson candidate mapping

| Candidate | Source question | Main numeric reasoning | Best collector | Fit | Effort | Recommendation |
|---|---|---|---|---:|---:|---|
| Lab 2A: shuffle-heavy aggregation | CertiQ Q254: Spark shuffle optimization | High shuffle read/write and executor runtime around `groupBy` | StageMetrics | High | Low | Start here |
| Lab 2B: stage metric interpretation drill | `spark_ui_metrics.md` Q1-Q3 | Which metrics exist, GC, spill, shuffle | StageMetrics | High | Medium | Good second lesson |
| Lab 2C: task duration skew | Derar data skew question | `max >> 75th percentile`, straggler tasks | TaskMetrics + UI | High | Medium | Later, after stage basics |
| Lab 2D: empty partitions | Derar empty partitions question | `min << median`, but `max ~= 75th percentile` | TaskMetrics + UI | Medium/High | Medium | Later, paired with skew |
| Lab 2E: predicate pushdown | CertiQ Q220 | Physical plan + input bytes | StageMetrics + explain/UI | Medium | Medium | Optional |
| Lab 2F: file sizing / partitioning | Parquet file sizing and Delta partitioning questions | file count, partition size, input bytes | StageMetrics + source inventory | Medium | Medium | Optional, lakehouse-focused |
| Lab 2G: driver bottleneck | Ganglia driver bottleneck question | network/CPU, idle executors | Outside sparkMeasure | Low | High | Do not prioritize locally |

## Recommended next lesson: Lab 2A shuffle-heavy aggregation

Reference:

```text
/home/philot/compendium/forge/certifications-vault/databricks-professional/certiQ/240-300/20260112_databricks-spark-shuffle-optimization_q02_a254.md
```

Source question theme:

```text
df = spark.read.table("sales")
result = df.groupBy("region").agg(sum("revenue"))
```

The question asks how to reduce excessive shuffle during aggregation. The answer
uses repartitioning by the grouping key before aggregation.

Workshop adaptation:

- Use the generated retail bronze tables.
- Build `sales_enriched` or a narrow projection from `sales` plus vendor region.
- Run a problematic aggregation that creates clear shuffle work.
- Run an improved variant that reduces unnecessary columns and/or intentionally
  controls partitioning before aggregation.

Expected sparkMeasure stage signals:

- `shuffleBytesWritten` / `shuffleTotalBytesRead` should be the primary signal;
- `executorRunTime` should show compute cost;
- `numStages` and `numTasks` should help explain the shape of the job;
- spill metrics should be observed, but not required for the first version.

Quality assessment:

- Stable on the current local stack.
- Does not require generator changes.
- Fits StageMetrics directly.
- Good bridge from certification theory to sparkMeasure output.

Implementation effort later:

- Low to medium.
- Add `src/apps/labs/lab_2/lab_2a_shuffle_aggregation_diagnosis.py`.
- Add `src/apps/labs/lab_2/lab_2a_shuffle_aggregation_diagnosis_class_notes.md`.
- Add `lab_2_utils/experiments.yaml` and `lab_2_utils/transformations.py`.
- Reuse the Lab 1 app contract style.

## Later lesson: task duration skew

Reference:

```text
/home/philot/compendium/forge/certifications-vault/databricks-professional/derar/01/20260103_databricks-spark-ui-data-skew_q01_7d4e.md
```

Source question numeric pattern:

```text
75th percentile duration: 3s
max duration: 30s
typical input: ~8 KiB
max input: 803 KiB
```

Interpretation:

```text
max >> 75th percentile = skew / straggler task
```

Workshop adaptation:

- Use generated hot vendor skew or create a controlled skewed join/aggregation.
- First show StageMetrics aggregate symptoms.
- Then switch to TaskMetrics or Spark UI task summary to show the outlier task.

Expected metrics:

- StageMetrics: high executor runtime, shuffle read/write, maybe elevated stage duration.
- TaskMetrics/UI: one or few tasks with much higher duration, input, or shuffle read.

Quality assessment:

- Very strong teaching value.
- Should come after students understand stage-level signals.
- Requires task-level view for a faithful match to the source question.

## Later lesson: empty or near-empty partitions

Reference:

```text
/home/philot/compendium/forge/certifications-vault/databricks-professional/derar/02/20260104_databricks-spark-ui-empty-partitions_q02.md
```

Source question numeric pattern:

```text
min duration: 0.1 ms
median duration: 2s
75th percentile: 3s
max: 3s
```

Interpretation:

```text
min << median = empty or near-empty partitions
max ~= 75th percentile = not skew at the high end
```

Workshop adaptation:

- Create too many partitions relative to the data volume.
- Use TaskMetrics or Spark UI summary metrics to show low-end outliers.
- Pair with the skew lesson to teach the difference between low-end and high-end
  distribution problems.

Expected metrics:

- StageMetrics: many tasks relative to data volume; possible scheduler overhead.
- TaskMetrics/UI: many tasks with very low records/duration.

Quality assessment:

- Good for reasoning, but less impactful than shuffle/skew.
- Requires task-level distribution to teach correctly.

## Later lesson: spill and GC interpretation

Reference:

```text
/home/philot/compendium/forge/certifications-vault/databricks-professional/resumes-topics/spark_ui_metrics.md
```

Useful source questions:

- Question 2: high GC time means memory pressure.
- Question 3: shuffle spill memory/disk means data was spilled during shuffle.
- Question 4: max task duration much greater than median suggests skew.

Workshop adaptation:

- Try to create a controlled wide aggregation or join that can produce spill.
- Keep this later because spill/GC can be unstable on a small local WSL cluster.

Expected metrics:

- `jvmGCTime`
- `memoryBytesSpilled`
- `diskBytesSpilled`
- `executorRunTime`
- shuffle read/write

Quality assessment:

- Excellent if calibrated.
- Higher implementation risk than shuffle-only.
- Should not be the first Lab 2 task.

## Optional lesson: predicate pushdown

Reference:

```text
/home/philot/compendium/forge/certifications-vault/databricks-professional/certiQ/180-240/20260111_databricks-spark-ui-predicate-pushdown_q01_220a.md
```

Source question theme:

- Stage input bytes can show excessive I/O.
- The actual reason is found in the physical plan, not just stage metrics.

Workshop adaptation:

- Compare a pushdown-friendly filter with a non-pushdown-friendly expression.
- Include `explain`/Spark SQL UI plan inspection.
- Use sparkMeasure only as supporting evidence for input/read cost.

Quality assessment:

- Good conceptual lesson.
- Less directly sparkMeasure-centric than shuffle.

## Optional lesson: file sizing and partitioning

References:

```text
/home/philot/compendium/forge/certifications-vault/databricks-professional/certiQ/60-120/20260109_databricks-parquet-file-sizing_q70_a4d2.md
/home/philot/compendium/forge/certifications-vault/databricks-professional/derar/03/20260105_delta-partitioning-performance_q01_p3x2.md
```

Source question themes:

- read-time partitioning vs shuffle partitioning;
- over-partitioning and small-file/partition-boundary problems;
- OPTIMIZE limitations across partition boundaries.

Workshop adaptation:

- Use source inventory/file stats plus a simple read workload.
- This is more lakehouse/file-layout than sparkMeasure, but can complement the
  observability story.

Quality assessment:

- Useful, but lower priority for the sparkMeasure workshop core.

## Decision notes

Recommended sequence:

1. Lab 2A: shuffle-heavy aggregation with StageMetrics.
2. Lab 2B: stage metric interpretation drill using GC/spill/shuffle concepts.
3. Later: task-level skew using TaskMetrics and/or Spark UI task summary.
4. Later: empty partitions using TaskMetrics and/or Spark UI task summary.

Reasoning:

- Start with StageMetrics because that is the workshop's current main story.
- Move to task-level only when the lesson requires distribution reasoning.
- Keep each lesson grounded in one certification-style source question.
- Keep every Lab 2 lesson small enough that students can read the `.py` file
  without being distracted by platform mechanics.
