# Lab 2: professional sparkMeasure diagnostics

Lab 2 connects a selected set of Spark questions from Databricks Certified Data
Engineer Professional practice exams to controlled sparkMeasure workloads.
Students begin with stage-level shuffle, spill, and GC evidence, then use
TaskMetrics to distinguish high-end skew from low-end empty partitions.

The practice questions provide a familiar professional context; the local labs
turn their numeric clues into observable Spark workloads. They are study
material, not official Databricks exam questions.

## Classroom material

- [Lab 2 classroom guide](guide_lab2.md): bootstrap, submit commands, teaching
  sequence, expected evidence, and validated local results.
- [Selected Databricks Data Engineer Professional questions](docs/exam_questions.md):
  the instructor's selected practice-exam questions, alternatives, visible
  answers, reasoning, and local workload mappings.

## Lesson order

| Exercise | Script | Collector | Focus |
|---|---|---|---|
| 2A | `lab_2a_shuffle_aggregation_diagnosis.py` | StageMetrics | shuffle-heavy aggregation |
| 2B | `lab_2b_stage_metrics_interpretation_drill.py` | StageMetrics | shuffle, spill, and GC interpretation |
| 2C | `lab_2c_task_duration_skew_diagnosis.py` | TaskMetrics | high-end max-versus-p75 skew |
| 2D | `lab_2d_empty_partitions_diagnosis.py` | TaskMetrics | low-end min-versus-median partitions |

## Prerequisite

Labs 1-6 share the generated Bronze retail source family:

```bash
make compose
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

Do not regenerate data when the same MinIO volume from Labs 0 or 1 is still
available. The classroom guide explains the correct sequence for a running,
stopped, cleaned, or completely new environment.
