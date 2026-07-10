# Lab 4: stage-level workload fingerprint

Lab 4 uses sparkMeasure StageMetrics to transform aggregate execution counters
into a small operational workload fingerprint.

The lab answers:

```text
What does this Spark workload look like from a stage-level execution perspective?
```

## Classroom material

- [Lab 4 classroom guide](guide_lab4.md): prerequisites, teaching sequence,
  command, expected diagnostic block, outputs, and interpretation.
- [Stage workload fingerprint class
  notes](docs/stage_workload_fingerprint_class_notes.md): ratios, profiles,
  limitations, and the relationship between raw metrics and interpretation.

## Workshop connection

```text
Lab 3: measure how much observability costs
  -> Lab 4: use StageMetrics to describe what a workload is doing
```

This lab remains stage-level only. It does not use TaskMetrics, Flight
Recorder, task distributions, or Spark event-log parsing.

## Entry points

```text
lab_4_stage_workload_fingerprint.py
run_stage_workload_fingerprint.sh
```

The Python app runs the workload and assigns the fingerprint. The shell runner
submits the app and verifies its classroom markers.

## Classroom run

After preparing the shared Bronze retail sources, move to this folder and run:

```bash
bash run_stage_workload_fingerprint.sh
```

If the platform and shared data have not been prepared, follow the prerequisite
reference at the beginning of the classroom guide.

## Outputs

Workload output:

```text
s3a://lakehouse/gold/lab4/stage_workload_fingerprint/workload_summary
```

Normalized StageMetrics:

```text
s3a://observability/lab4/stage_metrics
```

Workload fingerprints:

```text
s3a://observability/lab4/workload_fingerprints
```

The fingerprint is an explainable first diagnostic summary, not a complete
root-cause analysis or a universal production rule engine.
