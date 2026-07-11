# Lab 5: stage-level runtime budget guardrail

Lab 5 compares an approved baseline workload with a functionally equivalent
candidate and turns sparkMeasure StageMetrics into an engineering guardrail.

The lab asks:

```text
Can a functionally correct Spark change still be too expensive to promote?
```

## Classroom material

- [Lab 5 classroom guide](guide_lab5.md): prerequisites, workload comparison,
  command, terminal decision block, Delta evidence, and teaching sequence.
- [Runtime budget guardrail class
  notes](docs/stage_runtime_budget_guardrail_class_notes.md): rationale,
  metrics, budget semantics, low-signal behavior, and production mapping.

## Workshop connection

```text
Lab 4: classify what a workload looks like
  -> Lab 5: use stage-level evidence as an engineering policy
```

The lab remains stage-level only. TaskMetrics, Flight Recorder, and Spark
event-log parsing stay outside this guardrail.

## Entry points

```text
lab_5_stage_runtime_budget_guardrail.py
run_stage_runtime_budget_guardrail.sh
```

The Python app runs and compares both workload variants. The shell runner
submits the app and confirms exactly one final decision marker.

## Classroom run

After preparing the shared Bronze retail sources, move to this folder and run:

```bash
bash run_stage_runtime_budget_guardrail.sh
```

The default classroom scenario intentionally produces a guardrail `FAIL` while
the application exits successfully. A policy failure is evidence for the
lesson; it is not a technical execution failure.

## Outputs

Business outputs:

```text
s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline
s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate
```

StageMetrics rows:

```text
s3a://observability/lab5/stage_runtime_budget_runs
```

Guardrail decisions:

```text
s3a://observability/lab5/stage_runtime_budget_decisions
```
