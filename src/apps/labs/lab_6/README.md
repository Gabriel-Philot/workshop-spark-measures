# Lab 6: stage metrics contract gate

Lab 6 treats sparkMeasure StageMetrics as an operational data product and
validates whether the collected evidence is trustworthy enough for automation.

The lab asks:

```text
Can we trust this metrics row before using it for an engineering decision?
```

## Classroom material

- [Lab 6 classroom guide](guide_lab6.md): prerequisites, clean `PASS`, invalid
  demo `FAIL`, contract layers, terminal block, and optional Delta inspection.
- [Stage metrics contract gate class
  notes](docs/stage_metrics_contract_gate_class_notes.md): metric availability,
  consumer assumptions, failure types, and production data-product rationale.

## Workshop connection

```text
Lab 5: use stage-level evidence as engineering policy
  -> Lab 6: validate that the evidence is trustworthy enough for policy
```

The lab remains stage-level only. It does not use TaskMetrics, Flight Recorder,
task distributions, or Spark event-log parsing.

## Entry points

```text
lab_6_stage_metrics_contract_gate.py
run_stage_metrics_contract_gate.sh
```

The Python app collects, persists, and validates StageMetrics. The shell runner
supports both classroom scenarios and confirms exactly one final contract
marker.

## Classroom runs

Clean passing scenario:

```bash
bash run_stage_metrics_contract_gate.sh
```

Controlled failure demonstration:

```bash
LAB6_INJECT_INVALID_RECORDS=true \
bash run_stage_metrics_contract_gate.sh
```

The demo writes invalid rows to a separate Delta input and does not corrupt the
clean raw metrics table. An expected contract `FAIL` still exits successfully.

## Outputs

```text
s3a://lakehouse/gold/lab6/stage_metrics_contract_gate/business_output
s3a://observability/lab6/stage_metrics_raw
s3a://observability/lab6/stage_metrics_contract_demo_input
s3a://observability/lab6/stage_metrics_contract_results
s3a://observability/lab6/stage_metrics_contract_summary
```
