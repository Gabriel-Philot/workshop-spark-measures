# Lab 3: observability overhead benchmark

Lab 3 runs one Spark workload in `none`, `stage`, and `task` observability modes
to measure how collector granularity affects local runtime. The result is an
environment-specific benchmark post-mortem, not a universal sparkMeasure
overhead number.

## Classroom material

- [Lab 3 classroom guide](guide_lab3.md): platform preparation, smoke run,
  timing boundaries, outputs, full-run duration, and teaching sequence.
- [Benchmark post-mortem](docs/observability_overhead_postmortem.md): the two
  workload designs, validated local results, and interpretation tradeoffs.

## Entry points

```text
lab_3_observability_overhead_benchmark.py
run_observability_overhead_benchmark.sh
```

The Python app executes one mode. The shell runner executes modes sequentially,
rotates mode order, assigns unique run identities, and records total submit
time.

## Classroom demonstration

After preparing the shared Bronze retail sources, move to this folder and run:

```bash
LAB3_REPETITIONS=1 \
LAB3_WARMUP_REPETITIONS=0 \
bash run_observability_overhead_benchmark.sh
```

This performs three sequential submits: one measured run for each of `none`,
`stage`, and `task`. The live demonstration skips warmup to save classroom
time; the guide explains why a real benchmark uses warmup and filters it from
the measured evidence. The optional ten-repetition run uses one warmup, takes
approximately 43-44 minutes, and is documented near the end of the guide.

## Outputs

Unique workload outputs:

```text
s3a://lakehouse/gold/lab3/observability_overhead/workload
```

Benchmark metadata:

```text
s3a://observability/lab3/overhead_runs
```

Use `workload_wall_ms` for collector comparisons and filter
`is_warmup = false`. The guide explains why `spark_submit_wall_ms` measures a
different boundary.
