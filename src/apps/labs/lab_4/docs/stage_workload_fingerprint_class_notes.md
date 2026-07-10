# Lab 4 class notes: stage-level workload fingerprint

Classroom runbook:

[Lab 4 classroom guide](../guide_lab4.md)

## Teaching question

What does this Spark workload look like from a stage-level execution
perspective?

Lab 4 moves the workshop from reading raw counters to interpreting a workload
profile. StageMetrics are still the first diagnostic layer, but the lesson is
no longer only about individual values such as `shuffleBytesWritten` or
`jvmGCTime`.

## Workshop connection

The classroom progression is:

- Lab 3 measures the local cost of observability;
- Lab 4 uses StageMetrics to describe what a workload is doing.

The important change is from collection to interpretation. The same aggregate
signals that describe runtime, shuffle, spill, GC, stages, and tasks can become
a compact operational vocabulary for engineering discussions.

## StageMetrics as a first diagnostic layer

StageMetrics are a good first diagnostic layer because they summarize important
execution symptoms with low operational cost:

- shuffle read/write volume;
- executor runtime;
- JVM GC time;
- spill bytes;
- stage and task counts;
- scan/read counters when Spark reports them.

They do not replace the Spark UI, event logs, or TaskMetrics. They provide a
compact starting point.

## Raw metrics versus interpretation

Raw metrics are useful, but interpretation is more useful in a workshop and in
team reviews.

For example:

```text
shuffleBytesWritten=643MB
numTasks=2581
executorRunTime=136s
```

Those values matter, but the engineering question is:

```text
Does this look shuffle-heavy, memory-pressure-heavy, I/O-heavy, GC-heavy, or
low-signal?
```

Lab 4 turns aggregate counters into normalized ratios and a profile.

## Ratios used by the lab

`shuffle_amplification_ratio`

: Total shuffle bytes divided by input bytes when Spark reports a reliable
  input byte denominator.[^input-bytes]

`gc_time_ratio`

: JVM GC time divided by executor runtime.

`spill_ratio`

: Memory plus disk spill divided by the largest available data-volume signal.

`task_density_score`

: Number of tasks divided by number of stages. This is not task-level analysis;
  it is only a coarse stage-aggregate signal.

## Profiles

The lab can produce these profiles:

- `SHUFFLE_HEAVY`;
- `MEMORY_PRESSURE`;
- `IO_HEAVY_SCAN`;
- `GC_PRESSURE`;
- `MANY_SMALL_TASKS`;
- `LOW_PARALLELISM_SIGNAL`;
- `BALANCED_OR_LOW_SIGNAL`.

The profile is not a perfect root-cause analysis. It is a lightweight
operational summary.

## How to explain the result

Use this framing:

1. StageMetrics captured the workload symptoms.
2. Lab 4 normalized those symptoms into ratios.
3. Simple rules assigned a profile and flags.
4. The recommendation is the next diagnostic conversation, not a guaranteed
   fix.

Example:

```text
workload_profile=SHUFFLE_HEAVY
diagnostic_flags=HIGH_SHUFFLE_VOLUME,TASK_OVERHEAD_SIGNAL
recommended_next_step=Review joins, aggregations, partitioning, and unnecessary repartitions.
```

This is useful in:

- PR review;
- incident review;
- performance review;
- comparing workload versions;
- deciding when to move from StageMetrics to TaskMetrics.

## Scope guardrail

Lab 4 is stage-level only.

It does not:

- use TaskMetrics;
- use Flight Recorder;
- parse Spark event logs;
- inspect task-level distributions.

If the fingerprint says the workload is suspicious but not conclusive, the next
diagnostic step can use the Spark UI or decide whether task-level evidence is
actually needed.

[^input-bytes]: `input_bytes` is the StageMetrics-reported `bytesRead` counter,
    not the physical Delta table size. In local validation, the generated
    `sales` Delta table had a physical size of about `762 MB`, while
    StageMetrics reported `input_bytes` around `580-832 KB` for the Lab 4
    workload. At the same time, StageMetrics reported more than `5.2M` records
    read and about `1.06 GB` of total shuffle bytes. The workload definitely
    read and processed data, but this counter should not be treated as physical
    lake table size in this environment. Lab 4 keeps `input_bytes` as reported,
    marks low denominators with `INPUT_BYTES_LOW_CONFIDENCE_FOR_RATIO`, avoids
    using `HIGH_SHUFFLE_AMPLIFICATION` below `minimum_reliable_input_bytes`, and
    still uses `HIGH_SHUFFLE_VOLUME` when absolute shuffle bytes are large.
