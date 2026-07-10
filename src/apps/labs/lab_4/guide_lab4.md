# Lab 4 guide: stage-level workload fingerprint

This guide is the classroom runbook for Lab 4.

Goal:

```text
run one controlled retail workload
  -> collect aggregate StageMetrics
  -> normalize raw execution counters
  -> assign an explainable operational profile
  -> persist metrics and fingerprint evidence as Delta
```

Lab 3 asked how much observability costs. Lab 4 asks what the observed workload
looks like. The lesson remains stage-first: use the lower-cost aggregate layer
to form a useful first hypothesis before requesting more detailed evidence.

Class notes:

[Stage workload fingerprint class
notes](docs/stage_workload_fingerprint_class_notes.md)

Keep those notes open for the ratio definitions, profile limitations, and the
important `input_bytes` caveat.

## 0. Confirm the shared workshop prerequisites

Start from the repository root:

```bash
cd workshop-spark-measures
```

This guide assumes that the platform has already been prepared and that the
shared Bronze retail tables exist.

If this is the first workshop run, or images and MinIO data were removed,
follow [Lab 0 guide: bootstrap through Bronze data
generation](../lab_0/guide_lab0.md#1-bootstrap-local-dependencies) before
continuing. Follow sections 1 through 5 there; the full bootstrap sequence is
not duplicated in this guide.

If only the containers were stopped and MinIO data was retained, restart the
platform without regenerating data:

```bash
make compose
```

Expected shared inputs:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

Teacher notes:

```text
Labs 1-6 share the generated retail sources. Do not regenerate them before
every lab. `make down` stops containers but preserves MinIO data;
`make clean-data` removes that generated state.
```

## 1. Move to the Lab 4 folder

The remaining commands assume this directory:

```bash
cd src/apps/labs/lab_4
```

Optional sanity check:

```bash
ls
```

Expected classroom entry points:

```text
lab_4_stage_workload_fingerprint.py
run_stage_workload_fingerprint.sh
```

## 2. Establish the diagnostic question

Ask the class:

```text
What does this Spark workload look like from a stage-level execution
perspective?
```

The target is not a universal root-cause diagnosis. The target is a consistent
first summary that can say:

- shuffle-heavy;
- memory-pressure-heavy;
- I/O-heavy;
- GC-heavy;
- many-small-tasks;
- low-parallelism;
- balanced or low-signal.

Teacher notes:

```text
The fingerprint is an interpretation of evidence, not proof of root cause.
Use it to standardize the next engineering question. Spark UI inspection and
TaskMetrics remain available when the aggregate signal is insufficient.
```

## 3. Inspect the classroom control points

Open:

```text
lab_4_stage_workload_fingerprint.py
lab_4_utils/experiments.yaml
lab_4_utils/fingerprint_rules.yaml
```

The main app keeps one visible configuration selector:

```python
CONFIG_NAME = os.environ.get(
    "LAB4_CONFIG_NAME",
    "lab4-stage-workload-fingerprint",
)
```

The experiment config defines:

- the four Bronze inputs;
- `96` shuffle partitions;
- `512` deterministic fingerprint buckets;
- StageMetrics as the only collector;
- one Gold workload output;
- two observability Delta outputs.

The rules file defines classroom thresholds. These are deliberately simple and
environment-specific; they are not production SLAs.

Teacher notes:

```text
Separating workload configuration from fingerprint rules makes the decision
explainable. Students can see which execution evidence was collected and which
threshold converted that evidence into a flag.
```

## 4. Understand the workload before reading its metrics

The app performs this sequence:

```text
sales
  -> select the required fact columns
  -> derive deterministic fingerprint_bucket
  -> join vendors, products, and customers
  -> repartition by fingerprint_bucket
  -> aggregate by bucket, regions, category, and month
  -> repartition by the final business keys
  -> aggregate the final summary
  -> write Delta
```

This workload intentionally contains joins, repartitions, and two aggregations
so StageMetrics has meaningful shuffle and task signals to classify.

The business output contains:

```text
vendor_region
customer_region
category_id
sale_year_month
sale_count
customer_count
product_count
total_quantity
gross_sales_amount
average_sale_amount
fingerprint_bucket_count
```

Teacher notes:

```text
Read the transformation before looking at the profile. A classifier can label
symptoms, but the code explains which joins and repartitions could have caused
them.
```

## 5. Run the stage-level fingerprint

Run the classroom wrapper:

```bash
bash run_stage_workload_fingerprint.sh
```

The wrapper performs one `spark-submit`, preserves the application output, and
fails only if the Spark app fails or one of the required markers is absent.

Expected markers:

```text
LAB4_STAGE_METRICS_CAPTURED_OK
LAB4_WORKLOAD_FINGERPRINT_RULES_OK
LAB4_WORKLOAD_PROFILE_ASSIGNED_OK
LAB4_WORKLOAD_FINGERPRINT_WRITTEN_OK
```

The runner finishes with:

```text
LAB4_FINGERPRINT_COMPLETED output=s3a://observability/lab4/workload_fingerprints
```

Optional manual `spark-submit` equivalent from the Lab 4 folder:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB4_CONFIG_NAME=lab4-stage-workload-fingerprint \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    /opt/spark/src/apps/labs/lab_4/lab_4_stage_workload_fingerprint.py
```

## 6. Read the diagnostic block in order

Near the end of the submit, the app prints one boxed block:

```text
## STAGE WORKLOAD FINGERPRINT DIAGNOSTIC

### Profile
workload_profile: ...
diagnostic_flags: ...

### StageMetrics signals
num_stages: ...
num_tasks: ...
executor_run_time_ms: ...
input_bytes: ...
shuffle_bytes_read: ...
shuffle_bytes_written: ...
memory_bytes_spilled: ...
disk_bytes_spilled: ...
jvm_gc_time_ms: ...

### Normalized ratios
shuffle_amplification_ratio: ...
gc_time_ratio: ...
spill_ratio: ...
task_density_score: ...

### Recommended next step
...
```

Read the block in this order:

1. identify the assigned profile;
2. find the flags that caused it;
3. verify those flags against the raw StageMetrics signals;
4. inspect the normalized ratios;
5. treat the recommendation as the next investigation, not an automatic fix.

Teacher notes:

```text
Do not start with the recommendation. Make students prove that the profile is
consistent with the raw counters. The lab teaches interpretation, not blind
trust in a label.
```

## 7. Interpret the ratios carefully

The app derives:

```text
shuffle_amplification_ratio = total shuffle bytes / input bytes
gc_time_ratio                = JVM GC time / executor runtime
spill_ratio                  = spill bytes / largest available volume signal
task_density_score           = tasks / stages
```

`task_density_score` uses aggregate task count from StageMetrics. It is not
task-level analysis and does not inspect individual task distributions.

The `input_bytes` denominator needs special care. It is the sparkMeasure
StageMetrics `bytesRead` counter, not the physical size of the Delta source.
When it is absent or below `minimum_reliable_input_bytes`, the lab marks the
shuffle amplification ratio as unavailable or low-confidence and uses absolute
shuffle volume as the safer evidence.

Teacher notes:

```text
A normalized ratio is useful only when its denominator is trustworthy. The
low-confidence flag is part of the diagnosis, not an error to hide.
```

## 8. Inspect the persisted evidence

The workload output is overwritten so each class run has one current business
result:

```text
s3a://lakehouse/gold/lab4/stage_workload_fingerprint/workload_summary
```

The normalized StageMetrics rows are appended to:

```text
s3a://observability/lab4/stage_metrics
```

The interpreted fingerprint rows are appended to:

```text
s3a://observability/lab4/workload_fingerprints
```

Each observability row contains `run_id`, `application_id`, workload identity,
metrics, and creation time. This preserves both the evidence and its
interpretation for later comparison.

Use the MinIO Console to show the physical separation:

```text
http://127.0.0.1:29011
```

Default credentials:

```text
user:     sparkworkshop
password: sparkworkshop123
```

## 9. Optional: correlate with Spark History Server

Open:

```text
http://127.0.0.1:28090
```

Use the application name and `application_id` printed by the submit to find the
run. The History Server remains useful for plan, job, stage, and executor
detail. The fingerprint condenses aggregate evidence; it does not replace the
UI.

## 10. Classroom conclusion

End with this statement:

```text
Stage-level metrics are not only raw counters. With explicit and explainable
rules, they become a lightweight operational fingerprint that helps engineers
agree on what to inspect next.
```

The progression is:

```text
collect aggregate evidence
  -> normalize carefully
  -> classify transparently
  -> preserve evidence and interpretation
  -> choose the next diagnostic layer only when needed
```

## 11. Optional cleanup after class

From the repository root:

```bash
make down
```

This stops the containers but preserves generated MinIO data. Use
`make clean-data` only when the next run must start from empty storage.
