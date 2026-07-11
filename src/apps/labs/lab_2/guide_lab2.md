# Lab 2 guide: professional metric-reading diagnostics

This guide is the classroom runbook for Lab 2.

Goal:

```text
prepare the shared retail sources
  -> diagnose stage-level shuffle cost
  -> distinguish shuffle, spill, and GC evidence
  -> use TaskMetrics for high-end skew
  -> use TaskMetrics for low-end empty partitions
```

Lab 2 connects an instructor-selected set of Spark UI questions from Databricks
Certified Data Engineer Professional practice exams to controlled local
workloads. The exam context asks students to interpret numeric Spark signals;
the workshop makes analogous signals observable and comparable through
sparkMeasure.

The emphasis is not memorizing one magic threshold. Students should learn to
read a metric relationship, connect it to the physical workload, and choose the
right sparkMeasure granularity. sparkMeasure complements the Spark UI by
condensing useful evidence; it does not replace plan, stage, or executor
inspection.

Question reference:

```text
docs/exam_questions.md
```

Use that document to present each complete question and answer. Keep this guide
open for commands, expected evidence, and teacher notes.

The questions are a focused selection from certification practice exams. They
are not presented as the complete exam or as official Databricks exam material.

## 0. Start from the repository root

```bash
cd workshop-spark-measures
```

Expected:

- `Makefile` exists;
- `.env.example` exists;
- `src/apps/labs/lab_2` exists.



## 1. Prepare the local platform

If Labs 0 or 1 were already run and the stack is still available, do not
regenerate the shared retail data. Continue to step 2.

If the containers were stopped but images and MinIO data remain, restart only
the platform:

```bash
make compose
```

`make down` stops containers but does not erase generated MinIO data.

If `make clean-data` was used, generate the retail data again after starting the
stack:

```bash
make compose
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

When starting from a clean machine or after images were removed, use the full
sequence:

```bash
make bootstrap
make build
make compose
make dry-test
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

Why this sequence exists:

- `make bootstrap` prepares pinned dependencies and local configuration;
- `make build` builds the workshop images;
- `make compose` starts MinIO, Spark, workers, and Spark History Server;
- `make dry-test` proves Spark, Delta, S3A, MinIO, and sparkMeasure work
together;
- `make generate` creates the Bronze retail sources shared by Labs 1-6.

Expected Bronze sources:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/sales
```

Useful UIs:

```text
Spark Master UI:      http://127.0.0.1:28091
Spark History Server: http://127.0.0.1:28090
MinIO Console:        http://127.0.0.1:29011
```

Default MinIO credentials:

```text
user:     sparkworkshop
password: sparkworkshop123
```

Teacher notes:

```text
Do not diagnose a Spark workload before proving the environment is healthy.
If the dry test or source generation fails, fix the platform first. Otherwise
students may confuse missing infrastructure with a performance symptom.
```



## 2. Move to the Lab 2 folder

The next commands assume the current directory is the Lab 2 application folder.

```bash
cd src/apps/labs/lab_2
```

Optional sanity check:

```bash
ls
```

Expected scripts:

```text
lab_2a_shuffle_aggregation_diagnosis.py
lab_2b_stage_metrics_interpretation_drill.py
lab_2c_task_duration_skew_diagnosis.py
lab_2d_empty_partitions_diagnosis.py
```

Lesson progression:


| Exercise | Collector    | Diagnostic question                                             |
| -------- | ------------ | --------------------------------------------------------------- |
| 2A       | StageMetrics | Is unnecessary distribution increasing shuffle and task volume? |
| 2B       | StageMetrics | Do the aggregates support shuffle, spill, or GC pressure?       |
| 2C       | TaskMetrics  | Is one high-end task much larger than the typical task?         |
| 2D       | TaskMetrics  | Are a few low-end tasks empty or nearly empty?                  |


The collector change is intentional. StageMetrics remains the default first
diagnostic layer. TaskMetrics is introduced only when the question depends on
the distribution inside a stage.

## 3. Lab 2A: shuffle aggregation diagnosis

Read the complete source question first:

[Lab 2A: reducing shuffle during aggregation](docs/exam_questions.md)

### 3.1 Run the baseline

Open:

```text
lab_2a_shuffle_aggregation_diagnosis.py
```

Keep the classroom control point as:

```python
CONFIG_NAME = "lab2-shuffle-aggregation-baseline"
```

Run:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2a_shuffle_aggregation_diagnosis.py
```

The baseline performs this physical sequence:

```text
sales + vendors
  -> round-robin repartition(1024)
  -> select the regional fact
  -> groupBy(vendor_region, sale_year_month)
  -> Gold Delta write
```

The round-robin repartition is intentionally not aligned with the grouping
keys. It moves wide rows and creates far more tasks than the small local cluster
can use efficiently.

Watch the native sparkMeasure report for:

```text
numStages
numTasks
executorRunTime
shuffleBytesWritten
shuffleTotalBytesRead
memoryBytesSpilled
diskBytesSpilled
recordsRead
recordsWritten
```

Teacher notes:

```text
Start from evidence, not the fix. A tiny four-row result required millions of
input rows, meaningful shuffle, and many tasks. Zero spill means memory spill is
not the first suspect. The next place to inspect is the distribution decision
before the grouped aggregation.
```

Expected marker:

```text
LAB2_SHUFFLE_AGGREGATION_BASELINE_OK
```

Expected output:

```text
s3a://lakehouse/gold/lab2/shuffle_aggregation/baseline
```

Business output shape:

```text
vendor_region
sale_year_month
sale_count
total_quantity
gross_sales_amount
average_sale_amount
```



### 3.2 Run the optimized variant

Change only the top-level classroom switch:

```python
CONFIG_NAME = "lab2-shuffle-aggregation-optimized"
```

Run the same submit command again.

The optimized variant:

```text
sales + vendors
  -> narrow the aggregation fact
  -> repartition by vendor_region and sale_year_month
  -> groupBy the same keys
  -> Gold Delta write
```

Expected marker:

```text
LAB2_SHUFFLE_AGGREGATION_OPTIMIZED_OK
```

Expected output:

```text
s3a://lakehouse/gold/lab2/shuffle_aggregation/optimized
```



### 3.3 Compare the evidence

Latest validated `SCALE=xs` comparison:


| Variant   | Stages | Tasks | Executor runtime | Shuffle written | Shuffle read |
| --------- | ------ | ----- | ---------------- | --------------- | ------------ |
| baseline  | 14     | 1,296 | ~65s             | ~69.1 MiB       | ~69.1 MiB    |
| optimized | 13     | 301   | ~43s             | ~50.9 MiB       | ~50.9 MiB    |


The baseline reduced roughly five million input rows to four output rows. Its
compact sparkMeasure evidence included:

```text
recordsRead=5,000,367
recordsWritten=4
memoryBytesSpilled=0
diskBytesSpilled=0
```

Teacher notes:

```text
Do not claim that repartitioning by the key removes shuffle. groupBy remains a
wide operation. The evidence says the optimized path reduces unjustified work:
fewer tasks, lower executor runtime, and less shuffle on the validated run.

The lesson is the diagnostic method:
read the code -> inspect StageMetrics -> form a hypothesis -> rerun -> compare.
```

Optional Spark History review:

- open `workshop-lab2-shuffle-aggregation-baseline` and the optimized app;
- compare Jobs and Stages using the `LAB2` descriptions;
- inspect task count and shuffle read/write on the expensive stages;
- use SQL / DataFrame to connect `Exchange` and aggregation operators to the
sparkMeasure counters.



## 4. Lab 2B: stage metrics interpretation drill

Read both source questions before the run:

- [Lab 2B: interpreting high GC time](docs/exam_questions.md)
- [Lab 2B: interpreting shuffle spill](docs/exam_questions.md)

Teacher reminder:

```text
Lab 2B connects two separate practice-exam questions to one local workload.
Do not present them as one combined question.

Question 1 asks how to interpret a material GC-time ratio, using 25% as its
theoretical signal.

Question 2 asks how to interpret memory and disk shuffle spill.

The validated local run clearly reproduces the shuffle-spill scenario. Its GC
ratio stays around 3.4%, so it does not reproduce the 25% high-GC scenario.
This contrast is part of the lesson: diagnose only what the measured evidence
supports.
```

This exercise deliberately asks students to interpret the native aggregated
sparkMeasure report. It does not print a separate diagnosis line.

### 4.1 Run the pressure variant

Open:

```text
lab_2b_stage_metrics_interpretation_drill.py
```

Start with:

```python
CONFIG_NAME = "lab2-stage-metrics-drill-pressure"
```

Run:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2b_stage_metrics_interpretation_drill.py
```

The pressure variant carries wider payload columns through a round-robin
repartition before narrowing the aggregation fact:

```text
sales + vendors + products
  -> keep wide payload columns
  -> round-robin repartition(512)
  -> narrow the fact
  -> groupBy(region, category, month)
  -> Gold Delta write
```

Read these fields directly from the aggregated stage report:

```text
numStages
numTasks
executorRunTime
jvmGCTime
memoryBytesSpilled
diskBytesSpilled
shuffleTotalBytesRead
shuffleBytesWritten
```

Use this evidence table:


| Observation                        | Interpretation                                   |
| ---------------------------------- | ------------------------------------------------ |
| shuffle bytes > 0                  | a wide operation exchanged data between stages   |
| memory and disk spill > 0          | shuffle data exceeded available execution memory |
| spill = 0                          | this run does not show a spill symptom           |
| high `jvmGCTime / executorRunTime` | memory pressure may be present                   |
| low GC ratio and zero spill        | investigate movement and partitioning first      |
| one slow task                      | not proven by StageMetrics aggregates            |


Expected marker:

```text
LAB2_STAGE_METRICS_DRILL_PRESSURE_OK
```

Expected output:

```text
s3a://lakehouse/gold/lab2/stage_metrics_drill/pressure
```

Both Lab 2B variants produce a category/month summary at this grain:

```text
vendor_region, category_id, sale_year_month
```



### 4.2 Run the default variant

Change only:

```python
CONFIG_NAME = "lab2-stage-metrics-drill-default"
```

Run the same submit command again.

The default variant narrows data before a keyed repartition and produces the
same 50-row business summary.

Expected marker:

```text
LAB2_STAGE_METRICS_DRILL_DEFAULT_OK
```

Expected output:

```text
s3a://lakehouse/gold/lab2/stage_metrics_drill/default
```



### 4.3 Compare the evidence

Latest validated `SCALE=xs` comparison:


| Variant  | Stages | Tasks | Executor runtime | Shuffle written | Memory spilled | Disk spilled | GC time |
| -------- | ------ | ----- | ---------------- | --------------- | -------------- | ------------ | ------- |
| default  | 16     | 356   | ~59.3s           | ~40.0 MiB       | 0 B            | 0 B          | ~1.7s   |
| pressure | 17     | 839   | ~94.2s           | ~670.9 MiB      | ~800.0 MiB     | ~382.2 MiB   | ~3.2s   |


The pressure run's raw evidence was:

```text
numStages => 17
numTasks => 839
executorRunTime => 94175
jvmGCTime => 3246n
diskBytesSpilled => 400765052
memoryBytesSpilled => 838859488
shuffleTotalBytesRead => 703458127
shuffleBytesWritten => 703458127
```

The default run's raw evidence was:

```text
numStages => 16
numTasks => 356
executorRunTime => 59274
jvmGCTime => 1676
diskBytesSpilled => 0
memoryBytesSpilled => 0
shuffleTotalBytesRead => 41940653
shuffleBytesWritten => 41940653
```

Teacher notes:

```text
The strongest local evidence is shuffle plus memory/disk spill. The GC ratio is
only about 3.4%, far from the professional question's 25% example. Say that
explicitly: this run supports a spill diagnosis, not a strong GC diagnosis.

If spill is zero on another machine, the lesson still works. The honest result
is: shuffle exists, but this run does not show spill. Do not force every slow
stage into a memory-pressure narrative.
```

Both variants must remain semantically equivalent:

```text
default row count=50
pressure row count=50
default minus pressure=0
pressure minus default=0
```



## 5. Lab 2C: high-end task skew diagnosis

Read the source question:

[Lab 2C: diagnosing high-end task skew](docs/exam_questions.md)

StageMetrics was enough for Labs 2A and 2B because those questions concerned
aggregate stage pressure. Lab 2C asks whether one or a few tasks are much larger
than the rest. That requires a task distribution.

### 5.1 Run the TaskMetrics workload

Open:

```text
lab_2c_task_duration_skew_diagnosis.py
```

The classroom config is:

```python
CONFIG_NAME = "lab2c-task-skew-task"
```

Run:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2c_task_duration_skew_diagnosis.py
```

The workload keeps the generated hot vendor visible:

```text
sales
  -> repartition(27, vendor_id)
  -> shuffle join vendors
  -> aggregate by vendor_id and vendor_region
  -> Gold Delta write
```

AQE and automatic broadcast joins are disabled for this exercise so the
27-task hot-key shuffle remains visible.

Expected marker:

```text
LAB2C_TASK_SKEW_TASK_OK
```

Expected output:

```text
s3a://lakehouse/gold/lab2/task_skew/task
```

Business output grain:

```text
vendor_id, vendor_region
```



### 5.2 Read the boxed report

The native sparkMeasure TaskMetrics report is printed first. The lab then emits
one classroom projection of the same TaskMetrics DataFrame:

```text
LAB 2C TASKMETRICS DIAGNOSTIC REPORT
Selected stage
Metric summary
Top task outliers by shuffleTotalBytesRead
```

Primary decision rule:

```text
If max is much larger than p75 for both duration and data volume,
diagnose high-end task skew.
```

Latest validated `SCALE=xs` evidence:


| Metric                  | p75       | Max          | Max / p75 |
| ----------------------- | --------- | ------------ | --------- |
| `duration`              | 227 ms    | 2,033 ms     | 8.96x     |
| `shuffleTotalBytesRead` | 617,822 B | 28,946,218 B | 46.85x    |
| `shuffleRecordsRead`    | 75,904    | 3,561,478    | 46.92x    |


The highest-volume task in the validated report made the imbalance tangible:

```text
task=236
duration=2033 ms
shuffleRecordsRead=3,561,478
shuffleTotalBytesRead=27.6 MiB
memoryBytesSpilled=80.0 MiB
diskBytesSpilled=16.4 MiB
```

Teacher notes:

```text
The professional question calls its volume metric Input Size. This selected
stage consumes shuffle, so Shuffle Read is the equivalent local evidence. The
column name changes with stage type; the percentile reasoning does not.

Focus on the distribution shape, not exact milliseconds. Local WSL scheduling
can move duration values, while the hot-key data-volume ratio remains the
stronger signal.

The total number of tasks in the application may also vary after the first
Delta overwrite. The selected 27-task shuffle stage is the stable classroom
unit because it mirrors the professional question.
```

Discussion-only remediation options:

- salt the hot key;
- use two-step aggregation;
- process the dominant key separately;
- evaluate AQE skew handling in a production workload.

The exercise intentionally stops at diagnosis.

## 6. Lab 2D: low-end empty partitions diagnosis

Read the source question:

[Lab 2D: diagnosing near-empty partitions](docs/exam_questions.md)

Lab 2D uses TaskMetrics again but reverses the direction of the diagnostic
question. Instead of looking for one dominant maximum, inspect whether a few
tasks at the minimum processed no useful data.

### 6.1 Run the TaskMetrics workload

Open:

```text
lab_2d_empty_partitions_diagnosis.py
```

The classroom config is:

```python
CONFIG_NAME = "lab2d-empty-partitions-task"
```

Run:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2d_empty_partitions_diagnosis.py
```

The workload derives a synthetic bucket from `sale_id` so it does not inherit
the hot-vendor skew from Lab 2C:

```text
sales
  -> derive partition_bucket with 48 active buckets
  -> repartition(27, partition_bucket)
  -> aggregate by partition_bucket
  -> Gold Delta write
```

AQE is disabled so Spark does not coalesce away the distribution the lesson is
trying to expose.

Expected marker:

```text
LAB2D_EMPTY_PARTITIONS_TASK_OK
```

Expected output:

```text
s3a://lakehouse/gold/lab2/empty_partitions/task
```

Business output grain:

```text
partition_bucket
```



### 6.2 Read the boxed report

The native sparkMeasure report is followed by:

```text
LAB 2D TASKMETRICS EMPTY PARTITIONS REPORT
Selected stage
Metric summary
Lowest task outliers by shuffleRecordsRead
```

Primary decision rule:

```text
If data-volume min is much smaller than median, inspect low-end tasks.
If data-volume max remains close to p75, do not diagnose high-end skew.
```

Latest validated `SCALE=xs` evidence:


| Metric                  | Min | Median    | p75     | Max     | Max / p75 |
| ----------------------- | --- | --------- | ------- | ------- | --------- |
| `shuffleRecordsRead`    | 0   | 104,633   | 311,706 | 521,959 | 1.67x     |
| `shuffleTotalBytesRead` | 0 B | 831.9 KiB | 3.0 MiB | 5.4 MiB | 1.79x     |
| `recordsWritten`        | 0   | 1         | 3       | 5       | 1.67x     |


The selected stage contained four empty tasks.

The lowest-volume tasks in the validated report showed the intended pattern:

```text
task=193 shuffleRecordsRead=0 shuffleTotalBytesRead=0 B recordsWritten=0
task=192 shuffleRecordsRead=0 shuffleTotalBytesRead=0 B recordsWritten=0
task=191 shuffleRecordsRead=0 shuffleTotalBytesRead=0 B recordsWritten=0
task=190 shuffleRecordsRead=0 shuffleTotalBytesRead=0 B recordsWritten=0
```

Teacher notes:

```text
Use data volume as the primary diagnosis. A local duration maximum can move
because of scheduling noise even when records and bytes show a clear low-end
empty-partition pattern.

Contrast this explicitly with Lab 2C:
2C asks whether max >> p75.
2D asks whether min << median while the data-volume high end stays constrained.
```



## 7. Final classroom comparison

Use this table after completing all four exercises:


| Symptom                  | First collector | Evidence pattern                         | Next investigation                              |
| ------------------------ | --------------- | ---------------------------------------- | ----------------------------------------------- |
| excessive stage movement | StageMetrics    | high shuffle bytes and task volume       | joins, aggregations, repartitions               |
| shuffle memory pressure  | StageMetrics    | memory/disk spill greater than zero      | partition size, memory, wide operations         |
| GC pressure              | StageMetrics    | material and repeatable GC/runtime ratio | object pressure, caching, serialization, memory |
| high-end task skew       | TaskMetrics     | max much greater than p75                | hot keys and stragglers                         |
| low-end empty partitions | TaskMetrics     | min much lower than median               | partition count and key distribution            |


Main teaching message:

```text
Start with StageMetrics to classify the broad operational symptom.
Use TaskMetrics as a microscope only when the diagnosis depends on the
distribution inside a stage.
```



## 8. Optional: inspect Spark History Server and MinIO

Spark History Server:

```text
http://127.0.0.1:28090
```

Application names:

```text
workshop-lab2-shuffle-aggregation-baseline
workshop-lab2-shuffle-aggregation-optimized
workshop-lab2-stage-metrics-drill-pressure
workshop-lab2-stage-metrics-drill-default
workshop-lab2c-task-skew-task
workshop-lab2d-empty-partitions-task
```

What to inspect:

- `Jobs`: use `LAB2`, `LAB2C`, or `LAB2D` descriptions to identify workshop
materialization work;
- `Stages`: compare duration, task count, shuffle read/write, and spill;
- `SQL / DataFrame`: connect exchanges and aggregations to metric evidence;
- completed-stage Summary Metrics: compare the task distribution used by 2C
and 2D.

MinIO Console:

```text
http://127.0.0.1:29011
```

Expected Gold paths:

```text
s3a://lakehouse/gold/lab2/shuffle_aggregation/baseline
s3a://lakehouse/gold/lab2/shuffle_aggregation/optimized
s3a://lakehouse/gold/lab2/stage_metrics_drill/pressure
s3a://lakehouse/gold/lab2/stage_metrics_drill/default
s3a://lakehouse/gold/lab2/task_skew/task
s3a://lakehouse/gold/lab2/empty_partitions/task
```

Metric persistence is disabled for these exercises. The metrics appear in the
terminal and Spark event history; the business outputs are persisted as Delta.

## 9. Optional cleanup after class

From the repository root:

```bash
cd ../../../..
make down
```

To remove generated MinIO data as well:

```bash
make clean-data
```

To remove workshop images:

```bash
make removeimage
```

`make clean-data` removes data used by other labs. Do not run it between Lab 2
exercises.

## Appendix A: why the boxed TaskMetrics reports exist

Labs 2C and 2D retain the native sparkMeasure TaskMetrics output. Their boxed
reports do not collect new measurements. They create a compact classroom view
from:

```python
task_metrics = collector.create_taskmetrics_DF(...)
```

The shared flow is:

```text
TaskMetrics begin/end
  -> create_taskmetrics_DF
  -> select a useful completed stage
  -> aggregate task percentiles
  -> collect a small outlier table
  -> render one multiline logger block
```

Lab 2C selects the clearest high-end data-volume stage and computes:

```text
p75
max
max / p75
```

It sorts the outlier table by `shuffleTotalBytesRead` descending.

Lab 2D selects the clearest low-end data-volume stage and computes:

```text
min
median
p75
max
median / min
max / p75
```

It sorts the outlier table by `shuffleRecordsRead` ascending.

Both reports use one multiline project logger call so the diagnostic evidence
remains readable in `spark-submit`. The native collector report is still
available immediately before the classroom box.

## Appendix B: local validation context

The documented values were captured on the local WSL stack with two Spark
workers and generated `SCALE=xs` data:

```text
sales rows=5,000,000
sales files=114
sales total bytes≈762 MB
hot vendor share≈0.7001
```

Treat exact runtime and GC values as environment-specific. Docker, WSL, the
host, driver, and executors compete for the same local resources. The durable
teaching evidence is the relationship between counters:

- shuffle bytes compared across equivalent variants;
- spill greater than or equal to zero;
- GC time relative to executor runtime;
- max compared with p75 for high-end skew;
- min compared with median for low-end empty partitions.

`SCALE=s` was used during earlier stress testing, but the latest documented
workloads were calibrated and validated with `SCALE=xs`. Re-run the larger
scale before presenting any historical `SCALE=s` numbers.
