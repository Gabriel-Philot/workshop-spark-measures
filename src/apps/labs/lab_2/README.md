# Lab 2: certification-style sparkMeasure diagnostics

Lab 2 connects certification-style Spark UI questions to local sparkMeasure
workshop exercises. The goal is to make students practice numeric diagnosis:
shuffle read/write, executor runtime, spill, GC, task count, and later
task-level distribution.

## Prerequisites

Start the local stack and generate demo data first.

```bash
make compose
make generate SCALE=xs GENERATOR_RUN_ID=lab2-demo
```

Useful local UIs:

- Spark Master UI: <http://127.0.0.1:28091>
- Spark History Server: <http://127.0.0.1:28090>
- MinIO Console: <http://127.0.0.1:29011>

## Lab 2A: shuffle aggregation diagnosis

The first Lab 2 exercise uses a common aggregation pattern: join sales with
vendor metadata, aggregate by region/month, and inspect the shuffle cost with
sparkMeasure StageMetrics.

Use `CONFIG_NAME` in `lab_2a_shuffle_aggregation_diagnosis.py` as the classroom switch:

```python
CONFIG_NAME = "lab2-shuffle-aggregation-baseline"
CONFIG_NAME = "lab2-shuffle-aggregation-optimized"
```

Run the same submit command after changing `CONFIG_NAME`:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2a_shuffle_aggregation_diagnosis.py
```

Both variants collect sparkMeasure StageMetrics and write a Gold Delta aggregate.
The baseline intentionally adds an unnecessary round-robin repartition before the
aggregation. The optimized variant narrows the data and repartitions by the
grouping keys.

Expected metrics to compare:

- `shuffleBytesWritten`
- `shuffleTotalBytesRead`
- `executorRunTime`
- `numStages`
- `numTasks`
- spill metrics when present

Expected markers:

- `LAB2_SHUFFLE_AGGREGATION_BASELINE_OK`
- `LAB2_SHUFFLE_AGGREGATION_OPTIMIZED_OK`

Latest validated local behavior on the current WSL stack with `SCALE=xs`:

| Scale | Variant | Rows in `sales` | Stages | Tasks | Executor runtime | Shuffle written |
|---|---:|---:|---:|---:|---:|---:|
| `xs` | baseline | 5,000,000 | 14 | 1,296 | ~65s | ~69.1 MiB |
| `xs` | optimized | 5,000,000 | 13 | 301 | ~43s | ~50.9 MiB |

`SCALE=s` was used during stress testing and generated about 10.94 GiB for the
`sales` table. Re-run `s` before using larger-scale numbers in class because the
baseline was later adjusted to make the wide-row shuffle problem clearer.

See `lab_2a_shuffle_aggregation_diagnosis_class_notes.md` for the instructor narrative
and source-question summary.

## Lab 2B: stage metrics interpretation drill

The second Lab 2 exercise is a certification-style metric-reading drill. It
connects two common Spark UI questions to sparkMeasure StageMetrics:

- high GC time as a memory-pressure signal;
- shuffle spill memory/disk as evidence that Spark spilled shuffle data.

Use `CONFIG_NAME` in `lab_2b_stage_metrics_interpretation_drill.py` as the classroom
switch:

```python
CONFIG_NAME = "lab2-stage-metrics-drill-pressure"
CONFIG_NAME = "lab2-stage-metrics-drill-default"
```

Run the submit command:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2b_stage_metrics_interpretation_drill.py
```

The pressure variant carries wider payload columns through a round-robin
shuffle before narrowing the data. The default variant computes the same Gold
summary while narrowing payload width earlier and repartitioning by the
business keys.

Expected metrics to compare:

- `shuffleBytesWritten`
- `shuffleTotalBytesRead`
- `memoryBytesSpilled`
- `diskBytesSpilled`
- `jvmGCTime`
- `executorRunTime`
- `numTasks`

Expected markers:

- `LAB2_STAGE_METRICS_DRILL_PRESSURE_OK`
- `LAB2_STAGE_METRICS_DRILL_DEFAULT_OK`

Latest validated local behavior on the current WSL stack with `SCALE=xs`:

| Scale | Variant | Rows in `sales` | Stages | Tasks | Executor runtime | Shuffle written | Memory spilled | Disk spilled | GC ratio |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `xs` | default | 5,000,000 | 16 | 356 | ~59.3s | ~40.0 MiB | 0 B | 0 B | ~2.8% |
| `xs` | pressure | 5,000,000 | 17 | 839 | ~94.2s | ~670.9 MiB | ~800.0 MiB | ~382.2 MiB | ~3.4% |

Both variants write the same 50-row Gold summary, so the lesson compares
execution symptoms rather than different business logic.

See `lab_2b_stage_metrics_interpretation_drill_class_notes.md` for the source-question
summary, expected answers, and instructor narrative.
