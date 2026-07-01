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

Use `CONFIG_NAME` in `shuffle_aggregation_diagnosis.py` as the classroom switch:

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
  /opt/spark/src/apps/labs/lab_2/shuffle_aggregation_diagnosis.py
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

See `shuffle_aggregation_diagnosis_class_notes.md` for the instructor narrative
and source-question summary.
