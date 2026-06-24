# Spark Measure Workshop Platform

Local workshop platform for performance troubleshooting with Apache Spark 4, Delta Lake, sparkMeasure, and MinIO.

## Platform

- Spark `4.1.2` with Scala `2.13`
- Delta Lake `4.2.0`
- sparkMeasure `0.28.0` Python API and `0.28` JVM artifact
- MinIO object storage
- Spark History Server backed by MinIO event logs

MinIO uses three buckets:

- `lakehouse`: future `landing/`, `bronze/`, `silver/`, and `gold/` data
- `tests`: workshop test datasets and artifacts
- `observability`: Spark event logs and sparkMeasure Delta tables

Object-store folders are prefixes and are materialized when data is first written.

## Quick start

```bash
make bootstrap
make build
make tests
make validate
make compose
make dry-test
make services
```

`make dry-test` runs a deterministic shuffle workload, collects stage metrics, prints a sparkMeasure report, and writes the aggregate as Delta to:

```text
s3a://observability/spark-measure/stage/latest
```

The integration validator checks the report, Delta transaction log, data files, Spark event log, and History Server application.

Use `make down` to stop services without removing data. Use `make clean-data` to remove all local MinIO state.



## Local Spark workers

The default Compose topology starts two explicit Spark workers for a small WSL-friendly cluster:

```text
2 workers x 2 cores x 2g
```

Use the optional three-worker profile for executor/topology demos:

```bash
make compose-three-workers
```

The three-worker target uses `SPARK_WORKER_THREE_WORKER_MEMORY=1536m` by default to avoid overcommitting the current WSL memory cap.

## Data generator

The first generator slice creates related retail Delta tables in bronze for join-skew and file-layout labs:

```bash
make generate SCALE=demo
```

It writes `vendors`, `products`, `customers`, and `sales` under `s3a://lakehouse/bronze/retail/...`, validates foreign keys and hot-vendor skew, and writes a generator manifest under `s3a://observability/generator-runs/<run_id>/manifest.json`.

The generator is schema-first: the YAML contract owns relationships, distributions, scale, and file layout. The first materializer is Spark-native so local Delta/S3A behavior matches the later sparkMeasure labs.

## Why stage metrics first

Stage-level collection has lower overhead and is sufficient for the first workshop diagnostic flow. Task-level metrics and Flight Recorder mode are intentionally deferred.
