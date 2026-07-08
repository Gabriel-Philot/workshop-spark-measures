# Spark worker topology design

Date: 2026-06-24.

## Decision

Use explicit Spark worker services in Docker Compose instead of dynamic `docker compose --scale` for now.

Default local topology:

```text
spark-worker-1
spark-worker-2
```

Optional demo topology:

```text
spark-worker-1
spark-worker-2
spark-worker-3
```

## Why

The workshop is local and should not use more than three workers. Fixed names are easier to explain in Spark UI, History Server, and workshop material. They also avoid Docker Compose `container_name` scaling constraints.

## WSL sizing

Measured local WSL/Docker resources:

```text
Logical CPUs: 16
RAM visible to WSL/Docker: ~7.8 GiB
Swap: 4.0 GiB
Workspace disk available at measurement time: ~878 GiB
```

RAM is the constraint, not CPU.

Default sizing:

```env
SPARK_WORKER_REPLICAS=2
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
```

Optional three-worker sizing:

```env
SPARK_WORKER_REPLICAS=3
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=1536m
```

`make compose-three-workers` applies the lower memory value through `SPARK_WORKER_THREE_WORKER_MEMORY` so the cluster does not allocate 6 GiB only to workers by default.

## Commands

Default:

```bash
make compose
```

Three-worker demo mode:

```bash
make compose-three-workers
```

Validation checks the expected number of ALIVE workers in Spark Master UI.
