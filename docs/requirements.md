# Workshop requirements

This workshop runs locally with Docker Compose, Apache Spark, Delta Lake,
sparkMeasure, MinIO, and a small Python test/tooling environment.

## Required local tools

Install or make available in your shell:

```bash
docker --version
docker compose version
make --version
uv --version
curl --version
sha256sum --version
```

The bootstrap script validates `docker`, `docker compose`, `curl`,
`sha256sum`, and `uv`.

Validated local tool versions used while preparing this workshop:

| Tool | Validated version |
| --- | --- |
| Docker | `29.2.1` |
| Docker Compose | `v5.0.2` |
| GNU Make | `4.3` |
| uv | `0.10.0` |
| curl | `8.5.0` |
| sha256sum / GNU coreutils | `9.4` |

## Pinned workshop versions

The workshop pins the runtime stack through `.env.example`, Dockerfiles, and
requirements files.

| Component | Version |
| --- | --- |
| Apache Spark | `4.1.2` |
| Spark base image | `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu` |
| Scala binary line | `2.13` |
| Java runtime line | `17` |
| Delta Lake | `4.2.0` |
| Hadoop AWS | `3.4.2` |
| sparkMeasure JAR | `0.28` |
| sparkMeasure Python package | `0.28.0` |
| MinIO server image | `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1` |
| MinIO client image | `quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z-cpuv1` |
| Lab 7 dashboard base image | `python:3.12-slim-bookworm` |

Python dependencies used inside Spark jobs:

```text
PyYAML==6.0.2
sparkmeasure==0.28.0
```

Python dependencies used by the Lab 7 dashboard image:

```text
duckdb==1.4.2
pandas==2.2.3
plotly==5.24.1
streamlit==1.40.2
```

Local development uses Python `>=3.10` through `uv`; test dependencies include
`pytest>=8.0.0`.

## Recommended local resources

The workshop is designed for a local WSL/Docker setup and a small Spark
cluster.

Recommended minimum:

- 4 CPU cores available to Docker;
- 8 GiB RAM available to WSL/Docker;
- 50 GiB free disk for images, cached dependencies, Delta data, logs, and
  repeated lab runs.

Better for larger demos:

- 8+ CPU cores;
- 12-16 GiB RAM available to Docker;
- 100+ GiB free disk.

The default Compose topology starts two Spark workers. A three-worker mode is
available for demos, but the workshop should not require more than three local
workers.

## Default local ports

The default ports come from `.env.example` and can be overridden in `.env`.

| Service | Default URL |
| --- | --- |
| MinIO API | `http://127.0.0.1:29010` |
| MinIO Console | `http://127.0.0.1:29011` |
| Spark Master UI | `http://127.0.0.1:28091` |
| Spark History Server | `http://127.0.0.1:28090` |
| Lab 7 dashboard | `http://127.0.0.1:28501` |

Default MinIO credentials:

```text
MINIO_ROOT_USER=sparkworkshop
MINIO_ROOT_PASSWORD=sparkworkshop123
```

## Managed by bootstrap

`make bootstrap` prepares local pinned dependencies:

- creates or updates `.env`;
- syncs the Python environment with `uv`;
- pulls pinned base images;
- resolves Delta Lake and S3A Spark JARs;
- downloads the pinned sparkMeasure JAR;
- downloads Python wheels used by Spark jobs;
- downloads Python wheels used by the Lab 7 dashboard;
- validates that the local dependency cache is usable.

Generated data, runtime logs, downloaded artifacts, and Docker image contexts
are local build/runtime artifacts and should not be committed.
