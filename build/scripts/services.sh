#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
if [[ ! -f .env ]]; then
  echo "Missing .env. Run: make bootstrap" >&2
  exit 1
fi
set -a
source .env.example
source .env
set +a

cat <<INFO
Spark Measure workshop services

MinIO Console
  URL: http://127.0.0.1:${MINIO_CONSOLE_PORT}
  Credentials: see MINIO_ROOT_USER and MINIO_ROOT_PASSWORD in .env
  Buckets: ${MINIO_LAKEHOUSE_BUCKET}, ${MINIO_TESTS_BUCKET}, ${MINIO_OBSERVABILITY_BUCKET}

Spark Master
  URL: http://127.0.0.1:${SPARK_MASTER_UI_PORT}

Spark Workers
  Default topology: ${SPARK_WORKER_REPLICAS:-2} workers x ${SPARK_WORKER_CORES} cores x ${SPARK_WORKER_MEMORY}
  Optional 3-worker topology: make compose-three-workers

Spark History
  URL: http://127.0.0.1:${SPARK_HISTORY_UI_PORT}

Dry-test Delta table
  s3a://${MINIO_OBSERVABILITY_BUCKET}/spark-measure/stage/latest

Useful commands
  make dry-test    Run and validate the instrumented workload.
  make down        Stop services without deleting data.
  make clean-data  Remove all local MinIO data.
INFO
