#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
set -a
source .env.example
source .env
set +a

LOG_FILE="build/var/dry-test.log"
for marker in SPARKMEASURE_DRY_TEST_OK SPARKMEASURE_DELTA_PATH SPARKMEASURE_METRICS SPARK_SESSION_SINGLETON_OK WORKLOAD_VALIDATION_OK numStages numTasks executorRunTime; do
  if ! grep -q "$marker" "$LOG_FILE"; then
    echo "Dry-test output is missing marker: $marker" >&2
    exit 1
  fi
done

COMPOSE=(docker compose --env-file .env -f build/docker-compose.yml)
minio_ls() {
  local target="$1"
  "${COMPOSE[@]}" run --rm --no-deps --entrypoint /bin/sh minio-init -lc \
    "mc alias set local http://minio:9000 '$MINIO_ROOT_USER' '$MINIO_ROOT_PASSWORD' >/dev/null && mc ls --recursive \"$target\""
}

bucket_listing="$(minio_ls local)"
for bucket in "$MINIO_LAKEHOUSE_BUCKET" "$MINIO_TESTS_BUCKET" "$MINIO_OBSERVABILITY_BUCKET"; do
  if [[ "$bucket_listing" != *"$bucket/"* ]]; then
    echo "Expected MinIO bucket was not found: $bucket" >&2
    exit 1
  fi
done

lakehouse_listing="$(minio_ls "local/$MINIO_LAKEHOUSE_BUCKET")"
for prefix in landing bronze silver gold; do
  if [[ "$lakehouse_listing" != *"$prefix/.keep"* ]]; then
    echo "Expected lakehouse prefix was not found: $prefix" >&2
    exit 1
  fi
done

delta_listing="$(minio_ls "local/$MINIO_OBSERVABILITY_BUCKET/spark-measure/stage/latest")"
if [[ "$delta_listing" != *"_delta_log/"* || "$delta_listing" != *".parquet"* ]]; then
  echo "A valid Delta metrics table was not found in MinIO" >&2
  echo "$delta_listing" >&2
  exit 1
fi

event_listing="$(minio_ls "local/$MINIO_OBSERVABILITY_BUCKET/event-logs")"
if [[ "$event_listing" != *"eventlog_v2_"* ]]; then
  echo "Spark event log was not found in MinIO" >&2
  echo "$event_listing" >&2
  exit 1
fi

for _ in $(seq 1 60); do
  apps_json="$(curl -fsS "http://127.0.0.1:${SPARK_HISTORY_UI_PORT}/api/v1/applications")"
  if [[ "$apps_json" == *"workshop-spark-measures-dry-test"* ]]; then
    echo "Dry-test validation passed"
    exit 0
  fi
  sleep 2
done

echo "Dry-test application was not found in Spark History" >&2
exit 1
