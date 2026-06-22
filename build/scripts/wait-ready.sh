#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
set -a
source .env.example
source .env
set +a

wait_for() {
  local name="$1"
  shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for $name" >&2
  return 1
}

wait_for "MinIO" curl -fsS "http://127.0.0.1:${MINIO_API_PORT}/minio/health/ready"
wait_for "Spark Master" curl -fsS "http://127.0.0.1:${SPARK_MASTER_UI_PORT}"
wait_for "Spark Worker" bash -c "curl -fsS 'http://127.0.0.1:${SPARK_MASTER_UI_PORT}' | grep -q ALIVE"
wait_for "Spark History" curl -fsS "http://127.0.0.1:${SPARK_HISTORY_UI_PORT}/api/v1/applications"
