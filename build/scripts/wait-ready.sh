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
expected_workers="${SPARK_WORKER_EXPECTED_REPLICAS:-${SPARK_WORKER_REPLICAS:-2}}"
export SPARK_WORKER_EXPECTED_REPLICAS="${expected_workers}"
wait_for "Spark Workers (${expected_workers})" bash -c '
  page="$(curl -fsS "http://127.0.0.1:${SPARK_MASTER_UI_PORT}")"
  alive="$(sed -nE "s/.*<strong>Workers:<\/strong>[[:space:]]*([0-9]+)[[:space:]]+Alive,.*/\1/p" <<< "$page" | head -n 1)"
  test "${alive:-0}" -ge "${SPARK_WORKER_EXPECTED_REPLICAS}"
'
wait_for "Spark History" curl -fsS "http://127.0.0.1:${SPARK_HISTORY_UI_PORT}/api/v1/applications"
