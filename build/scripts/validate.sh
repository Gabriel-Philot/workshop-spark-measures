#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

build/scripts/validate-bootstrap.sh

set -a
source .env.example
source .env
set +a

for tool in docker make ss curl uv; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Missing required tool: $tool" >&2
    exit 1
  fi
done

docker compose version >/dev/null
docker compose --env-file .env -f build/docker-compose.yml config --quiet

for image in "$SPARK_RUNTIME_IMAGE" "$SPARK_HISTORY_IMAGE" "$MINIO_IMAGE" "$MINIO_MC_IMAGE"; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Missing required local image: $image" >&2
    echo "Run: make bootstrap && make build" >&2
    exit 1
  fi
done

known_containers=(wsm-minio wsm-spark-master wsm-spark-worker-1 wsm-spark-history)
known_running=false
for container in "${known_containers[@]}"; do
  if [[ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)" == "true" ]]; then
    known_running=true
    break
  fi
done

if [[ "$known_running" == "false" ]]; then
  for port in "$MINIO_API_PORT" "$MINIO_CONSOLE_PORT" "$SPARK_MASTER_UI_PORT" "$SPARK_HISTORY_UI_PORT"; do
    if ss -ltnH | awk '{print $4}' | grep -Eq "(^|:)${port}$"; then
      echo "Port $port is already in use" >&2
      exit 1
    fi
  done
fi

echo "Validation passed"
