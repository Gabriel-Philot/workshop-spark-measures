#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

cd "${REPO_ROOT}"

SPARK_PYTHONPATH="/opt/spark/src:/opt/spark/generator/src"
SPARK_APP="/opt/spark/src/apps/labs/lab_4/lab_4_stage_workload_fingerprint.py"
FINGERPRINT_OUTPUT="${LAB4_FINGERPRINT_OUTPUT_PATH:-s3a://observability/lab4/workload_fingerprints}"
LOG_FILE="$(mktemp)"

cleanup() {
  rm -f "${LOG_FILE}"
}
trap cleanup EXIT

EXPECTED_MARKERS=(
  "LAB4_STAGE_METRICS_CAPTURED_OK"
  "LAB4_WORKLOAD_FINGERPRINT_RULES_OK"
  "LAB4_WORKLOAD_PROFILE_ASSIGNED_OK"
  "LAB4_WORKLOAD_FINGERPRINT_WRITTEN_OK"
)

echo "LAB4_FINGERPRINT_STARTED output=${FINGERPRINT_OUTPUT}"
echo "LAB4_EXPECTED_MARKERS ${EXPECTED_MARKERS[*]}"

docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH="${SPARK_PYTHONPATH}" \
    LAB4_CONFIG_NAME="${LAB4_CONFIG_NAME:-lab4-stage-workload-fingerprint}" \
    LAB4_FINGERPRINT_OUTPUT_PATH="${FINGERPRINT_OUTPUT}" \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH="${SPARK_PYTHONPATH}" \
    "${SPARK_APP}" 2>&1 | tee "${LOG_FILE}"

for marker in "${EXPECTED_MARKERS[@]}"; do
  if ! grep -q "${marker}" "${LOG_FILE}"; then
    echo "LAB4_MARKER_MISSING marker=${marker}" >&2
    exit 1
  fi
  echo "LAB4_MARKER_CONFIRMED marker=${marker}"
done

echo "LAB4_FINGERPRINT_COMPLETED output=${FINGERPRINT_OUTPUT}"
