#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"

cd "${REPO_ROOT}"

SPARK_PYTHONPATH="/opt/spark/src:/opt/spark/generator/src"
SPARK_APP="/opt/spark/src/apps/labs/lab_7/lab_7_temporal_source_generator.py"
GENERATE_MODE="${LAB7_GENERATE_MODE:-full}"
APPEND_DATE="${LAB7_APPEND_DATE:-}"
APPEND_VOLUME_MULTIPLIER="${LAB7_APPEND_VOLUME_MULTIPLIER:-1}"
REPLACE_SOURCE="${LAB7_REPLACE_SOURCE:-false}"
SOURCE_OUTPUT="${LAB7_TEMPORAL_SOURCE_PATH:-s3a://lakehouse/bronze/lab7/source_events_temporal}"
VOLUME_PLAN_OUTPUT="${LAB7_TEMPORAL_VOLUME_PLAN_PATH:-s3a://observability/lab7/temporal_volume_plan}"
LOG_FILE="$(mktemp)"

cleanup() {
  rm -f "${LOG_FILE}"
}
trap cleanup EXIT

EXPECTED_MARKERS=(
  "LAB7_TEMPORAL_VOLUME_PLAN_OK"
  "LAB7_TEMPORAL_SOURCE_GENERATED_OK"
  "LAB7_TEMPORAL_SOURCE_VALIDATION_OK"
  "LAB7_TEMPORAL_SOURCE_GENERATOR_OK"
)

echo "LAB7_TEMPORAL_SOURCE_GENERATOR_STARTED mode=${GENERATE_MODE} append_date=${APPEND_DATE} replace_lab7_source=${REPLACE_SOURCE}"
echo "LAB7_TEMPORAL_SOURCE_OUTPUT source=${SOURCE_OUTPUT}"
echo "LAB7_TEMPORAL_VOLUME_PLAN_OUTPUT path=${VOLUME_PLAN_OUTPUT}"
echo "LAB7_EXPECTED_MARKERS ${EXPECTED_MARKERS[*]}"

docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH="${SPARK_PYTHONPATH}" \
    LAB7_CONFIG_NAME="${LAB7_CONFIG_NAME:-lab7-temporal-source-generator}" \
    LAB7_TEMPORAL_SOURCE_PATH="${SOURCE_OUTPUT}" \
    LAB7_TEMPORAL_VOLUME_PLAN_PATH="${VOLUME_PLAN_OUTPUT}" \
    LAB7_GENERATE_MODE="${GENERATE_MODE}" \
    LAB7_APPEND_DATE="${APPEND_DATE}" \
    LAB7_APPEND_VOLUME_MULTIPLIER="${APPEND_VOLUME_MULTIPLIER}" \
    LAB7_REPLACE_SOURCE="${REPLACE_SOURCE}" \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH="${SPARK_PYTHONPATH}" \
    "${SPARK_APP}" \
    --mode "${GENERATE_MODE}" \
    --append-date "${APPEND_DATE}" \
    --append-volume-multiplier "${APPEND_VOLUME_MULTIPLIER}" \
    --replace-lab7-source "${REPLACE_SOURCE}" 2>&1 | tee "${LOG_FILE}"

for marker in "${EXPECTED_MARKERS[@]}"; do
  if ! grep -q "${marker}" "${LOG_FILE}"; then
    echo "LAB7_MARKER_MISSING marker=${marker}" >&2
    exit 1
  fi
  echo "LAB7_MARKER_CONFIRMED marker=${marker}"
done

echo "LAB7_TEMPORAL_SOURCE_GENERATOR_COMPLETED source=${SOURCE_OUTPUT} volume_plan=${VOLUME_PLAN_OUTPUT}"
