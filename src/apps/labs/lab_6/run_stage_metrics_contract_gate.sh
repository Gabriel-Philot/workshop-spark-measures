#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

cd "${REPO_ROOT}"

SPARK_PYTHONPATH="/opt/spark/src:/opt/spark/generator/src"
SPARK_APP="/opt/spark/src/apps/labs/lab_6/lab_6_stage_metrics_contract_gate.py"
INJECT_INVALID_RECORDS="${LAB6_INJECT_INVALID_RECORDS:-false}"
BUSINESS_OUTPUT="${LAB6_BUSINESS_OUTPUT_PATH:-s3a://lakehouse/gold/lab6/stage_metrics_contract_gate/business_output}"
RAW_OUTPUT="${LAB6_STAGE_METRICS_RAW_PATH:-s3a://observability/lab6/stage_metrics_raw}"
DEMO_INPUT="${LAB6_STAGE_METRICS_CONTRACT_DEMO_INPUT_PATH:-s3a://observability/lab6/stage_metrics_contract_demo_input}"
RESULTS_OUTPUT="${LAB6_STAGE_METRICS_CONTRACT_RESULTS_PATH:-s3a://observability/lab6/stage_metrics_contract_results}"
SUMMARY_OUTPUT="${LAB6_STAGE_METRICS_CONTRACT_SUMMARY_PATH:-s3a://observability/lab6/stage_metrics_contract_summary}"
LOG_FILE="$(mktemp)"

cleanup() {
  rm -f "${LOG_FILE}"
}
trap cleanup EXIT

EXPECTED_MARKERS=(
  "LAB6_STAGE_METRICS_CAPTURED_OK"
  "LAB6_STAGE_METRICS_INPUT_OK"
  "LAB6_CONTRACT_RULES_LOADED_OK"
  "LAB6_SCHEMA_CONTRACT_EVALUATED"
  "LAB6_SEMANTIC_CONTRACT_EVALUATED"
  "LAB6_CORRELATION_CONTRACT_EVALUATED"
  "LAB6_CONTRACT_RESULTS_WRITTEN_OK"
)

FINAL_DECISION_MARKERS=(
  "LAB6_STAGE_METRICS_CONTRACT_PASS"
  "LAB6_STAGE_METRICS_CONTRACT_FAIL"
)

echo "LAB6_CONTRACT_GATE_STARTED inject_invalid_records=${INJECT_INVALID_RECORDS}"
echo "LAB6_BUSINESS_OUTPUT output=${BUSINESS_OUTPUT}"
echo "LAB6_CONTRACT_OUTPUTS raw=${RAW_OUTPUT} demo_input=${DEMO_INPUT} results=${RESULTS_OUTPUT} summary=${SUMMARY_OUTPUT}"
echo "LAB6_EXPECTED_MARKERS ${EXPECTED_MARKERS[*]}"

docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH="${SPARK_PYTHONPATH}" \
    LAB6_CONFIG_NAME="${LAB6_CONFIG_NAME:-lab6-stage-metrics-contract-gate}" \
    LAB6_BUSINESS_OUTPUT_PATH="${BUSINESS_OUTPUT}" \
    LAB6_STAGE_METRICS_RAW_PATH="${RAW_OUTPUT}" \
    LAB6_STAGE_METRICS_CONTRACT_DEMO_INPUT_PATH="${DEMO_INPUT}" \
    LAB6_STAGE_METRICS_CONTRACT_RESULTS_PATH="${RESULTS_OUTPUT}" \
    LAB6_STAGE_METRICS_CONTRACT_SUMMARY_PATH="${SUMMARY_OUTPUT}" \
    LAB6_INJECT_INVALID_RECORDS="${INJECT_INVALID_RECORDS}" \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH="${SPARK_PYTHONPATH}" \
    "${SPARK_APP}" \
    --inject-invalid-records "${INJECT_INVALID_RECORDS}" 2>&1 | tee "${LOG_FILE}"

for marker in "${EXPECTED_MARKERS[@]}"; do
  if ! grep -q "${marker}" "${LOG_FILE}"; then
    echo "LAB6_MARKER_MISSING marker=${marker}" >&2
    exit 1
  fi
  echo "LAB6_MARKER_CONFIRMED marker=${marker}"
done

decision_marker_count=0
for marker in "${FINAL_DECISION_MARKERS[@]}"; do
  if grep -q "${marker}" "${LOG_FILE}"; then
    decision_marker_count=$((decision_marker_count + 1))
    echo "LAB6_FINAL_DECISION_CONFIRMED marker=${marker}"
  fi
done

if [[ "${decision_marker_count}" -ne 1 ]]; then
  echo "LAB6_FINAL_DECISION_MARKER_COUNT_INVALID count=${decision_marker_count}" >&2
  exit 1
fi

echo "LAB6_CONTRACT_GATE_COMPLETED raw=${RAW_OUTPUT} results=${RESULTS_OUTPUT} summary=${SUMMARY_OUTPUT}"
