#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

cd "${REPO_ROOT}"

SPARK_PYTHONPATH="/opt/spark/src:/opt/spark/generator/src"
SPARK_APP="/opt/spark/src/apps/labs/lab_5/lab_5_stage_runtime_budget_guardrail.py"
RUNS_OUTPUT="${LAB5_STAGE_RUNTIME_BUDGET_RUNS_PATH:-s3a://observability/lab5/stage_runtime_budget_runs}"
DECISIONS_OUTPUT="${LAB5_STAGE_RUNTIME_BUDGET_DECISIONS_PATH:-s3a://observability/lab5/stage_runtime_budget_decisions}"
BASELINE_OUTPUT="${LAB5_BASELINE_OUTPUT_PATH:-s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline}"
CANDIDATE_OUTPUT="${LAB5_CANDIDATE_OUTPUT_PATH:-s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate}"
LOG_FILE="$(mktemp)"

cleanup() {
  rm -f "${LOG_FILE}"
}
trap cleanup EXIT

EXPECTED_MARKERS=(
  "LAB5_BASELINE_STAGE_METRICS_OK"
  "LAB5_CANDIDATE_STAGE_METRICS_OK"
  "LAB5_OUTPUT_COMPATIBILITY_OK"
  "LAB5_BUDGET_RULES_LOADED_OK"
  "LAB5_RUNTIME_BUDGET_DECISION_WRITTEN_OK"
)

FINAL_DECISION_MARKERS=(
  "LAB5_RUNTIME_BUDGET_PASS"
  "LAB5_RUNTIME_BUDGET_FAIL"
  "LAB5_RUNTIME_BUDGET_WARNING_LOW_SIGNAL"
)

echo "LAB5_GUARDRAIL_STARTED runs_output=${RUNS_OUTPUT} decisions_output=${DECISIONS_OUTPUT}"
echo "LAB5_BUSINESS_OUTPUTS baseline=${BASELINE_OUTPUT} candidate=${CANDIDATE_OUTPUT}"
echo "LAB5_EXPECTED_MARKERS ${EXPECTED_MARKERS[*]}"

docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH="${SPARK_PYTHONPATH}" \
    LAB5_CONFIG_NAME="${LAB5_CONFIG_NAME:-lab5-stage-runtime-budget-guardrail}" \
    LAB5_STAGE_RUNTIME_BUDGET_RUNS_PATH="${RUNS_OUTPUT}" \
    LAB5_STAGE_RUNTIME_BUDGET_DECISIONS_PATH="${DECISIONS_OUTPUT}" \
    LAB5_BASELINE_OUTPUT_PATH="${BASELINE_OUTPUT}" \
    LAB5_CANDIDATE_OUTPUT_PATH="${CANDIDATE_OUTPUT}" \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH="${SPARK_PYTHONPATH}" \
    "${SPARK_APP}" 2>&1 | tee "${LOG_FILE}"

for marker in "${EXPECTED_MARKERS[@]}"; do
  if ! grep -q "${marker}" "${LOG_FILE}"; then
    echo "LAB5_MARKER_MISSING marker=${marker}" >&2
    exit 1
  fi
  echo "LAB5_MARKER_CONFIRMED marker=${marker}"
done

decision_marker_count=0
for marker in "${FINAL_DECISION_MARKERS[@]}"; do
  if grep -q "${marker}" "${LOG_FILE}"; then
    decision_marker_count=$((decision_marker_count + 1))
    echo "LAB5_FINAL_DECISION_CONFIRMED marker=${marker}"
  fi
done

if [[ "${decision_marker_count}" -ne 1 ]]; then
  echo "LAB5_FINAL_DECISION_MARKER_COUNT_INVALID count=${decision_marker_count}" >&2
  exit 1
fi

echo "LAB5_GUARDRAIL_COMPLETED runs_output=${RUNS_OUTPUT} decisions_output=${DECISIONS_OUTPUT}"
