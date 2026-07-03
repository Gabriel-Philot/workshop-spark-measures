#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

cd "${REPO_ROOT}"

read -r -a MODES <<< "${LAB3_MODES:-none stage task}"

BENCHMARK_ID="${LAB3_BENCHMARK_ID:-lab3-overhead-$(date -u +%Y%m%d-%H%M%S)}"
REPETITIONS="${LAB3_REPETITIONS:-10}"
WARMUP_REPETITIONS="${LAB3_WARMUP_REPETITIONS:-1}"
EMIT_REPORT="${LAB3_EMIT_SPARKMEASURE_REPORT:-false}"
BENCHMARK_STARTED_MS="$(date +%s%3N)"

SPARK_PYTHONPATH="/opt/spark/src:/opt/spark/generator/src"
SPARK_APP="/opt/spark/src/apps/labs/lab_3/lab_3_observability_overhead_benchmark.py"

if [[ "${#MODES[@]}" -eq 0 ]]; then
  echo "LAB3_MODES must contain at least one mode: none, stage, or task" >&2
  exit 1
fi

if ! [[ "${REPETITIONS}" =~ ^[0-9]+$ ]]; then
  echo "LAB3_REPETITIONS must be a non-negative integer" >&2
  exit 1
fi

if ! [[ "${WARMUP_REPETITIONS}" =~ ^[0-9]+$ ]]; then
  echo "LAB3_WARMUP_REPETITIONS must be a non-negative integer" >&2
  exit 1
fi

echo "LAB3_BENCHMARK_STARTED benchmark_id=${BENCHMARK_ID} modes=${MODES[*]} repetitions=${REPETITIONS} warmups=${WARMUP_REPETITIONS}"

run_round() {
  local iteration="$1"
  local is_warmup="$2"
  local mode_count="${#MODES[@]}"
  local offset=$(( (iteration - 1) % mode_count ))
  local index

  for ((index = 0; index < mode_count; index++)); do
    run_one "${MODES[$(((index + offset) % mode_count))]}" "${iteration}" "${is_warmup}"
  done
}

run_one() {
  local mode="$1"
  local iteration="$2"
  local is_warmup="$3"
  local phase="measured"
  local config_name="lab3-overhead-${mode}"
  local run_id
  local output_suffix
  local started_ms
  local ended_ms

  if [[ "${is_warmup}" == "true" ]]; then
    phase="warmup"
  fi

  run_id="${BENCHMARK_ID}-${mode}-${phase}-${iteration}-$(date -u +%H%M%S)-${RANDOM}"
  output_suffix="benchmark_id=${BENCHMARK_ID}/mode=${mode}/iteration=${iteration}/run_id=${run_id}"

  echo "LAB3_SUBMIT_STARTED benchmark_id=${BENCHMARK_ID} run_id=${run_id} mode=${mode} iteration=${iteration} is_warmup=${is_warmup}"
  started_ms="$(date +%s%3N)"

  docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
    env PYTHONPATH="${SPARK_PYTHONPATH}" \
      LAB3_BENCHMARK_ID="${BENCHMARK_ID}" \
      LAB3_MODE="${mode}" \
      LAB3_ITERATION="${iteration}" \
      LAB3_IS_WARMUP="${is_warmup}" \
      LAB3_CONFIG_NAME="${config_name}" \
      LAB3_RUN_ID="${run_id}" \
      LAB3_OUTPUT_SUFFIX="${output_suffix}" \
      LAB3_EMIT_SPARKMEASURE_REPORT="${EMIT_REPORT}" \
      /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-master \
      --conf spark.eventLog.dir=s3a://observability/event-logs \
      --conf spark.executorEnv.PYTHONPATH="${SPARK_PYTHONPATH}" \
      "${SPARK_APP}"

  ended_ms="$(date +%s%3N)"
  echo "LAB3_SUBMIT_COMPLETED benchmark_id=${BENCHMARK_ID} run_id=${run_id} mode=${mode} iteration=${iteration} is_warmup=${is_warmup} spark_submit_wall_ms=$((ended_ms - started_ms))"
}

for ((iteration = 1; iteration <= WARMUP_REPETITIONS; iteration++)); do
  run_round "${iteration}" "true"
done

for ((iteration = 1; iteration <= REPETITIONS; iteration++)); do
  run_round "${iteration}" "false"
done

BENCHMARK_ENDED_MS="$(date +%s%3N)"
BENCHMARK_TOTAL_MS=$((BENCHMARK_ENDED_MS - BENCHMARK_STARTED_MS))
echo "LAB3_BENCHMARK_COMPLETED benchmark_id=${BENCHMARK_ID} total_wall_ms=${BENCHMARK_TOTAL_MS} total_wall_seconds=$((BENCHMARK_TOTAL_MS / 1000))"
