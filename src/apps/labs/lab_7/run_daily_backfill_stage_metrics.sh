#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

cd "${REPO_ROOT}"

SPARK_PYTHONPATH="/opt/spark/src:/opt/spark/generator/src"
SPARK_APP="/opt/spark/src/apps/labs/lab_7/lab_7_daily_backfill_stage_metrics.py"
VOLUME_PLAN_FILE="src/apps/labs/lab_7/lab_7_utils/volume_plan.yaml"
FILTER_STRATEGY="${LAB7_FILTER_STRATEGY:-early_partition_filter}"
BACKFILL_RUN_ID="${LAB7_BACKFILL_RUN_ID:-lab7-backfill-$(date -u +%Y%m%dT%H%M%SZ)}"
SOURCE_PATH="${LAB7_TEMPORAL_SOURCE_PATH:-s3a://lakehouse/bronze/lab7/source_events_temporal}"
DASHBOARD_BASE="${LAB7_DAILY_DASHBOARD_BASE_PATH:-s3a://lakehouse/gold/lab7/daily_activity_dashboard}"
METRICS_OUTPUT="${LAB7_DAILY_BACKFILL_STAGE_METRICS_PATH:-s3a://observability/lab7/daily_backfill_stage_metrics}"
LOG_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "${LOG_DIR}"
}
trap cleanup EXIT

EXPECTED_MARKERS=(
  "LAB7_DAILY_BACKFILL_CONFIG_OK"
  "LAB7_DAILY_BACKFILL_RUN_OK"
  "LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK"
  "LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK"
  "LAB7_DAILY_BACKFILL_STAGE_METRICS_OK"
)

load_processing_dates() {
  if [[ -n "${LAB7_PROCESSING_DATES:-}" ]]; then
    tr ',' '\n' <<< "${LAB7_PROCESSING_DATES}" | sed '/^[[:space:]]*$/d'
    return
  fi

  python3 - <<'PY'
from datetime import date, datetime, timedelta
from pathlib import Path
import yaml

path = Path("src/apps/labs/lab_7/lab_7_utils/volume_plan.yaml")
data = yaml.safe_load(path.read_text()) or {}
start = data["date_range"]["start"]
end = data["date_range"]["end"]
if isinstance(start, date) and not isinstance(start, datetime):
    start_date = start
else:
    start_date = datetime.strptime(str(start), "%Y-%m-%d").date()
if isinstance(end, date) and not isinstance(end, datetime):
    end_date = end
else:
    end_date = datetime.strptime(str(end), "%Y-%m-%d").date()
current = start_date
while current <= end_date:
    print(current.isoformat())
    current += timedelta(days=1)
PY
}

mapfile -t PROCESSING_DATES < <(load_processing_dates)

if [[ "${#PROCESSING_DATES[@]}" -lt 1 ]]; then
  echo "LAB7_DAILY_BACKFILL_NO_DATES" >&2
  exit 1
fi

echo "LAB7_DAILY_BACKFILL_BATCH_STARTED run_id=${BACKFILL_RUN_ID} dates=${#PROCESSING_DATES[@]} filter_strategy=${FILTER_STRATEGY}"
echo "LAB7_DAILY_BACKFILL_SOURCE source=${SOURCE_PATH}"
echo "LAB7_DAILY_BACKFILL_OUTPUTS dashboard_base=${DASHBOARD_BASE} metrics=${METRICS_OUTPUT}"
echo "LAB7_EXPECTED_MARKERS ${EXPECTED_MARKERS[*]}"

for processing_date in "${PROCESSING_DATES[@]}"; do
  output_path="${DASHBOARD_BASE}/processing_date=${processing_date}/filter_strategy=${FILTER_STRATEGY}"
  log_file="${LOG_DIR}/lab7-backfill-${processing_date}.log"

  echo "LAB7_DAILY_BACKFILL_SUBMIT_STARTED processing_date=${processing_date} output=${output_path}"

  docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
    env PYTHONPATH="${SPARK_PYTHONPATH}" \
      LAB7_CONFIG_NAME="${LAB7_CONFIG_NAME:-lab7-daily-backfill-stage-metrics}" \
      LAB7_TEMPORAL_SOURCE_PATH="${SOURCE_PATH}" \
      LAB7_DAILY_DASHBOARD_OUTPUT_PATH="${output_path}" \
      LAB7_DAILY_BACKFILL_STAGE_METRICS_PATH="${METRICS_OUTPUT}" \
      LAB7_PROCESSING_DATE="${processing_date}" \
      LAB7_FILTER_STRATEGY="${FILTER_STRATEGY}" \
      LAB7_BACKFILL_RUN_ID="${BACKFILL_RUN_ID}" \
      /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-master \
      --conf spark.eventLog.dir=s3a://observability/event-logs \
      --conf spark.executorEnv.PYTHONPATH="${SPARK_PYTHONPATH}" \
      "${SPARK_APP}" \
      --processing-date "${processing_date}" \
      --filter-strategy "${FILTER_STRATEGY}" \
      --batch-run-id "${BACKFILL_RUN_ID}" 2>&1 | tee "${log_file}"

  for marker in "${EXPECTED_MARKERS[@]}"; do
    if ! grep -q "${marker}" "${log_file}"; then
      echo "LAB7_MARKER_MISSING processing_date=${processing_date} marker=${marker}" >&2
      exit 1
    fi
    echo "LAB7_MARKER_CONFIRMED processing_date=${processing_date} marker=${marker}"
  done

  echo "LAB7_DAILY_BACKFILL_SUBMIT_COMPLETED processing_date=${processing_date} output=${output_path}"
done

echo "LAB7_DAILY_BACKFILL_BATCH_COMPLETED run_id=${BACKFILL_RUN_ID} dates=${#PROCESSING_DATES[@]} metrics=${METRICS_OUTPUT}"
