#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

cd "${REPO_ROOT}"

SOURCE_RUNNER="src/apps/labs/lab_7/lab_7_utils/runners/run_temporal_source_generator.sh"
BACKFILL_RUNNER="src/apps/labs/lab_7/lab_7_utils/runners/run_daily_backfill_stage_metrics.sh"

SKIP_COMPOSE="${LAB7_SKIP_COMPOSE:-false}"
SKIP_SOURCE_GENERATION="${LAB7_SKIP_SOURCE_GENERATION:-false}"

echo "LAB7_TEMPORAL_BACKFILL_OBSERVABILITY_STARTED"
echo "LAB7_WORKFLOW_SOURCE_RUNNER=${SOURCE_RUNNER}"
echo "LAB7_WORKFLOW_BACKFILL_RUNNER=${BACKFILL_RUNNER}"
echo "LAB7_WORKFLOW_PROCESSING_DATES=${LAB7_PROCESSING_DATES:-full_volume_plan}"

if [[ "${SKIP_COMPOSE}" != "true" ]]; then
  echo "LAB7_WORKFLOW_COMPOSE_STARTED"
  make compose
  echo "LAB7_WORKFLOW_COMPOSE_OK"
else
  echo "LAB7_WORKFLOW_COMPOSE_SKIPPED"
fi

if [[ "${SKIP_SOURCE_GENERATION}" != "true" ]]; then
  echo "LAB7_WORKFLOW_SOURCE_ENSURE_STARTED"
  bash "${SOURCE_RUNNER}"
  echo "LAB7_WORKFLOW_SOURCE_READY"
else
  echo "LAB7_WORKFLOW_SOURCE_ENSURE_SKIPPED"
fi

echo "LAB7_WORKFLOW_BACKFILL_STARTED"
bash "${BACKFILL_RUNNER}"
echo "LAB7_WORKFLOW_BACKFILL_READY"

cat <<'EOF'
LAB7_TEMPORAL_BACKFILL_OBSERVABILITY_COMPLETED

Next classroom step:
  make lab7-dashboard

Open:
  http://127.0.0.1:28501

Metrics:
  s3a://observability/lab7/daily_backfill_stage_metrics
EOF
