#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo "Missing .env. Run: make bootstrap" >&2
  exit 1
fi

set -a
source .env.example
source .env
set +a

for image in "$SPARK_BASE_IMAGE" "$MINIO_BASE_IMAGE" "$MINIO_MC_BASE_IMAGE" "$LAB7_DASHBOARD_BASE_IMAGE"; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Missing bootstrapped base image: $image" >&2
    exit 1
  fi
done

JARS_DIR="$ROOT_DIR/build/config/spark/jars"
MANIFEST="$JARS_DIR/.bootstrap-manifest"
EXPECTED_SPEC="spec:spark=${SPARK_VERSION};delta=${DELTA_VERSION};hadoop-aws=${HADOOP_AWS_VERSION};sparkmeasure=${SPARKMEASURE_VERSION}"
if [[ ! -f "$MANIFEST" || "$(head -n 1 "$MANIFEST")" != "$EXPECTED_SPEC" ]]; then
  echo "Missing or stale JAR manifest. Run: make bootstrap" >&2
  exit 1
fi

while IFS= read -r jar_name; do
  [[ "$jar_name" == spec:* || -z "$jar_name" ]] && continue
  if [[ ! -f "$JARS_DIR/$jar_name" ]]; then
    echo "Missing bootstrapped JAR: $jar_name" >&2
    exit 1
  fi
done < "$MANIFEST"

required_patterns=(
  "io.delta_delta-spark_4.1_2.13-${DELTA_VERSION}.jar"
  "io.delta_delta-storage-${DELTA_VERSION}.jar"
  "org.apache.hadoop_hadoop-aws-${HADOOP_AWS_VERSION}.jar"
  "spark-measure_2.13-${SPARKMEASURE_VERSION}.jar"
  "software.amazon.awssdk_bundle-*.jar"
)
for pattern in "${required_patterns[@]}"; do
  if ! compgen -G "$JARS_DIR/$pattern" >/dev/null; then
    echo "Missing expected JAR matching: $pattern" >&2
    exit 1
  fi
done

REQ_FILE="$ROOT_DIR/build/images/spark/requirements.txt"
WHEELS_DIR="$ROOT_DIR/build/cache/python-wheels"
WHEELS_MANIFEST="$WHEELS_DIR/.requirements.sha256"
requirements_hash="$(sha256sum "$REQ_FILE" | awk '{print $1}')"
if [[ ! -f "$WHEELS_MANIFEST" || "$(cat "$WHEELS_MANIFEST")" != "$requirements_hash" ]]; then
  echo "Missing or stale Python wheel cache. Run: make bootstrap" >&2
  exit 1
fi
if ! find "$WHEELS_DIR" -maxdepth 1 -type f -name '*.whl' | grep -q .; then
  echo "Python wheel cache is empty" >&2
  exit 1
fi

DASHBOARD_REQ_FILE="$ROOT_DIR/build/images/lab7-dashboard/requirements.txt"
DASHBOARD_WHEELS_DIR="$ROOT_DIR/build/cache/lab7-dashboard-wheels"
DASHBOARD_WHEELS_MANIFEST="$DASHBOARD_WHEELS_DIR/.requirements.sha256"
dashboard_requirements_hash="$(sha256sum "$DASHBOARD_REQ_FILE" | awk '{print $1}')"
if [[ ! -f "$DASHBOARD_WHEELS_MANIFEST" || "$(cat "$DASHBOARD_WHEELS_MANIFEST")" != "$dashboard_requirements_hash" ]]; then
  echo "Missing or stale Lab 7 dashboard Python wheel cache. Run: make bootstrap" >&2
  exit 1
fi
if ! find "$DASHBOARD_WHEELS_DIR" -maxdepth 1 -type f -name '*.whl' | grep -q .; then
  echo "Lab 7 dashboard Python wheel cache is empty" >&2
  exit 1
fi
