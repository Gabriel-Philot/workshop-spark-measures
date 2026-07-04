#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
else
  added_header=false
  while IFS= read -r line; do
    [[ -z "$line" || "$line" =~ ^# ]] && continue
    key="${line%%=*}"
    if ! grep -Eq "^${key}=" .env; then
      if [[ "$added_header" == "false" ]]; then
        printf '\n# Added by make bootstrap after .env.example changed.\n' >> .env
        added_header=true
      fi
      printf '%s\n' "$line" >> .env
      echo "Added missing .env key: $key"
    fi
  done < .env.example
fi

set -a
source .env.example
source .env
set +a

for tool in docker curl sha256sum uv; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Missing required tool: $tool" >&2
    exit 1
  fi
done

docker compose version >/dev/null
uv sync

JARS_DIR="$ROOT_DIR/build/config/spark/jars"
MANIFEST="$JARS_DIR/.bootstrap-manifest"
REQ_FILE="$ROOT_DIR/build/images/spark/requirements.txt"
WHEELS_DIR="$ROOT_DIR/build/cache/python-wheels"
WHEELS_MANIFEST="$WHEELS_DIR/.requirements.sha256"
DASHBOARD_REQ_FILE="$ROOT_DIR/build/images/lab7-dashboard/requirements.txt"
DASHBOARD_WHEELS_DIR="$ROOT_DIR/build/cache/lab7-dashboard-wheels"
DASHBOARD_WHEELS_MANIFEST="$DASHBOARD_WHEELS_DIR/.requirements.sha256"
SPEC="spark=${SPARK_VERSION};delta=${DELTA_VERSION};hadoop-aws=${HADOOP_AWS_VERSION};sparkmeasure=${SPARKMEASURE_VERSION}"

mkdir -p "$JARS_DIR" "$WHEELS_DIR" "$DASHBOARD_WHEELS_DIR" build/var/minio-data

echo "Pulling pinned base images..."
docker pull "$SPARK_BASE_IMAGE"
docker pull "$MINIO_BASE_IMAGE"
docker pull "$MINIO_MC_BASE_IMAGE"
docker pull "$LAB7_DASHBOARD_BASE_IMAGE"

bootstrap_current=false
if [[ -f "$MANIFEST" ]] && [[ "$(head -n 1 "$MANIFEST")" == "spec:$SPEC" ]]; then
  bootstrap_current=true
  while IFS= read -r jar_name; do
    [[ "$jar_name" == spec:* || -z "$jar_name" ]] && continue
    if [[ ! -f "$JARS_DIR/$jar_name" ]]; then
      bootstrap_current=false
      break
    fi
  done < "$MANIFEST"
fi

if [[ "$bootstrap_current" == "true" ]]; then
  echo "Spark JAR cache is current"
else
  echo "Resolving Delta Lake and S3A dependencies..."
  rm -rf "$JARS_DIR"
  mkdir -p "$JARS_DIR"
  docker run --rm \
    --user root \
    --entrypoint /bin/bash \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    -v "$JARS_DIR:/resolved-jars" \
    "$SPARK_BASE_IMAGE" \
    -lc '
      set -euo pipefail
      rm -rf /root/.ivy2.5.2
      cat > /tmp/resolve_packages.py <<PYSPARK
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("resolve-workshop-packages").getOrCreate()
spark.stop()
PYSPARK
      /opt/spark/bin/spark-submit \
        --master local[1] \
        --conf spark.ui.enabled=false \
        --packages "io.delta:delta-spark_4.1_2.13:'"$DELTA_VERSION"',org.apache.hadoop:hadoop-aws:'"$HADOOP_AWS_VERSION"'" \
        /tmp/resolve_packages.py
      cp /root/.ivy2.5.2/jars/*.jar /resolved-jars/
      chown -R "$HOST_UID:$HOST_GID" /resolved-jars
    '

  echo "Downloading sparkMeasure ${SPARKMEASURE_VERSION}..."
  sparkmeasure_jar="spark-measure_2.13-${SPARKMEASURE_VERSION}.jar"
  curl -fL --retry 3 \
    "https://github.com/LucaCanali/sparkMeasure/releases/download/v${SPARKMEASURE_VERSION}/${sparkmeasure_jar}" \
    -o "$JARS_DIR/${sparkmeasure_jar}.tmp"
  mv "$JARS_DIR/${sparkmeasure_jar}.tmp" "$JARS_DIR/$sparkmeasure_jar"

  {
    printf 'spec:%s\n' "$SPEC"
    find "$JARS_DIR" -maxdepth 1 -type f -name '*.jar' -printf '%f\n' | sort
  } > "$MANIFEST"
fi

requirements_hash="$(sha256sum "$REQ_FILE" | awk '{print $1}')"
if [[ -f "$WHEELS_MANIFEST" && "$(cat "$WHEELS_MANIFEST")" == "$requirements_hash" ]] \
  && find "$WHEELS_DIR" -maxdepth 1 -type f -name '*.whl' | grep -q .; then
  echo "Python wheel cache is current"
else
  echo "Downloading pinned Python wheels..."
  rm -rf "$WHEELS_DIR"
  mkdir -p "$WHEELS_DIR"
  docker run --rm \
    --user root \
    --entrypoint /bin/bash \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    -v "$REQ_FILE:/requirements.txt:ro" \
    -v "$WHEELS_DIR:/python-wheels" \
    "$SPARK_BASE_IMAGE" \
    -lc 'set -euo pipefail; python3 -m pip download --dest /python-wheels -r /requirements.txt; chown -R "$HOST_UID:$HOST_GID" /python-wheels'
  printf '%s\n' "$requirements_hash" > "$WHEELS_MANIFEST"
fi

dashboard_requirements_hash="$(sha256sum "$DASHBOARD_REQ_FILE" | awk '{print $1}')"
if [[ -f "$DASHBOARD_WHEELS_MANIFEST" && "$(cat "$DASHBOARD_WHEELS_MANIFEST")" == "$dashboard_requirements_hash" ]] \
  && find "$DASHBOARD_WHEELS_DIR" -maxdepth 1 -type f -name '*.whl' | grep -q .; then
  echo "Lab 7 dashboard Python wheel cache is current"
else
  echo "Downloading pinned Lab 7 dashboard Python wheels..."
  rm -rf "$DASHBOARD_WHEELS_DIR"
  mkdir -p "$DASHBOARD_WHEELS_DIR"
  docker run --rm \
    --user root \
    --entrypoint /bin/bash \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    -v "$DASHBOARD_REQ_FILE:/requirements.txt:ro" \
    -v "$DASHBOARD_WHEELS_DIR:/python-wheels" \
    "$LAB7_DASHBOARD_BASE_IMAGE" \
    -lc 'set -euo pipefail; python -m pip download --dest /python-wheels -r /requirements.txt; chown -R "$HOST_UID:$HOST_GID" /python-wheels'
  printf '%s\n' "$dashboard_requirements_hash" > "$DASHBOARD_WHEELS_MANIFEST"
fi

build/scripts/validate-bootstrap.sh
echo "Bootstrap completed"
