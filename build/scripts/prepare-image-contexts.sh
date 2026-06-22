#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPARK_CONTEXT="$ROOT_DIR/build/images/spark/context"

rm -rf "$SPARK_CONTEXT"
mkdir -p "$SPARK_CONTEXT/jars" "$SPARK_CONTEXT/python-wheels" "$SPARK_CONTEXT/conf"
cp "$ROOT_DIR/build/config/spark/jars"/*.jar "$SPARK_CONTEXT/jars/"
cp "$ROOT_DIR/build/cache/python-wheels"/* "$SPARK_CONTEXT/python-wheels/"
cp "$ROOT_DIR/build/config/spark/spark-defaults.conf" "$SPARK_CONTEXT/conf/"
cp "$ROOT_DIR/build/config/spark/log4j2.properties" "$SPARK_CONTEXT/conf/"

find "$SPARK_CONTEXT" -type f | sort
