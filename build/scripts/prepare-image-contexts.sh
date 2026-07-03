#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPARK_CONTEXT="$ROOT_DIR/build/images/spark/context"
LAB7_DASHBOARD_CONTEXT="$ROOT_DIR/build/images/lab7-dashboard/context"

rm -rf "$SPARK_CONTEXT"
mkdir -p "$SPARK_CONTEXT/jars" "$SPARK_CONTEXT/python-wheels" "$SPARK_CONTEXT/conf"
cp "$ROOT_DIR/build/config/spark/jars"/*.jar "$SPARK_CONTEXT/jars/"
cp "$ROOT_DIR/build/cache/python-wheels"/* "$SPARK_CONTEXT/python-wheels/"
cp "$ROOT_DIR/build/config/spark/spark-defaults.conf" "$SPARK_CONTEXT/conf/"
cp "$ROOT_DIR/build/config/spark/log4j2.properties" "$SPARK_CONTEXT/conf/"

rm -rf "$LAB7_DASHBOARD_CONTEXT"
mkdir -p "$LAB7_DASHBOARD_CONTEXT/python-wheels" "$LAB7_DASHBOARD_CONTEXT/dashboard"
cp "$ROOT_DIR/build/cache/lab7-dashboard-wheels"/* "$LAB7_DASHBOARD_CONTEXT/python-wheels/"
cp "$ROOT_DIR/src/apps/labs/lab_7/dashboard"/*.py "$LAB7_DASHBOARD_CONTEXT/dashboard/"

find "$SPARK_CONTEXT" -type f | sort
find "$LAB7_DASHBOARD_CONTEXT" -type f | sort
