#!/bin/sh
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc ready local

for bucket in "$MINIO_LAKEHOUSE_BUCKET" "$MINIO_TESTS_BUCKET" "$MINIO_OBSERVABILITY_BUCKET"; do
  mc mb --ignore-existing "local/$bucket"
done

touch /tmp/.keep
for prefix in landing bronze silver gold; do
  mc cp /tmp/.keep "local/$MINIO_LAKEHOUSE_BUCKET/$prefix/.keep"
done
mc cp /tmp/.keep "local/$MINIO_TESTS_BUCKET/.keep"
mc cp /tmp/.keep "local/$MINIO_OBSERVABILITY_BUCKET/event-logs/.keep"

echo "MinIO workshop buckets are ready"
