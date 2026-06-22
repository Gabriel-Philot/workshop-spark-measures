SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

ROOT_DIR := $(CURDIR)
ENV_FILE := $(ROOT_DIR)/.env
COMPOSE_FILE := $(ROOT_DIR)/build/docker-compose.yml
COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

include .env.example
-include .env

.PHONY: bootstrap build validate compose dry-test services test tests down clean-data removeimage

SPARK_SUBMIT := $(COMPOSE) exec -T spark-master env PYTHONPATH=/opt/spark/src /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 \
	--deploy-mode client \
	--conf spark.driver.host=spark-master \
	--conf spark.eventLog.dir=s3a://$(MINIO_OBSERVABILITY_BUCKET)/event-logs \
	--conf spark.executorEnv.PYTHONPATH=/opt/spark/src

bootstrap:
	@build/scripts/bootstrap.sh

build:
	@build/scripts/validate-bootstrap.sh
	@build/scripts/prepare-image-contexts.sh >/dev/null
	@docker build \
		--build-arg MINIO_BASE_IMAGE=$(MINIO_BASE_IMAGE) \
		-f build/images/minio/Dockerfile.server \
		-t $(MINIO_IMAGE) \
		build/images/minio
	@docker build \
		--build-arg MINIO_MC_BASE_IMAGE=$(MINIO_MC_BASE_IMAGE) \
		-f build/images/minio/Dockerfile.client \
		-t $(MINIO_MC_IMAGE) \
		build/images/minio
	@docker build \
		--build-arg SPARK_BASE_IMAGE=$(SPARK_BASE_IMAGE) \
		-f build/images/spark/Dockerfile \
		-t $(SPARK_RUNTIME_IMAGE) \
		build/images/spark
	@docker build \
		--build-arg SPARK_RUNTIME_IMAGE=$(SPARK_RUNTIME_IMAGE) \
		-f build/images/spark-history/Dockerfile \
		-t $(SPARK_HISTORY_IMAGE) \
		build/images/spark-history

validate:
	@build/scripts/validate.sh

compose: validate
	@$(COMPOSE) up -d minio minio-init spark-master spark-worker spark-history
	@build/scripts/wait-ready.sh

dry-test:
	@mkdir -p build/var
	@$(SPARK_SUBMIT) /opt/spark/src/apps/sparkmeasure_dry_test.py 2>&1 | tee build/var/dry-test.log
	@build/scripts/validate-dry-test.sh

services:
	@build/scripts/services.sh

test: tests

tests:
	@uv run pytest

down:
	@$(COMPOSE) down

clean-data:
	@$(COMPOSE) down -v --remove-orphans || true
	@mkdir -p build/var
	@if ! rm -rf build/var/minio-data build/var/dry-test.log 2>/dev/null; then \
		echo "Local data contains root-owned files; cleaning through Docker."; \
		cleanup_image="$(SPARK_RUNTIME_IMAGE)"; \
		if ! docker image inspect "$$cleanup_image" >/dev/null 2>&1; then cleanup_image="$(SPARK_BASE_IMAGE)"; fi; \
		docker run --rm --pull=never --user 0 \
			-v "$(ROOT_DIR)/build/var:/target" \
			--entrypoint /bin/sh \
			"$$cleanup_image" \
			-c 'rm -rf /target/minio-data /target/dry-test.log'; \
	fi
	@mkdir -p build/var/minio-data

removeimage:
	@docker rmi $(SPARK_RUNTIME_IMAGE) $(SPARK_HISTORY_IMAGE) $(MINIO_IMAGE) $(MINIO_MC_IMAGE) || true
