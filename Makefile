SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

ROOT_DIR := $(CURDIR)
ENV_FILE := $(ROOT_DIR)/.env
COMPOSE_FILE := $(ROOT_DIR)/build/docker-compose.yml
COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

include .env.example
-include .env

.PHONY: bootstrap build validate compose compose-three-workers dry-test generate generate-lab7 generate-all lab7-dashboard-build lab7-dashboard services test tests down clean-data removeimage

SPARK_PYTHONPATH := /opt/spark/src:/opt/spark/generator/src
GENERATOR_CONFIG ?= /opt/spark/generator/configs/retail_sales_skew.yaml
SCALE ?= demo
GENERATOR_RUN_ID ?=
GENERATOR_VALIDATE ?= 1
GENERATOR_VALIDATE_ARGS := $(if $(filter 0 false no,$(GENERATOR_VALIDATE)),--skip-validation,)
GENERATOR_RUN_ID_ARGS := $(if $(GENERATOR_RUN_ID),--run-id $(GENERATOR_RUN_ID),)
LAB7_GENERATE_MODE ?= full
LAB7_APPEND_DATE ?=
LAB7_APPEND_VOLUME_MULTIPLIER ?= 1
LAB7_REPLACE_SOURCE ?= false

SPARK_SUBMIT := $(COMPOSE) exec -T spark-master env PYTHONPATH=$(SPARK_PYTHONPATH) /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 \
	--deploy-mode client \
	--conf spark.driver.host=spark-master \
	--conf spark.eventLog.dir=s3a://$(MINIO_OBSERVABILITY_BUCKET)/event-logs \
	--conf spark.executorEnv.PYTHONPATH=$(SPARK_PYTHONPATH)

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
	@docker build \
		--build-arg LAB7_DASHBOARD_BASE_IMAGE=$(LAB7_DASHBOARD_BASE_IMAGE) \
		-f build/images/lab7-dashboard/Dockerfile \
		-t $(LAB7_DASHBOARD_IMAGE) \
		build/images/lab7-dashboard

validate:
	@build/scripts/validate.sh

compose: validate
	@$(COMPOSE) up -d minio minio-init spark-master spark-worker-1 spark-worker-2 spark-history
	@build/scripts/wait-ready.sh

compose-three-workers: validate
	@SPARK_WORKER_MEMORY=$(SPARK_WORKER_THREE_WORKER_MEMORY) $(COMPOSE) --profile three-workers up -d minio minio-init spark-master spark-worker-1 spark-worker-2 spark-worker-3 spark-history
	@SPARK_WORKER_EXPECTED_REPLICAS=3 build/scripts/wait-ready.sh

dry-test:
	@mkdir -p build/var
	@$(SPARK_SUBMIT) /opt/spark/src/apps/sparkmeasure_dry_test.py 2>&1 | tee build/var/dry-test.log
	@build/scripts/validate-dry-test.sh

generate: compose
	@mkdir -p build/var
	@$(SPARK_SUBMIT) /opt/spark/generator/src/workshop_generator/cli.py \
		--config $(GENERATOR_CONFIG) \
		--scale $(SCALE) \
		$(GENERATOR_RUN_ID_ARGS) \
		$(GENERATOR_VALIDATE_ARGS) \
		2>&1 | tee build/var/generate-$(SCALE).log

generate-all: generate
	@LAB7_GENERATE_MODE=$(LAB7_GENERATE_MODE) \
		LAB7_APPEND_DATE=$(LAB7_APPEND_DATE) \
		LAB7_APPEND_VOLUME_MULTIPLIER=$(LAB7_APPEND_VOLUME_MULTIPLIER) \
		LAB7_REPLACE_SOURCE=$(LAB7_REPLACE_SOURCE) \
		bash src/apps/labs/lab_7/run_temporal_source_generator.sh \
		2>&1 | tee build/var/generate-lab7.log

generate-lab7: compose
	@mkdir -p build/var
	@LAB7_GENERATE_MODE=$(LAB7_GENERATE_MODE) \
		LAB7_APPEND_DATE=$(LAB7_APPEND_DATE) \
		LAB7_APPEND_VOLUME_MULTIPLIER=$(LAB7_APPEND_VOLUME_MULTIPLIER) \
		LAB7_REPLACE_SOURCE=$(LAB7_REPLACE_SOURCE) \
		bash src/apps/labs/lab_7/run_temporal_source_generator.sh \
		2>&1 | tee build/var/generate-lab7.log

lab7-dashboard-build:
	@build/scripts/validate-bootstrap.sh
	@build/scripts/prepare-image-contexts.sh >/dev/null
	@docker build \
		--build-arg LAB7_DASHBOARD_BASE_IMAGE=$(LAB7_DASHBOARD_BASE_IMAGE) \
		-f build/images/lab7-dashboard/Dockerfile \
		-t $(LAB7_DASHBOARD_IMAGE) \
		build/images/lab7-dashboard

lab7-dashboard: compose
	@$(MAKE) lab7-dashboard-build
	@$(COMPOSE) --profile lab7-dashboard up -d lab7-dashboard
	@echo "Lab 7 dashboard: http://127.0.0.1:$(LAB7_DASHBOARD_PORT)"

services:
	@build/scripts/services.sh

test: tests

tests:
	@uv run pytest

down:
	@$(COMPOSE) --profile three-workers down --remove-orphans

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
	@docker rmi $(SPARK_RUNTIME_IMAGE) $(SPARK_HISTORY_IMAGE) $(MINIO_IMAGE) $(MINIO_MC_IMAGE) $(LAB7_DASHBOARD_IMAGE) || true
