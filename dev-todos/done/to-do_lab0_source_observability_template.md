# TODO: add app template and Lab 0 source observability script

Date: 2026-06-24.

## Context

The workshop needs a first lab that introduces the platform execution pattern before adding intentional Spark pathologies. Lab 0 should make the current generated Delta sources visible and compare the same simple workload with and without sparkMeasure.

This task should also bring the reference app template into this repository, adapted to the Spark Measures workshop context.

## Goal

Add a workshop-oriented app template and a first Lab 0 script under `src/apps` that demonstrates:

- named artifact reads through the workshop platform;
- Spark session lifecycle through the existing singleton/session pattern;
- source profiling of the generated bronze retail tables;
- native Spark/log/UI observability first;
- sparkMeasure observability second, using the same workload.

## Proposed structure

```text
src/apps/template.md
src/apps/labs/lab0_source_observability.py
```

Lab 0 should be one script with two execution sections:

1. native mode, without sparkMeasure collection;
2. observed mode, with sparkMeasure collection and metrics persisted to the `observability` bucket.

The same workload should be reused in both modes so the workshop comparison is fair.

## Experiment configuration

Add two named experiments to `src/config/experiments.yaml`:

```text
lab0-source-observability-native
lab0-source-observability-sparkmeasure
```

The native experiment should disable sparkMeasure:

```yaml
observability:
  enabled: false
```

The sparkMeasure experiment should enable stage-level collection and persist metrics to a Lab 0 path, for example:

```text
s3a://observability/spark-measure/lab0/source-observability/latest
```

Both experiments should define named Delta input artifacts for:

- `vendors`
- `products`
- `customers`
- `sales`

Default paths should point to:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/sales
```

## Lab workload

The workload should stay intentionally simple and readable. It should produce source-profile information such as:

- row count per source table;
- data file count per table using Spark metadata such as `input_file_name()`;
- sales skew summary by `vendor_id`;
- a lightweight relationship check using joins between sales and dimensions.

The workload may use small bounded driver results for workshop logging. Keep this explicit as a lab-only exception, not as a production pattern.

## Template requirements

`src/apps/template.md` should be adapted from the reference template in `dataship/spark-plat-v0`, but rewritten for this repository:

- use `SparkExperiment`, `ExperimentRunner`, `load_experiment_config`, and named artifacts;
- show manual `spark-submit` command shape for this Compose stack;
- explain the measured boundary: only `workload()` is measured;
- explain how to disable or enable sparkMeasure by config;
- prefer logger usage for normal lab output.

## Explain snippets

Keep `.explain()` examples commented by default in both:

- `src/apps/template.md`;
- `src/apps/labs/lab0_source_observability.py`.

The snippets should be ready to uncomment during the live demo, but should not print physical plans by default because that would add noise before the sparkMeasure comparison.

## Acceptance criteria

- The TODO implementation adds the template and Lab 0 script only when this backlog item is picked up.
- Lab 0 can be run after `make generate SCALE=demo`.
- The native section completes without sparkMeasure metrics.
- The sparkMeasure section completes and prints the persisted metrics path plus a compact metrics summary.
- The output clearly shows source-profile information for the generated bronze tables.
- No new unit test is required for this task unless the implementation changes shared platform behavior.

## Runtime validation for implementation

When implementing this TODO, validate with:

```bash
make tests
make validate
make generate SCALE=demo GENERATOR_RUN_ID=lab0-demo
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab0_source_observability.py
```

## Notes

- This is a Lab 0 teaching artifact, not a production profiler.
- Avoid adding extra framework abstractions in this task.
- Do not introduce intentional performance problems yet; later labs should own those scenarios.
