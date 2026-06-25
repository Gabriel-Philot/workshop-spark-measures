# Spark Measures Workshop App Template

Use this template when creating a new Spark workshop script under `src/apps`, especially `src/apps/labs`.

## Script header

Every app script should start with a markdown-style module docstring that includes:

- Short lab or job purpose.
- Manual `spark-submit` command.
- Required named experiment configuration.
- Required input and output artifacts.
- Execution sequence.
- Whether sparkMeasure is enabled or disabled by config.

Example command shape. Assumes the Compose stack is already running:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/<script_name>.py
```

## Configuration contract

Register each runnable app as one or more named experiments in `src/config/experiments.yaml`.

Required fields:

- `app_name`: Spark application name visible in Spark UI and History Server.
- `artifacts.inputs`: named inputs consumed with `context.read("<name>")`.
- `observability.enabled`: controls whether sparkMeasure wraps `workload()`.
- `artifacts.outputs.metrics`: required when `observability.persist=true`.

Keep storage paths in config. App code should reference artifact names, not hard-coded lake paths.

## Job contract

Use `SparkExperiment` so config loading, logging, Spark session lifecycle, named artifact IO, sparkMeasure collection, metric persistence, and clean shutdown are handled consistently.

```python
"""# Example lab

## Submit command

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/example_lab.py
```

## Required configuration

This script expects an `example-lab` experiment in `src/config/experiments.yaml`
with a `source` input artifact and, when sparkMeasure persistence is enabled, a
`metrics` output artifact.
"""

from typing import Any

from spark_workshop.config import load_experiment_config
from spark_workshop.experiments import (
    ExperimentContext,
    ExperimentRunner,
    SparkExperiment,
)
from spark_workshop.utils import logger


EXPERIMENT_NAME = "example-lab"


class ExampleLab(SparkExperiment):
    def prepare(self, context: ExperimentContext) -> None:
        # Optional setup that should not be measured by sparkMeasure.
        pass

    def workload(self, context: ExperimentContext) -> Any:
        source = context.read("source")
        result = source.groupBy("key").count()

        # Uncomment during live demos when you want to compare Spark's native
        # physical plan output with sparkMeasure metrics.
        #
        # result.explain(mode="formatted")

        # Small bounded driver results are acceptable for lab summaries.
        return result.limit(10).collect()

    def validate(self, result: Any, context: ExperimentContext) -> None:
        if not result:
            raise RuntimeError("Example lab returned no rows")
        context.logger.info("EXAMPLE_LAB_VALIDATION_OK")

    def cleanup(self, context: ExperimentContext) -> None:
        # Optional cleanup that should not be measured by sparkMeasure.
        pass


def main() -> int:
    config = load_experiment_config(EXPERIMENT_NAME)
    run = ExperimentRunner(config).run(ExampleLab())

    logger.info(f"EXPERIMENT={run.experiment_name}")
    if run.metrics:
        logger.info(f"SPARKMEASURE_DELTA_PATH={run.metrics_output_path}")
        logger.info(
            "SPARKMEASURE_METRICS "
            f"numStages={run.metrics.get('numStages', 0)} "
            f"numTasks={run.metrics.get('numTasks', 0)} "
            f"executorRunTime={run.metrics.get('executorRunTime', 0)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

## Rules

- Keep the measured Spark actions inside `workload()`; setup, validation, metric persistence, and cleanup stay outside the measured boundary.
- Use `context.read()` and `context.write()` for named artifacts.
- Use `context.logger` or the shared `logger`; do not use `print()` for lab output.
- Keep app identity and artifact paths in `src/config/experiments.yaml`.
- Keep `.explain()` snippets commented by default to avoid noisy output before the sparkMeasure comparison.
- Avoid broad `collect()`, `show()`, and `toPandas()` patterns. Use small bounded driver results only when they are intentional lab output.
