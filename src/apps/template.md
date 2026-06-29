# Spark Measures Workshop App Template

Use this template when creating a Spark workshop script under `src/apps`, especially `src/apps/labs`.

## Script header

Every app script should start with a markdown-style module docstring that includes:

- Short lab or job purpose.
- Manual `spark-submit` command.
- Required named experiment configuration.
- Required input and output artifacts.
- Whether sparkMeasure is enabled or disabled by YAML config.

Example command shape. Assumes the Compose stack is already running:

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/<lab_name>/<script_name>.py
```

## Configuration contract

Register each runnable app as one or more named experiments in `src/config/experiments.yaml`.

Required fields:

- `app_name`: Spark application name visible in Spark UI and History Server.
- `artifacts.inputs`: named inputs consumed with `self.read("<name>")`.
- `artifacts.outputs`: named outputs written with `self.write("<name>", dataframe)`.
- `observability.enabled`: controls whether sparkMeasure wraps the measured contract.
- `observability.collector`: `stage` or `task`; keep this in YAML, not in the lab script.
- `artifacts.outputs.metrics`: required only when `observability.persist=true`.

Keep storage paths in config. App code should reference artifact names, not hard-coded lake paths.

## Single job contract

Use `SparkWorkshopJob` so config loading, logging, Spark session lifecycle, named artifact IO, sparkMeasure collection, metric persistence, and clean shutdown are handled consistently.

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
  /opt/spark/src/apps/labs/lab_0/example_lab.py
```

## Required configuration

This script expects an `example-lab` experiment in `src/config/experiments.yaml`.
"""

from pyspark.sql import DataFrame, functions as F

from spark_workshop.jobs import SparkWorkshopJob


class ExampleLab(SparkWorkshopJob):
    config_name = "example-lab"
    title = "Example lab"
    description = "Readable separator for submit logs and live demos"
    success_marker = "EXAMPLE_LAB_OK"

    def extract(self) -> DataFrame:
        return self.read("source")

    def transform(self, source: DataFrame) -> DataFrame:
        return source.groupBy("key").agg(F.count("*").alias("row_count"))

    def load(self, result: DataFrame) -> str:
        self.write("target", result)
        return self.output_path("target")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Example lab returned no output path")
        self.logger.info("EXAMPLE_LAB_VALIDATION_OK")


def main() -> int:
    return ExampleLab().run()


if __name__ == "__main__":
    raise SystemExit(main())
```

## Native vs sparkMeasure comparison contract

Use `SparkWorkshopComparisonJob` when the same Spark contract should run once natively and once with sparkMeasure enabled by config.

```python
from pyspark.sql import DataFrame, functions as F

from spark_workshop.jobs import SparkWorkshopComparisonJob


class ExampleComparisonLab(SparkWorkshopComparisonJob):
    native_config = "example-native"
    observed_config = "example-observed"

    native_title = "Native Spark run"
    observed_title = "sparkMeasure observed run"
    success_marker = "EXAMPLE_COMPARISON_OK"

    explain_plan = True

    def extract(self) -> DataFrame:
        return self.read("source")

    def transform(self, source: DataFrame) -> DataFrame:
        return source.groupBy("key").agg(F.count("*").alias("row_count"))

    def load(self, result: DataFrame) -> str:
        self.write("target", result)
        return self.output_path("target")


def main() -> int:
    return ExampleComparisonLab().run()
```

## Log namespaces

Keep logs segmented by concern:

- `WORKSHOP_*`: lab narrative, sections, run summaries, output summaries.
- `SPARK_*`: Spark artifact IO and physical plan output.
- `SPARKMEASURE_*`: collector state, aggregate metrics, optional persisted metrics path.
- `LAB*_*` or lab-specific markers: assertions or teaching markers for one lab.

## Rules

- Keep transformation logic in `transform()` whenever possible.
- Use `extract()` only to read configured artifacts or construct input DataFrames.
- Use `load()` for writes and for the Spark action you want sparkMeasure to observe.
- Use `validate_result()` for validation after the measured workload.
- Use `self.read()`, `self.write()`, `self.input_path()`, and `self.output_path()` for named artifacts.
- Use `self.logger` or the shared logger; do not use `print()` for lab output.
- Keep app identity, artifact paths, and `collector: stage | task` in `src/config/experiments.yaml`.
- Avoid broad `collect()`, `show()`, and `toPandas()` in normal workload scripts.
- Keep `.explain()` controlled by `explain_plan=True`; the base contract prints it outside the measured sparkMeasure boundary.
