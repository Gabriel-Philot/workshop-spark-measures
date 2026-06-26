"""# Lab 0: sparkMeasure presentation

## Submit command

Assumes the Compose stack is running and the bronze `sales` Delta table exists
at the configured input artifact path.

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/sparkmeasure_presentation.py
```

## Required configuration

This script uses two named experiments from `src/config/experiments.yaml`:

- `lab0-sparkmeasure-presentation-native`
- `lab0-sparkmeasure-presentation-observed`

Both run the same Bronze to Silver refinement workload. The observed experiment
enables sparkMeasure and keeps metric persistence disabled so the History Server
view stays focused on the workload rather than metrics-write jobs.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, functions as F

from spark_workshop.config import load_experiment_config
from spark_workshop.experiments import (
    ExperimentContext,
    ExperimentRun,
    ExperimentRunner,
    SparkExperiment,
)
from spark_workshop.utils import logger, terminal_section


NATIVE_EXPERIMENT_NAME = "lab0-sparkmeasure-presentation-native"
OBSERVED_EXPERIMENT_NAME = "lab0-sparkmeasure-presentation-observed"


class Lab0SparkMeasurePresentation(SparkExperiment):
    """Runs one small Bronze to Silver refinement for sparkMeasure demos."""

    def prepare(self, context: ExperimentContext) -> None:
        if context.config.observability.enabled:
            return

        sales = context.read("sales")
        summary = _build_vendor_sales_summary(sales)
        context.logger.info(
            terminal_section(
                "Native Spark explain output",
                "The plan is useful but verbose before sparkMeasure",
            )
        )
        context.logger.info(
            "LAB0_PRESENTATION_NATIVE_EXPLAIN plan=vendor_sales_summary mode=formatted"
        )
        summary.explain(mode="formatted")

    def workload(self, context: ExperimentContext) -> str:
        sales = context.read("sales")
        summary = _build_vendor_sales_summary(sales)
        context.write("vendor_sales_summary", summary)
        return context.config.artifacts.output("vendor_sales_summary").path

    def validate(self, result: str, context: ExperimentContext) -> None:
        if not result:
            raise RuntimeError("Vendor sales summary output path was not returned")
        context.logger.info(
            f"LAB0_PRESENTATION_VALIDATION_OK experiment={context.config.name} output_path={result}"
        )


def _build_vendor_sales_summary(sales: DataFrame) -> DataFrame:
    return sales.groupBy("vendor_id").agg(
        F.count("*").alias("sales_count"),
        F.round(F.sum("sale_amount"), 2).alias("gross_sales_amount"),
        F.round(F.avg("sale_amount"), 2).alias("avg_sale_amount"),
    )


def _run_experiment(experiment_name: str) -> ExperimentRun:
    config = load_experiment_config(experiment_name)
    return ExperimentRunner(config).run(Lab0SparkMeasurePresentation())


def _log_run_summary(run: ExperimentRun) -> None:
    logger.info(f"LAB0_PRESENTATION_EXPERIMENT={run.experiment_name}")
    logger.info(f"LAB0_PRESENTATION_OUTPUT_PATH={run.workload_result}")
    if not run.metrics:
        logger.info("LAB0_PRESENTATION_SPARKMEASURE_ENABLED=false")
        logger.info("LAB0_PRESENTATION_NATIVE_OK")
        return

    logger.info("LAB0_PRESENTATION_SPARKMEASURE_ENABLED=true")
    logger.info(
        "LAB0_PRESENTATION_SPARKMEASURE_METRICS "
        f"numStages={run.metrics.get('numStages', 0)} "
        f"numTasks={run.metrics.get('numTasks', 0)} "
        f"executorRunTime={run.metrics.get('executorRunTime', 0)} "
        f"shuffleBytesWritten={run.metrics.get('shuffleBytesWritten', 0)}"
    )
    logger.info("LAB0_PRESENTATION_SPARKMEASURE_OK")


def main() -> int:
    logger.info(
        terminal_section(
            "Lab 0 - Native Bronze to Silver refinement",
            "Spark explain and Spark UI before sparkMeasure",
        )
    )
    native_run = _run_experiment(NATIVE_EXPERIMENT_NAME)
    _log_run_summary(native_run)

    logger.info(
        terminal_section(
            "Lab 0 - sparkMeasure presentation",
            "Same refinement with stage metrics collected",
        )
    )
    observed_run = _run_experiment(OBSERVED_EXPERIMENT_NAME)
    _log_run_summary(observed_run)

    logger.info(
        terminal_section(
            "Lab 0 presentation complete",
            "Compare native plan output with compact sparkMeasure metrics",
        )
    )
    logger.info("LAB0_SPARKMEASURE_PRESENTATION_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
