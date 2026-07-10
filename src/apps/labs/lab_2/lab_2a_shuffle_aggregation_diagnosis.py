"""# Lab 2A: shuffle aggregation diagnosis

Change only `CONFIG_NAME` below to switch between the baseline and optimized
variants. The selected YAML config controls the sparkMeasure collector and the
workload partitioning knobs.

## Submit command

Run from `src/apps/labs/lab_2`. Assumes the Compose stack is running and the
bronze `sales` and `vendors` Delta tables exist at the configured input artifact
paths.

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2a_shuffle_aggregation_diagnosis.py
```
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_2.lab_2_utils.shuffle_aggregation_runtime import (
    ShuffleAggregationSettings,
    load_shuffle_aggregation_settings,
)
from apps.labs.lab_2.lab_2_utils.transformations import (
    build_regional_monthly_sales_baseline,
    build_regional_monthly_sales_optimized,
)
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_2_utils" / "experiments.yaml"

# Classroom control point: change this single value before the submit.
CONFIG_NAME = "lab2-shuffle-aggregation-baseline"

# Useful alternative for the live demo:
# CONFIG_NAME = "lab2-shuffle-aggregation-optimized"
# CONFIG_NAME = "lab2-shuffle-aggregation-baseline"



class Lab2ShuffleAggregationDiagnosis(SparkWorkshopJob):
    """Diagnoses shuffle-heavy regional sales aggregation with StageMetrics."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 2A - shuffle aggregation diagnosis"
    description = "Use stage metrics to identify shuffle cost in a grouped sales job."

    def __init__(self) -> None:
        super().__init__()
        self.workload_settings = ShuffleAggregationSettings()

    def before_extract(self) -> None:
        self.workload_settings = load_shuffle_aggregation_settings(
            self.context.config.name,
            CONFIG_PATH,
        )
        self.logger.info(
            "LAB2_SHUFFLE_AGGREGATION_CONFIG "
            f"config_name={self.context.config.name} "
            f"collector={self.context.config.observability.collector} "
            f"variant={self.workload_settings.variant} "
            f"round_robin_partitions={self.workload_settings.round_robin_partitions} "
            f"keyed_partitions={self.workload_settings.keyed_partitions}"
        )

    def extract(self) -> dict[str, DataFrame]:
        return {
            "sales": self.read("sales"),
            "vendors": self.read("vendors"),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        if self.workload_settings.variant == "optimized":
            return build_regional_monthly_sales_optimized(
                inputs,
                keyed_partitions=self.workload_settings.keyed_partitions,
            )

        # LIVE FIX OPTION:
        # During the workshop, replace the baseline transform below with the
        # optimized transform to narrow data and partition by the grouping keys:
        # return build_regional_monthly_sales_optimized(
        #     inputs,
        #     keyed_partitions=self.workload_settings.keyed_partitions,
        # )
        return build_regional_monthly_sales_baseline(
            inputs,
            round_robin_partitions=self.workload_settings.round_robin_partitions,
        )

    def load(self, regional_sales: DataFrame) -> str:
        with spark_job_description(
            self.context.spark,
            "LAB2 | shuffle_aggregation | "
            f"variant={self.workload_settings.variant} | write_regional_monthly_sales",
        ):
            self.write("regional_monthly_sales", regional_sales)
        return self.output_path("regional_monthly_sales")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Regional monthly sales output path was not returned")
        self.logger.info(
            "LAB2_SHUFFLE_AGGREGATION_VALIDATION_OK "
            f"experiment={self.context.config.name} "
            f"collector={self.context.config.observability.collector} "
            f"variant={self.workload_settings.variant} "
            f"output_path={output_path}"
        )
        self.logger.info(self.workload_settings.success_marker)


def main() -> int:
    return Lab2ShuffleAggregationDiagnosis().run()


if __name__ == "__main__":
    raise SystemExit(main())
