"""# Lab 2B: stage metrics interpretation drill

Change only `CONFIG_NAME` below to switch between the safer default variant and
the pressure variant. The selected YAML config controls the sparkMeasure stage
collector and the workload partitioning knobs.

## Submit command

Assumes the Compose stack is running and the bronze `sales`, `vendors`, and
`products` Delta tables exist at the configured input artifact paths.

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2b_stage_metrics_interpretation_drill.py
```
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_2.lab_2_utils.stage_metrics_runtime import (
    StageMetricsDrillSettings,
    load_stage_metrics_drill_settings,
)
from apps.labs.lab_2.lab_2_utils.transformations import (
    build_stage_metrics_drill_default,
    build_stage_metrics_drill_pressure,
)
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_2_utils" / "experiments.yaml"

# Classroom control point: change this single value before the submit.
CONFIG_NAME = "lab2-stage-metrics-drill-pressure"

# Useful alternative for the live demo:
# CONFIG_NAME = "lab2-stage-metrics-drill-default"


class Lab2StageMetricsInterpretationDrill(SparkWorkshopJob):
    """Reads stage-level shuffle, spill, and GC signals with sparkMeasure."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 2B - stage metrics interpretation drill"
    description = "Read shuffle, spill, GC, and executor runtime from StageMetrics."

    def __init__(self) -> None:
        super().__init__()
        self.workload_settings = StageMetricsDrillSettings()

    def before_extract(self) -> None:
        self.workload_settings = load_stage_metrics_drill_settings(
            self.context.config.name,
            CONFIG_PATH,
        )
        self.logger.info(
            "LAB2_STAGE_METRICS_DRILL_CONFIG "
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
            "products": self.read("products"),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        if self.workload_settings.variant == "pressure":
            return build_stage_metrics_drill_pressure(
                inputs,
                round_robin_partitions=self.workload_settings.round_robin_partitions,
            )

        return build_stage_metrics_drill_default(
            inputs,
            keyed_partitions=self.workload_settings.keyed_partitions,
        )

    def load(self, summary: DataFrame) -> str:
        with spark_job_description(
            self.context.spark,
            "LAB2 | stage_metrics_drill | "
            f"variant={self.workload_settings.variant} | write_stage_metric_summary",
        ):
            self.write("stage_metrics_summary", summary)
        return self.output_path("stage_metrics_summary")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Stage metrics drill output path was not returned")
        self.logger.info(
            "LAB2_STAGE_METRICS_DRILL_VALIDATION_OK "
            f"experiment={self.context.config.name} "
            f"collector={self.context.config.observability.collector} "
            f"variant={self.workload_settings.variant} "
            f"output_path={output_path}"
        )
        self.logger.info(self.workload_settings.success_marker)


def main() -> int:
    return Lab2StageMetricsInterpretationDrill().run()


if __name__ == "__main__":
    raise SystemExit(main())
