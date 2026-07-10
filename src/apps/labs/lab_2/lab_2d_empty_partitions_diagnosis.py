"""# Lab 2D: empty partitions diagnosis

The selected YAML config controls the sparkMeasure TaskMetrics collector and
the workload partitioning knobs.

## Submit command

Run from `src/apps/labs/lab_2`. Assumes the Compose stack is running and the
bronze `sales` Delta table exists at the configured input artifact path.

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2d_empty_partitions_diagnosis.py
```
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_2.lab_2_utils.empty_partitions_runtime import (
    Lab2EmptyPartitionsDiagnosticJob,
)
from apps.labs.lab_2.lab_2_utils.transformations import (
    build_empty_partitions_sales_summary,
)
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_2_utils" / "experiments.yaml"

# Classroom control point: this lab is intentionally task-level only.
CONFIG_NAME = "lab2d-empty-partitions-task"


class Lab2DEmptyPartitionsDiagnosis(Lab2EmptyPartitionsDiagnosticJob):
    """Diagnoses near-empty task partitions from sparkMeasure TaskMetrics."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 2D - empty partitions diagnosis"
    description = "Read task-level Summary Metrics to identify low-end outliers."

    def extract(self) -> dict[str, DataFrame]:
        return {
            "sales": self.read("sales"),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        return build_empty_partitions_sales_summary(
            inputs,
            shuffle_partitions=self.workload_settings.shuffle_partitions,
            active_buckets=self.workload_settings.active_buckets,
        )

    def load(self, partition_summary: DataFrame) -> str:
        with spark_job_description(
            self.context.spark,
            "LAB2D | empty_partitions | "
            f"collector={self.collector_name} | "
            f"variant={self.workload_settings.variant} | "
            "write_partition_bucket_summary",
        ):
            self.write("empty_partitions_summary", partition_summary)
        return self.output_path("empty_partitions_summary")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Empty partitions output path was not returned")
        self.logger.info(
            "LAB2D_EMPTY_PARTITIONS_VALIDATION_OK "
            f"experiment={self.context.config.name} "
            f"collector={self.collector_name} "
            f"variant={self.workload_settings.variant} "
            f"output_path={output_path}"
        )


def main() -> int:
    return Lab2DEmptyPartitionsDiagnosis().run()


if __name__ == "__main__":
    raise SystemExit(main())
