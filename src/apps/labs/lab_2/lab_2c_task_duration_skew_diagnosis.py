"""# Lab 2C: task duration skew diagnosis

The selected YAML config controls the sparkMeasure TaskMetrics collector and
the workload partitioning knobs.

## Submit command

Assumes the Compose stack is running and the bronze `sales` and `vendors` Delta
tables exist at the configured input artifact paths.

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_2/lab_2c_task_duration_skew_diagnosis.py
```
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_2.lab_2_utils.task_duration_skew_runtime import (
    Lab2TaskSkewDiagnosticJob,
)
from apps.labs.lab_2.lab_2_utils.transformations import (
    build_task_skew_vendor_summary,
)
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_2_utils" / "experiments.yaml"

# Classroom control point: change this single value before the submit.
CONFIG_NAME = "lab2c-task-skew-task"


class Lab2CTaskDurationSkewDiagnosis(Lab2TaskSkewDiagnosticJob):
    """Diagnoses task duration skew from sparkMeasure TaskMetrics."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 2C - task duration skew diagnosis"
    description = "Read task-level Summary Metrics to identify skewed stragglers."

    def extract(self) -> dict[str, DataFrame]:
        return {
            "sales": self.read("sales"),
            "vendors": self.read("vendors"),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        # DISCUSSION FIX OPTION:
        # In a production skew remediation, this is where a salted or two-step
        # aggregation strategy would be introduced. This lab keeps the skewed
        # path so students can focus on reading the task distribution.
        return build_task_skew_vendor_summary(
            inputs,
            shuffle_partitions=self.workload_settings.shuffle_partitions,
        )

    def load(self, vendor_sales: DataFrame) -> str:
        with spark_job_description(
            self.context.spark,
            "LAB2C | task_skew | "
            f"collector={self.collector_name} | "
            f"variant={self.workload_settings.variant} | write_vendor_sales_summary",
        ):
            self.write("vendor_sales_skew_summary", vendor_sales)
        return self.output_path("vendor_sales_skew_summary")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Task skew output path was not returned")
        self.logger.info(
            "LAB2C_TASK_SKEW_VALIDATION_OK "
            f"experiment={self.context.config.name} "
            f"collector={self.collector_name} "
            f"variant={self.workload_settings.variant} "
            f"output_path={output_path}"
        )


def main() -> int:
    return Lab2CTaskDurationSkewDiagnosis().run()


if __name__ == "__main__":
    raise SystemExit(main())
