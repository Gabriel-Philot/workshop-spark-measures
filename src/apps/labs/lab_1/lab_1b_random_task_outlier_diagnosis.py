"""# Lab 1B: random task outlier diagnosis

Change only `CONFIG_NAME` below to switch between stage metrics, task metrics,
and the fixed variant. The selected YAML config controls the sparkMeasure
collector and the workload variant.

## Submit command

Assumes the Compose stack is running and the bronze `sales`, `vendors`, and
`products` Delta tables exist at the configured input artifact paths.

```bash
cd src/apps/labs/lab_1

docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_1/lab_1b_random_task_outlier_diagnosis.py
```
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_1.lab_1_utils.random_task_outlier_runtime import (
    SparkMeasureDiagnosticJob,
)
from apps.labs.lab_1.lab_1_utils.transformations import (
    build_random_task_outlier_fixed,
    build_random_task_outlier_problem,
    build_sales_enriched,
)
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_1_utils" / "experiments.yaml"

# Classroom control point: change this single value before the submit.
CONFIG_NAME = "lab1-random-task-outlier-stage"

# Useful alternatives for the live demo:
# CONFIG_NAME = "lab1-random-task-outlier-stage"
# CONFIG_NAME = "lab1-random-task-outlier-task"
# CONFIG_NAME = "lab1-random-task-outlier-fixed-task"


class Lab1RandomTaskOutlierDiagnosis(SparkMeasureDiagnosticJob):
    """Diagnoses a random task outlier with stage or task sparkMeasure metrics."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 1B - random task outlier diagnosis"
    description = "A technical audit bucket creates a task straggler."

    def extract(self) -> dict[str, DataFrame]:
        return {
            "sales": self.read("sales"),
            "vendors": self.read("vendors"),
            "products": self.read("products"),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        sales_enriched = build_sales_enriched(inputs)
        if self.workload_settings.variant == "fixed":
            return sales_enriched.transform(build_random_task_outlier_fixed)

        # LIVE FIX OPTION:
        # During the workshop, replace the problematic transform below with the
        # fixed transform to spread the expensive audit bucket across more tasks:
        # return sales_enriched.transform(build_random_task_outlier_fixed)
        return sales_enriched.transform(build_random_task_outlier_problem)

    def load(self, audit_output: DataFrame) -> str:
        with spark_job_description(
            self.context.spark,
            "LAB1 | random_task_outlier | "
            f"collector={self.collector_name} | "
            f"variant={self.workload_settings.variant} | write_audit_output",
        ):
            self.write("audit_outlier", audit_output)
        return self.output_path("audit_outlier")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Random task outlier output path was not returned")
        self.logger.info(
            "LAB1_RANDOM_TASK_OUTLIER_VALIDATION_OK "
            f"experiment={self.context.config.name} "
            f"collector={self.collector_name} "
            f"variant={self.workload_settings.variant} "
            f"output_path={output_path}"
        )


def main() -> int:
    return Lab1RandomTaskOutlierDiagnosis().run()


if __name__ == "__main__":
    raise SystemExit(main())
