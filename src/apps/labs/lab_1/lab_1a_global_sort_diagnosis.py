"""# Lab 1: global sort diagnosis

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
  /opt/spark/src/apps/labs/lab_1/lab_1a_global_sort_diagnosis.py
```

## Required configuration

Change only `CONFIG_NAME` below to switch between the native Spark view and
the sparkMeasure stage view. The selected YAML config controls whether
sparkMeasure is enabled.

Use the same submit command after changing `CONFIG_NAME`.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from apps.labs.lab_1.lab_1_utils.transformations import (
    build_sales_enriched,
    build_top_sales_global_sort,
)
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_1_utils" / "experiments.yaml"

# Classroom control point: change this single value before the submit.
CONFIG_NAME = "lab1-global-sort-diagnosis-native"

# Useful alternatives for the live demo:
# CONFIG_NAME = "lab1-global-sort-diagnosis-native"
# CONFIG_NAME = "lab1-global-sort-diagnosis-observed-stage"

SUCCESS_MARKERS = {
    "lab1-global-sort-diagnosis-native": "LAB1_GLOBAL_SORT_NATIVE_OK",
    "lab1-global-sort-diagnosis-observed-stage": "LAB1_GLOBAL_SORT_SPARKMEASURE_STAGE_OK",
}


class Lab1GlobalSortDiagnosis(SparkWorkshopJob):
    """Diagnoses the stage-level cost of a global sales ranking workload."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 1A - global sort diagnosis"
    description = "Use Spark UI first, then stage metrics, to diagnose global sort cost."
    explain_plan = True
    explain_plan_modes = (None,)
    explain_plan_title = "Native Spark explain output"
    explain_plan_description = "The plan exposes the global orderBy, but the signal is verbose"
    explain_plan_mode = "formatted"

    def extract(self) -> dict[str, DataFrame]:
        return {
            "sales": self.read("sales"),
            "vendors": self.read("vendors"),
            "products": self.read("products"),
        }

    def _should_explain_plan(self) -> bool:
        return self.context.config.name == "lab1-global-sort-diagnosis-native"

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        return build_sales_enriched(inputs).transform(build_top_sales_global_sort)

    def load(self, top_sales: DataFrame) -> str:
        mode = (
            "observed-stage"
            if self.context.config.name == "lab1-global-sort-diagnosis-observed-stage"
            else "native"
        )
        with spark_job_description(
            self.context.spark,
            f"LAB1 | global_sort | mode={mode} | write_top_sales",
        ):
            self.write("top_sales_global_sort", top_sales)
        return self.output_path("top_sales_global_sort")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Top sales global sort output path was not returned")
        self.logger.info(
            "LAB1_GLOBAL_SORT_VALIDATION_OK "
            f"experiment={self.context.config.name} output_path={output_path}"
        )
        if marker := SUCCESS_MARKERS.get(self.context.config.name):
            self.logger.info(marker)
        if self.context.config.name == "lab1-global-sort-diagnosis-observed-stage":
            self.logger.info("LAB1_GLOBAL_SORT_DIAGNOSIS_OK")


def main() -> int:
    return Lab1GlobalSortDiagnosis().run()


if __name__ == "__main__":
    raise SystemExit(main())
