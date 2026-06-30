"""# Lab 1: global sort diagnosis

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
  /opt/spark/src/apps/labs/lab_1/global_sort_diagnosis.py
```

## Required configuration

This script reads comparison metadata from `lab_1_utils/experiments.yaml`:

- `lab1-global-sort-diagnosis-native`
- `lab1-global-sort-diagnosis-observed-stage`

Both run the same enriched sales ranking workload. The observed experiment
enables sparkMeasure stage metrics through YAML config and keeps metric
path persistence disabled so the History Server stays focused on the workload.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from apps.labs.lab_1.lab_1_utils.transformations import (
    build_sales_enriched,
    build_top_sales_global_sort,
)
from spark_workshop.jobs import SparkWorkshopComparisonJob
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_1_utils" / "experiments.yaml"


class Lab1GlobalSortDiagnosis(SparkWorkshopComparisonJob):
    """Diagnoses the stage-level cost of a global sales ranking workload."""

    config_path = CONFIG_PATH
    job_name = "lab1-global-sort-diagnosis"

    def extract(self) -> dict[str, DataFrame]:
        return {
            "sales": self.read("sales"),
            "vendors": self.read("vendors"),
            "products": self.read("products"),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        return build_sales_enriched(inputs).transform(build_top_sales_global_sort)

    def load(self, top_sales: DataFrame) -> str:
        with spark_job_description(
            self.context.spark,
            f"LAB1 | global_sort | mode={self._run_mode} | write_top_sales",
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


def main() -> int:
    return Lab1GlobalSortDiagnosis().run()


if __name__ == "__main__":
    raise SystemExit(main())
