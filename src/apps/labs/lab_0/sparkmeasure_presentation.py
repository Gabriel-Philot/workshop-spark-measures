"""# Lab 0: sparkMeasure presentation

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
  /opt/spark/src/apps/labs/lab_0/sparkmeasure_presentation.py
```

## Required configuration

This script compares two named experiments from the local `experiments.yaml`:

- `lab0-sparkmeasure-presentation-native`
- `lab0-sparkmeasure-presentation-observed`

Both run the same Bronze-to-Silver enrichment. The observed experiment enables
sparkMeasure through YAML config and keeps metric persistence disabled so the
History Server view stays focused on the workload rather than metrics writes.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from apps.labs.lab_0.transformations import build_sales_enriched
from spark_workshop.jobs import SparkWorkshopComparisonJob


CONFIG_PATH = Path(__file__).with_name("experiments.yaml")


class Lab0SparkMeasurePresentation(SparkWorkshopComparisonJob):
    """Runs one Bronze-to-Silver enrichment for sparkMeasure demos."""

    config_path = CONFIG_PATH
    native_config = "lab0-sparkmeasure-presentation-native"
    observed_config = "lab0-sparkmeasure-presentation-observed"

    native_title = "Lab 0 - Native Bronze to Silver enrichment"
    native_description = "Spark explain and Spark UI before sparkMeasure"
    observed_title = "Lab 0 - sparkMeasure presentation"
    observed_description = "Same enrichment with stage metrics collected"
    completion_title = "Lab 0 presentation complete"
    completion_description = "Compare native plan output with compact sparkMeasure metrics"

    explain_plan = True
    explain_plan_title = "Native Spark explain output"
    explain_plan_description = "The plan is useful but verbose before sparkMeasure"

    native_success_marker = "LAB0_PRESENTATION_NATIVE_OK"
    observed_success_marker = "LAB0_PRESENTATION_SPARKMEASURE_OK"
    success_marker = "LAB0_SPARKMEASURE_PRESENTATION_OK"

    def extract(self) -> dict[str, DataFrame]:
        return {
            "sales": self.read("sales"),
            "vendors": self.read("vendors"),
            "products": self.read("products"),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        return build_sales_enriched(inputs)

    def load(self, sales_enriched: DataFrame) -> str:
        self.write("sales_enriched", sales_enriched)
        return self.output_path("sales_enriched")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Sales enriched output path was not returned")
        self.logger.info(
            "LAB0_PRESENTATION_VALIDATION_OK "
            f"experiment={self.context.config.name} output_path={output_path}"
        )


def main() -> int:
    return Lab0SparkMeasurePresentation().run()


if __name__ == "__main__":
    raise SystemExit(main())
