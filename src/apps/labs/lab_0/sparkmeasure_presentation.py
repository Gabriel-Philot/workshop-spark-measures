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

This script compares two named experiments from `src/config/experiments.yaml`:

- `lab0-sparkmeasure-presentation-native`
- `lab0-sparkmeasure-presentation-observed`

Both run the same Bronze-to-Silver refinement. The observed experiment enables
sparkMeasure through YAML config and keeps metric persistence disabled so the
History Server view stays focused on the workload rather than metrics writes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from spark_workshop.jobs import SparkWorkshopComparisonJob


class Lab0SparkMeasurePresentation(SparkWorkshopComparisonJob):
    """Runs one small Bronze-to-Silver refinement for sparkMeasure demos."""

    native_config = "lab0-sparkmeasure-presentation-native"
    observed_config = "lab0-sparkmeasure-presentation-observed"

    native_title = "Lab 0 - Native Bronze to Silver refinement"
    native_description = "Spark explain and Spark UI before sparkMeasure"
    observed_title = "Lab 0 - sparkMeasure presentation"
    observed_description = "Same refinement with stage metrics collected"
    completion_title = "Lab 0 presentation complete"
    completion_description = "Compare native plan output with compact sparkMeasure metrics"

    explain_plan = True
    explain_plan_title = "Native Spark explain output"
    explain_plan_description = "The plan is useful but verbose before sparkMeasure"

    native_success_marker = "LAB0_PRESENTATION_NATIVE_OK"
    observed_success_marker = "LAB0_PRESENTATION_SPARKMEASURE_OK"
    success_marker = "LAB0_SPARKMEASURE_PRESENTATION_OK"

    def extract(self) -> DataFrame:
        return self.read("sales")

    def transform(self, sales: DataFrame) -> DataFrame:
        return build_vendor_sales_summary(sales)

    def load(self, summary: DataFrame) -> str:
        self.write("vendor_sales_summary", summary)
        return self.output_path("vendor_sales_summary")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Vendor sales summary output path was not returned")
        self.logger.info(
            "LAB0_PRESENTATION_VALIDATION_OK "
            f"experiment={self.context.config.name} output_path={output_path}"
        )


def build_vendor_sales_summary(sales: DataFrame) -> DataFrame:
    return sales.groupBy("vendor_id").agg(
        F.count("*").alias("sales_count"),
        F.round(F.sum("sale_amount"), 2).alias("gross_sales_amount"),
        F.round(F.avg("sale_amount"), 2).alias("avg_sale_amount"),
    )


def main() -> int:
    return Lab0SparkMeasurePresentation().run()


if __name__ == "__main__":
    raise SystemExit(main())
