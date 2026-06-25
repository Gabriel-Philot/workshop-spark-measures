"""# Lab 0: Source observability baseline

## Submit command

Assumes the Compose stack is running and the bronze retail sources exist at
the configured input artifact paths.

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab0_source_observability.py
```

## Required configuration

This script uses two named experiments from `src/config/experiments.yaml`:

- `lab0-source-observability-native`: runs the workload with sparkMeasure disabled.
- `lab0-source-observability-sparkmeasure`: runs the same workload with sparkMeasure enabled.

Both experiments require these Delta input artifacts:

- `vendors`
- `products`
- `customers`
- `sales`

The sparkMeasure experiment persists stage metrics to:

```text
s3a://observability/spark-measure/lab0/source-observability/latest
```
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
from spark_workshop.utils import logger


NATIVE_EXPERIMENT_NAME = "lab0-source-observability-native"
SPARKMEASURE_EXPERIMENT_NAME = "lab0-source-observability-sparkmeasure"
SOURCE_TABLES = ("vendors", "products", "customers", "sales")


class Lab0SourceObservability(SparkExperiment):
    """Profiles generated bronze sources with a small, repeatable workload."""

    def workload(self, context: ExperimentContext) -> dict[str, Any]:
        tables = {name: context.read(name) for name in SOURCE_TABLES}

        # Uncomment during live demos to show Spark's native physical plan
        # before comparing it with sparkMeasure stage metrics.
        #
        # tables["sales"].groupBy("vendor_id").count().explain(mode="formatted")

        source_profiles = _profile_sources(context, tables)
        sales_skew = _profile_sales_skew(
            context=context,
            sales=tables["sales"],
            total_sales=source_profiles["sales"]["rows"],
        )
        relationship_checks = _check_relationships(context, tables)

        return {
            "source_profiles": source_profiles,
            "sales_skew": sales_skew,
            "relationship_checks": relationship_checks,
        }

    def validate(self, result: dict[str, Any], context: ExperimentContext) -> None:
        empty_tables = [
            table
            for table, profile in result["source_profiles"].items()
            if profile["rows"] <= 0
        ]
        if empty_tables:
            raise RuntimeError(f"Lab 0 found empty source tables: {empty_tables}")

        violations = result["relationship_checks"]
        if any(value != 0 for value in violations.values()):
            raise RuntimeError(f"Lab 0 relationship checks failed: {violations}")

        context.logger.info(f"LAB0_VALIDATION_OK experiment={context.config.name}")


def _profile_sources(
    context: ExperimentContext,
    tables: dict[str, DataFrame],
) -> dict[str, dict[str, int]]:
    profiles: dict[str, dict[str, int]] = {}
    for table_name, dataframe in tables.items():
        row_count = dataframe.count()
        file_count = (
            dataframe.select(F.input_file_name().alias("file_name"))
            .where(F.col("file_name") != "")
            .distinct()
            .count()
        )
        profile = {
            "rows": int(row_count),
            "files": int(file_count),
            "columns": len(dataframe.columns),
            "partitions": int(dataframe.rdd.getNumPartitions()),
        }
        profiles[table_name] = profile
        context.logger.info(
            "LAB0_SOURCE_PROFILE "
            f"experiment={context.config.name} "
            f"table={table_name} "
            f"rows={profile['rows']} "
            f"files={profile['files']} "
            f"columns={profile['columns']} "
            f"partitions={profile['partitions']}"
        )
    return profiles


def _profile_sales_skew(
    context: ExperimentContext,
    sales: DataFrame,
    total_sales: int,
) -> list[dict[str, int | float]]:
    top_vendors = (
        sales.groupBy("vendor_id")
        .agg(
            F.count("*").alias("row_count"),
            F.round(F.sum("sale_amount"), 2).alias("sale_amount"),
        )
        .orderBy(F.desc("row_count"))
        .limit(5)
        .collect()
    )

    summary: list[dict[str, int | float]] = []
    for rank, row in enumerate(top_vendors, start=1):
        share = float(row.row_count / total_sales) if total_sales else 0.0
        item = {
            "rank": rank,
            "vendor_id": int(row.vendor_id),
            "rows": int(row.row_count),
            "share": share,
            "sale_amount": float(row.sale_amount or 0.0),
        }
        summary.append(item)
        context.logger.info(
            "LAB0_SALES_SKEW "
            f"experiment={context.config.name} "
            f"rank={item['rank']} "
            f"vendor_id={item['vendor_id']} "
            f"rows={item['rows']} "
            f"share={item['share']:.4f} "
            f"sale_amount={item['sale_amount']:.2f}"
        )
    return summary


def _check_relationships(
    context: ExperimentContext,
    tables: dict[str, DataFrame],
) -> dict[str, int]:
    sales = tables["sales"]
    vendors = tables["vendors"]
    products = tables["products"]
    customers = tables["customers"]

    checks = {
        "vendor_fk_violations": sales.join(
            vendors.select("vendor_id"),
            "vendor_id",
            "left_anti",
        ).count(),
        "product_fk_violations": sales.join(
            products.select("product_id", "vendor_id"),
            ["product_id", "vendor_id"],
            "left_anti",
        ).count(),
        "customer_fk_violations": sales.join(
            customers.select("customer_id"),
            "customer_id",
            "left_anti",
        ).count(),
    }
    normalized = {name: int(value) for name, value in checks.items()}
    context.logger.info(
        "LAB0_RELATIONSHIP_CHECK "
        f"experiment={context.config.name} "
        f"vendor_fk_violations={normalized['vendor_fk_violations']} "
        f"product_fk_violations={normalized['product_fk_violations']} "
        f"customer_fk_violations={normalized['customer_fk_violations']}"
    )
    return normalized


def _run_experiment(experiment_name: str) -> ExperimentRun:
    config = load_experiment_config(experiment_name)
    return ExperimentRunner(config).run(Lab0SourceObservability())


def _log_run_summary(run: ExperimentRun) -> None:
    logger.info(f"LAB0_EXPERIMENT={run.experiment_name}")
    if not run.metrics:
        logger.info("LAB0_NATIVE_SPARKMEASURE_ENABLED=false")
        logger.info("LAB0_NATIVE_OK")
        return

    logger.info("LAB0_SPARKMEASURE_ENABLED=true")
    logger.info(f"LAB0_SPARKMEASURE_DELTA_PATH={run.metrics_output_path}")
    logger.info(
        "LAB0_SPARKMEASURE_METRICS "
        f"numStages={run.metrics.get('numStages', 0)} "
        f"numTasks={run.metrics.get('numTasks', 0)} "
        f"executorRunTime={run.metrics.get('executorRunTime', 0)} "
        f"shuffleBytesWritten={run.metrics.get('shuffleBytesWritten', 0)}"
    )
    logger.info("LAB0_SPARKMEASURE_OK")


def main() -> int:
    native_run = _run_experiment(NATIVE_EXPERIMENT_NAME)
    _log_run_summary(native_run)

    sparkmeasure_run = _run_experiment(SPARKMEASURE_EXPERIMENT_NAME)
    _log_run_summary(sparkmeasure_run)

    logger.info("LAB0_SOURCE_OBSERVABILITY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
