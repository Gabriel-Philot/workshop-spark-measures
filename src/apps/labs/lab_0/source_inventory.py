"""# Lab 0: Source inventory

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
  /opt/spark/src/apps/labs/lab_0/source_inventory.py
```

## Required configuration

This script uses `lab0-source-inventory` from `src/config/experiments.yaml`.
It reads `vendors`, `products`, `customers`, and `sales` as Delta inputs.

This lab intentionally keeps sparkMeasure disabled. Its purpose is to show
source row counts, physical file counts, physical byte size, relationship
readiness, and a final note about the generated vendor imbalance.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, functions as F

from spark_workshop.artifacts import data_file_stats_for_dataframe
from spark_workshop.jobs import SparkWorkshopJob


class Lab0SourceInventory(SparkWorkshopJob):
    """Profiles generated bronze sources before diagnostic labs."""

    config_name = "lab0-source-inventory"
    title = "Lab 0 - Source inventory"
    description = "Rows, physical bytes, file layout, FK readiness, and imbalance note"
    success_marker = "LAB0_SOURCE_INVENTORY_OK"
    source_tables = ("vendors", "products", "customers", "sales")

    def extract(self) -> dict[str, DataFrame]:
        return {name: self.read(name) for name in self.source_tables}

    def transform(self, tables: dict[str, DataFrame]) -> dict[str, Any]:
        source_profiles = _profile_sources(self.logger, tables)
        relationship_checks = _check_relationships(self.logger, tables)
        imbalance_note = _profile_sales_vendor_imbalance(
            logger=self.logger,
            sales=tables["sales"],
            total_sales=source_profiles["sales"]["rows"],
        )
        return {
            "source_profiles": source_profiles,
            "relationship_checks": relationship_checks,
            "imbalance_note": imbalance_note,
        }

    def load(self, inventory: dict[str, Any]) -> dict[str, Any]:
        return inventory

    def validate_result(self, inventory: dict[str, Any]) -> None:
        empty_tables = [
            table
            for table, profile in inventory["source_profiles"].items()
            if profile["rows"] <= 0
        ]
        if empty_tables:
            raise RuntimeError(f"Lab 0 found empty source tables: {empty_tables}")

        violations = inventory["relationship_checks"]
        if any(value != 0 for value in violations.values()):
            raise RuntimeError(f"Lab 0 relationship checks failed: {violations}")

        self.logger.info(
            f"LAB0_SOURCE_INVENTORY_VALIDATION_OK experiment={self.context.config.name}"
        )


def _profile_sources(
    logger: Any,
    tables: dict[str, DataFrame],
) -> dict[str, dict[str, int | float]]:
    profiles: dict[str, dict[str, int | float]] = {}
    for table_name, dataframe in tables.items():
        row_count = int(dataframe.count())
        file_stats = data_file_stats_for_dataframe(dataframe)
        profile = {
            "rows": row_count,
            "files": file_stats.file_count,
            "total_bytes": file_stats.total_bytes,
            "min_file_bytes": file_stats.min_file_bytes,
            "avg_file_bytes": file_stats.avg_file_bytes,
            "max_file_bytes": file_stats.max_file_bytes,
            "columns": len(dataframe.columns),
        }
        profiles[table_name] = profile
        logger.info(
            "LAB0_SOURCE_VOLUME "
            f"table={table_name} "
            f"rows={profile['rows']} "
            f"files={profile['files']} "
            f"total_bytes={profile['total_bytes']} "
            f"min_file_bytes={profile['min_file_bytes']} "
            f"avg_file_bytes={profile['avg_file_bytes']:.1f} "
            f"max_file_bytes={profile['max_file_bytes']} "
            f"columns={profile['columns']}"
        )
    return profiles


def _check_relationships(
    logger: Any,
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
    logger.info(
        "LAB0_RELATIONSHIP_CHECK "
        f"vendor_fk_violations={normalized['vendor_fk_violations']} "
        f"product_fk_violations={normalized['product_fk_violations']} "
        f"customer_fk_violations={normalized['customer_fk_violations']}"
    )
    return normalized


def _profile_sales_vendor_imbalance(
    logger: Any,
    sales: DataFrame,
    total_sales: int | float,
) -> dict[str, int | float]:
    top_vendor = (
        sales.groupBy("vendor_id")
        .agg(F.count("*").alias("row_count"))
        .orderBy(F.desc("row_count"))
        .limit(1)
        .collect()
    )
    if not top_vendor:
        note = {"top_vendor_id": 0, "top_vendor_rows": 0, "top_vendor_share": 0.0}
    else:
        row = top_vendor[0]
        top_vendor_rows = int(row.row_count)
        note = {
            "top_vendor_id": int(row.vendor_id),
            "top_vendor_rows": top_vendor_rows,
            "top_vendor_share": (
                float(top_vendor_rows / total_sales) if total_sales else 0.0
            ),
        }

    logger.info(
        "LAB0_SOURCE_CHARACTERISTIC "
        "table=sales characteristic=vendor_imbalance "
        f"top_vendor_id={note['top_vendor_id']} "
        f"top_vendor_rows={note['top_vendor_rows']} "
        f"top_vendor_share={note['top_vendor_share']:.4f}"
    )
    return note


def main() -> int:
    return Lab0SourceInventory().run()


if __name__ == "__main__":
    raise SystemExit(main())
