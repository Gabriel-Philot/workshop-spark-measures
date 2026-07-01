"""Lab 2 transformations kept local for workshop readability."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


# -----------------------------------------------------------------------------
# Lab 2A: shuffle-heavy regional aggregation with stage-level sparkMeasure
# -----------------------------------------------------------------------------

REGIONAL_MONTHLY_SALES_COLUMNS = (
    "vendor_region",
    "sale_year_month",
    "sale_count",
    "total_quantity",
    "gross_sales_amount",
    "average_sale_amount",
)


def build_regional_monthly_sales_baseline(
    inputs: dict[str, "DataFrame"],
    *,
    round_robin_partitions: int,
) -> "DataFrame":
    """Build the intentionally inefficient shuffle aggregation baseline."""

    return (
        inputs["sales"]
        .alias("s")
        .transform(_join_vendor_region, inputs["vendors"].alias("v"))
        .repartition(round_robin_partitions)
        .transform(_select_regional_sales_fact)
        .transform(_aggregate_regional_monthly_sales)
    )


def build_regional_monthly_sales_optimized(
    inputs: dict[str, "DataFrame"],
    *,
    keyed_partitions: int,
) -> "DataFrame":
    """Build the optimized variant using a narrow projection and keyed shuffle."""

    return (
        inputs["sales"]
        .transform(_select_sales_columns_for_aggregation)
        .alias("s")
        .transform(_join_vendor_region, inputs["vendors"].alias("v"))
        .transform(_select_regional_sales_fact)
        .repartition(keyed_partitions, "vendor_region", "sale_year_month")
        .transform(_aggregate_regional_monthly_sales)
    )


def _select_sales_columns_for_aggregation(sales: "DataFrame") -> "DataFrame":
    return sales.select("sale_date", "vendor_id", "quantity", "sale_amount")


def _join_vendor_region(sales: "DataFrame", vendors: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    vendor_regions = vendors.select(
        F.col("v.vendor_id").alias("vendor_id"),
        F.col("v.region").alias("region"),
    ).alias("v")

    return sales.join(
        vendor_regions,
        F.col("s.vendor_id") == F.col("v.vendor_id"),
        "left",
    )


def _select_regional_sales_fact(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.select(
        F.coalesce(F.col("v.region"), F.lit("UNKNOWN")).alias("vendor_region"),
        F.date_format(F.col("s.sale_date"), "yyyy-MM").alias("sale_year_month"),
        F.col("s.quantity").cast("long").alias("quantity"),
        F.col("s.sale_amount").cast("double").alias("sale_amount"),
    )


def _aggregate_regional_monthly_sales(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        sales.groupBy("vendor_region", "sale_year_month")
        .agg(
            F.count("*").cast("long").alias("sale_count"),
            F.sum("quantity").cast("long").alias("total_quantity"),
            F.round(F.sum("sale_amount"), 2).alias("gross_sales_amount"),
            F.round(F.avg("sale_amount"), 2).alias("average_sale_amount"),
        )
        .select(*REGIONAL_MONTHLY_SALES_COLUMNS)
    )
