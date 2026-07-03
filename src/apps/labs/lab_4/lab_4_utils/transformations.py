"""Lab 4 workload transformations kept local for classroom readability."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


WORKLOAD_FINGERPRINT_COLUMNS = (
    "vendor_region",
    "customer_region",
    "category_id",
    "sale_year_month",
    "sale_count",
    "customer_count",
    "product_count",
    "total_quantity",
    "gross_sales_amount",
    "average_sale_amount",
    "fingerprint_bucket_count",
)


def build_stage_workload_fingerprint_summary(
    inputs: dict[str, "DataFrame"],
    *,
    shuffle_partitions: int,
    fingerprint_buckets: int,
) -> "DataFrame":
    """Build a stable retail workload for StageMetrics fingerprinting."""

    return (
        inputs["sales"]
        .transform(_select_sales_for_fingerprint, fingerprint_buckets)
        .transform(_join_vendor_dimension, inputs["vendors"])
        .transform(_join_product_dimension, inputs["products"])
        .transform(_join_customer_dimension, inputs["customers"])
        .repartition(shuffle_partitions, "fingerprint_bucket")
        .transform(_aggregate_bucketed_summary)
        .repartition(
            shuffle_partitions,
            "vendor_region",
            "customer_region",
            "category_id",
            "sale_year_month",
        )
        .transform(_aggregate_final_summary)
    )


def _select_sales_for_fingerprint(
    sales: "DataFrame",
    fingerprint_buckets: int,
) -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.select(
        F.col("sale_id").cast("long").alias("sale_id"),
        F.col("vendor_id").cast("long").alias("vendor_id"),
        F.col("product_id").cast("long").alias("product_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("sale_date").cast("date").alias("sale_date"),
        F.col("quantity").cast("long").alias("quantity"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
        F.pmod(F.xxhash64("sale_id", "customer_id"), F.lit(fingerprint_buckets))
        .cast("int")
        .alias("fingerprint_bucket"),
    )


def _join_vendor_dimension(
    sales: "DataFrame",
    vendors: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    vendor_dimension = vendors.select(
        F.col("vendor_id").cast("long").alias("vendor_join_id"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("vendor_region"),
    )
    return sales.join(
        vendor_dimension,
        F.col("vendor_id") == F.col("vendor_join_id"),
        "left",
    ).drop("vendor_join_id")


def _join_product_dimension(
    sales: "DataFrame",
    products: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    product_dimension = products.select(
        F.col("product_id").cast("long").alias("product_join_id"),
        F.col("vendor_id").cast("long").alias("product_vendor_join_id"),
        F.coalesce(F.col("category_id"), F.lit(-1)).cast("long").alias("category_id"),
    )
    return sales.join(
        product_dimension,
        (F.col("product_id") == F.col("product_join_id"))
        & (F.col("vendor_id") == F.col("product_vendor_join_id")),
        "left",
    ).drop("product_join_id", "product_vendor_join_id")


def _join_customer_dimension(
    sales: "DataFrame",
    customers: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    customer_dimension = customers.select(
        F.col("customer_id").cast("long").alias("customer_join_id"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("customer_region"),
    )
    return sales.join(
        customer_dimension,
        F.col("customer_id") == F.col("customer_join_id"),
        "left",
    ).drop("customer_join_id")


def _aggregate_bucketed_summary(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        sales.groupBy(
            "vendor_region",
            "customer_region",
            "category_id",
            F.date_format("sale_date", "yyyy-MM").alias("sale_year_month"),
            "fingerprint_bucket",
        )
        .agg(
            F.count("*").cast("long").alias("sale_count"),
            F.approx_count_distinct("customer_id").cast("long").alias("customer_count"),
            F.approx_count_distinct("product_id").cast("long").alias("product_count"),
            F.sum("quantity").cast("long").alias("total_quantity"),
            F.sum("sale_amount").alias("gross_sales_amount"),
        )
    )


def _aggregate_final_summary(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        sales.groupBy(
            "vendor_region",
            "customer_region",
            "category_id",
            "sale_year_month",
        )
        .agg(
            F.sum("sale_count").cast("long").alias("sale_count"),
            F.sum("customer_count").cast("long").alias("customer_count"),
            F.sum("product_count").cast("long").alias("product_count"),
            F.sum("total_quantity").cast("long").alias("total_quantity"),
            F.round(F.sum("gross_sales_amount"), 2).alias("gross_sales_amount"),
            F.round(F.sum("gross_sales_amount") / F.sum("sale_count"), 2).alias(
                "average_sale_amount"
            ),
            F.count("*").cast("long").alias("fingerprint_bucket_count"),
        )
        .select(*WORKLOAD_FINGERPRINT_COLUMNS)
    )
