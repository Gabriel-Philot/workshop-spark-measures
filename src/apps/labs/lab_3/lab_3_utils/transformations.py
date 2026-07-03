"""Lab 3 transformations kept local for benchmark readability."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


OBSERVABILITY_OVERHEAD_COLUMNS = (
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
    "payload_bytes_observed",
    "revenue_rank_in_region",
)


def build_observability_overhead_summary(
    inputs: dict[str, "DataFrame"],
    *,
    shuffle_partitions: int,
    benchmark_buckets: int,
) -> "DataFrame":
    """Build the stable multi-join benchmark used across all observability modes."""

    return (
        inputs["sales"]
        .transform(_select_overhead_sales, benchmark_buckets)
        .transform(_join_vendor_dimension, inputs["vendors"])
        .transform(_join_product_dimension, inputs["products"])
        .transform(_join_customer_dimension, inputs["customers"])
        .repartition(shuffle_partitions, "benchmark_bucket")
        .transform(_aggregate_bucketed_observability_summary)
        .repartition(
            shuffle_partitions,
            "vendor_region",
            "customer_region",
            "category_id",
            "sale_year_month",
        )
        .transform(_aggregate_final_observability_summary)
        .transform(_rank_category_revenue)
    )


def _select_overhead_sales(
    sales: "DataFrame",
    benchmark_buckets: int,
) -> "DataFrame":
    from pyspark.sql import functions as F

    payload_columns = _payload_columns(sales)
    return sales.select(
        F.col("sale_id").cast("long").alias("sale_id"),
        F.col("vendor_id").cast("long").alias("vendor_id"),
        F.col("product_id").cast("long").alias("product_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("sale_date").cast("date").alias("sale_date"),
        F.col("quantity").cast("long").alias("quantity"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
        _payload_width_expression(F, payload_columns).alias("payload_width"),
        F.pmod(F.xxhash64("sale_id", "customer_id"), F.lit(benchmark_buckets))
        .cast("int")
        .alias("benchmark_bucket"),
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


def _aggregate_bucketed_observability_summary(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        sales.groupBy(
            "vendor_region",
            "customer_region",
            "category_id",
            F.date_format("sale_date", "yyyy-MM").alias("sale_year_month"),
            "benchmark_bucket",
        )
        .agg(
            F.count("*").cast("long").alias("sale_count"),
            F.approx_count_distinct("customer_id").cast("long").alias("customer_count"),
            F.approx_count_distinct("product_id").cast("long").alias("product_count"),
            F.sum("quantity").cast("long").alias("total_quantity"),
            F.sum("sale_amount").alias("gross_sales_amount"),
            F.sum("payload_width").cast("long").alias("payload_bytes_observed"),
        )
    )


def _aggregate_final_observability_summary(sales: "DataFrame") -> "DataFrame":
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
            F.sum("payload_bytes_observed")
            .cast("long")
            .alias("payload_bytes_observed"),
        )
    )


def _rank_category_revenue(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    rank_window = Window.partitionBy(
        "vendor_region",
        "customer_region",
        "sale_year_month",
    ).orderBy(F.desc("gross_sales_amount"), F.asc("category_id"))

    return (
        sales.withColumn("revenue_rank_in_region", F.dense_rank().over(rank_window))
        .select(*OBSERVABILITY_OVERHEAD_COLUMNS)
    )


def _payload_columns(dataframe: "DataFrame") -> tuple[str, ...]:
    return tuple(column for column in dataframe.columns if column.startswith("payload_"))


def _payload_width_expression(F: object, payload_columns: tuple[str, ...]) -> object:
    if not payload_columns:
        return F.lit(0)
    return F.length(F.concat_ws("", *(F.col(column) for column in payload_columns)))
