"""Lab 6 workload transformations kept local for classroom readability."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


CONTRACT_GATE_OUTPUT_COLUMNS = (
    "order_month",
    "region",
    "category",
    "gross_revenue",
    "order_count",
    "customer_count",
)


def build_stage_metrics_contract_gate_output(
    inputs: dict[str, "DataFrame"],
    *,
    shuffle_partitions: int,
) -> "DataFrame":
    """Build a stable retail workload used to capture contract-ready metrics."""

    return (
        inputs["sales"]
        .transform(_select_sales_for_contract_gate)
        .transform(_join_vendor_region, inputs["vendors"])
        .transform(_join_product_category, inputs["products"])
        .transform(_join_customer_context, inputs["customers"])
        .transform(_select_business_fact)
        .repartition(shuffle_partitions, "order_month", "region", "category")
        .transform(_aggregate_business_output)
    )


def _select_sales_for_contract_gate(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.select(
        F.col("sale_id").cast("long").alias("sale_id"),
        F.col("sale_date").cast("date").alias("sale_date"),
        F.col("vendor_id").cast("long").alias("vendor_id"),
        F.col("product_id").cast("long").alias("product_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
    )


def _join_vendor_region(
    sales: "DataFrame",
    vendors: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    vendor_regions = vendors.select(
        F.col("vendor_id").cast("long").alias("vendor_join_id"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region"),
    )
    return sales.join(
        vendor_regions,
        F.col("vendor_id") == F.col("vendor_join_id"),
        "left",
    ).drop("vendor_join_id")


def _join_product_category(
    sales: "DataFrame",
    products: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    product_categories = products.select(
        F.col("product_id").cast("long").alias("product_join_id"),
        F.col("vendor_id").cast("long").alias("product_vendor_join_id"),
        F.coalesce(F.col("category_id"), F.lit(-1)).cast("long").alias("category_id"),
    )
    return sales.join(
        product_categories,
        (F.col("product_id") == F.col("product_join_id"))
        & (F.col("vendor_id") == F.col("product_vendor_join_id")),
        "left",
    ).drop("product_join_id", "product_vendor_join_id")


def _join_customer_context(
    sales: "DataFrame",
    customers: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    customer_context = customers.select(
        F.col("customer_id").cast("long").alias("customer_join_id"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("customer_region"),
    )
    return sales.join(
        customer_context,
        F.col("customer_id") == F.col("customer_join_id"),
        "left",
    ).drop("customer_join_id")


def _select_business_fact(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.select(
        F.date_format(F.col("sale_date"), "yyyy-MM").alias("order_month"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region"),
        F.concat(
            F.lit("category_"),
            F.lpad(
                F.coalesce(F.col("category_id"), F.lit(-1)).cast("string"),
                2,
                "0",
            ),
        ).alias("category"),
        F.col("sale_id").cast("long").alias("sale_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
    )


def _aggregate_business_output(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        sales.groupBy("order_month", "region", "category")
        .agg(
            F.round(F.sum("sale_amount"), 2).alias("gross_revenue"),
            F.count("*").cast("long").alias("order_count"),
            F.countDistinct("customer_id").cast("long").alias("customer_count"),
        )
        .select(*CONTRACT_GATE_OUTPUT_COLUMNS)
    )

