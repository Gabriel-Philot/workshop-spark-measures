"""Shared Lab 0 transformations."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


SALES_ENRICHED_COLUMNS = (
    "sale_id",
    "sale_date",
    "sale_year_month",
    "vendor_id",
    "vendor_name",
    "vendor_region",
    "product_id",
    "product_name",
    "category_id",
    "customer_id",
    "quantity",
    "unit_price",
    "sale_amount",
)


def build_sales_enriched(inputs: dict[str, "DataFrame"]) -> "DataFrame":
    """Build a Silver-ready sales table enriched with vendor and product metadata."""

    vendors = inputs["vendors"].select("vendor_id", "vendor_name", "region").alias("v")
    products = (
        inputs["products"]
        .select("product_id", "vendor_id", "product_name", "category_id")
        .alias("p")
    )

    return (
        inputs["sales"]
        .alias("s")
        .transform(_join_vendor_metadata, vendors)
        .transform(_join_product_metadata, products)
        .transform(_select_sales_enriched_columns)
    )


def _join_vendor_metadata(sales: "DataFrame", vendors: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.join(vendors, F.col("s.vendor_id") == F.col("v.vendor_id"), "left")


def _join_product_metadata(sales: "DataFrame", products: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.join(
        products,
        (F.col("s.product_id") == F.col("p.product_id"))
        & (F.col("s.vendor_id") == F.col("p.vendor_id")),
        "left",
    )


def _select_sales_enriched_columns(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.select(
        F.col("s.sale_id").cast("long").alias("sale_id"),
        F.col("s.sale_date").alias("sale_date"),
        F.date_format(F.col("s.sale_date"), "yyyy-MM").alias("sale_year_month"),
        F.col("s.vendor_id").cast("long").alias("vendor_id"),
        F.col("v.vendor_name").alias("vendor_name"),
        F.col("v.region").alias("vendor_region"),
        F.col("s.product_id").cast("long").alias("product_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.category_id").cast("long").alias("category_id"),
        F.col("s.customer_id").cast("long").alias("customer_id"),
        F.col("s.quantity").cast("int").alias("quantity"),
        F.col("s.unit_price").cast("double").alias("unit_price"),
        F.col("s.sale_amount").cast("double").alias("sale_amount"),
    )
