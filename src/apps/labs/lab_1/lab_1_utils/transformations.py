"""Lab 1 transformations kept local for workshop readability."""

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

TOP_SALES_GLOBAL_SORT_COLUMNS = SALES_ENRICHED_COLUMNS


def build_sales_enriched(inputs: dict[str, "DataFrame"]) -> "DataFrame":
    """Build the enriched sales dataset used by the Lab 1 ranking workload."""

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


def build_top_sales_global_sort(sales_enriched: "DataFrame") -> "DataFrame":
    """Create the diagnostic workload with an intentionally global sort."""

    from pyspark.sql import functions as F

    return sales_enriched.select(*TOP_SALES_GLOBAL_SORT_COLUMNS).orderBy(
        F.desc("sale_amount"),
        F.asc("sale_id"),
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

AUDIT_BUCKETS = 16
SLOW_AUDIT_BUCKET = 7
HEAVY_AUDIT_REPEAT_FACTOR = 512
FIXED_AUDIT_SALT_BUCKETS = 8
FIXED_AUDIT_PARTITIONS = 64
AUDIT_OUTLIER_COLUMNS = SALES_ENRICHED_COLUMNS + (
    "audit_bucket",
    "audit_fingerprint",
)


def build_random_task_outlier_problem(sales_enriched: "DataFrame") -> "DataFrame":
    """Create a compute outlier concentrated in one technical audit bucket."""

    return (
        sales_enriched
        .transform(_add_audit_bucket)
        .repartition(AUDIT_BUCKETS, "audit_bucket")
        .transform(_add_audit_fingerprint)
        .select(*AUDIT_OUTLIER_COLUMNS)
    )


def build_random_task_outlier_fixed(sales_enriched: "DataFrame") -> "DataFrame":
    """Spread the expensive audit bucket so the slow task is split up."""

    from pyspark.sql import functions as F

    return (
        sales_enriched
        .transform(_add_audit_bucket)
        .withColumn(
            "audit_salt",
            F.when(
                F.col("audit_bucket") == F.lit(SLOW_AUDIT_BUCKET),
                F.pmod(F.xxhash64("sale_id"), F.lit(FIXED_AUDIT_SALT_BUCKETS)),
            ).otherwise(F.lit(0)),
        )
        .repartition(FIXED_AUDIT_PARTITIONS, "audit_bucket", "audit_salt")
        .transform(_add_audit_fingerprint)
        .drop("audit_salt")
        .select(*AUDIT_OUTLIER_COLUMNS)
    )


def _add_audit_bucket(sales_enriched: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales_enriched.withColumn(
        "audit_bucket",
        F.pmod(F.xxhash64("sale_id"), F.lit(AUDIT_BUCKETS)).cast("int"),
    )


def _add_audit_fingerprint(sales_enriched: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    heavy_fingerprint = _audit_fingerprint(HEAVY_AUDIT_REPEAT_FACTOR)
    light_fingerprint = _audit_fingerprint(1)
    return sales_enriched.withColumn(
        "audit_fingerprint",
        F.when(
            F.col("audit_bucket") == F.lit(SLOW_AUDIT_BUCKET),
            heavy_fingerprint,
        ).otherwise(light_fingerprint),
    )


def _audit_fingerprint(repeat_factor: int) -> "Column":
    from pyspark.sql import functions as F

    base_expression = F.concat_ws(
        ":",
        F.col("sale_id").cast("string"),
        F.col("product_id").cast("string"),
        F.col("customer_id").cast("string"),
        F.col("sale_amount").cast("string"),
        F.col("sale_date").cast("string"),
    )
    return F.sha2(F.repeat(base_expression, repeat_factor), 256)
