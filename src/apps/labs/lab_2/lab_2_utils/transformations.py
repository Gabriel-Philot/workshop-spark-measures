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


# -----------------------------------------------------------------------------
# Lab 2B: stage metrics interpretation drill with shuffle, spill, and GC signals
# -----------------------------------------------------------------------------

STAGE_METRICS_DRILL_COLUMNS = (
    "vendor_region",
    "category_id",
    "sale_year_month",
    "sale_count",
    "gross_sales_amount",
    "payload_bytes_observed",
    "average_sale_amount",
)


def build_stage_metrics_drill_default(
    inputs: dict[str, "DataFrame"],
    *,
    keyed_partitions: int,
) -> "DataFrame":
    """Build the safer variant that narrows payloads before the shuffle."""

    return (
        inputs["sales"]
        .transform(_select_sales_with_payload_width)
        .transform(_join_vendor_region_for_drill, inputs["vendors"])
        .transform(_join_product_category_for_drill, inputs["products"])
        .transform(_select_stage_metrics_fact_from_width)
        .repartition(keyed_partitions, "vendor_region", "category_id", "sale_year_month")
        .transform(_aggregate_stage_metrics_fact)
    )


def build_stage_metrics_drill_pressure(
    inputs: dict[str, "DataFrame"],
    *,
    round_robin_partitions: int,
) -> "DataFrame":
    """Build the pressure variant that carries wider rows through a shuffle."""

    return (
        inputs["sales"]
        .transform(_select_sales_with_payload_columns)
        .transform(_join_vendor_region_for_drill, inputs["vendors"])
        .transform(_join_product_category_for_drill, inputs["products"])
        .repartition(round_robin_partitions)
        .transform(_select_stage_metrics_fact_from_payload_columns)
        .transform(_aggregate_stage_metrics_fact)
    )


def _select_sales_with_payload_width(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    payload_columns = _payload_columns(sales)
    return sales.select(
        "sale_date",
        "vendor_id",
        "product_id",
        "sale_amount",
        _payload_width_expression(F, payload_columns).alias("payload_width"),
    )


def _select_sales_with_payload_columns(sales: "DataFrame") -> "DataFrame":
    return sales.select(
        "sale_date",
        "vendor_id",
        "product_id",
        "sale_amount",
        *_payload_columns(sales),
    )


def _join_vendor_region_for_drill(
    sales: "DataFrame",
    vendors: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    vendor_regions = vendors.select(
        F.col("vendor_id").alias("vendor_join_id"),
        F.col("region").alias("vendor_region"),
    )
    return (
        sales.join(
            vendor_regions,
            F.col("vendor_id") == F.col("vendor_join_id"),
            "left",
        )
        .drop("vendor_join_id")
    )


def _join_product_category_for_drill(
    sales: "DataFrame",
    products: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    product_categories = products.select(
        F.col("product_id").alias("product_join_id"),
        F.col("vendor_id").alias("product_vendor_join_id"),
        F.col("category_id").alias("category_id"),
    )
    return (
        sales.join(
            product_categories,
            (F.col("product_id") == F.col("product_join_id"))
            & (F.col("vendor_id") == F.col("product_vendor_join_id")),
            "left",
        )
        .drop("product_join_id", "product_vendor_join_id")
    )


def _select_stage_metrics_fact_from_width(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.select(
        F.coalesce(F.col("vendor_region"), F.lit("UNKNOWN")).alias("vendor_region"),
        F.coalesce(F.col("category_id"), F.lit(-1)).cast("long").alias("category_id"),
        F.date_format(F.col("sale_date"), "yyyy-MM").alias("sale_year_month"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
        F.col("payload_width").cast("long").alias("payload_width"),
    )


def _select_stage_metrics_fact_from_payload_columns(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    payload_columns = _payload_columns(sales)
    return sales.select(
        F.coalesce(F.col("vendor_region"), F.lit("UNKNOWN")).alias("vendor_region"),
        F.coalesce(F.col("category_id"), F.lit(-1)).cast("long").alias("category_id"),
        F.date_format(F.col("sale_date"), "yyyy-MM").alias("sale_year_month"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
        _payload_width_expression(F, payload_columns).alias("payload_width"),
    )


def _aggregate_stage_metrics_fact(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        sales.groupBy("vendor_region", "category_id", "sale_year_month")
        .agg(
            F.count("*").cast("long").alias("sale_count"),
            F.round(F.sum("sale_amount"), 2).alias("gross_sales_amount"),
            F.sum("payload_width").cast("long").alias("payload_bytes_observed"),
            F.round(F.avg("sale_amount"), 2).alias("average_sale_amount"),
        )
        .select(*STAGE_METRICS_DRILL_COLUMNS)
    )


def _payload_columns(dataframe: "DataFrame") -> tuple[str, ...]:
    return tuple(column for column in dataframe.columns if column.startswith("payload_"))


def _payload_width_expression(F: object, payload_columns: tuple[str, ...]) -> object:
    if not payload_columns:
        return F.lit(0)
    return F.length(F.concat_ws("", *(F.col(column) for column in payload_columns)))


# -----------------------------------------------------------------------------
# Lab 2C: task duration skew diagnosis with task-level sparkMeasure
# -----------------------------------------------------------------------------

TASK_SKEW_VENDOR_SUMMARY_COLUMNS = (
    "vendor_id",
    "vendor_region",
    "sale_count",
    "gross_sales_amount",
    "payload_bytes_observed",
    "average_sale_amount",
)


def build_task_skew_vendor_summary(
    inputs: dict[str, "DataFrame"],
    *,
    shuffle_partitions: int,
) -> "DataFrame":
    """Build a hot-key join and aggregation that exposes task skew."""

    return (
        inputs["sales"]
        .transform(_select_task_skew_sales)
        .repartition(shuffle_partitions, "vendor_id")
        .transform(
            _join_task_skew_vendors,
            inputs["vendors"].transform(_select_task_skew_vendors),
            shuffle_partitions,
        )
        .transform(_select_task_skew_fact)
        .transform(_aggregate_task_skew_vendor_summary)
    )


def _select_task_skew_sales(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    payload_columns = _payload_columns(sales)
    return sales.select(
        F.col("vendor_id").cast("long").alias("vendor_id"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
        _payload_width_expression(F, payload_columns).cast("long").alias("payload_width"),
    )


def _select_task_skew_vendors(vendors: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return vendors.select(
        F.col("vendor_id").cast("long").alias("vendor_id"),
        F.col("region").alias("vendor_region"),
    )


def _join_task_skew_vendors(
    sales: "DataFrame",
    vendors: "DataFrame",
    shuffle_partitions: int,
) -> "DataFrame":
    return sales.join(
        vendors.repartition(shuffle_partitions, "vendor_id"),
        "vendor_id",
        "left",
    )


def _select_task_skew_fact(joined_sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return joined_sales.select(
        F.col("vendor_id"),
        F.coalesce(F.col("vendor_region"), F.lit("UNKNOWN")).alias("vendor_region"),
        F.col("sale_amount"),
        F.col("payload_width"),
    )


def _aggregate_task_skew_vendor_summary(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        sales.groupBy("vendor_id", "vendor_region")
        .agg(
            F.count("*").cast("long").alias("sale_count"),
            F.round(F.sum("sale_amount"), 2).alias("gross_sales_amount"),
            F.sum("payload_width").cast("long").alias("payload_bytes_observed"),
            F.round(F.avg("sale_amount"), 2).alias("average_sale_amount"),
        )
        .select(*TASK_SKEW_VENDOR_SUMMARY_COLUMNS)
    )
