"""Lab 5 transformations kept local and readable for classroom use."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


RUNTIME_BUDGET_OUTPUT_COLUMNS = (
    "order_month",
    "region",
    "category",
    "gross_revenue",
    "order_count",
    "customer_count",
)


def build_runtime_budget_baseline(
    inputs: dict[str, "DataFrame"],
    *,
    keyed_partitions: int,
) -> "DataFrame":
    """Build the approved workload used as the runtime budget baseline."""

    return (
        inputs["sales"]
        .transform(_select_sales_for_guardrail)
        .transform(_join_vendor_region, inputs["vendors"])
        .transform(_join_product_category, inputs["products"])
        .transform(_select_business_fact)
        .repartition(keyed_partitions, "order_month", "region", "category")
        .transform(_aggregate_business_output)
    )


def build_runtime_budget_candidate(
    inputs: dict[str, "DataFrame"],
    *,
    round_robin_partitions: int,
    keyed_partitions: int,
    guardrail_buckets: int,
) -> "DataFrame":
    """Build a functionally equivalent PR candidate with extra shuffle pressure."""

    return (
        inputs["sales"]
        .transform(_select_wide_sales_for_candidate)
        .transform(_join_vendor_region, inputs["vendors"])
        .transform(_join_product_category, inputs["products"])
        .transform(_join_customer_region_as_unused_context, inputs["customers"])
        # Intentional regression: a PR introduced a broad shuffle before the
        # business projection. The final numbers are still correct, but the
        # operational cost should be visible in StageMetrics.
        .repartition(round_robin_partitions)
        .transform(_select_candidate_business_fact, guardrail_buckets)
        # Intentional regression: a second shuffle by an unused bucket happens
        # before the final business aggregation.
        .repartition(round_robin_partitions, "guardrail_bucket")
        .transform(_force_candidate_cpu_burden_without_changing_revenue)
        .repartition(keyed_partitions, "order_month", "region", "category")
        .transform(_aggregate_business_output)
    )


def _select_sales_for_guardrail(sales: "DataFrame") -> "DataFrame":
    return sales.select(
        "sale_id",
        "sale_date",
        "vendor_id",
        "product_id",
        "customer_id",
        "sale_amount",
    )


def _select_wide_sales_for_candidate(sales: "DataFrame") -> "DataFrame":
    return sales.select(
        "sale_id",
        "sale_date",
        "vendor_id",
        "product_id",
        "customer_id",
        "sale_amount",
        *_payload_columns(sales),
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


def _join_customer_region_as_unused_context(
    sales: "DataFrame",
    customers: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    customer_regions = customers.select(
        F.col("customer_id").cast("long").alias("customer_join_id"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("customer_region"),
    )
    return sales.join(
        customer_regions,
        F.col("customer_id") == F.col("customer_join_id"),
        "left",
    ).drop("customer_join_id")


def _select_business_fact(sales: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return sales.select(
        F.date_format(F.col("sale_date"), "yyyy-MM").alias("order_month"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region"),
        _category_label(F).alias("category"),
        F.col("sale_id").cast("long").alias("sale_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
    )


def _select_candidate_business_fact(
    sales: "DataFrame",
    guardrail_buckets: int,
) -> "DataFrame":
    from pyspark.sql import functions as F

    payload_columns = _payload_columns(sales)
    payload_width = _payload_width_expression(F, payload_columns)
    return sales.select(
        F.date_format(F.col("sale_date"), "yyyy-MM").alias("order_month"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region"),
        _category_label(F).alias("category"),
        F.col("sale_id").cast("long").alias("sale_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("sale_amount").cast("double").alias("sale_amount"),
        payload_width.alias("payload_width"),
        _candidate_cpu_burden_expression(F, payload_width).alias(
            "guardrail_cpu_burden"
        ),
        F.pmod(F.xxhash64("customer_id"), F.lit(guardrail_buckets))
        .cast("int")
        .alias("guardrail_bucket"),
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
        .select(*RUNTIME_BUDGET_OUTPUT_COLUMNS)
    )


def _force_candidate_cpu_burden_without_changing_revenue(
    sales: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    zero_adjustment = F.when(
        F.col("guardrail_cpu_burden") < F.lit(0),
        F.lit(0.01),
    ).otherwise(F.lit(0.0))
    return sales.select(
        "order_month",
        "region",
        "category",
        "sale_id",
        "customer_id",
        (F.col("sale_amount") + zero_adjustment).alias("sale_amount"),
    )


def _category_label(F: object) -> object:
    return F.concat(
        F.lit("category_"),
        F.lpad(F.coalesce(F.col("category_id"), F.lit(-1)).cast("string"), 2, "0"),
    )


def _payload_columns(dataframe: "DataFrame") -> tuple[str, ...]:
    return tuple(column for column in dataframe.columns if column.startswith("payload_"))


def _payload_width_expression(F: object, payload_columns: tuple[str, ...]) -> object:
    if not payload_columns:
        return F.lit(0)
    return F.length(F.concat_ws("", *(F.col(column) for column in payload_columns)))


def _candidate_cpu_burden_expression(F: object, payload_width: object) -> object:
    return F.length(
        F.sha2(
            F.concat_ws(
                ":",
                F.col("sale_id").cast("string"),
                F.col("customer_id").cast("string"),
                F.col("product_id").cast("string"),
                payload_width.cast("string"),
            ),
            512,
        )
    ).cast("long")
