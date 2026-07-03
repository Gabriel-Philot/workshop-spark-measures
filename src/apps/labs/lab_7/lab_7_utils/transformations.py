"""Lab 7 temporal backfill transformations."""

from __future__ import annotations

from typing import Any


EARLY_PARTITION_FILTER = "early_partition_filter"
SUPPORTED_FILTER_STRATEGIES = frozenset({EARLY_PARTITION_FILTER})


def build_daily_activity_dashboard(
    source: Any,
    *,
    processing_date: str,
    filter_strategy: str,
) -> Any:
    """Build the daily activity dashboard for one processing date."""

    from pyspark.sql import functions as F

    if filter_strategy != EARLY_PARTITION_FILTER:
        raise ValueError(
            f"Unsupported Lab 7 filter strategy '{filter_strategy}'. "
            f"Supported strategies: {sorted(SUPPORTED_FILTER_STRATEGIES)}"
        )

    filtered = source.where(F.col("event_date") == F.to_date(F.lit(processing_date)))
    return (
        filtered.groupBy("event_date", "region", "channel", "event_type")
        .agg(
            F.count("*").cast("long").alias("event_count"),
            F.countDistinct("customer_id").cast("long").alias("customer_count"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
        )
        .withColumn(
            "avg_ticket",
            F.round(F.col("gross_revenue") / F.col("event_count"), 4),
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "event_date",
            "region",
            "channel",
            "event_type",
            "event_count",
            "customer_count",
            "gross_revenue",
            "avg_ticket",
            "created_at",
        )
    )
