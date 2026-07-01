from pathlib import Path

import pytest

from apps.labs.lab_2.lab_2_utils.shuffle_aggregation_runtime import (
    ShuffleAggregationSettings,
    load_shuffle_aggregation_settings,
)
from apps.labs.lab_2.lab_2_utils.transformations import (
    REGIONAL_MONTHLY_SALES_COLUMNS,
)


LAB2_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "apps"
    / "labs"
    / "lab_2"
    / "lab_2_utils"
    / "experiments.yaml"
)


def test_lab2_regional_monthly_sales_schema_is_classroom_friendly():
    assert REGIONAL_MONTHLY_SALES_COLUMNS == (
        "vendor_region",
        "sale_year_month",
        "sale_count",
        "total_quantity",
        "gross_sales_amount",
        "average_sale_amount",
    )


def test_lab2_shuffle_aggregation_settings_are_loaded_from_yaml():
    baseline = load_shuffle_aggregation_settings(
        "lab2-shuffle-aggregation-baseline",
        LAB2_CONFIG,
    )
    optimized = load_shuffle_aggregation_settings(
        "lab2-shuffle-aggregation-optimized",
        LAB2_CONFIG,
    )

    assert baseline == ShuffleAggregationSettings(
        variant="baseline",
        success_marker="LAB2_SHUFFLE_AGGREGATION_BASELINE_OK",
        round_robin_partitions=1024,
        keyed_partitions=32,
    )
    assert optimized == ShuffleAggregationSettings(
        variant="optimized",
        success_marker="LAB2_SHUFFLE_AGGREGATION_OPTIMIZED_OK",
        round_robin_partitions=96,
        keyed_partitions=32,
    )


def test_lab2_shuffle_aggregation_rejects_unknown_variants(tmp_path):
    config = tmp_path / "experiments.yaml"
    config.write_text(
        """
experiments:
  broken:
    workload:
      variant: mystery
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported Lab 2A workload variant"):
        load_shuffle_aggregation_settings("broken", config)
