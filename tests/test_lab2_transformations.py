from pathlib import Path

import pytest

from apps.labs.lab_2.lab_2_utils.shuffle_aggregation_runtime import (
    ShuffleAggregationSettings,
    load_shuffle_aggregation_settings,
)
from apps.labs.lab_2.lab_2_utils.stage_metrics_runtime import (
    StageMetricsDrillSettings,
    load_stage_metrics_drill_settings,
)
from apps.labs.lab_2.lab_2_utils.transformations import (
    REGIONAL_MONTHLY_SALES_COLUMNS,
    STAGE_METRICS_DRILL_COLUMNS,
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


def test_lab2_stage_metrics_drill_schema_is_classroom_friendly():
    assert STAGE_METRICS_DRILL_COLUMNS == (
        "vendor_region",
        "category_id",
        "sale_year_month",
        "sale_count",
        "gross_sales_amount",
        "payload_bytes_observed",
        "average_sale_amount",
    )


def test_lab2_stage_metrics_drill_settings_are_loaded_from_yaml():
    default = load_stage_metrics_drill_settings(
        "lab2-stage-metrics-drill-default",
        LAB2_CONFIG,
    )
    pressure = load_stage_metrics_drill_settings(
        "lab2-stage-metrics-drill-pressure",
        LAB2_CONFIG,
    )

    assert default == StageMetricsDrillSettings(
        variant="default",
        success_marker="LAB2_STAGE_METRICS_DRILL_DEFAULT_OK",
        keyed_partitions=32,
        round_robin_partitions=96,
    )
    assert pressure == StageMetricsDrillSettings(
        variant="pressure",
        success_marker="LAB2_STAGE_METRICS_DRILL_PRESSURE_OK",
        keyed_partitions=32,
        round_robin_partitions=512,
    )


def test_lab2_stage_metrics_drill_rejects_unknown_variants(tmp_path):
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

    with pytest.raises(ValueError, match="Unsupported Lab 2B workload variant"):
        load_stage_metrics_drill_settings("broken", config)
