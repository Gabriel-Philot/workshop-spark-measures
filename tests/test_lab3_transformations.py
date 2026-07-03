from pathlib import Path

import pytest

from apps.labs.lab_3.lab_3_utils.overhead_runtime import (
    OverheadBenchmarkSettings,
    load_overhead_benchmark_settings,
)
from apps.labs.lab_3.lab_3_utils.transformations import (
    OBSERVABILITY_OVERHEAD_COLUMNS,
)


LAB3_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "apps"
    / "labs"
    / "lab_3"
    / "lab_3_utils"
    / "experiments.yaml"
)


def test_lab3_overhead_summary_schema_is_classroom_friendly():
    assert OBSERVABILITY_OVERHEAD_COLUMNS == (
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


def test_lab3_overhead_settings_are_loaded_from_yaml():
    none = load_overhead_benchmark_settings("lab3-overhead-none", LAB3_CONFIG)
    stage = load_overhead_benchmark_settings("lab3-overhead-stage", LAB3_CONFIG)
    task = load_overhead_benchmark_settings("lab3-overhead-task", LAB3_CONFIG)

    assert none == OverheadBenchmarkSettings(
        mode="none",
        success_marker="LAB3_OBSERVABILITY_OVERHEAD_NONE_OK",
        shuffle_partitions=384,
        benchmark_buckets=2048,
    )
    assert stage == OverheadBenchmarkSettings(
        mode="stage",
        success_marker="LAB3_OBSERVABILITY_OVERHEAD_STAGE_OK",
        shuffle_partitions=384,
        benchmark_buckets=2048,
    )
    assert task == OverheadBenchmarkSettings(
        mode="task",
        success_marker="LAB3_OBSERVABILITY_OVERHEAD_TASK_OK",
        shuffle_partitions=384,
        benchmark_buckets=2048,
    )


def test_lab3_overhead_settings_reject_unknown_modes(tmp_path):
    config = tmp_path / "experiments.yaml"
    config.write_text(
        """
experiments:
  broken:
    workload:
      mode: mystery
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported Lab 3 overhead mode"):
        load_overhead_benchmark_settings("broken", config)
