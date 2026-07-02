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
from apps.labs.lab_2.lab_2_utils.task_duration_skew_runtime import (
    TaskSkewSettings,
    load_task_skew_settings,
    render_selected_stage_line,
    render_task_skew_report,
    render_task_outlier_line,
    render_task_summary_line,
)
from apps.labs.lab_2.lab_2_utils.transformations import (
    REGIONAL_MONTHLY_SALES_COLUMNS,
    STAGE_METRICS_DRILL_COLUMNS,
    TASK_SKEW_VENDOR_SUMMARY_COLUMNS,
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


def test_lab2c_task_skew_schema_is_classroom_friendly():
    assert TASK_SKEW_VENDOR_SUMMARY_COLUMNS == (
        "vendor_id",
        "vendor_region",
        "sale_count",
        "gross_sales_amount",
        "payload_bytes_observed",
        "average_sale_amount",
    )


def test_lab2c_task_skew_settings_are_loaded_from_yaml():
    task = load_task_skew_settings(
        "lab2c-task-skew-task",
        LAB2_CONFIG,
    )

    assert task == TaskSkewSettings(
        variant="skewed",
        success_marker="LAB2C_TASK_SKEW_TASK_OK",
        shuffle_partitions=27,
    )


def test_lab2c_task_skew_rejects_unknown_variants(tmp_path):
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

    with pytest.raises(ValueError, match="Unsupported Lab 2C workload variant"):
        load_task_skew_settings("broken", config)


def test_lab2c_task_skew_rendered_summary_matches_classroom_shape():
    line = render_task_summary_line(
        {
            "stageId": 7,
            "metric": "duration",
            "taskCount": 27,
            "min": 100,
            "p25": 2000,
            "median": 2000,
            "p75": 3000,
            "max": 30000,
            "maxToP75": 10.0,
        }
    )

    assert line == (
        "LAB2C_TASK_SUMMARY "
        "stageId=7 metric=duration taskCount=27 "
        "min=100 p25=2000 median=2000 p75=3000 max=30000 max_to_p75=10"
    )


def test_lab2c_task_skew_rendered_stage_and_outlier_are_compact():
    stage_line = render_selected_stage_line(
        {
            "stageId": 7,
            "taskCount": 27,
            "dataMetric": "shuffleTotalBytesRead",
            "dataMaxToP75": 100.375,
            "durationMaxToP75": 10.0,
        }
    )
    outlier_line = render_task_outlier_line(
        1,
        {
            "stageId": 7,
            "index": 26,
            "executorId": "1",
            "duration": 30000,
            "executorRunTime": 29000,
            "recordsRead": 0,
            "bytesRead": 0,
            "shuffleRecordsRead": 276,
            "shuffleTotalBytesRead": 822272,
            "recordsWritten": 1,
            "shuffleBytesWritten": 0,
            "memoryBytesSpilled": 0,
            "diskBytesSpilled": 0,
        },
    )

    assert "stageId=7 taskCount=27" in stage_line
    assert "data_metric=shuffleTotalBytesRead" in stage_line
    assert "data_max_to_p75=100.3750" in stage_line
    assert "LAB2C_TASK_OUTLIER rank=1 stageId=7 taskIndex=26" in outlier_line
    assert "shuffleTotalBytesRead=822272" in outlier_line


def test_lab2c_task_skew_report_is_single_readable_block():
    report = render_task_skew_report(
        {
            "stageId": 7,
            "taskCount": 27,
            "dataMetric": "shuffleTotalBytesRead",
            "dataMaxToP75": 100.375,
            "durationMaxToP75": 10.0,
        },
        [
            {
                "stageId": 7,
                "metric": "duration",
                "taskCount": 27,
                "p75": 3000,
                "max": 30000,
                "maxToP75": 10.0,
            },
            {
                "stageId": 7,
                "metric": "shuffleTotalBytesRead",
                "taskCount": 27,
                "p75": 8192,
                "max": 822272,
                "maxToP75": 100.375,
            },
        ],
        [
            {
                "stageId": 7,
                "index": 26,
                "executorId": "1",
                "duration": 30000,
                "shuffleRecordsRead": 276,
                "shuffleTotalBytesRead": 822272,
                "memoryBytesSpilled": 0,
                "diskBytesSpilled": 0,
            }
        ],
    )

    assert "LAB 2C TASKMETRICS DIAGNOSTIC REPORT" in report
    assert "stageId=7 | tasks=27 | dataMetric=shuffleTotalBytesRead" in report
    assert "duration" in report
    assert "803.0 KiB" in report
    assert "strong skew signal" in report
    assert "Top 1 task outliers by shuffleTotalBytesRead" in report
