from pathlib import Path

import pytest

from apps.labs.lab_7.lab_7_utils.generator import (
    APPEND_DAY,
    FULL,
    build_volume_plan_records,
    load_temporal_generator_settings,
    load_temporal_volume_plan,
)
from apps.labs.lab_7.lab_7_utils.backfill import (
    DailyBackfillMetrics,
    build_daily_backfill_metrics_record,
    expected_volume_for_date,
    load_daily_backfill_settings,
    load_processing_dates_from_plan,
    normalize_daily_backfill_metrics,
)
from apps.labs.lab_7.lab_7_utils.transformations import (
    EARLY_PARTITION_FILTER,
    SUPPORTED_FILTER_STRATEGIES,
)
from spark_workshop.config import load_experiment_config


LAB7_DIR = Path(__file__).resolve().parents[1] / "src" / "apps" / "labs" / "lab_7"
LAB7_CONFIG = LAB7_DIR / "lab_7_utils" / "experiments.yaml"
LAB7_VOLUME_PLAN = LAB7_DIR / "lab_7_utils" / "volume_plan.yaml"


def test_lab7_config_writes_only_lab7_paths():
    config = load_experiment_config(
        "lab7-temporal-source-generator",
        config_path=LAB7_CONFIG,
    )

    source = config.artifacts.output("source_events_temporal")
    plan = config.artifacts.output("temporal_volume_plan")

    assert config.app_name == "workshop-lab7-temporal-source-generator"
    assert config.observability.enabled is False
    assert source.path == "s3a://lakehouse/bronze/lab7/source_events_temporal"
    assert source.mode == "append"
    assert source.partition_by == ("event_date",)
    assert plan.path == "s3a://observability/lab7/temporal_volume_plan"
    assert plan.mode == "append"


def test_lab7_daily_backfill_config_uses_stage_metrics_and_expected_paths():
    config = load_experiment_config(
        "lab7-daily-backfill-stage-metrics",
        config_path=LAB7_CONFIG,
    )

    assert config.app_name == "workshop-lab7-daily-backfill-stage-metrics"
    assert config.observability.enabled is True
    assert config.observability.collector == "stage"
    assert config.observability.persist is False
    assert (
        config.artifacts.input("source_events_temporal").path
        == "s3a://lakehouse/bronze/lab7/source_events_temporal"
    )
    assert (
        config.artifacts.output("daily_backfill_stage_metrics").path
        == "s3a://observability/lab7/daily_backfill_stage_metrics"
    )
    assert (
        config.artifacts.output("daily_activity_dashboard").path
        == "s3a://lakehouse/gold/lab7/daily_activity_dashboard/manual"
    )


def test_lab7_generator_settings_are_loaded_from_yaml():
    settings = load_temporal_generator_settings(
        "lab7-temporal-source-generator",
        LAB7_CONFIG,
    )

    assert settings.workload_name == "temporal_source_generator"
    assert settings.success_marker == "LAB7_TEMPORAL_SOURCE_GENERATOR_OK"


def test_lab7_daily_backfill_settings_are_loaded_from_yaml():
    settings = load_daily_backfill_settings(
        "lab7-daily-backfill-stage-metrics",
        LAB7_CONFIG,
    )

    assert settings.workload_name == "temporal_daily_backfill"
    assert settings.filter_strategy == EARLY_PARTITION_FILTER
    assert settings.success_marker == "LAB7_DAILY_BACKFILL_STAGE_METRICS_OK"


def test_lab7_volume_plan_has_expected_temporal_shape():
    plan = load_temporal_volume_plan(LAB7_VOLUME_PLAN)
    volumes = {item.event_date: item for item in plan.date_volumes}

    assert plan.source_name == "source_events_temporal"
    assert plan.start_date == "2026-01-01"
    assert plan.end_date == "2026-01-14"
    assert len(plan.date_volumes) == 14
    assert plan.total_rows == 2_210_000
    assert volumes["2026-01-01"].rows == 10_000
    assert volumes["2026-01-04"].rows == 1_000_000
    assert volumes["2026-01-04"].spike_label == "VOLUME_SPIKE"
    assert volumes["2026-01-07"].rows == 100_000
    assert volumes["2026-01-07"].spike_label == "MEDIUM_SPIKE"
    assert volumes["2026-01-11"].volume_multiplier == 100


def test_lab7_backfill_uses_volume_plan_dates_and_expected_volume():
    dates = load_processing_dates_from_plan(LAB7_VOLUME_PLAN)
    plan, volume = expected_volume_for_date(LAB7_VOLUME_PLAN, "2026-01-04")

    assert dates[0] == "2026-01-01"
    assert dates[-1] == "2026-01-14"
    assert len(dates) == 14
    assert plan.source_name == "source_events_temporal"
    assert volume.rows == 1_000_000
    assert volume.volume_multiplier == 100
    assert volume.spike_label == "VOLUME_SPIKE"


def test_lab7_volume_plan_partition_calibration_is_bounded():
    plan = load_temporal_volume_plan(LAB7_VOLUME_PLAN)

    assert plan.partitions_for_rows(10_000) == 1
    assert plan.partitions_for_rows(100_000) == 1
    assert plan.partitions_for_rows(1_000_000) == 4
    assert plan.partitions_for_rows(100_000_000) == 16


def test_lab7_builds_auditable_generation_plan_records():
    plan = load_temporal_volume_plan(LAB7_VOLUME_PLAN)
    selected = (
        plan.date_volume("2026-01-01"),
        plan.date_volume("2026-01-04"),
    )

    records = build_volume_plan_records(
        run_id="run-1",
        mode=FULL,
        plan=plan,
        planned_dates=selected,
        generated_dates=("2026-01-04",),
        source_path="s3a://lakehouse/bronze/lab7/source_events_temporal",
    )

    assert [record["event_date"] for record in records] == [
        "2026-01-01",
        "2026-01-04",
    ]
    assert records[0]["write_status"] == "already_exists"
    assert records[1]["write_status"] == "generated"
    assert records[1]["expected_rows"] == 1_000_000
    assert records[1]["spike_label"] == "VOLUME_SPIKE"
    assert records[1]["generation_mode"] == FULL


def test_lab7_append_day_mode_is_documented_by_constants():
    assert FULL == "full"
    assert APPEND_DAY == "append_day"


def test_lab7_daily_backfill_filter_strategy_scope_is_stage_first():
    assert SUPPORTED_FILTER_STRATEGIES == {EARLY_PARTITION_FILTER}


def test_lab7_daily_backfill_normalizes_stage_metrics_by_source_volume():
    normalized = normalize_daily_backfill_metrics(
        {
            "numStages": 4,
            "numTasks": 25,
            "executorRunTime": 5000,
            "bytesRead": 1000,
            "recordsRead": 10000,
            "recordsWritten": 12,
            "shuffleTotalBytesRead": 1500,
            "shuffleBytesWritten": 2500,
            "memoryBytesSpilled": 100,
            "diskBytesSpilled": 50,
            "jvmGCTime": 500,
        },
        source_rows_for_date=10_000,
    )

    assert normalized.executor_run_time_ms == 5000
    assert normalized.records_read == 10000
    assert normalized.records_written == 12
    assert normalized.shuffle_total_bytes == 4000
    assert normalized.runtime_per_million_rows == 500000.0
    assert normalized.shuffle_per_million_rows == 400000.0
    assert normalized.tasks_per_million_rows == 2500.0


def test_lab7_daily_backfill_rejects_unsupported_metric_schema():
    with pytest.raises(ValueError, match="unsupported stage metrics schema"):
        normalize_daily_backfill_metrics(
            {"numTasks": 1, "executorRunTime": 10},
            source_rows_for_date=10_000,
        )


def test_lab7_daily_backfill_builds_metrics_record():
    plan, volume = expected_volume_for_date(LAB7_VOLUME_PLAN, "2026-01-07")
    record = build_daily_backfill_metrics_record(
        run_id="batch-1",
        date_run_id="date-1",
        app_name="app",
        application_id="application-1",
        workload_name="temporal_daily_backfill",
        filter_strategy=EARLY_PARTITION_FILTER,
        processing_date="2026-01-07",
        plan=plan,
        date_volume=volume,
        output_path="s3a://lakehouse/gold/lab7/daily_activity_dashboard/processing_date=2026-01-07",
        metrics=DailyBackfillMetrics(
            executor_run_time_ms=100,
            input_bytes=10,
            records_read=100,
            records_written=12,
            shuffle_bytes_read=30,
            shuffle_bytes_written=20,
            memory_bytes_spilled=0,
            disk_bytes_spilled=0,
            jvm_gc_time_ms=5,
            num_stages=2,
            num_tasks=8,
            shuffle_total_bytes=50,
            runtime_per_million_rows=1000.0,
            shuffle_per_million_rows=500.0,
            input_bytes_per_million_rows=100.0,
            tasks_per_million_rows=80.0,
        ),
    )

    assert record["run_id"] == "batch-1"
    assert record["date_run_id"] == "date-1"
    assert record["lab_id"] == "lab_7"
    assert record["processing_date"] == "2026-01-07"
    assert record["source_rows_for_date"] == 100_000
    assert record["volume_multiplier"] == 10
    assert record["spike_label"] == "MEDIUM_SPIKE"
    assert record["collector_type"] == "stage"
    assert record["created_at"]


def test_lab7_volume_plan_rejects_out_of_range_spike_days(tmp_path):
    invalid_plan = tmp_path / "invalid_volume_plan.yaml"
    invalid_plan.write_text(
        """
source:
  name: source_events_temporal
date_range:
  start: "2026-01-01"
  end: "2026-01-02"
base_rows_per_day: 10
spike_days:
  "2026-01-03": 100
generation:
  target_rows_per_partition: 10
  max_partitions_per_day: 2
dimensions:
  accounts: 1
  customers: 1
  vendors: 1
  products: 1
  regions: [BR_SP]
  channels: [APP]
  event_types: [ORDER_CREATED]
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="outside the configured date range"):
        load_temporal_volume_plan(invalid_plan)
