from pathlib import Path

import pytest

from apps.labs.lab_4.lab_4_utils.fingerprint import (
    BALANCED_OR_LOW_SIGNAL,
    GC_PRESSURE,
    MANY_SMALL_TASKS,
    MEMORY_PRESSURE,
    REQUIRED_STAGE_METRICS,
    SHUFFLE_HEAVY,
    FingerprintRules,
    NormalizedStageMetrics,
    StageWorkloadFingerprintSettings,
    classify_workload,
    load_fingerprint_rules,
    load_stage_workload_fingerprint_settings,
    normalize_stage_metrics,
    render_fingerprint_diagnostic_block,
)
from apps.labs.lab_4.lab_4_utils.transformations import WORKLOAD_FINGERPRINT_COLUMNS
from spark_workshop.config import load_experiment_config


LAB4_DIR = Path(__file__).resolve().parents[1] / "src" / "apps" / "labs" / "lab_4"
LAB4_CONFIG = LAB4_DIR / "lab_4_utils" / "experiments.yaml"
LAB4_RULES = LAB4_DIR / "lab_4_utils" / "fingerprint_rules.yaml"


def test_lab4_config_uses_stage_metrics_and_expected_paths():
    config = load_experiment_config(
        "lab4-stage-workload-fingerprint",
        config_path=LAB4_CONFIG,
    )

    assert config.observability.enabled is True
    assert config.observability.collector == "stage"
    assert config.observability.persist is False
    assert (
        config.artifacts.output("workload_fingerprints").path
        == "s3a://observability/lab4/workload_fingerprints"
    )
    assert (
        config.artifacts.output("stage_metrics").path
        == "s3a://observability/lab4/stage_metrics"
    )
    assert (
        config.artifacts.output("workload_summary").path
        == "s3a://lakehouse/gold/lab4/stage_workload_fingerprint/workload_summary"
    )


def test_lab4_workload_settings_are_loaded_from_yaml():
    settings = load_stage_workload_fingerprint_settings(
        "lab4-stage-workload-fingerprint",
        LAB4_CONFIG,
    )

    assert settings == StageWorkloadFingerprintSettings(
        workload_name="retail_stage_fingerprint",
        workload_variant="shuffle_fingerprint",
        success_marker="LAB4_STAGE_WORKLOAD_FINGERPRINT_OK",
        shuffle_partitions=96,
        fingerprint_buckets=512,
    )


def test_lab4_fingerprint_rules_are_loaded_from_yaml():
    rules = load_fingerprint_rules(LAB4_RULES)

    assert rules.high_shuffle_amplification_ratio == 2.0
    assert rules.high_gc_time_ratio == 0.10
    assert rules.spill_detected_bytes == 1
    assert rules.minimum_reliable_input_bytes == 1_048_576
    assert rules.high_total_tasks == 1000


def test_lab4_required_stage_metrics_are_stage_aggregate_fields():
    assert REQUIRED_STAGE_METRICS == ("numStages", "numTasks", "executorRunTime")


def test_lab4_normalizes_actual_sparkmeasure_stage_metric_names():
    normalized = normalize_stage_metrics(
        {
            "numStages": 4,
            "numTasks": 25,
            "executorRunTime": 5000,
            "bytesRead": 1000,
            "shuffleTotalBytesRead": 1500,
            "shuffleBytesWritten": 2500,
            "memoryBytesSpilled": 100,
            "diskBytesSpilled": 50,
            "jvmGCTime": 500,
        }
    )

    assert normalized.input_bytes == 1000
    assert normalized.shuffle_bytes_read == 1500
    assert normalized.shuffle_bytes_written == 2500
    assert normalized.shuffle_total_bytes == 4000
    assert normalized.shuffle_amplification_ratio == 4.0
    assert normalized.gc_time_ratio == 0.1
    assert normalized.spill_ratio == 0.0375
    assert normalized.task_density_score == 6.25


def test_lab4_normalization_rejects_unsupported_metric_schema():
    with pytest.raises(ValueError, match="unsupported stage metrics schema"):
        normalize_stage_metrics({"numTasks": 1, "executorRunTime": 10})


def test_lab4_classifier_detects_shuffle_heavy_workload():
    decision = classify_workload(
        NormalizedStageMetrics(
            executor_run_time_ms=20_000,
            input_bytes=10_000,
            shuffle_bytes_read=200,
            shuffle_bytes_written=400,
            memory_bytes_spilled=0,
            disk_bytes_spilled=0,
            jvm_gc_time_ms=100,
            num_stages=4,
            num_tasks=100,
            shuffle_total_bytes=600,
            shuffle_amplification_ratio=6.0,
            gc_time_ratio=0.005,
            spill_ratio=0.0,
            task_density_score=25.0,
        ),
        FingerprintRules(
            high_shuffle_amplification_ratio=2.0,
            minimum_reliable_input_bytes=1_000,
        ),
    )

    assert decision.workload_profile == SHUFFLE_HEAVY
    assert "HIGH_SHUFFLE_AMPLIFICATION" in decision.diagnostic_flags


def test_lab4_classifier_does_not_trust_tiny_input_bytes_for_amplification():
    decision = classify_workload(
        NormalizedStageMetrics(
            executor_run_time_ms=20_000,
            input_bytes=100,
            shuffle_bytes_read=200,
            shuffle_bytes_written=400,
            memory_bytes_spilled=0,
            disk_bytes_spilled=0,
            jvm_gc_time_ms=100,
            num_stages=4,
            num_tasks=100,
            shuffle_total_bytes=600,
            shuffle_amplification_ratio=6.0,
            gc_time_ratio=0.005,
            spill_ratio=0.0,
            task_density_score=25.0,
        ),
        FingerprintRules(
            high_shuffle_amplification_ratio=2.0,
            minimum_reliable_input_bytes=1_000,
            high_shuffle_total_bytes=1_000_000,
        ),
    )

    assert decision.workload_profile == BALANCED_OR_LOW_SIGNAL
    assert "INPUT_BYTES_LOW_CONFIDENCE_FOR_RATIO" in decision.diagnostic_flags
    assert "HIGH_SHUFFLE_AMPLIFICATION" not in decision.diagnostic_flags


def test_lab4_classifier_prioritizes_spill_as_memory_pressure():
    decision = classify_workload(
        NormalizedStageMetrics(
            executor_run_time_ms=20_000,
            input_bytes=10_000,
            shuffle_bytes_read=200,
            shuffle_bytes_written=400,
            memory_bytes_spilled=1,
            disk_bytes_spilled=0,
            jvm_gc_time_ms=100,
            num_stages=4,
            num_tasks=100,
            shuffle_total_bytes=600,
            shuffle_amplification_ratio=6.0,
            gc_time_ratio=0.005,
            spill_ratio=0.001,
            task_density_score=25.0,
        ),
        FingerprintRules(spill_detected_bytes=1, minimum_reliable_input_bytes=1_000),
    )

    assert decision.workload_profile == MEMORY_PRESSURE
    assert "SPILL_DETECTED" in decision.diagnostic_flags


def test_lab4_classifier_detects_gc_pressure():
    decision = classify_workload(
        NormalizedStageMetrics(
            executor_run_time_ms=10_000,
            input_bytes=0,
            shuffle_bytes_read=0,
            shuffle_bytes_written=0,
            memory_bytes_spilled=0,
            disk_bytes_spilled=0,
            jvm_gc_time_ms=2_000,
            num_stages=4,
            num_tasks=100,
            shuffle_total_bytes=0,
            shuffle_amplification_ratio=None,
            gc_time_ratio=0.2,
            spill_ratio=0.0,
            task_density_score=25.0,
        ),
        FingerprintRules(high_gc_time_ratio=0.10),
    )

    assert decision.workload_profile == GC_PRESSURE
    assert "HIGH_GC_RATIO" in decision.diagnostic_flags


def test_lab4_classifier_detects_many_small_tasks():
    decision = classify_workload(
        NormalizedStageMetrics(
            executor_run_time_ms=10_000,
            input_bytes=0,
            shuffle_bytes_read=0,
            shuffle_bytes_written=0,
            memory_bytes_spilled=0,
            disk_bytes_spilled=0,
            jvm_gc_time_ms=0,
            num_stages=4,
            num_tasks=2_000,
            shuffle_total_bytes=0,
            shuffle_amplification_ratio=None,
            gc_time_ratio=0.0,
            spill_ratio=0.0,
            task_density_score=500.0,
        ),
        FingerprintRules(high_total_tasks=1_000),
    )

    assert decision.workload_profile == MANY_SMALL_TASKS
    assert "TASK_OVERHEAD_SIGNAL" in decision.diagnostic_flags


def test_lab4_classifier_keeps_low_signal_as_valid_result():
    decision = classify_workload(
        NormalizedStageMetrics(
            executor_run_time_ms=100,
            input_bytes=0,
            shuffle_bytes_read=0,
            shuffle_bytes_written=0,
            memory_bytes_spilled=0,
            disk_bytes_spilled=0,
            jvm_gc_time_ms=0,
            num_stages=1,
            num_tasks=1,
            shuffle_total_bytes=0,
            shuffle_amplification_ratio=None,
            gc_time_ratio=0.0,
            spill_ratio=0.0,
            task_density_score=1.0,
        ),
        FingerprintRules(),
    )

    assert decision.workload_profile == BALANCED_OR_LOW_SIGNAL
    assert "LOW_SIGNAL" in decision.diagnostic_flags


def test_lab4_diagnostic_block_uses_classroom_markdown_markers():
    normalized = NormalizedStageMetrics(
        executor_run_time_ms=10_000,
        input_bytes=0,
        shuffle_bytes_read=100,
        shuffle_bytes_written=200,
        memory_bytes_spilled=0,
        disk_bytes_spilled=0,
        jvm_gc_time_ms=100,
        num_stages=4,
        num_tasks=100,
        shuffle_total_bytes=300,
        shuffle_amplification_ratio=None,
        gc_time_ratio=0.01,
        spill_ratio=0.0,
        task_density_score=25.0,
    )
    decision = classify_workload(normalized, FingerprintRules())

    rendered = render_fingerprint_diagnostic_block(normalized, decision, width=80)

    assert "## STAGE WORKLOAD FINGERPRINT DIAGNOSTIC" in rendered
    assert "### Profile" in rendered
    assert "### StageMetrics signals" in rendered
    assert "### Normalized ratios" in rendered
    assert "### Recommended next step" in rendered
    assert "shuffle_amplification_ratio: unavailable" in rendered


def test_lab4_diagnostic_block_marks_low_confidence_shuffle_ratio():
    normalized = NormalizedStageMetrics(
        executor_run_time_ms=10_000,
        input_bytes=100,
        shuffle_bytes_read=300,
        shuffle_bytes_written=300,
        memory_bytes_spilled=0,
        disk_bytes_spilled=0,
        jvm_gc_time_ms=100,
        num_stages=4,
        num_tasks=100,
        shuffle_total_bytes=600,
        shuffle_amplification_ratio=6.0,
        gc_time_ratio=0.01,
        spill_ratio=0.0,
        task_density_score=25.0,
    )
    decision = classify_workload(
        normalized,
        FingerprintRules(minimum_reliable_input_bytes=1_000),
    )

    rendered = render_fingerprint_diagnostic_block(normalized, decision, width=100)

    assert (
        "shuffle_amplification_ratio: 6.0 (not used: low-confidence input_bytes)"
        in rendered
    )


def test_lab4_workload_summary_columns_are_classroom_friendly():
    assert WORKLOAD_FINGERPRINT_COLUMNS == (
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
        "fingerprint_bucket_count",
    )
