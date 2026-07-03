from pathlib import Path

import pytest

from apps.labs.lab_6.lab_6_utils.contract import (
    CORRELATION,
    FAIL,
    PASS,
    REQUIRED_STAGE_METRICS,
    SCHEMA,
    SEMANTIC,
    ContractRuleResult,
    ContractSummary,
    NormalizedStageMetrics,
    StageMetricsContractSettings,
    build_invalid_demo_records,
    build_stage_metrics_record,
    layer_decision,
    load_contract_rules,
    load_stage_metrics_contract_settings,
    normalize_stage_metrics,
    render_contract_gate_block,
)
from apps.labs.lab_6.lab_6_utils.transformations import (
    CONTRACT_GATE_OUTPUT_COLUMNS,
)
from spark_workshop.config import load_experiment_config


LAB6_DIR = Path(__file__).resolve().parents[1] / "src" / "apps" / "labs" / "lab_6"
LAB6_CONFIG = LAB6_DIR / "lab_6_utils" / "experiments.yaml"
LAB6_RULES = LAB6_DIR / "lab_6_utils" / "contract_rules.yaml"


def test_lab6_config_uses_stage_metrics_and_expected_paths():
    config = load_experiment_config(
        "lab6-stage-metrics-contract-gate",
        config_path=LAB6_CONFIG,
    )

    assert config.observability.enabled is True
    assert config.observability.collector == "stage"
    assert config.observability.persist is False
    assert (
        config.artifacts.output("stage_metrics_raw").path
        == "s3a://observability/lab6/stage_metrics_raw"
    )
    assert (
        config.artifacts.output("stage_metrics_contract_results").path
        == "s3a://observability/lab6/stage_metrics_contract_results"
    )
    assert (
        config.artifacts.output("stage_metrics_contract_summary").path
        == "s3a://observability/lab6/stage_metrics_contract_summary"
    )
    assert (
        config.artifacts.output("business_output").path
        == "s3a://lakehouse/gold/lab6/stage_metrics_contract_gate/business_output"
    )


def test_lab6_workload_settings_are_loaded_from_yaml():
    settings = load_stage_metrics_contract_settings(
        "lab6-stage-metrics-contract-gate",
        LAB6_CONFIG,
    )

    assert settings == StageMetricsContractSettings(
        workload_name="retail_stage_metrics_contract_gate",
        workload_variant="contract_ready_metrics",
        success_marker="LAB6_STAGE_METRICS_CONTRACT_GATE_OK",
        shuffle_partitions=64,
    )


def test_lab6_contract_rules_are_loaded_from_yaml():
    rules = load_contract_rules(LAB6_RULES)

    assert rules.version == "1.0.0"
    assert "run_id" in rules.required_columns
    assert "executor_run_time_ms" in rules.required_columns
    assert rules.required_metrics == (
        "num_stages",
        "num_tasks",
        "executor_run_time_ms",
    )
    assert "shuffle_bytes_written" in rules.optional_metrics
    assert rules.semantic_rules.num_stages_gt_zero is True
    assert "shuffle_bytes_written" in rules.semantic_rules.non_negative_metrics
    assert rules.correlation_rules.expected_values == {
        "collector_name": "sparkmeasure_stage_metrics",
        "metric_scope": "stage",
    }
    assert rules.correlation_rules.uniqueness_key == (
        "run_id",
        "workload_name",
        "workload_variant",
        "metric_scope",
    )


def test_lab6_required_stage_metrics_are_stage_aggregate_fields():
    assert REQUIRED_STAGE_METRICS == ("numStages", "numTasks", "executorRunTime")


def test_lab6_output_columns_match_business_contract():
    assert CONTRACT_GATE_OUTPUT_COLUMNS == (
        "order_month",
        "region",
        "category",
        "gross_revenue",
        "order_count",
        "customer_count",
    )


def test_lab6_normalizes_actual_sparkmeasure_stage_metric_names():
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

    assert normalized.executor_run_time_ms == 5000
    assert normalized.shuffle_bytes_read == 1500
    assert normalized.shuffle_bytes_written == 2500
    assert normalized.jvm_gc_time_ms == 500
    assert normalized.memory_bytes_spilled == 100
    assert normalized.disk_bytes_spilled == 50
    assert normalized.input_bytes == 1000
    assert normalized.num_stages == 4
    assert normalized.num_tasks == 25
    assert normalized.shuffle_bytes_written_available is True
    assert normalized.shuffle_bytes_read_available is True
    assert normalized.jvm_gc_time_ms_available is True
    assert normalized.memory_bytes_spilled_available is True
    assert normalized.disk_bytes_spilled_available is True
    assert normalized.input_bytes_available is True


def test_lab6_distinguishes_unavailable_optional_metrics_from_zero_values():
    normalized = normalize_stage_metrics(
        {
            "numStages": 4,
            "numTasks": 25,
            "executorRunTime": 5000,
        }
    )

    assert normalized.shuffle_bytes_written == 0
    assert normalized.shuffle_bytes_written_available is False
    assert normalized.shuffle_bytes_read == 0
    assert normalized.shuffle_bytes_read_available is False


def test_lab6_normalization_rejects_unsupported_metric_schema():
    with pytest.raises(ValueError, match="unsupported stage metrics schema"):
        normalize_stage_metrics({"numTasks": 1, "executorRunTime": 10})


def test_lab6_builds_contract_ready_raw_metrics_record():
    record = build_stage_metrics_record(
        run_id="run-1",
        app_name="app",
        application_id="application-1",
        settings=StageMetricsContractSettings(
            workload_name="retail_contract",
            workload_variant="stage",
        ),
        contract_version="1.0.0",
        metrics=NormalizedStageMetrics(
            executor_run_time_ms=100,
            shuffle_bytes_written=20,
            shuffle_bytes_read=30,
            jvm_gc_time_ms=5,
            memory_bytes_spilled=0,
            disk_bytes_spilled=0,
            input_bytes=10,
            num_stages=2,
            num_tasks=8,
            shuffle_bytes_written_available=True,
            shuffle_bytes_read_available=True,
            jvm_gc_time_ms_available=True,
            memory_bytes_spilled_available=True,
            disk_bytes_spilled_available=True,
            input_bytes_available=True,
        ),
    )

    assert record["run_id"] == "run-1"
    assert record["lab_id"] == "lab_6"
    assert record["collector_name"] == "sparkmeasure_stage_metrics"
    assert record["metric_scope"] == "stage"
    assert record["contract_version"] == "1.0.0"
    assert record["num_stages"] == 2
    assert record["num_tasks"] == 8
    assert record["shuffle_bytes_written_available"] is True
    assert record["input_bytes_available"] is True
    assert record["created_at"]


def test_lab6_invalid_demo_records_do_not_mutate_clean_record():
    clean = {
        "run_id": "run-1",
        "app_name": "app",
        "application_id": "application-1",
        "lab_id": "lab_6",
        "workload_name": "retail_contract",
        "workload_variant": "stage",
        "collector_name": "sparkmeasure_stage_metrics",
        "metric_scope": "stage",
        "contract_version": "1.0.0",
        "created_at": "2026-07-03T00:00:00+00:00",
        "num_stages": 2,
        "num_tasks": 8,
        "executor_run_time_ms": 100,
        "shuffle_bytes_written": 20,
        "shuffle_bytes_read": 30,
        "jvm_gc_time_ms": 5,
        "memory_bytes_spilled": 0,
        "disk_bytes_spilled": 0,
        "input_bytes": 10,
        "shuffle_bytes_written_available": True,
        "shuffle_bytes_read_available": True,
        "jvm_gc_time_ms_available": True,
        "memory_bytes_spilled_available": True,
        "disk_bytes_spilled_available": True,
        "input_bytes_available": True,
    }

    records = build_invalid_demo_records(clean)

    assert clean["run_id"] == "run-1"
    assert records[0] == clean
    assert any(record["run_id"] is None for record in records)
    assert any(record["num_stages"] == 0 for record in records)
    assert any(record["num_tasks"] == 0 for record in records)
    assert any(record["shuffle_bytes_written"] == -1 for record in records)
    assert any(record["metric_scope"] == "task" for record in records)
    assert sum(record == clean for record in records) == 2


def test_lab6_layer_decision_fails_when_one_layer_rule_fails():
    results = [
        _result("schema", SCHEMA, PASS),
        _result("semantic", SEMANTIC, PASS),
        _result("correlation", CORRELATION, FAIL),
    ]

    assert layer_decision(results, SCHEMA) == PASS
    assert layer_decision(results, SEMANTIC) == PASS
    assert layer_decision(results, CORRELATION) == FAIL


def test_lab6_render_contract_block_is_classroom_friendly():
    results = [
        _result("schema", SCHEMA, PASS),
        _result("semantic", SEMANTIC, PASS),
        _result("correlation", CORRELATION, FAIL),
    ]
    summary = ContractSummary(
        validation_run_id="validation-1",
        source_path="s3a://observability/lab6/stage_metrics_contract_demo_input",
        contract_version="1.0.0",
        total_rules=3,
        passed_rules=2,
        failed_rules=1,
        decision=FAIL,
        created_at="2026-07-03T00:00:00+00:00",
    )

    rendered = render_contract_gate_block(
        summary=summary,
        results=results,
        raw_metrics_path="s3a://observability/lab6/stage_metrics_raw",
        validation_input_path="s3a://observability/lab6/stage_metrics_contract_demo_input",
        results_output_path="s3a://observability/lab6/stage_metrics_contract_results",
        summary_output_path="s3a://observability/lab6/stage_metrics_contract_summary",
        demo_mode=True,
    )

    assert "## LAB 6 STAGE METRICS CONTRACT GATE" in rendered
    assert "decision: FAIL" in rendered
    assert "### Contract layers" in rendered
    assert "correlation: FAIL" in rendered


def _result(rule_id: str, rule_type: str, decision: str) -> ContractRuleResult:
    return ContractRuleResult(
        validation_run_id="validation-1",
        source_path="s3a://observability/lab6/source",
        contract_version="1.0.0",
        rule_id=rule_id,
        rule_name=rule_id,
        rule_type=rule_type,
        severity="ERROR",
        decision=decision,
        failed_count=1 if decision == FAIL else 0,
        sample_failed_keys="sample" if decision == FAIL else "",
        recommendation="fix the metric contract",
        created_at="2026-07-03T00:00:00+00:00",
    )
