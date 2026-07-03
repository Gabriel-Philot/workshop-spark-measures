from pathlib import Path

import pytest

from apps.labs.lab_5.lab_5_utils.budget import (
    FAIL,
    MEMORY_PRESSURE,
    PASS,
    REQUIRED_STAGE_METRICS,
    SHUFFLE_HEAVY,
    WARNING_LOW_SIGNAL,
    LowSignalRules,
    OutputCompatibilityResult,
    RuntimeBudgetComparison,
    RuntimeBudgetDecision,
    RuntimeBudgetRules,
    RuntimeBudgetSettings,
    RuntimeBudgetThresholds,
    StageRuntimeMetrics,
    apply_runtime_budget,
    compare_stage_metrics,
    load_budget_rules,
    load_runtime_budget_settings,
    normalize_stage_metrics,
    render_budget_decision_block,
)
from apps.labs.lab_5.lab_5_utils.transformations import (
    RUNTIME_BUDGET_OUTPUT_COLUMNS,
)
from spark_workshop.config import load_experiment_config


LAB5_DIR = Path(__file__).resolve().parents[1] / "src" / "apps" / "labs" / "lab_5"
LAB5_CONFIG = LAB5_DIR / "lab_5_utils" / "experiments.yaml"
LAB5_RULES = LAB5_DIR / "lab_5_utils" / "budget_rules.yaml"


def test_lab5_config_uses_stage_metrics_and_expected_paths():
    config = load_experiment_config(
        "lab5-stage-runtime-budget-guardrail",
        config_path=LAB5_CONFIG,
    )

    assert config.observability.enabled is True
    assert config.observability.collector == "stage"
    assert config.observability.persist is False
    assert (
        config.artifacts.output("stage_runtime_budget_runs").path
        == "s3a://observability/lab5/stage_runtime_budget_runs"
    )
    assert (
        config.artifacts.output("stage_runtime_budget_decisions").path
        == "s3a://observability/lab5/stage_runtime_budget_decisions"
    )
    assert (
        config.artifacts.output("baseline_business_output").path
        == "s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline"
    )
    assert (
        config.artifacts.output("candidate_business_output").path
        == "s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate"
    )


def test_lab5_runtime_budget_settings_are_loaded_from_yaml():
    settings = load_runtime_budget_settings(
        "lab5-stage-runtime-budget-guardrail",
        LAB5_CONFIG,
    )

    assert settings == RuntimeBudgetSettings(
        workload_name="retail_runtime_budget_guardrail",
        baseline_variant="approved_baseline",
        candidate_variant="candidate_pr_regression",
        success_marker="LAB5_STAGE_RUNTIME_BUDGET_GUARDRAIL_OK",
        baseline_keyed_partitions=48,
        candidate_round_robin_partitions=192,
        candidate_keyed_partitions=192,
        candidate_guardrail_buckets=1024,
        revenue_tolerance=0.01,
    )


def test_lab5_budget_rules_are_loaded_from_yaml():
    rules = load_budget_rules(LAB5_RULES)

    assert rules.default_budget.max_executor_runtime_growth_pct == 20.0
    assert rules.default_budget.max_shuffle_written_growth_pct == 25.0
    assert rules.low_signal.min_executor_runtime_ms == 1000
    assert rules.low_signal.min_shuffle_bytes == 1_048_576
    assert rules.profiles["shuffle_heavy"].max_shuffle_written_growth_pct == 15.0


def test_lab5_required_stage_metrics_are_stage_aggregate_fields():
    assert REQUIRED_STAGE_METRICS == ("numStages", "numTasks", "executorRunTime")


def test_lab5_output_columns_match_business_contract():
    assert RUNTIME_BUDGET_OUTPUT_COLUMNS == (
        "order_month",
        "region",
        "category",
        "gross_revenue",
        "order_count",
        "customer_count",
    )


def test_lab5_normalizes_actual_sparkmeasure_stage_metric_names():
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
            "recordsRead": 200,
            "recordsWritten": 10,
        }
    )

    assert normalized.executor_run_time_ms == 5000
    assert normalized.shuffle_bytes_read == 1500
    assert normalized.shuffle_bytes_written == 2500
    assert normalized.shuffle_total_bytes == 4000
    assert normalized.spill_total_bytes == 150
    assert normalized.jvm_gc_time_ms == 500
    assert normalized.input_bytes == 1000
    assert normalized.records_read == 200
    assert normalized.records_written == 10


def test_lab5_normalization_rejects_unsupported_metric_schema():
    with pytest.raises(ValueError, match="unsupported stage metrics schema"):
        normalize_stage_metrics({"numTasks": 1, "executorRunTime": 10})


def test_lab5_compare_stage_metrics_computes_candidate_growth():
    baseline = _metrics(
        executor_run_time_ms=1000,
        shuffle_bytes_written=100,
        shuffle_bytes_read=200,
        num_tasks=10,
        num_stages=4,
    )
    candidate = _metrics(
        executor_run_time_ms=1250,
        shuffle_bytes_written=150,
        shuffle_bytes_read=300,
        num_tasks=15,
        num_stages=5,
    )

    comparison = compare_stage_metrics(baseline, candidate)

    assert comparison.executor_run_time_delta_pct == 25.0
    assert comparison.shuffle_written_delta_pct == 50.0
    assert comparison.shuffle_read_delta_pct == 50.0
    assert comparison.num_tasks_delta_pct == 50.0
    assert comparison.num_stages_delta_pct == 25.0


def test_lab5_budget_passes_candidate_inside_thresholds():
    rules = RuntimeBudgetRules(
        default_budget=RuntimeBudgetThresholds(
            max_executor_runtime_growth_pct=20.0,
            max_shuffle_written_growth_pct=20.0,
            max_shuffle_read_growth_pct=20.0,
            max_num_tasks_growth_pct=20.0,
            max_num_stages_growth_pct=20.0,
            fail_on_memory_spill_bytes_above=0,
            fail_on_disk_spill_bytes_above=0,
        ),
        low_signal=LowSignalRules(min_executor_runtime_ms=100, min_shuffle_bytes=100),
        profiles={},
    )

    decision = apply_runtime_budget(
        baseline=_metrics(executor_run_time_ms=5000, shuffle_bytes_written=1000),
        candidate=_metrics(executor_run_time_ms=5050, shuffle_bytes_written=1010),
        rules=rules,
    )

    assert decision.decision == PASS
    assert decision.failed_rules == ()


def test_lab5_budget_fails_candidate_regression():
    rules = RuntimeBudgetRules(
        default_budget=RuntimeBudgetThresholds(max_num_tasks_growth_pct=10.0),
        low_signal=LowSignalRules(min_executor_runtime_ms=100, min_shuffle_bytes=100),
        profiles={},
    )

    decision = apply_runtime_budget(
        baseline=_metrics(executor_run_time_ms=5000, num_tasks=100),
        candidate=_metrics(executor_run_time_ms=5000, num_tasks=200),
        rules=rules,
    )

    assert decision.decision == FAIL
    assert "MAX_NUM_TASKS_GROWTH_PCT" in decision.failed_rules


def test_lab5_budget_reports_warning_low_signal_instead_of_fail():
    rules = RuntimeBudgetRules(
        default_budget=RuntimeBudgetThresholds(max_num_tasks_growth_pct=10.0),
        low_signal=LowSignalRules(
            min_executor_runtime_ms=1000,
            min_shuffle_bytes=1_048_576,
        ),
        profiles={},
    )

    decision = apply_runtime_budget(
        baseline=_metrics(executor_run_time_ms=100, shuffle_bytes_written=100, num_tasks=1),
        candidate=_metrics(executor_run_time_ms=120, shuffle_bytes_written=200, num_tasks=3),
        rules=rules,
    )

    assert decision.decision == WARNING_LOW_SIGNAL
    assert "LOW_SIGNAL_LOCAL_RUN" in decision.warning_flags


def test_lab5_budget_uses_profile_specific_thresholds():
    rules = RuntimeBudgetRules(
        default_budget=RuntimeBudgetThresholds(max_shuffle_written_growth_pct=100.0),
        low_signal=LowSignalRules(min_executor_runtime_ms=100, min_shuffle_bytes=100),
        profiles={
            "shuffle_heavy": RuntimeBudgetThresholds(
                max_shuffle_written_growth_pct=10.0,
            )
        },
    )

    decision = apply_runtime_budget(
        baseline=_metrics(
            executor_run_time_ms=5000,
            shuffle_bytes_written=1_000_000,
            shuffle_bytes_read=1_000_000,
        ),
        candidate=_metrics(
            executor_run_time_ms=5000,
            shuffle_bytes_written=1_200_000,
            shuffle_bytes_read=1_200_000,
        ),
        rules=rules,
    )

    assert decision.workload_profile == SHUFFLE_HEAVY
    assert decision.decision == FAIL
    assert "MAX_SHUFFLE_WRITTEN_GROWTH_PCT" in decision.failed_rules


def test_lab5_budget_classifies_spill_as_memory_pressure():
    decision = apply_runtime_budget(
        baseline=_metrics(executor_run_time_ms=5000),
        candidate=_metrics(executor_run_time_ms=5000, memory_bytes_spilled=1),
        rules=RuntimeBudgetRules(
            default_budget=RuntimeBudgetThresholds(),
            low_signal=LowSignalRules(min_executor_runtime_ms=100, min_shuffle_bytes=100),
            profiles={},
        ),
    )

    assert decision.workload_profile == MEMORY_PRESSURE
    assert decision.decision == FAIL
    assert "MEMORY_SPILL_BYTES_ABOVE_BUDGET" in decision.failed_rules


def test_lab5_budget_does_not_fail_existing_baseline_spill_as_candidate_regression():
    decision = apply_runtime_budget(
        baseline=_metrics(
            executor_run_time_ms=5000,
            shuffle_bytes_written=1_000_000,
            shuffle_bytes_read=1_000_000,
            memory_bytes_spilled=100,
            disk_bytes_spilled=50,
        ),
        candidate=_metrics(
            executor_run_time_ms=5000,
            shuffle_bytes_written=1_000_100,
            shuffle_bytes_read=1_000_100,
            memory_bytes_spilled=100,
            disk_bytes_spilled=50,
        ),
        rules=RuntimeBudgetRules(
            default_budget=RuntimeBudgetThresholds(
                max_shuffle_written_growth_pct=10.0,
                max_shuffle_read_growth_pct=10.0,
                fail_on_memory_spill_bytes_above=0,
                fail_on_disk_spill_bytes_above=0,
            ),
            low_signal=LowSignalRules(min_executor_runtime_ms=100, min_shuffle_bytes=100),
            profiles={},
        ),
    )

    assert decision.workload_profile == SHUFFLE_HEAVY
    assert "MEMORY_SPILL_BYTES_ABOVE_BUDGET" not in decision.failed_rules
    assert "DISK_SPILL_BYTES_ABOVE_BUDGET" not in decision.failed_rules


def test_lab5_decision_block_uses_visible_classroom_markers():
    rendered = render_budget_decision_block(
        decision=RuntimeBudgetDecision(
            workload_profile=SHUFFLE_HEAVY,
            decision=FAIL,
            failed_rules=("MAX_NUM_TASKS_GROWTH_PCT",),
            warning_flags=(),
            comparison=RuntimeBudgetComparison(
                executor_run_time_delta_pct=10.0,
                shuffle_written_delta_pct=20.0,
                shuffle_read_delta_pct=20.0,
                num_tasks_delta_pct=50.0,
                num_stages_delta_pct=0.0,
                gc_time_delta_pct=0.0,
                spill_delta_pct=0.0,
            ),
        ),
        baseline=_metrics(executor_run_time_ms=1000, num_tasks=10),
        candidate=_metrics(executor_run_time_ms=1100, num_tasks=15),
        compatibility=OutputCompatibilityResult(
            baseline_row_count=10,
            candidate_row_count=10,
            baseline_total_revenue=100.0,
            candidate_total_revenue=100.0,
            baseline_total_order_count=50,
            candidate_total_order_count=50,
        ),
        metrics_output_path="s3a://observability/lab5/stage_runtime_budget_runs",
        decisions_output_path="s3a://observability/lab5/stage_runtime_budget_decisions",
        baseline_output_path="s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline",
        candidate_output_path="s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate",
        width=96,
    )

    assert "## LAB 5 RUNTIME BUDGET GUARDRAIL" in rendered
    assert "### Final decision" in rendered
    assert "decision: FAIL" in rendered
    assert "### Functional compatibility" in rendered
    assert "### Baseline StageMetrics" in rendered
    assert "### Candidate StageMetrics" in rendered
    assert "### Candidate delta versus baseline" in rendered
    assert "executor runtime: 1000 ms -> 1100 ms | +10.00% | 1.10x baseline" in rendered
    assert "tasks: 10 -> 15 | +50.00% | 1.50x baseline" in rendered
    assert "### Delta outputs" in rendered


def _metrics(**overrides):
    values = {
        "executor_run_time_ms": 5000,
        "shuffle_bytes_written": 1000,
        "shuffle_bytes_read": 1000,
        "memory_bytes_spilled": 0,
        "disk_bytes_spilled": 0,
        "jvm_gc_time_ms": 0,
        "num_stages": 4,
        "num_tasks": 100,
        "input_bytes": 0,
        "records_read": 0,
        "records_written": 0,
    }
    values.update(overrides)
    return StageRuntimeMetrics(**values)
