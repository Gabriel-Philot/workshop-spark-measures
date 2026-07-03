"""Lab 5 runtime budget settings, StageMetrics normalization, and decisions."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml


PASS = "PASS"
FAIL = "FAIL"
WARNING_LOW_SIGNAL = "WARNING_LOW_SIGNAL"

SHUFFLE_HEAVY = "SHUFFLE_HEAVY"
MEMORY_PRESSURE = "MEMORY_PRESSURE"
BALANCED_OR_LOW_SIGNAL = "BALANCED_OR_LOW_SIGNAL"

REQUIRED_STAGE_METRICS = ("numStages", "numTasks", "executorRunTime")


@dataclass(frozen=True)
class RuntimeBudgetSettings:
    """Classroom workload settings loaded from the selected Lab 5 experiment."""

    workload_name: str = "retail_runtime_budget_guardrail"
    baseline_variant: str = "approved_baseline"
    candidate_variant: str = "candidate_pr_regression"
    success_marker: str = "LAB5_STAGE_RUNTIME_BUDGET_GUARDRAIL_OK"
    baseline_keyed_partitions: int = 48
    candidate_round_robin_partitions: int = 192
    candidate_keyed_partitions: int = 192
    candidate_guardrail_buckets: int = 1024
    revenue_tolerance: float = 0.01


@dataclass(frozen=True)
class StageRuntimeMetrics:
    """Normalized sparkMeasure StageMetrics fields used by the guardrail."""

    executor_run_time_ms: int
    shuffle_bytes_written: int
    shuffle_bytes_read: int
    memory_bytes_spilled: int
    disk_bytes_spilled: int
    jvm_gc_time_ms: int
    num_stages: int
    num_tasks: int
    input_bytes: int
    records_read: int
    records_written: int

    @property
    def shuffle_total_bytes(self) -> int:
        return self.shuffle_bytes_written + self.shuffle_bytes_read

    @property
    def spill_total_bytes(self) -> int:
        return self.memory_bytes_spilled + self.disk_bytes_spilled


@dataclass(frozen=True)
class RuntimeBudgetThresholds:
    """Budget thresholds applied to candidate growth versus baseline."""

    max_executor_runtime_growth_pct: float = 20.0
    max_shuffle_written_growth_pct: float = 25.0
    max_shuffle_read_growth_pct: float = 25.0
    max_num_tasks_growth_pct: float = 35.0
    max_num_stages_growth_pct: float = 25.0
    fail_on_memory_spill_bytes_above: int = 0
    fail_on_disk_spill_bytes_above: int = 0


@dataclass(frozen=True)
class LowSignalRules:
    """Minimum evidence required before treating a local run as decisive."""

    min_executor_runtime_ms: int = 1000
    min_shuffle_bytes: int = 1024 * 1024


@dataclass(frozen=True)
class RuntimeBudgetRules:
    """All Lab 5 rules loaded from budget_rules.yaml."""

    default_budget: RuntimeBudgetThresholds
    low_signal: LowSignalRules
    profiles: Mapping[str, RuntimeBudgetThresholds]


@dataclass(frozen=True)
class RuntimeBudgetComparison:
    """Candidate-vs-baseline percentage deltas."""

    executor_run_time_delta_pct: float
    shuffle_written_delta_pct: float
    shuffle_read_delta_pct: float
    num_tasks_delta_pct: float
    num_stages_delta_pct: float
    gc_time_delta_pct: float
    spill_delta_pct: float


@dataclass(frozen=True)
class RuntimeBudgetDecision:
    """Final guardrail decision and the evidence used to produce it."""

    workload_profile: str
    decision: str
    failed_rules: tuple[str, ...]
    warning_flags: tuple[str, ...]
    comparison: RuntimeBudgetComparison

    @property
    def rendered_failed_rules(self) -> str:
        return ",".join(self.failed_rules)

    @property
    def rendered_warning_flags(self) -> str:
        return ",".join(self.warning_flags)

    @property
    def final_marker(self) -> str:
        return f"LAB5_RUNTIME_BUDGET_{self.decision}"


@dataclass(frozen=True)
class OutputCompatibilityResult:
    """Functional compatibility evidence for baseline and candidate outputs."""

    baseline_row_count: int
    candidate_row_count: int
    baseline_total_revenue: float
    candidate_total_revenue: float
    baseline_total_order_count: int
    candidate_total_order_count: int


def load_runtime_budget_settings(
    config_name: str,
    config_path: Path,
) -> RuntimeBudgetSettings:
    """Read Lab 5 workload settings from the local YAML config."""

    raw = _load_yaml(config_path)
    experiments = raw.get("experiments") or {}
    if config_name not in experiments:
        raise KeyError(
            f"Unknown Lab 5 experiment '{config_name}'. "
            f"Available experiments: {sorted(experiments)}"
        )

    workload = (experiments[config_name] or {}).get("workload") or {}
    return RuntimeBudgetSettings(
        workload_name=str(workload.get("workload_name", "retail_runtime_budget_guardrail")),
        baseline_variant=str(workload.get("baseline_variant", "approved_baseline")),
        candidate_variant=str(workload.get("candidate_variant", "candidate_pr_regression")),
        success_marker=str(
            workload.get("success_marker", "LAB5_STAGE_RUNTIME_BUDGET_GUARDRAIL_OK")
        ),
        baseline_keyed_partitions=_positive_int(
            workload.get("baseline_keyed_partitions", 48),
            "baseline_keyed_partitions",
        ),
        candidate_round_robin_partitions=_positive_int(
            workload.get("candidate_round_robin_partitions", 192),
            "candidate_round_robin_partitions",
        ),
        candidate_keyed_partitions=_positive_int(
            workload.get("candidate_keyed_partitions", 192),
            "candidate_keyed_partitions",
        ),
        candidate_guardrail_buckets=_positive_int(
            workload.get("candidate_guardrail_buckets", 1024),
            "candidate_guardrail_buckets",
        ),
        revenue_tolerance=_non_negative_float(
            workload.get("revenue_tolerance", 0.01),
            "revenue_tolerance",
        ),
    )


def load_budget_rules(rules_path: Path) -> RuntimeBudgetRules:
    """Read Lab 5 budget thresholds from YAML."""

    raw = _load_yaml(rules_path)
    default_budget = _thresholds(raw.get("default_budget") or {})
    low_signal = raw.get("low_signal") or {}
    profiles_raw = raw.get("profiles") or {}
    if not isinstance(profiles_raw, Mapping):
        raise ValueError(f"Lab 5 budget rules require profiles to be a mapping: {rules_path}")

    return RuntimeBudgetRules(
        default_budget=default_budget,
        low_signal=LowSignalRules(
            min_executor_runtime_ms=_non_negative_int(
                low_signal.get("min_executor_runtime_ms", 1000),
                "min_executor_runtime_ms",
            ),
            min_shuffle_bytes=_non_negative_int(
                low_signal.get("min_shuffle_bytes", 1024 * 1024),
                "min_shuffle_bytes",
            ),
        ),
        profiles={
            str(name): _thresholds(
                profile,
                base=default_budget,
            )
            for name, profile in profiles_raw.items()
        },
    )


def normalize_stage_metrics(metrics: Mapping[str, int | float]) -> StageRuntimeMetrics:
    """Map actual sparkMeasure StageMetrics aggregate names to Lab 5 fields."""

    missing = tuple(key for key in REQUIRED_STAGE_METRICS if key not in metrics)
    if missing:
        raise ValueError(
            "Lab 5 received an unsupported stage metrics schema. "
            f"Missing required metrics: {', '.join(missing)}"
        )

    normalized = StageRuntimeMetrics(
        executor_run_time_ms=_metric_int(metrics, "executorRunTime"),
        shuffle_bytes_written=_metric_int(metrics, "shuffleBytesWritten"),
        shuffle_bytes_read=_metric_int(metrics, "shuffleTotalBytesRead"),
        memory_bytes_spilled=_metric_int(metrics, "memoryBytesSpilled"),
        disk_bytes_spilled=_metric_int(metrics, "diskBytesSpilled"),
        jvm_gc_time_ms=_metric_int(metrics, "jvmGCTime"),
        num_stages=_metric_int(metrics, "numStages"),
        num_tasks=_metric_int(metrics, "numTasks"),
        input_bytes=_metric_int(metrics, "bytesRead"),
        records_read=_metric_int(metrics, "recordsRead"),
        records_written=_metric_int(metrics, "recordsWritten"),
    )
    if normalized.num_stages < 1 or normalized.num_tasks < 1:
        raise ValueError(
            "Lab 5 captured no useful stage-level metrics: "
            f"numStages={normalized.num_stages}, numTasks={normalized.num_tasks}"
        )
    return normalized


def classify_budget_profile(
    baseline: StageRuntimeMetrics,
    candidate: StageRuntimeMetrics,
) -> str:
    """Assign a lightweight profile so the guardrail can annotate its policy."""

    if (
        baseline.spill_total_bytes == 0
        and candidate.spill_total_bytes > baseline.spill_total_bytes
    ):
        return MEMORY_PRESSURE
    if candidate.shuffle_total_bytes >= 1024 * 1024:
        return SHUFFLE_HEAVY
    return BALANCED_OR_LOW_SIGNAL


def compare_stage_metrics(
    baseline: StageRuntimeMetrics,
    candidate: StageRuntimeMetrics,
) -> RuntimeBudgetComparison:
    """Compute candidate growth percentages relative to baseline."""

    return RuntimeBudgetComparison(
        executor_run_time_delta_pct=_pct_delta(
            baseline.executor_run_time_ms,
            candidate.executor_run_time_ms,
        ),
        shuffle_written_delta_pct=_pct_delta(
            baseline.shuffle_bytes_written,
            candidate.shuffle_bytes_written,
        ),
        shuffle_read_delta_pct=_pct_delta(
            baseline.shuffle_bytes_read,
            candidate.shuffle_bytes_read,
        ),
        num_tasks_delta_pct=_pct_delta(baseline.num_tasks, candidate.num_tasks),
        num_stages_delta_pct=_pct_delta(baseline.num_stages, candidate.num_stages),
        gc_time_delta_pct=_pct_delta(baseline.jvm_gc_time_ms, candidate.jvm_gc_time_ms),
        spill_delta_pct=_pct_delta(baseline.spill_total_bytes, candidate.spill_total_bytes),
    )


def apply_runtime_budget(
    *,
    baseline: StageRuntimeMetrics,
    candidate: StageRuntimeMetrics,
    rules: RuntimeBudgetRules,
) -> RuntimeBudgetDecision:
    """Apply Lab 5 runtime budget rules to candidate-vs-baseline metrics."""

    workload_profile = classify_budget_profile(baseline, candidate)
    thresholds = rules.profiles.get(
        workload_profile.lower(),
        rules.default_budget,
    )
    comparison = compare_stage_metrics(baseline, candidate)
    warning_flags: list[str] = []
    failed_rules: list[str] = []

    if (
        max(baseline.executor_run_time_ms, candidate.executor_run_time_ms)
        < rules.low_signal.min_executor_runtime_ms
        and max(baseline.shuffle_total_bytes, candidate.shuffle_total_bytes)
        < rules.low_signal.min_shuffle_bytes
    ):
        warning_flags.append("LOW_SIGNAL_LOCAL_RUN")

    _append_growth_failure(
        failed_rules,
        "MAX_EXECUTOR_RUNTIME_GROWTH_PCT",
        comparison.executor_run_time_delta_pct,
        thresholds.max_executor_runtime_growth_pct,
    )
    _append_growth_failure(
        failed_rules,
        "MAX_SHUFFLE_WRITTEN_GROWTH_PCT",
        comparison.shuffle_written_delta_pct,
        thresholds.max_shuffle_written_growth_pct,
    )
    _append_growth_failure(
        failed_rules,
        "MAX_SHUFFLE_READ_GROWTH_PCT",
        comparison.shuffle_read_delta_pct,
        thresholds.max_shuffle_read_growth_pct,
    )
    _append_growth_failure(
        failed_rules,
        "MAX_NUM_TASKS_GROWTH_PCT",
        comparison.num_tasks_delta_pct,
        thresholds.max_num_tasks_growth_pct,
    )
    _append_growth_failure(
        failed_rules,
        "MAX_NUM_STAGES_GROWTH_PCT",
        comparison.num_stages_delta_pct,
        thresholds.max_num_stages_growth_pct,
    )

    if (
        baseline.memory_bytes_spilled <= thresholds.fail_on_memory_spill_bytes_above
        and candidate.memory_bytes_spilled > thresholds.fail_on_memory_spill_bytes_above
    ):
        failed_rules.append("MEMORY_SPILL_BYTES_ABOVE_BUDGET")
    if (
        baseline.disk_bytes_spilled <= thresholds.fail_on_disk_spill_bytes_above
        and candidate.disk_bytes_spilled > thresholds.fail_on_disk_spill_bytes_above
    ):
        failed_rules.append("DISK_SPILL_BYTES_ABOVE_BUDGET")

    if warning_flags:
        decision = WARNING_LOW_SIGNAL
    elif failed_rules:
        decision = FAIL
    else:
        decision = PASS

    return RuntimeBudgetDecision(
        workload_profile=workload_profile,
        decision=decision,
        failed_rules=tuple(dict.fromkeys(failed_rules)),
        warning_flags=tuple(dict.fromkeys(warning_flags)),
        comparison=comparison,
    )


def validate_business_outputs(
    spark: Any,
    *,
    baseline_path: str,
    candidate_path: str,
    revenue_tolerance: float,
) -> OutputCompatibilityResult:
    """Validate that baseline and candidate are functionally comparable."""

    from pyspark.sql import functions as F

    baseline = spark.read.format("delta").load(baseline_path)
    candidate = spark.read.format("delta").load(candidate_path)

    if baseline.schema != candidate.schema:
        raise RuntimeError(
            "Lab 5 baseline and candidate outputs have incompatible schemas: "
            f"baseline={baseline.schema.simpleString()} "
            f"candidate={candidate.schema.simpleString()}"
        )

    baseline_count = baseline.count()
    candidate_count = candidate.count()
    if baseline_count != candidate_count:
        raise RuntimeError(
            "Lab 5 baseline and candidate outputs have different row counts: "
            f"baseline={baseline_count} candidate={candidate_count}"
        )

    baseline_totals = baseline.agg(
        F.round(F.sum("gross_revenue"), 2).alias("total_revenue"),
        F.sum("order_count").cast("long").alias("total_order_count"),
    ).first()
    candidate_totals = candidate.agg(
        F.round(F.sum("gross_revenue"), 2).alias("total_revenue"),
        F.sum("order_count").cast("long").alias("total_order_count"),
    ).first()

    baseline_revenue = float(baseline_totals.total_revenue or 0.0)
    candidate_revenue = float(candidate_totals.total_revenue or 0.0)
    if abs(baseline_revenue - candidate_revenue) > revenue_tolerance:
        raise RuntimeError(
            "Lab 5 baseline and candidate outputs have different total revenue: "
            f"baseline={baseline_revenue} candidate={candidate_revenue} "
            f"tolerance={revenue_tolerance}"
        )

    baseline_order_count = int(baseline_totals.total_order_count or 0)
    candidate_order_count = int(candidate_totals.total_order_count or 0)
    if baseline_order_count != candidate_order_count:
        raise RuntimeError(
            "Lab 5 baseline and candidate outputs have different total order counts: "
            f"baseline={baseline_order_count} candidate={candidate_order_count}"
        )

    return OutputCompatibilityResult(
        baseline_row_count=baseline_count,
        candidate_row_count=candidate_count,
        baseline_total_revenue=baseline_revenue,
        candidate_total_revenue=candidate_revenue,
        baseline_total_order_count=baseline_order_count,
        candidate_total_order_count=candidate_order_count,
    )


def build_stage_metrics_record(
    *,
    run_id: str,
    app_name: str,
    application_id: str,
    workload_name: str,
    workload_variant: str,
    metrics: StageRuntimeMetrics,
) -> dict[str, Any]:
    """Build one persisted StageMetrics row for a Lab 5 variant."""

    return {
        "run_id": run_id,
        "app_name": app_name,
        "application_id": application_id,
        "workload_name": workload_name,
        "workload_variant": workload_variant,
        "collector_type": "stage",
        "executor_run_time_ms": metrics.executor_run_time_ms,
        "shuffle_bytes_written": metrics.shuffle_bytes_written,
        "shuffle_bytes_read": metrics.shuffle_bytes_read,
        "memory_bytes_spilled": metrics.memory_bytes_spilled,
        "disk_bytes_spilled": metrics.disk_bytes_spilled,
        "jvm_gc_time_ms": metrics.jvm_gc_time_ms,
        "num_stages": metrics.num_stages,
        "num_tasks": metrics.num_tasks,
        "input_bytes": metrics.input_bytes,
        "records_read": metrics.records_read,
        "records_written": metrics.records_written,
        "created_at": _utc_now(),
    }


def build_decision_record(
    *,
    run_id: str,
    app_name: str,
    application_id: str,
    baseline_run_id: str,
    candidate_run_id: str,
    workload_name: str,
    baseline: StageRuntimeMetrics,
    candidate: StageRuntimeMetrics,
    decision: RuntimeBudgetDecision,
) -> dict[str, Any]:
    """Build the persisted final guardrail decision row."""

    return {
        "run_id": run_id,
        "app_name": app_name,
        "application_id": application_id,
        "baseline_run_id": baseline_run_id,
        "candidate_run_id": candidate_run_id,
        "workload_name": workload_name,
        "workload_profile": decision.workload_profile,
        "decision": decision.decision,
        "failed_rules": decision.rendered_failed_rules,
        "warning_flags": decision.rendered_warning_flags,
        "executor_run_time_delta_pct": decision.comparison.executor_run_time_delta_pct,
        "shuffle_written_delta_pct": decision.comparison.shuffle_written_delta_pct,
        "shuffle_read_delta_pct": decision.comparison.shuffle_read_delta_pct,
        "num_tasks_delta_pct": decision.comparison.num_tasks_delta_pct,
        "num_stages_delta_pct": decision.comparison.num_stages_delta_pct,
        "gc_time_delta_pct": decision.comparison.gc_time_delta_pct,
        "spill_delta_pct": decision.comparison.spill_delta_pct,
        "baseline_metrics": json.dumps(asdict(baseline), sort_keys=True),
        "candidate_metrics": json.dumps(asdict(candidate), sort_keys=True),
        "created_at": _utc_now(),
    }


def render_budget_decision_block(
    *,
    decision: RuntimeBudgetDecision,
    baseline: StageRuntimeMetrics,
    candidate: StageRuntimeMetrics,
    compatibility: OutputCompatibilityResult,
    metrics_output_path: str,
    decisions_output_path: str,
    baseline_output_path: str,
    candidate_output_path: str,
    width: int = 104,
) -> str:
    """Render a prominent classroom-friendly final decision block."""

    lines = [
        "## LAB 5 RUNTIME BUDGET GUARDRAIL",
        "",
        "### Final decision",
        f"decision: {decision.decision}",
        f"workload_profile: {decision.workload_profile}",
        f"failed_rules: {_render_sequence(decision.failed_rules)}",
        f"warning_flags: {_render_sequence(decision.warning_flags)}",
        "",
        "### Functional compatibility",
        "status: OK",
        f"rows: baseline={compatibility.baseline_row_count} candidate={compatibility.candidate_row_count}",
        f"total_revenue: baseline={compatibility.baseline_total_revenue} candidate={compatibility.candidate_total_revenue}",
        f"total_order_count: baseline={compatibility.baseline_total_order_count} candidate={compatibility.candidate_total_order_count}",
        "",
        "### Baseline StageMetrics",
        _metrics_line(baseline),
        "",
        "### Candidate StageMetrics",
        _metrics_line(candidate),
        "",
        "### Candidate delta versus baseline",
        _delta_line(
            "executor runtime",
            baseline.executor_run_time_ms,
            candidate.executor_run_time_ms,
            decision.comparison.executor_run_time_delta_pct,
            "ms",
        ),
        _delta_line(
            "shuffle written",
            baseline.shuffle_bytes_written,
            candidate.shuffle_bytes_written,
            decision.comparison.shuffle_written_delta_pct,
            "bytes",
        ),
        _delta_line(
            "shuffle read",
            baseline.shuffle_bytes_read,
            candidate.shuffle_bytes_read,
            decision.comparison.shuffle_read_delta_pct,
            "bytes",
        ),
        _delta_line(
            "tasks",
            baseline.num_tasks,
            candidate.num_tasks,
            decision.comparison.num_tasks_delta_pct,
            "count",
        ),
        _delta_line(
            "stages",
            baseline.num_stages,
            candidate.num_stages,
            decision.comparison.num_stages_delta_pct,
            "count",
        ),
        _delta_line(
            "GC time",
            baseline.jvm_gc_time_ms,
            candidate.jvm_gc_time_ms,
            decision.comparison.gc_time_delta_pct,
            "ms",
        )
        + " | supporting signal",
        _delta_line(
            "spill total",
            baseline.spill_total_bytes,
            candidate.spill_total_bytes,
            decision.comparison.spill_delta_pct,
            "bytes",
        )
        + " | supporting signal",
        "",
        "### Delta outputs",
        f"baseline_output: {baseline_output_path}",
        f"candidate_output: {candidate_output_path}",
        f"metrics_output: {metrics_output_path}",
        f"decisions_output: {decisions_output_path}",
    ]
    return _boxed_lines(lines, width=width)


def _metrics_line(metrics: StageRuntimeMetrics) -> str:
    return (
        f"executor_run_time_ms={metrics.executor_run_time_ms} "
        f"shuffle_written={metrics.shuffle_bytes_written} "
        f"shuffle_read={metrics.shuffle_bytes_read} "
        f"memory_spill={metrics.memory_bytes_spilled} "
        f"disk_spill={metrics.disk_bytes_spilled} "
        f"jvm_gc_time_ms={metrics.jvm_gc_time_ms} "
        f"num_stages={metrics.num_stages} "
        f"num_tasks={metrics.num_tasks}"
    )


def _delta_line(
    label: str,
    baseline_value: int,
    candidate_value: int,
    delta_pct: float,
    unit: str,
) -> str:
    return (
        f"{label}: {_format_metric_value(baseline_value, unit)} -> "
        f"{_format_metric_value(candidate_value, unit)} | "
        f"{_format_signed_pct(delta_pct)} | "
        f"{_format_multiplier(baseline_value, candidate_value)} baseline"
    )


def _format_metric_value(value: int, unit: str) -> str:
    if unit == "bytes":
        return f"{value} B"
    if unit == "ms":
        return f"{value} ms"
    return str(value)


def _format_signed_pct(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def _format_multiplier(baseline_value: int, candidate_value: int) -> str:
    if baseline_value == 0:
        if candidate_value == 0:
            return "1.00x"
        return "new"
    return f"{candidate_value / baseline_value:.2f}x"


def _render_sequence(values: tuple[str, ...]) -> str:
    if not values:
        return "none"
    return ", ".join(values)


def _thresholds(
    raw: Mapping[str, Any],
    *,
    base: RuntimeBudgetThresholds | None = None,
) -> RuntimeBudgetThresholds:
    if not isinstance(raw, Mapping):
        raise ValueError("Lab 5 budget threshold entries must be mappings")
    defaults = base or RuntimeBudgetThresholds()
    return RuntimeBudgetThresholds(
        max_executor_runtime_growth_pct=_non_negative_float(
            raw.get(
                "max_executor_runtime_growth_pct",
                defaults.max_executor_runtime_growth_pct,
            ),
            "max_executor_runtime_growth_pct",
        ),
        max_shuffle_written_growth_pct=_non_negative_float(
            raw.get(
                "max_shuffle_written_growth_pct",
                defaults.max_shuffle_written_growth_pct,
            ),
            "max_shuffle_written_growth_pct",
        ),
        max_shuffle_read_growth_pct=_non_negative_float(
            raw.get(
                "max_shuffle_read_growth_pct",
                defaults.max_shuffle_read_growth_pct,
            ),
            "max_shuffle_read_growth_pct",
        ),
        max_num_tasks_growth_pct=_non_negative_float(
            raw.get("max_num_tasks_growth_pct", defaults.max_num_tasks_growth_pct),
            "max_num_tasks_growth_pct",
        ),
        max_num_stages_growth_pct=_non_negative_float(
            raw.get("max_num_stages_growth_pct", defaults.max_num_stages_growth_pct),
            "max_num_stages_growth_pct",
        ),
        fail_on_memory_spill_bytes_above=_non_negative_int(
            raw.get(
                "fail_on_memory_spill_bytes_above",
                defaults.fail_on_memory_spill_bytes_above,
            ),
            "fail_on_memory_spill_bytes_above",
        ),
        fail_on_disk_spill_bytes_above=_non_negative_int(
            raw.get(
                "fail_on_disk_spill_bytes_above",
                defaults.fail_on_disk_spill_bytes_above,
            ),
            "fail_on_disk_spill_bytes_above",
        ),
    )


def _append_growth_failure(
    failed_rules: list[str],
    rule_name: str,
    observed_delta_pct: float,
    allowed_delta_pct: float,
) -> None:
    if observed_delta_pct > allowed_delta_pct:
        failed_rules.append(rule_name)


def _pct_delta(baseline: int | float, candidate: int | float) -> float:
    if baseline == 0:
        if candidate == 0:
            return 0.0
        return 100.0
    return round(((float(candidate) - float(baseline)) / float(baseline)) * 100.0, 4)


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as config_file:
            loaded = yaml.safe_load(config_file) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid Lab 5 YAML: {path}") from exc
    if not isinstance(loaded, dict):
        raise ValueError(f"Lab 5 YAML must contain a mapping: {path}")
    return loaded


def _metric_int(metrics: Mapping[str, int | float], key: str) -> int:
    value = metrics.get(key, 0)
    if value is None:
        return 0
    return int(value)


def _boxed_lines(lines: list[str], *, width: int) -> str:
    normalized_width = max(width, 72)
    content_width = normalized_width - 4
    border = "═" * (normalized_width - 2)
    rendered = [f"\n╔{border}╗"]
    for line in lines:
        if not line:
            rendered.append(f"║ {' ' * content_width} ║")
            continue
        for wrapped in _wrap_line(line, content_width):
            rendered.append(f"║ {wrapped.ljust(content_width)} ║")
    rendered.append(f"╚{border}╝")
    return "\n".join(rendered)


def _wrap_line(line: str, width: int) -> list[str]:
    words = line.split()
    if not words:
        return [""]
    wrapped: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + 1 + len(word) > width:
            wrapped.append(current)
            current = word
        else:
            current = f"{current} {word}"
    wrapped.append(current)
    return wrapped


def _positive_int(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"Lab 5 {field_name} must be >= 1")
    return parsed


def _non_negative_int(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"Lab 5 {field_name} must be >= 0")
    return parsed


def _non_negative_float(value: Any, field_name: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise ValueError(f"Lab 5 {field_name} must be >= 0")
    return parsed


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
