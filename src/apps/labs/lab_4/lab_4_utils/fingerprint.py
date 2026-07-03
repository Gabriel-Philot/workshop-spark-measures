"""Lab 4 fingerprint settings, metric normalization, and rules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml


SHUFFLE_HEAVY = "SHUFFLE_HEAVY"
MEMORY_PRESSURE = "MEMORY_PRESSURE"
IO_HEAVY_SCAN = "IO_HEAVY_SCAN"
GC_PRESSURE = "GC_PRESSURE"
MANY_SMALL_TASKS = "MANY_SMALL_TASKS"
LOW_PARALLELISM_SIGNAL = "LOW_PARALLELISM_SIGNAL"
BALANCED_OR_LOW_SIGNAL = "BALANCED_OR_LOW_SIGNAL"

SUPPORTED_WORKLOAD_PROFILES = (
    SHUFFLE_HEAVY,
    MEMORY_PRESSURE,
    IO_HEAVY_SCAN,
    GC_PRESSURE,
    MANY_SMALL_TASKS,
    LOW_PARALLELISM_SIGNAL,
    BALANCED_OR_LOW_SIGNAL,
)

REQUIRED_STAGE_METRICS = ("numStages", "numTasks", "executorRunTime")


@dataclass(frozen=True)
class StageWorkloadFingerprintSettings:
    """Classroom workload settings loaded from the selected YAML experiment."""

    workload_name: str = "retail_stage_fingerprint"
    workload_variant: str = "shuffle_fingerprint"
    success_marker: str = "LAB4_STAGE_WORKLOAD_FINGERPRINT_OK"
    shuffle_partitions: int = 96
    fingerprint_buckets: int = 512


@dataclass(frozen=True)
class FingerprintRules:
    """Simple thresholds for the stage-level workload fingerprint."""

    high_shuffle_amplification_ratio: float = 2.0
    high_shuffle_total_bytes: int = 64 * 1024 * 1024
    high_input_bytes: int = 128 * 1024 * 1024
    minimum_reliable_input_bytes: int = 1024 * 1024
    low_shuffle_bytes: int = 1024 * 1024
    high_gc_time_ratio: float = 0.10
    spill_detected_bytes: int = 1
    high_task_density_score: float = 128.0
    high_total_tasks: int = 1000
    low_signal_executor_runtime_ms: int = 1000
    low_signal_shuffle_bytes: int = 1024 * 1024
    low_parallelism_task_threshold: int = 4


@dataclass(frozen=True)
class NormalizedStageMetrics:
    """Normalized stage aggregate fields used by the Lab 4 classifier."""

    executor_run_time_ms: int
    input_bytes: int
    shuffle_bytes_read: int
    shuffle_bytes_written: int
    memory_bytes_spilled: int
    disk_bytes_spilled: int
    jvm_gc_time_ms: int
    num_stages: int
    num_tasks: int
    shuffle_total_bytes: int
    shuffle_amplification_ratio: float | None
    gc_time_ratio: float
    spill_ratio: float
    task_density_score: float


@dataclass(frozen=True)
class FingerprintDecision:
    """Result of applying Lab 4 rules to normalized stage metrics."""

    workload_profile: str
    diagnostic_flags: tuple[str, ...]
    recommended_next_step: str

    @property
    def rendered_flags(self) -> str:
        return ",".join(self.diagnostic_flags)


def load_stage_workload_fingerprint_settings(
    config_name: str,
    config_path: Path,
) -> StageWorkloadFingerprintSettings:
    """Read Lab 4 workload settings from the local YAML config."""

    raw = _load_yaml(config_path)
    experiments = raw.get("experiments") or {}
    if config_name not in experiments:
        raise KeyError(
            f"Unknown Lab 4 experiment '{config_name}'. "
            f"Available experiments: {sorted(experiments)}"
        )
    workload = (experiments[config_name] or {}).get("workload") or {}

    return StageWorkloadFingerprintSettings(
        workload_name=str(workload.get("workload_name", "retail_stage_fingerprint")),
        workload_variant=str(workload.get("workload_variant", "shuffle_fingerprint")),
        success_marker=str(
            workload.get("success_marker", "LAB4_STAGE_WORKLOAD_FINGERPRINT_OK")
        ),
        shuffle_partitions=_positive_int(
            workload.get("shuffle_partitions", 96),
            "shuffle_partitions",
        ),
        fingerprint_buckets=_positive_int(
            workload.get("fingerprint_buckets", 512),
            "fingerprint_buckets",
        ),
    )


def load_fingerprint_rules(rules_path: Path) -> FingerprintRules:
    """Read Lab 4 fingerprint thresholds from YAML."""

    raw = _load_yaml(rules_path)
    rules = raw.get("rules")
    if not isinstance(rules, Mapping):
        raise ValueError(f"Lab 4 rules YAML requires a 'rules' mapping: {rules_path}")

    return FingerprintRules(
        high_shuffle_amplification_ratio=_non_negative_float(
            rules.get("high_shuffle_amplification_ratio", 2.0),
            "high_shuffle_amplification_ratio",
        ),
        high_shuffle_total_bytes=_non_negative_int(
            rules.get("high_shuffle_total_bytes", 64 * 1024 * 1024),
            "high_shuffle_total_bytes",
        ),
        high_input_bytes=_non_negative_int(
            rules.get("high_input_bytes", 128 * 1024 * 1024),
            "high_input_bytes",
        ),
        minimum_reliable_input_bytes=_non_negative_int(
            rules.get("minimum_reliable_input_bytes", 1024 * 1024),
            "minimum_reliable_input_bytes",
        ),
        low_shuffle_bytes=_non_negative_int(
            rules.get("low_shuffle_bytes", 1024 * 1024),
            "low_shuffle_bytes",
        ),
        high_gc_time_ratio=_non_negative_float(
            rules.get("high_gc_time_ratio", 0.10),
            "high_gc_time_ratio",
        ),
        spill_detected_bytes=_non_negative_int(
            rules.get("spill_detected_bytes", 1),
            "spill_detected_bytes",
        ),
        high_task_density_score=_non_negative_float(
            rules.get("high_task_density_score", 128.0),
            "high_task_density_score",
        ),
        high_total_tasks=_non_negative_int(
            rules.get("high_total_tasks", 1000),
            "high_total_tasks",
        ),
        low_signal_executor_runtime_ms=_non_negative_int(
            rules.get("low_signal_executor_runtime_ms", 1000),
            "low_signal_executor_runtime_ms",
        ),
        low_signal_shuffle_bytes=_non_negative_int(
            rules.get("low_signal_shuffle_bytes", 1024 * 1024),
            "low_signal_shuffle_bytes",
        ),
        low_parallelism_task_threshold=_non_negative_int(
            rules.get("low_parallelism_task_threshold", 4),
            "low_parallelism_task_threshold",
        ),
    )


def normalize_stage_metrics(metrics: Mapping[str, int | float]) -> NormalizedStageMetrics:
    """Map actual sparkMeasure StageMetrics names to Lab 4 fields."""

    missing = tuple(key for key in REQUIRED_STAGE_METRICS if key not in metrics)
    if missing:
        raise ValueError(
            "Lab 4 received an unsupported stage metrics schema. "
            f"Missing required metrics: {', '.join(missing)}"
        )

    num_stages = _metric_int(metrics, "numStages")
    num_tasks = _metric_int(metrics, "numTasks")
    executor_run_time_ms = _metric_int(metrics, "executorRunTime")
    if num_stages < 1 or num_tasks < 1:
        raise ValueError(
            "Lab 4 captured no useful stage-level metrics: "
            f"numStages={num_stages}, numTasks={num_tasks}"
        )

    input_bytes = _metric_int(metrics, "bytesRead")
    shuffle_bytes_read = _metric_int(metrics, "shuffleTotalBytesRead")
    shuffle_bytes_written = _metric_int(metrics, "shuffleBytesWritten")
    shuffle_total_bytes = shuffle_bytes_read + shuffle_bytes_written
    memory_bytes_spilled = _metric_int(metrics, "memoryBytesSpilled")
    disk_bytes_spilled = _metric_int(metrics, "diskBytesSpilled")
    jvm_gc_time_ms = _metric_int(metrics, "jvmGCTime")

    return NormalizedStageMetrics(
        executor_run_time_ms=executor_run_time_ms,
        input_bytes=input_bytes,
        shuffle_bytes_read=shuffle_bytes_read,
        shuffle_bytes_written=shuffle_bytes_written,
        memory_bytes_spilled=memory_bytes_spilled,
        disk_bytes_spilled=disk_bytes_spilled,
        jvm_gc_time_ms=jvm_gc_time_ms,
        num_stages=num_stages,
        num_tasks=num_tasks,
        shuffle_total_bytes=shuffle_total_bytes,
        shuffle_amplification_ratio=_safe_ratio(shuffle_total_bytes, input_bytes),
        gc_time_ratio=_safe_ratio(jvm_gc_time_ms, executor_run_time_ms) or 0.0,
        spill_ratio=_safe_ratio(
            memory_bytes_spilled + disk_bytes_spilled,
            max(input_bytes, shuffle_total_bytes, 1),
        )
        or 0.0,
        task_density_score=round(num_tasks / max(num_stages, 1), 4),
    )


def classify_workload(
    normalized: NormalizedStageMetrics,
    rules: FingerprintRules,
) -> FingerprintDecision:
    """Assign a simple operational profile from stage-level aggregate metrics."""

    flags: list[str] = []
    spill_bytes = normalized.memory_bytes_spilled + normalized.disk_bytes_spilled
    input_bytes_are_reliable = (
        normalized.input_bytes >= rules.minimum_reliable_input_bytes
    )

    if normalized.input_bytes == 0:
        flags.append("INPUT_BYTES_UNAVAILABLE_FOR_RATIO")
    elif not input_bytes_are_reliable:
        flags.append("INPUT_BYTES_LOW_CONFIDENCE_FOR_RATIO")

    if (
        input_bytes_are_reliable
        and
        normalized.shuffle_amplification_ratio is not None
        and normalized.shuffle_amplification_ratio
        >= rules.high_shuffle_amplification_ratio
    ):
        flags.append("HIGH_SHUFFLE_AMPLIFICATION")

    if normalized.shuffle_total_bytes >= rules.high_shuffle_total_bytes:
        flags.append("HIGH_SHUFFLE_VOLUME")

    if spill_bytes >= rules.spill_detected_bytes:
        flags.append("SPILL_DETECTED")

    if normalized.gc_time_ratio >= rules.high_gc_time_ratio:
        flags.append("HIGH_GC_RATIO")

    if (
        normalized.input_bytes >= rules.high_input_bytes
        and normalized.shuffle_total_bytes <= rules.low_shuffle_bytes
    ):
        flags.append("HIGH_INPUT_LOW_SHUFFLE")

    if (
        normalized.num_tasks >= rules.high_total_tasks
        or normalized.task_density_score >= rules.high_task_density_score
    ):
        flags.append("TASK_OVERHEAD_SIGNAL")

    if (
        normalized.num_tasks <= rules.low_parallelism_task_threshold
        and normalized.executor_run_time_ms >= rules.low_signal_executor_runtime_ms
    ):
        flags.append("LOW_PARALLELISM_SIGNAL")

    if (
        normalized.executor_run_time_ms < rules.low_signal_executor_runtime_ms
        and normalized.shuffle_total_bytes < rules.low_signal_shuffle_bytes
        and spill_bytes == 0
    ):
        flags.append("LOW_SIGNAL")
        return _decision(BALANCED_OR_LOW_SIGNAL, flags)

    if "SPILL_DETECTED" in flags:
        return _decision(MEMORY_PRESSURE, flags)
    if "HIGH_GC_RATIO" in flags:
        return _decision(GC_PRESSURE, flags)
    if "HIGH_INPUT_LOW_SHUFFLE" in flags:
        return _decision(IO_HEAVY_SCAN, flags)
    if "HIGH_SHUFFLE_AMPLIFICATION" in flags or "HIGH_SHUFFLE_VOLUME" in flags:
        return _decision(SHUFFLE_HEAVY, flags)
    if "TASK_OVERHEAD_SIGNAL" in flags:
        return _decision(MANY_SMALL_TASKS, flags)
    if "LOW_PARALLELISM_SIGNAL" in flags:
        return _decision(LOW_PARALLELISM_SIGNAL, flags)

    flags.append("LOW_SIGNAL")
    return _decision(BALANCED_OR_LOW_SIGNAL, flags)


def render_fingerprint_diagnostic_block(
    normalized: NormalizedStageMetrics,
    decision: FingerprintDecision,
    *,
    width: int = 96,
) -> str:
    """Render a classroom-friendly diagnostic block for submit logs."""

    lines = [
        "## STAGE WORKLOAD FINGERPRINT DIAGNOSTIC",
        "",
        "### Profile",
        f"workload_profile: {decision.workload_profile}",
        f"diagnostic_flags: {decision.rendered_flags}",
        "",
        "### StageMetrics signals",
        f"num_stages: {normalized.num_stages}",
        f"num_tasks: {normalized.num_tasks}",
        f"executor_run_time_ms: {normalized.executor_run_time_ms}",
        f"input_bytes: {normalized.input_bytes}",
        f"shuffle_bytes_read: {normalized.shuffle_bytes_read}",
        f"shuffle_bytes_written: {normalized.shuffle_bytes_written}",
        f"memory_bytes_spilled: {normalized.memory_bytes_spilled}",
        f"disk_bytes_spilled: {normalized.disk_bytes_spilled}",
        f"jvm_gc_time_ms: {normalized.jvm_gc_time_ms}",
        "",
        "### Normalized ratios",
        "shuffle_amplification_ratio: "
        f"{_render_shuffle_ratio(normalized, decision)}",
        f"gc_time_ratio: {normalized.gc_time_ratio}",
        f"spill_ratio: {normalized.spill_ratio}",
        f"task_density_score: {normalized.task_density_score}",
        "",
        "### Recommended next step",
        decision.recommended_next_step,
    ]
    return _boxed_lines(lines, width=width)


def build_stage_metrics_record(
    *,
    run_id: str,
    app_name: str,
    workload_name: str,
    workload_variant: str,
    application_id: str,
    metrics: Mapping[str, int | float],
    normalized: NormalizedStageMetrics,
) -> dict[str, Any]:
    """Build a raw normalized StageMetrics Delta row."""

    return {
        "run_id": run_id,
        "app_name": app_name,
        "application_id": application_id,
        "workload_name": workload_name,
        "workload_variant": workload_variant,
        "collector_type": "stage",
        "executor_run_time_ms": normalized.executor_run_time_ms,
        "input_bytes": normalized.input_bytes,
        "shuffle_total_bytes": normalized.shuffle_total_bytes,
        "shuffle_bytes_read": normalized.shuffle_bytes_read,
        "shuffle_bytes_written": normalized.shuffle_bytes_written,
        "memory_bytes_spilled": normalized.memory_bytes_spilled,
        "disk_bytes_spilled": normalized.disk_bytes_spilled,
        "jvm_gc_time_ms": normalized.jvm_gc_time_ms,
        "num_stages": normalized.num_stages,
        "num_tasks": normalized.num_tasks,
        "records_read": _metric_int(metrics, "recordsRead"),
        "records_written": _metric_int(metrics, "recordsWritten"),
        "created_at": _utc_now(),
    }


def build_fingerprint_record(
    *,
    run_id: str,
    app_name: str,
    workload_name: str,
    workload_variant: str,
    application_id: str,
    normalized: NormalizedStageMetrics,
    decision: FingerprintDecision,
) -> dict[str, Any]:
    """Build the required Lab 4 workload fingerprint Delta row."""

    return {
        "run_id": run_id,
        "app_name": app_name,
        "application_id": application_id,
        "workload_name": workload_name,
        "workload_variant": workload_variant,
        "executor_run_time_ms": normalized.executor_run_time_ms,
        "input_bytes": normalized.input_bytes,
        "shuffle_bytes_read": normalized.shuffle_bytes_read,
        "shuffle_bytes_written": normalized.shuffle_bytes_written,
        "memory_bytes_spilled": normalized.memory_bytes_spilled,
        "disk_bytes_spilled": normalized.disk_bytes_spilled,
        "jvm_gc_time_ms": normalized.jvm_gc_time_ms,
        "num_stages": normalized.num_stages,
        "num_tasks": normalized.num_tasks,
        "shuffle_amplification_ratio": normalized.shuffle_amplification_ratio,
        "gc_time_ratio": normalized.gc_time_ratio,
        "spill_ratio": normalized.spill_ratio,
        "task_density_score": normalized.task_density_score,
        "workload_profile": decision.workload_profile,
        "diagnostic_flags": decision.rendered_flags,
        "recommended_next_step": decision.recommended_next_step,
        "created_at": _utc_now(),
    }


def _decision(profile: str, flags: list[str]) -> FingerprintDecision:
    return FingerprintDecision(
        workload_profile=profile,
        diagnostic_flags=tuple(dict.fromkeys(flags)),
        recommended_next_step=_recommendation(profile),
    )


def _recommendation(profile: str) -> str:
    if profile == SHUFFLE_HEAVY:
        return "Review joins, aggregations, partitioning, and unnecessary repartitions."
    if profile == MEMORY_PRESSURE:
        return "Review wide transformations, executor memory, partition sizing, caching, and aggregation strategy."
    if profile == IO_HEAVY_SCAN:
        return "Review partition pruning, column pruning, file layout, and predicate pushdown."
    if profile == GC_PRESSURE:
        return "Review object pressure, caching, UDFs, serialization, executor memory, and partition sizing."
    if profile == MANY_SMALL_TASKS:
        return "Review partition count, small files, and unnecessary repartitions."
    if profile == LOW_PARALLELISM_SIGNAL:
        return "Review available cores, partition count, scheduler behavior, and whether the workload exposes enough parallelism."
    return "No strong diagnosis; run at a larger scale or compare against a baseline."


def _boxed_lines(lines: list[str], *, width: int) -> str:
    normalized_width = max(width, 60)
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


def _render_optional_float(value: float | None) -> str:
    if value is None:
        return "unavailable"
    return str(value)


def _render_shuffle_ratio(
    normalized: NormalizedStageMetrics,
    decision: FingerprintDecision,
) -> str:
    if normalized.shuffle_amplification_ratio is None:
        return "unavailable"
    if "INPUT_BYTES_LOW_CONFIDENCE_FOR_RATIO" in decision.diagnostic_flags:
        return f"{normalized.shuffle_amplification_ratio} (not used: low-confidence input_bytes)"
    return str(normalized.shuffle_amplification_ratio)


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as config_file:
            loaded = yaml.safe_load(config_file) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid Lab 4 YAML: {path}") from exc
    if not isinstance(loaded, dict):
        raise ValueError(f"Lab 4 YAML must contain a mapping: {path}")
    return loaded


def _metric_int(metrics: Mapping[str, int | float], key: str) -> int:
    value = metrics.get(key, 0)
    if value is None:
        return 0
    return int(value)


def _safe_ratio(numerator: int | float, denominator: int | float) -> float | None:
    if denominator <= 0:
        return None
    return round(float(numerator) / float(denominator), 6)


def _positive_int(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"Lab 4 {field_name} must be >= 1")
    return parsed


def _non_negative_int(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"Lab 4 {field_name} must be >= 0")
    return parsed


def _non_negative_float(value: Any, field_name: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise ValueError(f"Lab 4 {field_name} must be >= 0")
    return parsed


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
