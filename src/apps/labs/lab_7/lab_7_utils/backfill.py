"""Lab 7 daily backfill settings, metric records, and terminal rendering."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from apps.labs.lab_7.lab_7_utils.generator import (
    DateVolume,
    TemporalVolumePlan,
    load_temporal_volume_plan,
)
from apps.labs.lab_7.lab_7_utils.transformations import EARLY_PARTITION_FILTER


REQUIRED_STAGE_METRICS = ("numStages", "numTasks", "executorRunTime")


@dataclass(frozen=True)
class DailyBackfillSettings:
    """Classroom settings for one daily backfill run."""

    workload_name: str = "temporal_daily_backfill"
    success_marker: str = "LAB7_DAILY_BACKFILL_STAGE_METRICS_OK"
    filter_strategy: str = EARLY_PARTITION_FILTER


@dataclass(frozen=True)
class DailyBackfillMetrics:
    """Normalized stage-level aggregate metrics for one processing date."""

    executor_run_time_ms: int
    input_bytes: int
    records_read: int
    records_written: int
    shuffle_bytes_read: int
    shuffle_bytes_written: int
    memory_bytes_spilled: int
    disk_bytes_spilled: int
    jvm_gc_time_ms: int
    num_stages: int
    num_tasks: int
    shuffle_total_bytes: int
    runtime_per_million_rows: float
    shuffle_per_million_rows: float
    input_bytes_per_million_rows: float
    tasks_per_million_rows: float


def load_daily_backfill_settings(
    config_name: str,
    config_path: Path,
) -> DailyBackfillSettings:
    """Read Lab 7 daily backfill settings from experiments.yaml."""

    raw = _load_yaml(config_path)
    experiments = raw.get("experiments") or {}
    if config_name not in experiments:
        raise KeyError(
            f"Unknown Lab 7 experiment '{config_name}'. "
            f"Available experiments: {sorted(experiments)}"
        )
    workload = (experiments[config_name] or {}).get("workload") or {}
    return DailyBackfillSettings(
        workload_name=str(workload.get("workload_name", "temporal_daily_backfill")),
        success_marker=str(
            workload.get("success_marker", "LAB7_DAILY_BACKFILL_STAGE_METRICS_OK")
        ),
        filter_strategy=str(
            workload.get("filter_strategy", EARLY_PARTITION_FILTER)
        ),
    )


def load_processing_dates_from_plan(volume_plan_path: Path) -> tuple[str, ...]:
    """Return the default processing dates from the Lab 7 volume plan."""

    plan = load_temporal_volume_plan(volume_plan_path)
    return tuple(item.event_date for item in plan.date_volumes)


def expected_volume_for_date(
    volume_plan_path: Path,
    processing_date: str,
) -> tuple[TemporalVolumePlan, DateVolume]:
    """Return the configured expected volume for a processing date."""

    plan = load_temporal_volume_plan(volume_plan_path)
    for item in plan.date_volumes:
        if item.event_date == processing_date:
            return plan, item
    raise ValueError(
        f"Processing date {processing_date} is not present in the Lab 7 volume plan"
    )


def normalize_daily_backfill_metrics(
    metrics: Mapping[str, int | float],
    *,
    source_rows_for_date: int,
) -> DailyBackfillMetrics:
    """Map sparkMeasure StageMetrics names to Lab 7 daily backfill fields."""

    missing = tuple(key for key in REQUIRED_STAGE_METRICS if key not in metrics)
    if missing:
        raise ValueError(
            "Lab 7 received an unsupported stage metrics schema. "
            f"Missing required metrics: {', '.join(missing)}"
        )

    num_stages = _metric_int(metrics, "numStages")
    num_tasks = _metric_int(metrics, "numTasks")
    executor_run_time_ms = _metric_int(metrics, "executorRunTime")
    if num_stages < 1 or num_tasks < 1:
        raise ValueError(
            "Lab 7 captured no useful stage-level metrics: "
            f"numStages={num_stages}, numTasks={num_tasks}"
        )

    input_bytes = _metric_int(metrics, "bytesRead")
    records_read = _metric_int(metrics, "recordsRead")
    records_written = _metric_int(metrics, "recordsWritten")
    shuffle_bytes_read = _metric_int(metrics, "shuffleTotalBytesRead")
    shuffle_bytes_written = _metric_int(metrics, "shuffleBytesWritten")
    shuffle_total_bytes = shuffle_bytes_read + shuffle_bytes_written
    memory_bytes_spilled = _metric_int(metrics, "memoryBytesSpilled")
    disk_bytes_spilled = _metric_int(metrics, "diskBytesSpilled")
    jvm_gc_time_ms = _metric_int(metrics, "jvmGCTime")
    rows_per_million = max(source_rows_for_date / 1_000_000.0, 0.000001)

    return DailyBackfillMetrics(
        executor_run_time_ms=executor_run_time_ms,
        input_bytes=input_bytes,
        records_read=records_read,
        records_written=records_written,
        shuffle_bytes_read=shuffle_bytes_read,
        shuffle_bytes_written=shuffle_bytes_written,
        memory_bytes_spilled=memory_bytes_spilled,
        disk_bytes_spilled=disk_bytes_spilled,
        jvm_gc_time_ms=jvm_gc_time_ms,
        num_stages=num_stages,
        num_tasks=num_tasks,
        shuffle_total_bytes=shuffle_total_bytes,
        runtime_per_million_rows=round(executor_run_time_ms / rows_per_million, 4),
        shuffle_per_million_rows=round(shuffle_total_bytes / rows_per_million, 4),
        input_bytes_per_million_rows=round(input_bytes / rows_per_million, 4),
        tasks_per_million_rows=round(num_tasks / rows_per_million, 4),
    )


def build_daily_backfill_metrics_record(
    *,
    run_id: str,
    date_run_id: str,
    app_name: str,
    application_id: str,
    workload_name: str,
    filter_strategy: str,
    processing_date: str,
    plan: TemporalVolumePlan,
    date_volume: DateVolume,
    output_path: str,
    metrics: DailyBackfillMetrics,
) -> dict[str, Any]:
    """Build one persisted StageMetrics row for a processing date."""

    return {
        "run_id": run_id,
        "date_run_id": date_run_id,
        "app_name": app_name,
        "application_id": application_id,
        "lab_id": "lab_7",
        "workload_name": workload_name,
        "filter_strategy": filter_strategy,
        "processing_date": processing_date,
        "source_start_date": plan.start_date,
        "source_end_date": plan.end_date,
        "source_rows_for_date": date_volume.rows,
        "volume_multiplier": date_volume.volume_multiplier,
        "spike_label": date_volume.spike_label,
        "collector_type": "stage",
        "executor_run_time_ms": metrics.executor_run_time_ms,
        "records_read": metrics.records_read,
        "records_written": metrics.records_written,
        "input_bytes": metrics.input_bytes,
        "shuffle_bytes_written": metrics.shuffle_bytes_written,
        "shuffle_bytes_read": metrics.shuffle_bytes_read,
        "shuffle_total_bytes": metrics.shuffle_total_bytes,
        "memory_bytes_spilled": metrics.memory_bytes_spilled,
        "disk_bytes_spilled": metrics.disk_bytes_spilled,
        "jvm_gc_time_ms": metrics.jvm_gc_time_ms,
        "num_stages": metrics.num_stages,
        "num_tasks": metrics.num_tasks,
        "runtime_per_million_rows": metrics.runtime_per_million_rows,
        "shuffle_per_million_rows": metrics.shuffle_per_million_rows,
        "input_bytes_per_million_rows": metrics.input_bytes_per_million_rows,
        "tasks_per_million_rows": metrics.tasks_per_million_rows,
        "business_output_path": output_path,
        "created_at": _utc_now(),
    }


def render_daily_backfill_block(
    *,
    processing_date: str,
    filter_strategy: str,
    date_volume: DateVolume,
    output_path: str,
    metrics_output_path: str,
    metrics: DailyBackfillMetrics,
    width: int = 104,
) -> str:
    """Render a classroom-friendly summary for one daily backfill."""

    lines = [
        "## LAB 7 DAILY BACKFILL STAGE METRICS",
        "",
        "### Processing date",
        f"processing_date: {processing_date}",
        f"filter_strategy: {filter_strategy}",
        f"source_rows_for_date: {date_volume.rows}",
        f"volume_multiplier: {date_volume.volume_multiplier}x",
        f"spike_label: {date_volume.spike_label}",
        "",
        "### StageMetrics",
        f"executor_run_time_ms: {metrics.executor_run_time_ms}",
        f"records_read: {metrics.records_read}",
        f"input_bytes: {metrics.input_bytes}",
        f"shuffle_bytes_written: {metrics.shuffle_bytes_written}",
        f"shuffle_bytes_read: {metrics.shuffle_bytes_read}",
        f"num_stages: {metrics.num_stages}",
        f"num_tasks: {metrics.num_tasks}",
        f"memory_bytes_spilled: {metrics.memory_bytes_spilled}",
        f"disk_bytes_spilled: {metrics.disk_bytes_spilled}",
        f"jvm_gc_time_ms: {metrics.jvm_gc_time_ms}",
        "",
        "### Normalized by expected source volume",
        f"runtime_per_million_rows: {metrics.runtime_per_million_rows}",
        f"shuffle_per_million_rows: {metrics.shuffle_per_million_rows}",
        f"input_bytes_per_million_rows: {metrics.input_bytes_per_million_rows}",
        f"tasks_per_million_rows: {metrics.tasks_per_million_rows}",
        "",
        "### Delta outputs",
        f"business_output: {output_path}",
        f"metrics_output: {metrics_output_path}",
    ]
    return _boxed_lines(lines, width=width)


def _metric_int(metrics: Mapping[str, int | float], key: str) -> int:
    value = metrics.get(key, 0)
    if value is None:
        return 0
    return int(value)


def _boxed_lines(lines: list[str], *, width: int) -> str:
    normalized_width = max(width, 60)
    content_width = normalized_width - 4
    border = "═" * (normalized_width - 2)
    rendered = []
    for line in lines:
        trimmed = line[: content_width - 1] + "…" if len(line) > content_width else line
        rendered.append(f"║ {trimmed.ljust(content_width)} ║")
    return f"\n╔{border}╗\n" + "\n".join(rendered) + f"\n╚{border}╝"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file) or {}
