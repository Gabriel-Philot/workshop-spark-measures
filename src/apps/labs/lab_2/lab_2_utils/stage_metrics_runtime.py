"""Lab-local settings helpers for Lab 2B.

This module stays under `lab_2_utils` because the workload variants are
classroom controls, not shared platform abstractions.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


VALID_STAGE_METRICS_VARIANTS = frozenset({"default", "pressure"})


@dataclass(frozen=True)
class StageMetricsDrillSettings:
    """Classroom settings loaded from the selected YAML experiment."""

    variant: str = "default"
    success_marker: str = "LAB2_STAGE_METRICS_DRILL_OK"
    keyed_partitions: int = 32
    round_robin_partitions: int = 96


def load_stage_metrics_drill_settings(
    config_name: str,
    config_path: Path,
) -> StageMetricsDrillSettings:
    """Read Lab 2B workload settings from the local YAML config."""

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    experiments = raw.get("experiments") or {}
    experiment = experiments.get(config_name) or {}
    workload = experiment.get("workload") or {}

    variant = str(workload.get("variant", "default")).lower()
    if variant not in VALID_STAGE_METRICS_VARIANTS:
        raise ValueError(
            f"Unsupported Lab 2B workload variant '{variant}'. "
            f"Expected one of {sorted(VALID_STAGE_METRICS_VARIANTS)}"
        )

    return StageMetricsDrillSettings(
        variant=variant,
        success_marker=str(workload.get("success_marker", "LAB2_STAGE_METRICS_DRILL_OK")),
        keyed_partitions=_positive_int(
            workload.get("keyed_partitions", 32),
            "keyed_partitions",
        ),
        round_robin_partitions=_positive_int(
            workload.get("round_robin_partitions", 96),
            "round_robin_partitions",
        ),
    )


def _positive_int(value: object, field_name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"Lab 2B {field_name} must be >= 1")
    return parsed
