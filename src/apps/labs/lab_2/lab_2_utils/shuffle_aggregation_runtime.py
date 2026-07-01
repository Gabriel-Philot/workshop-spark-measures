"""Lab-local settings for the Lab 2A shuffle aggregation lesson.

This module intentionally stays under `lab_2_utils` because the workload
variant and partition knobs are classroom controls, not shared platform
abstractions.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


VALID_SHUFFLE_AGGREGATION_VARIANTS = frozenset({"baseline", "optimized"})


@dataclass(frozen=True)
class ShuffleAggregationSettings:
    """Classroom settings loaded from the selected YAML experiment."""

    variant: str = "baseline"
    success_marker: str = "LAB2_SHUFFLE_AGGREGATION_OK"
    round_robin_partitions: int = 96
    keyed_partitions: int = 32


def load_shuffle_aggregation_settings(
    config_name: str,
    config_path: Path,
) -> ShuffleAggregationSettings:
    """Read Lab 2A workload settings from the local YAML config."""

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    experiments = raw.get("experiments") or {}
    experiment = experiments.get(config_name) or {}
    workload = experiment.get("workload") or {}

    variant = str(workload.get("variant", "baseline")).lower()
    if variant not in VALID_SHUFFLE_AGGREGATION_VARIANTS:
        raise ValueError(
            f"Unsupported Lab 2A workload variant '{variant}'. "
            f"Expected one of {sorted(VALID_SHUFFLE_AGGREGATION_VARIANTS)}"
        )

    return ShuffleAggregationSettings(
        variant=variant,
        success_marker=str(
            workload.get("success_marker", "LAB2_SHUFFLE_AGGREGATION_OK")
        ),
        round_robin_partitions=_positive_int(
            workload.get("round_robin_partitions", 96),
            "round_robin_partitions",
        ),
        keyed_partitions=_positive_int(
            workload.get("keyed_partitions", 32),
            "keyed_partitions",
        ),
    )


def _positive_int(value: object, field_name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"Lab 2A {field_name} must be >= 1")
    return parsed
