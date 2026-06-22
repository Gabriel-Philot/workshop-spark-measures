"""Validation and serialization helpers for sparkMeasure output."""

from collections.abc import Mapping
from numbers import Number
from typing import Any


REQUIRED_METRICS = ("numStages", "numTasks", "executorRunTime")
SHUFFLE_METRICS = ("shuffleBytesWritten", "shuffleTotalBytesRead")


def normalize_metrics(metrics: Mapping[str, Any]) -> dict[str, int | float]:
    """Return numeric sparkMeasure metrics with plain Python scalar values."""
    normalized: dict[str, int | float] = {}
    for key, value in metrics.items():
        if isinstance(value, bool) or not isinstance(value, Number):
            continue
        normalized[str(key)] = value
    return normalized


def validate_aggregate_metrics(metrics: Mapping[str, Any]) -> None:
    """Raise ValueError when a stage metrics aggregate is not useful."""
    missing = [key for key in REQUIRED_METRICS if key not in metrics]
    if missing:
        raise ValueError(f"Missing required sparkMeasure metrics: {', '.join(missing)}")

    for key in ("numStages", "numTasks"):
        if not isinstance(metrics[key], Number) or metrics[key] <= 0:
            raise ValueError(f"Metric {key} must be greater than zero")

    if not isinstance(metrics["executorRunTime"], Number) or metrics["executorRunTime"] < 0:
        raise ValueError("Metric executorRunTime must be non-negative")

    if not any(key in metrics for key in SHUFFLE_METRICS):
        raise ValueError("Expected at least one shuffle metric")
