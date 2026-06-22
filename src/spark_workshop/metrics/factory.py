"""Factory and adapters for sparkMeasure collectors."""

from abc import ABC, abstractmethod
from typing import Any, Mapping


class MetricsCollector(ABC):
    @abstractmethod
    def begin(self) -> None:
        pass

    @abstractmethod
    def end(self) -> None:
        pass

    @abstractmethod
    def print_report(self) -> None:
        pass

    @abstractmethod
    def aggregate(self) -> Mapping[str, Any]:
        pass


class SparkMeasureCollector(MetricsCollector):
    def __init__(self, delegate: Any, aggregate_method: str):
        self.delegate = delegate
        self.aggregate_method = aggregate_method

    def begin(self) -> None:
        self.delegate.begin()

    def end(self) -> None:
        self.delegate.end()

    def print_report(self) -> None:
        self.delegate.print_report()

    def aggregate(self) -> Mapping[str, Any]:
        return dict(getattr(self.delegate, self.aggregate_method)())


class SparkMeasureFactory:
    @staticmethod
    def create(collector_type: str, spark: Any) -> MetricsCollector:
        normalized = collector_type.lower()
        delegate, aggregate_method = _create_delegate(normalized, spark)
        return SparkMeasureCollector(delegate, aggregate_method)


def _create_delegate(collector_type: str, spark: Any) -> tuple[Any, str]:
    if collector_type == "stage":
        from sparkmeasure import StageMetrics

        return StageMetrics(spark), "aggregate_stagemetrics"
    if collector_type == "task":
        from sparkmeasure import TaskMetrics

        return TaskMetrics(spark), "aggregate_taskmetrics"
    raise ValueError(f"Unsupported sparkMeasure collector: {collector_type}")
