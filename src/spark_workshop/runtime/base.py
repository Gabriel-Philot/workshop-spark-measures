"""Runtime workload interface and execution context."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from spark_workshop.artifacts import read_artifact, write_artifact
from spark_workshop.config import ExperimentConfig
from spark_workshop.utils import logger


@dataclass(frozen=True)
class ExperimentContext:
    spark: Any
    config: ExperimentConfig

    @property
    def logger(self) -> Any:
        return logger

    def read(self, artifact_name: str) -> Any:
        return read_artifact(
            self.spark, self.config.artifacts.input(artifact_name)
        )

    def write(self, artifact_name: str, dataframe: Any) -> None:
        write_artifact(dataframe, self.config.artifacts.output(artifact_name))

    def artifact_path(self, location_name: str) -> str:
        return self.config.artifacts.location(location_name)


class SparkExperiment(ABC):
    """Behavior implemented by each runtime-managed workshop workload."""

    def prepare(self, context: ExperimentContext) -> None:
        pass

    @abstractmethod
    def workload(self, context: ExperimentContext) -> Any:
        pass

    def validate(self, result: Any, context: ExperimentContext) -> None:
        pass

    def cleanup(self, context: ExperimentContext) -> None:
        pass
