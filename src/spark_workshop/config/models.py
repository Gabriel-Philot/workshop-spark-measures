"""Typed configuration models for workshop experiments."""

from dataclasses import dataclass, field
from typing import Any, Mapping


SUPPORTED_ARTIFACT_FORMATS = frozenset({"delta", "json"})
SUPPORTED_COLLECTORS = frozenset({"stage", "task"})


@dataclass(frozen=True)
class ArtifactSpec:
    path: str
    format: str = "delta"
    mode: str = "errorifexists"
    options: Mapping[str, Any] = field(default_factory=dict)
    partition_by: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ArtifactSpec":
        artifact_format = str(value.get("format", "delta")).lower()
        path = str(value.get("path", ""))
        options = value.get("options") or {}
        partition_by = value.get("partition_by") or ()

        if artifact_format not in SUPPORTED_ARTIFACT_FORMATS:
            raise ValueError(
                f"Unsupported artifact format '{artifact_format}'. "
                f"Expected one of {sorted(SUPPORTED_ARTIFACT_FORMATS)}"
            )
        if not path:
            raise ValueError("Artifact configuration requires a path")
        if not isinstance(options, Mapping):
            raise ValueError("Artifact options must be a mapping")
        if isinstance(partition_by, str):
            partition_by = (partition_by,)

        return cls(
            path=path,
            format=artifact_format,
            mode=str(value.get("mode", "errorifexists")),
            options=dict(options),
            partition_by=tuple(str(item) for item in partition_by),
        )


@dataclass(frozen=True)
class ArtifactCatalog:
    locations: Mapping[str, str] = field(default_factory=dict)
    inputs: Mapping[str, ArtifactSpec] = field(default_factory=dict)
    outputs: Mapping[str, ArtifactSpec] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ArtifactCatalog":
        locations = {
            str(name): str(path) for name, path in (value.get("locations") or {}).items()
        }
        inputs = {
            str(name): ArtifactSpec.from_mapping(spec)
            for name, spec in (value.get("inputs") or {}).items()
        }
        outputs = {
            str(name): ArtifactSpec.from_mapping(spec)
            for name, spec in (value.get("outputs") or {}).items()
        }
        return cls(locations=locations, inputs=inputs, outputs=outputs)

    def input(self, name: str) -> ArtifactSpec:
        try:
            return self.inputs[name]
        except KeyError as exc:
            raise KeyError(f"Unknown input artifact '{name}'") from exc

    def output(self, name: str) -> ArtifactSpec:
        try:
            return self.outputs[name]
        except KeyError as exc:
            raise KeyError(f"Unknown output artifact '{name}'") from exc

    def location(self, name: str) -> str:
        try:
            return self.locations[name]
        except KeyError as exc:
            raise KeyError(f"Unknown artifact location '{name}'") from exc


@dataclass(frozen=True)
class ObservabilityConfig:
    enabled: bool = True
    collector: str = "stage"
    persist: bool = True
    output_artifact: str = "metrics"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ObservabilityConfig":
        collector = str(value.get("collector", "stage")).lower()
        if collector not in SUPPORTED_COLLECTORS:
            raise ValueError(
                f"Unsupported collector '{collector}'. "
                f"Expected one of {sorted(SUPPORTED_COLLECTORS)}"
            )
        return cls(
            enabled=bool(value.get("enabled", True)),
            collector=collector,
            persist=bool(value.get("persist", True)),
            output_artifact=str(value.get("output_artifact", "metrics")),
        )


@dataclass(frozen=True)
class ExperimentConfig:
    name: str
    app_name: str
    log_level: str = "INFO"
    spark_log_level: str = "WARN"
    spark_config: Mapping[str, Any] = field(default_factory=dict)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    artifacts: ArtifactCatalog = field(default_factory=ArtifactCatalog)

    @classmethod
    def from_mapping(cls, name: str, value: Mapping[str, Any]) -> "ExperimentConfig":
        app_name = str(value.get("app_name", ""))
        if not app_name:
            raise ValueError(f"Experiment '{name}' requires app_name")
        spark = value.get("spark") or {}
        return cls(
            name=name,
            app_name=app_name,
            log_level=str(value.get("log_level", "INFO")),
            spark_log_level=str(spark.get("log_level", "WARN")),
            spark_config=dict(spark.get("config") or {}),
            observability=ObservabilityConfig.from_mapping(
                value.get("observability") or {}
            ),
            artifacts=ArtifactCatalog.from_mapping(value.get("artifacts") or {}),
        )
