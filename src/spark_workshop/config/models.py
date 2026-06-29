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


@dataclass(frozen=True)
class ComparisonJobConfig:
    """Runtime metadata for a native-vs-observed workshop comparison job."""

    name: str
    native_config: str
    observed_config: str
    native_title: str = "Native Spark run"
    native_description: str | None = None
    observed_title: str = "sparkMeasure observed run"
    observed_description: str | None = None
    completion_title: str = "Workshop comparison complete"
    completion_description: str | None = None
    success_marker: str | None = None
    native_success_marker: str | None = None
    observed_success_marker: str | None = None
    explain_plan: bool = False
    explain_plan_modes: tuple[str | None, ...] = ("native",)
    explain_plan_title: str = "Native Spark explain output"
    explain_plan_description: str = "Physical plan before sparkMeasure"
    explain_plan_mode: str = "formatted"

    @classmethod
    def from_mapping(cls, name: str, value: Mapping[str, Any]) -> "ComparisonJobConfig":
        native = value.get("native") or {}
        observed = value.get("observed") or {}
        completion = value.get("completion") or {}
        explain = value.get("explain") or {}

        native_config = str(native.get("config", ""))
        observed_config = str(observed.get("config", ""))
        if not native_config:
            raise ValueError(f"Comparison job '{name}' requires native.config")
        if not observed_config:
            raise ValueError(f"Comparison job '{name}' requires observed.config")

        explain_modes = explain.get("modes", ("native",))
        if isinstance(explain_modes, str):
            explain_modes = (explain_modes,)

        return cls(
            name=name,
            native_config=native_config,
            observed_config=observed_config,
            native_title=str(native.get("title", "Native Spark run")),
            native_description=_optional_string(native.get("description")),
            observed_title=str(observed.get("title", "sparkMeasure observed run")),
            observed_description=_optional_string(observed.get("description")),
            completion_title=str(
                completion.get("title", "Workshop comparison complete")
            ),
            completion_description=_optional_string(completion.get("description")),
            success_marker=_optional_string(completion.get("success_marker")),
            native_success_marker=_optional_string(native.get("success_marker")),
            observed_success_marker=_optional_string(observed.get("success_marker")),
            explain_plan=bool(explain.get("enabled", False)),
            explain_plan_modes=tuple(
                None if item in ("single", "none", None) else str(item)
                for item in explain_modes
            ),
            explain_plan_title=str(explain.get("title", "Native Spark explain output")),
            explain_plan_description=str(
                explain.get("description", "Physical plan before sparkMeasure")
            ),
            explain_plan_mode=str(explain.get("mode", "formatted")),
        )


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
