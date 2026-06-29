import pytest

from spark_workshop.config import ArtifactSpec, load_experiment_config


def test_load_experiment_config_merges_defaults_and_named_experiment():
    config = load_experiment_config("sparkmeasure-dry-test")

    assert config.app_name == "workshop-spark-measures-dry-test"
    assert config.observability.enabled is True
    assert config.spark_log_level == "WARN"
    assert config.artifacts.output("metrics").options["overwriteSchema"] is True
    assert config.observability.collector == "stage"
    assert config.artifacts.location("lakehouse") == "s3a://lakehouse"
    assert config.artifacts.output("metrics").format == "delta"
    assert config.artifacts.output("metrics").mode == "overwrite"


def test_load_experiment_config_expands_environment(monkeypatch):
    monkeypatch.setenv("SPARK_WORKSHOP_METRICS_PATH", "s3a://tests/custom-metrics")

    config = load_experiment_config("sparkmeasure-dry-test")

    assert config.artifacts.output("metrics").path == "s3a://tests/custom-metrics"


def test_load_experiment_config_rejects_unknown_experiment():
    with pytest.raises(KeyError, match="Unknown experiment"):
        load_experiment_config("missing")


def test_artifact_spec_rejects_unsupported_format():
    with pytest.raises(ValueError, match="Unsupported artifact format"):
        ArtifactSpec.from_mapping({"path": "s3a://tests/data", "format": "csv"})


def test_load_local_experiment_config_inherits_global_defaults(tmp_path):
    local_config = tmp_path / "experiments.yaml"
    local_config.write_text(
        """experiments:
  local-lab:
    app_name: workshop-local-lab
    observability:
      enabled: false
    artifacts:
      inputs:
        source:
          format: delta
          path: ${LOCAL_SOURCE_PATH:-s3a://lakehouse/bronze/source}
""",
        encoding="utf-8",
    )

    config = load_experiment_config("local-lab", config_path=local_config)

    assert config.app_name == "workshop-local-lab"
    assert config.observability.enabled is False
    assert config.spark_log_level == "WARN"
    assert config.spark_config["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert config.artifacts.location("lakehouse") == "s3a://lakehouse"
    assert config.artifacts.input("source").path == "s3a://lakehouse/bronze/source"
