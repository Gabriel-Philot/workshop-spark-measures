from pathlib import Path

from spark_workshop.config import load_experiment_config


LAB3_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "apps"
    / "labs"
    / "lab_3"
    / "lab_3_utils"
    / "experiments.yaml"
)


def test_lab3_overhead_configs_select_expected_observability_modes():
    none = load_experiment_config("lab3-overhead-none", config_path=LAB3_CONFIG)
    stage = load_experiment_config("lab3-overhead-stage", config_path=LAB3_CONFIG)
    task = load_experiment_config("lab3-overhead-task", config_path=LAB3_CONFIG)

    assert none.observability.enabled is False
    assert stage.observability.enabled is True
    assert task.observability.enabled is True
    assert stage.observability.collector == "stage"
    assert task.observability.collector == "task"
    assert none.observability.persist is False
    assert stage.observability.persist is False
    assert task.observability.persist is False


def test_lab3_overhead_configs_share_same_artifact_locations():
    configs = [
        load_experiment_config("lab3-overhead-none", config_path=LAB3_CONFIG),
        load_experiment_config("lab3-overhead-stage", config_path=LAB3_CONFIG),
        load_experiment_config("lab3-overhead-task", config_path=LAB3_CONFIG),
    ]

    for config in configs:
        assert (
            config.artifacts.location("workload_output_base")
            == "s3a://lakehouse/gold/lab3/observability_overhead/workload"
        )
        assert (
            config.artifacts.location("metadata_output_path")
            == "s3a://observability/lab3/overhead_runs"
        )
        assert (
            config.artifacts.input("sales").path
            == "s3a://lakehouse/bronze/retail/sales"
        )
        assert (
            config.artifacts.input("vendors").path
            == "s3a://lakehouse/bronze/retail/vendors"
        )
        assert (
            config.artifacts.input("products").path
            == "s3a://lakehouse/bronze/retail/products"
        )
        assert (
            config.artifacts.input("customers").path
            == "s3a://lakehouse/bronze/retail/customers"
        )


def test_lab3_overhead_configs_keep_workload_spark_settings_equal():
    configs = [
        load_experiment_config("lab3-overhead-none", config_path=LAB3_CONFIG),
        load_experiment_config("lab3-overhead-stage", config_path=LAB3_CONFIG),
        load_experiment_config("lab3-overhead-task", config_path=LAB3_CONFIG),
    ]

    assert {
        config.spark_config["spark.sql.shuffle.partitions"] for config in configs
    } == {384}
    assert {config.spark_config["spark.sql.adaptive.enabled"] for config in configs} == {
        False
    }
    assert {
        config.spark_config["spark.sql.autoBroadcastJoinThreshold"]
        for config in configs
    } == {-1}
