from pathlib import Path

from spark_workshop.config import load_experiment_config


LAB2_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "apps"
    / "labs"
    / "lab_2"
    / "lab_2_utils"
    / "experiments.yaml"
)


def test_lab2_shuffle_aggregation_configs_use_stage_metrics_without_persistence():
    baseline = load_experiment_config(
        "lab2-shuffle-aggregation-baseline",
        config_path=LAB2_CONFIG,
    )
    optimized = load_experiment_config(
        "lab2-shuffle-aggregation-optimized",
        config_path=LAB2_CONFIG,
    )

    assert baseline.observability.enabled is True
    assert optimized.observability.enabled is True
    assert baseline.observability.collector == "stage"
    assert optimized.observability.collector == "stage"
    assert baseline.observability.persist is False
    assert optimized.observability.persist is False
    assert (
        baseline.artifacts.output("regional_monthly_sales").path
        == "s3a://lakehouse/gold/lab2/shuffle_aggregation/baseline"
    )
    assert (
        optimized.artifacts.output("regional_monthly_sales").path
        == "s3a://lakehouse/gold/lab2/shuffle_aggregation/optimized"
    )


def test_lab2_shuffle_aggregation_spark_configs_are_explicit():
    baseline = load_experiment_config(
        "lab2-shuffle-aggregation-baseline",
        config_path=LAB2_CONFIG,
    )
    optimized = load_experiment_config(
        "lab2-shuffle-aggregation-optimized",
        config_path=LAB2_CONFIG,
    )

    assert baseline.spark_config["spark.sql.shuffle.partitions"] == 96
    assert optimized.spark_config["spark.sql.shuffle.partitions"] == 96


def test_lab2_stage_metrics_drill_configs_use_stage_metrics_without_persistence():
    default = load_experiment_config(
        "lab2-stage-metrics-drill-default",
        config_path=LAB2_CONFIG,
    )
    pressure = load_experiment_config(
        "lab2-stage-metrics-drill-pressure",
        config_path=LAB2_CONFIG,
    )

    assert default.observability.enabled is True
    assert pressure.observability.enabled is True
    assert default.observability.collector == "stage"
    assert pressure.observability.collector == "stage"
    assert default.observability.persist is False
    assert pressure.observability.persist is False
    assert (
        default.artifacts.output("stage_metrics_summary").path
        == "s3a://lakehouse/gold/lab2/stage_metrics_drill/default"
    )
    assert (
        pressure.artifacts.output("stage_metrics_summary").path
        == "s3a://lakehouse/gold/lab2/stage_metrics_drill/pressure"
    )


def test_lab2_stage_metrics_drill_spark_configs_are_explicit():
    default = load_experiment_config(
        "lab2-stage-metrics-drill-default",
        config_path=LAB2_CONFIG,
    )
    pressure = load_experiment_config(
        "lab2-stage-metrics-drill-pressure",
        config_path=LAB2_CONFIG,
    )

    assert default.spark_config["spark.sql.shuffle.partitions"] == 96
    assert pressure.spark_config["spark.sql.shuffle.partitions"] == 96
