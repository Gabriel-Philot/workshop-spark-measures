from pathlib import Path

from spark_workshop.config import load_comparison_job_config, load_experiment_config


LAB1_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "apps"
    / "labs"
    / "lab_1"
    / "lab_1_utils"
    / "experiments.yaml"
)


def test_lab1_comparison_config_uses_stage_observed_run():
    config = load_comparison_job_config(
        "lab1-global-sort-diagnosis",
        config_path=LAB1_CONFIG,
    )

    assert config.native_config == "lab1-global-sort-diagnosis-native"
    assert config.observed_config == "lab1-global-sort-diagnosis-observed-stage"
    assert config.native_success_marker == "LAB1_GLOBAL_SORT_NATIVE_OK"
    assert config.observed_success_marker == "LAB1_GLOBAL_SORT_SPARKMEASURE_STAGE_OK"
    assert config.success_marker == "LAB1_GLOBAL_SORT_DIAGNOSIS_OK"
    assert config.explain_plan is True
    assert config.explain_plan_modes == ("native",)


def test_lab1_observed_config_uses_stage_metrics_without_persistence():
    config = load_experiment_config(
        "lab1-global-sort-diagnosis-observed-stage",
        config_path=LAB1_CONFIG,
    )

    assert config.app_name == "workshop-lab1-global-sort-observed-stage"
    assert config.observability.enabled is True
    assert config.observability.collector == "stage"
    assert config.observability.persist is False
    assert (
        config.artifacts.output("top_sales_global_sort").path
        == "s3a://lakehouse/gold/lab1/top_sales_global_sort"
    )
    assert config.artifacts.output("top_sales_global_sort").mode == "overwrite"


def test_lab1_random_task_outlier_configs_are_diagnostic_only():
    stage = load_experiment_config(
        "lab1-random-task-outlier-stage",
        config_path=LAB1_CONFIG,
    )
    task = load_experiment_config(
        "lab1-random-task-outlier-task",
        config_path=LAB1_CONFIG,
    )
    fixed = load_experiment_config(
        "lab1-random-task-outlier-fixed-task",
        config_path=LAB1_CONFIG,
    )

    assert stage.observability.collector == "stage"
    assert task.observability.collector == "task"
    assert fixed.observability.collector == "task"
    assert stage.observability.persist is False
    assert task.observability.persist is False
    assert fixed.observability.persist is False
    assert "metrics" not in task.artifacts.outputs
    assert task.spark_config["spark.sql.shuffle.partitions"] == 64
    assert (
        task.artifacts.output("audit_outlier").path
        == "s3a://lakehouse/gold/lab1/random_task_outlier/problematic"
    )
    assert (
        fixed.artifacts.output("audit_outlier").path
        == "s3a://lakehouse/gold/lab1/random_task_outlier/fixed"
    )
