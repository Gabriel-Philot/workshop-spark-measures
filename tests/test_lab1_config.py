import ast
from pathlib import Path

from spark_workshop.config import load_experiment_config


LAB1_DIR = Path(__file__).resolve().parents[1] / "src" / "apps" / "labs" / "lab_1"
LAB1_CONFIG = LAB1_DIR / "lab_1_utils" / "experiments.yaml"


def test_lab1_global_sort_script_uses_config_name_switch():
    source = (LAB1_DIR / "lab_1a_global_sort_diagnosis.py").read_text()
    module = ast.parse(source)
    assignments = {
        node.targets[0].id: node.value.value
        for node in module.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Constant)
    }
    class_assignments = {
        node.targets[0].id: node.value.id
        for item in module.body
        if isinstance(item, ast.ClassDef)
        and item.name == "Lab1GlobalSortDiagnosis"
        for node in item.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Name)
    }

    assert assignments["CONFIG_NAME"] == "lab1-global-sort-diagnosis-native"
    assert assignments["CONFIG_NAME"] != "lab1-global-sort-diagnosis"
    assert class_assignments["config_name"] == "CONFIG_NAME"
    assert "SparkWorkshopComparisonJob" not in source


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
