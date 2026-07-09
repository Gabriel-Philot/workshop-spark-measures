from apps.labs.lab_1 import lab_1b_random_task_outlier_diagnosis
from apps.labs.lab_1.lab_1b_random_task_outlier_diagnosis import CONFIG_PATH
from apps.labs.lab_1.lab_1_utils.random_task_outlier_runtime import (
    load_workload_settings,
    render_task_outlier_line,
    render_task_outlier_report,
)


def test_random_task_outlier_default_config_is_stage_problematic():
    assert (
        lab_1b_random_task_outlier_diagnosis.CONFIG_NAME
        == "lab1-random-task-outlier-stage"
    )

    settings = load_workload_settings(
        lab_1b_random_task_outlier_diagnosis.CONFIG_NAME,
        CONFIG_PATH,
    )

    assert settings.variant == "problematic"
    assert settings.success_marker == "LAB1_RANDOM_TASK_OUTLIER_STAGE_OK"


def test_random_task_outlier_fixed_task_settings_come_from_yaml():
    settings = load_workload_settings(
        "lab1-random-task-outlier-fixed-task",
        CONFIG_PATH,
    )

    assert settings.variant == "fixed"
    assert settings.success_marker == "LAB1_RANDOM_TASK_OUTLIER_FIXED_TASK_OK"


def test_render_task_outlier_line_includes_task_identity_and_cost():
    line = render_task_outlier_line(
        1,
        {
            "stageId": 3,
            "index": 42,
            "executorId": "1",
            "duration": 9000,
            "executorRunTime": 8500,
            "recordsRead": 100,
            "recordsWritten": 50,
            "shuffleTotalBytesRead": 2048,
            "shuffleBytesWritten": 1024,
            "memoryBytesSpilled": 512,
            "diskBytesSpilled": 256,
        },
    )

    assert line.startswith("LAB1_TASK_OUTLIER rank=1")
    assert "stageId=3" in line
    assert "taskIndex=42" in line
    assert "executorRunTime=8500" in line
    assert "shuffleTotalBytesRead=2048" in line


def test_render_task_outlier_report_boxes_extra_task_metrics():
    report = render_task_outlier_report(
        [
            {
                "stageId": 3,
                "index": 42,
                "executorId": "1",
                "duration": 9000,
                "executorRunTime": 8500,
                "recordsWritten": 50,
                "shuffleTotalBytesRead": 2048,
                "memoryBytesSpilled": 512,
                "diskBytesSpilled": 256,
            }
        ]
    )

    assert "╔" in report
    assert "LAB 1B TASK OUTLIER DIAGNOSTIC REPORT" in report
    assert "LAB1_TASK_OUTLIER rank=1" in report
    assert "executorRunTime=8500" in report
    assert "shuffleTotalBytesRead=2048" in report
    assert "╚" in report
