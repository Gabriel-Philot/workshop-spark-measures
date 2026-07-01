from apps.labs.lab_1.random_task_outlier_diagnosis import (
    CONFIG_NAME,
    _load_workload_settings,
    render_task_outlier_line,
)


def test_random_task_outlier_default_config_is_stage_problematic():
    assert CONFIG_NAME == "lab1-random-task-outlier-stage"

    settings = _load_workload_settings(CONFIG_NAME)

    assert settings.variant == "problematic"
    assert settings.success_marker == "LAB1_RANDOM_TASK_OUTLIER_STAGE_OK"


def test_random_task_outlier_fixed_task_settings_come_from_yaml():
    settings = _load_workload_settings("lab1-random-task-outlier-fixed-task")

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
