from apps.labs.lab_1.random_task_outlier_diagnosis import (
    CONFIG_BY_MODE,
    parse_args,
    render_task_outlier_line,
)


def test_random_task_outlier_parse_args_defaults_to_stage_problematic():
    args = parse_args([])

    assert args.collector == "stage"
    assert args.variant == "problematic"
    assert CONFIG_BY_MODE[(args.collector, args.variant)] == "lab1-random-task-outlier-stage"


def test_random_task_outlier_parse_args_supports_task_fixed():
    args = parse_args(["--collector", "task", "--variant", "fixed"])

    assert args.collector == "task"
    assert args.variant == "fixed"
    assert CONFIG_BY_MODE[(args.collector, args.variant)] == "lab1-random-task-outlier-fixed-task"


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
