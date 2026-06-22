import pytest

from spark_workshop.metrics import normalize_metrics, validate_aggregate_metrics


def test_validate_aggregate_metrics_accepts_useful_stage_report():
    metrics = {
        "numStages": 2,
        "numTasks": 8,
        "executorRunTime": 120,
        "shuffleBytesWritten": 256,
    }

    validate_aggregate_metrics(metrics)


@pytest.mark.parametrize("key", ["numStages", "numTasks", "executorRunTime"])
def test_validate_aggregate_metrics_rejects_missing_required_metric(key):
    metrics = {
        "numStages": 2,
        "numTasks": 8,
        "executorRunTime": 120,
        "shuffleBytesWritten": 256,
    }
    del metrics[key]

    with pytest.raises(ValueError, match="Missing required"):
        validate_aggregate_metrics(metrics)


def test_validate_aggregate_metrics_rejects_report_without_shuffle_metrics():
    with pytest.raises(ValueError, match="shuffle"):
        validate_aggregate_metrics(
            {"numStages": 1, "numTasks": 1, "executorRunTime": 1}
        )


def test_normalize_metrics_keeps_only_numeric_values():
    metrics = normalize_metrics(
        {
            "numStages": 2,
            "ratio": 1.5,
            "description": "ignored",
            "enabled": True,
        }
    )

    assert metrics == {"numStages": 2, "ratio": 1.5}
