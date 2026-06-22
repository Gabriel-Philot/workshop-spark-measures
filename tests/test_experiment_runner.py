from types import SimpleNamespace

from spark_workshop.config import (
    ArtifactCatalog,
    ArtifactSpec,
    ExperimentConfig,
    ObservabilityConfig,
)
from spark_workshop.experiments import (
    ExperimentContext,
    ExperimentRunner,
    SparkExperiment,
)
from spark_workshop.experiments import runner as runner_module


class RecordingExperiment(SparkExperiment):
    def __init__(self):
        self.events = []

    def prepare(self, context):
        self.events.append("prepare")

    def workload(self, context):
        self.events.append("workload")
        return "result"

    def validate(self, result, context):
        self.events.append(("validate", result))

    def cleanup(self, context):
        self.events.append("cleanup")


class FakeCollector:
    def __init__(self):
        self.events = []

    def begin(self):
        self.events.append("begin")

    def end(self):
        self.events.append("end")

    def print_report(self):
        self.events.append("report")

    def aggregate(self):
        return {
            "numStages": 2,
            "numTasks": 4,
            "executorRunTime": 10,
            "shuffleBytesWritten": 20,
        }


def fake_spark():
    spark_context = SimpleNamespace(
        applicationId="app-test",
        setLogLevel=lambda level: None,
    )
    return SimpleNamespace(
        sparkContext=spark_context,
        version="4.1.2",
        createDataFrame=lambda records: ("dataframe", records),
    )


def measured_config(enabled=True, persist=True):
    return ExperimentConfig(
        name="unit-experiment",
        app_name="unit-app",
        observability=ObservabilityConfig(
            enabled=enabled,
            collector="stage",
            persist=persist,
            output_artifact="metrics",
        ),
        artifacts=ArtifactCatalog(
            outputs={
                "metrics": ArtifactSpec(
                    path="s3a://observability/unit", mode="overwrite"
                )
            }
        ),
    )


def test_runner_wraps_only_workload_with_collector(monkeypatch):
    spark = fake_spark()
    collector = FakeCollector()
    stopped = []
    writes = []
    monkeypatch.setattr(
        runner_module.SparkSessionSingleton,
        "get_or_create",
        classmethod(lambda cls, *args, **kwargs: spark),
    )
    monkeypatch.setattr(
        runner_module.SparkSessionSingleton,
        "stop",
        classmethod(lambda cls: stopped.append(True)),
    )
    monkeypatch.setattr(
        runner_module.SparkMeasureFactory,
        "create",
        staticmethod(lambda *_: collector),
    )
    monkeypatch.setattr(
        ExperimentContext,
        "write",
        lambda self, name, dataframe: writes.append((name, dataframe)),
    )
    experiment = RecordingExperiment()

    run = ExperimentRunner(measured_config()).run(experiment)

    assert experiment.events == [
        "prepare",
        "workload",
        ("validate", "result"),
        "cleanup",
    ]
    assert collector.events == ["begin", "end", "report"]
    assert run.metrics["numStages"] == 2
    assert run.metrics_output_path == "s3a://observability/unit"
    assert writes[0][0] == "metrics"
    assert stopped == [True]


def test_runner_executes_without_sparkmeasure_when_disabled(monkeypatch):
    spark = fake_spark()
    monkeypatch.setattr(
        runner_module.SparkSessionSingleton,
        "get_or_create",
        classmethod(lambda cls, *args, **kwargs: spark),
    )
    monkeypatch.setattr(
        runner_module.SparkSessionSingleton,
        "stop",
        classmethod(lambda cls: None),
    )
    monkeypatch.setattr(
        runner_module.SparkMeasureFactory,
        "create",
        staticmethod(lambda *_: (_ for _ in ()).throw(AssertionError("must not run"))),
    )

    run = ExperimentRunner(measured_config(enabled=False)).run(
        RecordingExperiment()
    )

    assert run.metrics == {}
    assert run.metrics_output_path is None
