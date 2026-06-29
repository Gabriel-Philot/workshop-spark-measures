from types import SimpleNamespace

from spark_workshop.experiments import ExperimentRun
from spark_workshop.jobs import SparkWorkshopComparisonJob, SparkWorkshopJob
from spark_workshop.jobs import base as jobs_module


class FakeRunner:
    calls = []

    def __init__(self, config):
        self.config = config

    def run(self, experiment):
        context = SimpleNamespace(config=self.config)
        experiment.prepare(context)
        result = experiment.workload(context)
        experiment.validate(result, context)
        experiment.cleanup(context)
        self.calls.append((self.config.name, result))
        return ExperimentRun(
            run_id=f"run-{self.config.name}",
            experiment_name=self.config.name,
            application_id=f"app-{self.config.name}",
            workload_result=result,
            metrics={"numStages": 1, "numTasks": 2} if "observed" in self.config.name else {},
            metrics_output_path=None,
        )


def patch_runner(monkeypatch):
    FakeRunner.calls = []
    monkeypatch.setattr(jobs_module, "ExperimentRunner", FakeRunner)
    monkeypatch.setattr(
        jobs_module,
        "load_experiment_config",
        lambda name, config_path=None: SimpleNamespace(name=name, config_path=config_path),
    )


def test_workshop_job_runs_extract_transform_load_validate(monkeypatch):
    patch_runner(monkeypatch)

    class RecordingJob(SparkWorkshopJob):
        config_name = "unit-single"
        success_marker = "UNIT_OK"

        def __init__(self):
            super().__init__()
            self.events = []

        def extract(self):
            self.events.append("extract")
            return "source"

        def transform(self, data):
            self.events.append(("transform", data))
            return "transformed"

        def load(self, data):
            self.events.append(("load", data))
            return "output"

        def validate_result(self, result):
            self.events.append(("validate", result))

    job = RecordingJob()

    assert job.run() == 0
    assert job.events == [
        "extract",
        ("transform", "source"),
        ("load", "transformed"),
        ("validate", "output"),
    ]
    assert FakeRunner.calls == [("unit-single", "output")]


def test_comparison_job_runs_native_then_observed(monkeypatch):
    patch_runner(monkeypatch)

    class RecordingComparison(SparkWorkshopComparisonJob):
        native_config = "unit-native"
        observed_config = "unit-observed"

        def __init__(self):
            super().__init__()
            self.modes = []

        def extract(self):
            self.modes.append(("extract", self._run_mode))
            return self._run_mode

        def transform(self, data):
            self.modes.append(("transform", data))
            return f"{data}-transformed"

        def load(self, data):
            self.modes.append(("load", data))
            return data

    job = RecordingComparison()

    assert job.run() == 0
    assert FakeRunner.calls == [
        ("unit-native", "native-transformed"),
        ("unit-observed", "observed-transformed"),
    ]
    assert job.modes == [
        ("extract", "native"),
        ("transform", "native"),
        ("load", "native-transformed"),
        ("extract", "observed"),
        ("transform", "observed"),
        ("load", "observed-transformed"),
    ]


def test_explain_plan_runs_only_for_native_comparison(monkeypatch):
    patch_runner(monkeypatch)

    class ExplainableDataFrame:
        def __init__(self):
            self.explains = 0

        def explain(self, mode):
            self.explains += 1
            assert mode == "formatted"

    dataframe = ExplainableDataFrame()

    class ExplainComparison(SparkWorkshopComparisonJob):
        native_config = "unit-native"
        observed_config = "unit-observed"
        explain_plan = True

        def extract(self):
            return dataframe

        def transform(self, data):
            return data

        def load(self, data):
            return "output"

    assert ExplainComparison().run() == 0
    assert dataframe.explains == 1


def test_workshop_job_passes_config_path_to_loader(monkeypatch):
    seen = []

    monkeypatch.setattr(jobs_module, "ExperimentRunner", FakeRunner)
    monkeypatch.setattr(
        jobs_module,
        "load_experiment_config",
        lambda name, config_path=None: seen.append((name, config_path))
        or SimpleNamespace(name=name, config_path=config_path),
    )

    class LocalConfigJob(SparkWorkshopJob):
        config_name = "unit-local"
        config_path = "local/experiments.yaml"

        def load(self, data):
            return "ok"

    assert LocalConfigJob().run() == 0
    assert seen == [("unit-local", "local/experiments.yaml")]
