"""Readable workshop job contracts built on top of ExperimentRunner."""

from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Any

from spark_workshop.config import load_comparison_job_config, load_experiment_config
from spark_workshop.runtime import (
    ExperimentContext,
    ExperimentRun,
    ExperimentRunner,
    SparkExperiment,
)
from spark_workshop.utils import logger, terminal_section


class SparkWorkshopJob(SparkExperiment, ABC):
    """ETL-style workshop contract for a single configured Spark experiment."""

    config_name: str = ""
    config_path: str | Path | None = None
    title: str = "Spark workshop job"
    description: str | None = None
    success_marker: str | None = None
    explain_plan: bool = False
    explain_plan_modes: tuple[str | None, ...] = (None, "native")
    explain_plan_title: str = "Native Spark explain output"
    explain_plan_description: str = "Physical plan before sparkMeasure"
    explain_plan_mode: str = "formatted"

    def __init__(self) -> None:
        self._context: ExperimentContext | None = None
        self._run_mode: str | None = None

    @property
    def context(self) -> ExperimentContext:
        if self._context is None:
            raise RuntimeError("Workshop job context is not initialized")
        return self._context

    @property
    def logger(self) -> Any:
        return logger

    def run(self) -> int:
        run = self.run_once(self.config_name, mode=None, log_section=True)
        self.log_run_summary(run)
        if self.success_marker:
            logger.info(self.success_marker)
        self._run_mode = None
        return 0

    def run_once(
        self,
        config_name: str,
        *,
        mode: str | None = None,
        log_section: bool = False,
        title: str | None = None,
        description: str | None = None,
    ) -> ExperimentRun:
        if not config_name:
            raise ValueError("Workshop job requires a config_name")
        self._run_mode = mode
        if log_section:
            self.log_section(title or self.title, description or self.description)
        config = load_experiment_config(config_name, config_path=self.config_path)
        try:
            return ExperimentRunner(config).run(self)
        finally:
            self._context = None

    def prepare(self, context: ExperimentContext) -> None:
        self._context = context
        self.before_extract()
        if self._should_explain_plan():
            source = self.extract()
            transformed = self.transform(source)
            logger.info(
                terminal_section(
                    self.explain_plan_title,
                    self.explain_plan_description,
                )
            )
            logger.info(
                f"SPARK_EXPLAIN mode={self.explain_plan_mode} experiment={context.config.name}"
            )
            transformed.explain(mode=self.explain_plan_mode)

    def workload(self, context: ExperimentContext) -> Any:
        self._context = context
        source = self.extract()
        transformed = self.transform(source)
        return self.load(transformed)

    def validate(self, result: Any, context: ExperimentContext) -> None:
        self._context = context
        self.validate_result(result)

    def cleanup(self, context: ExperimentContext) -> None:
        self._context = context
        self.after_run()

    def before_extract(self) -> None:
        """Optional setup hook outside the measured workload."""

    def extract(self) -> Any:
        """Return source data for transform()."""
        return None

    def transform(self, data: Any) -> Any:
        """Return transformed data. Keep the lab logic visible here."""
        return data

    def load(self, data: Any) -> Any:
        """Write or return transformed data."""
        return data

    def validate_result(self, result: Any) -> None:
        """Optional validation hook outside the measured workload."""

    def after_run(self) -> None:
        """Optional cleanup hook outside the measured workload."""

    def read(self, artifact_name: str) -> Any:
        return self.context.read(artifact_name)

    def write(self, artifact_name: str, dataframe: Any) -> None:
        self.context.write(artifact_name, dataframe)

    def output_path(self, artifact_name: str) -> str:
        return self.context.config.artifacts.output(artifact_name).path

    def input_path(self, artifact_name: str) -> str:
        return self.context.config.artifacts.input(artifact_name).path

    def log_section(self, title: str, description: str | None = None) -> None:
        logger.info(terminal_section(title, description))
        logger.info(
            "WORKSHOP_SECTION "
            f"title={_quote(title)} "
            f"description={_quote(description or '')}"
        )

    def log_run_summary(self, run: ExperimentRun) -> None:
        logger.info(
            "WORKSHOP_RUN_COMPLETED "
            f"experiment={run.experiment_name} "
            f"application_id={run.application_id} "
            f"mode={self._run_mode or 'single'}"
        )
        if run.workload_result is not None:
            logger.info(_workshop_output_line(run))
        if run.metrics:
            _log_sparkmeasure_metrics(run)
        if run.metrics_output_path:
            logger.info(f"SPARKMEASURE_DELTA_PATH={run.metrics_output_path}")

    def _should_explain_plan(self) -> bool:
        return self.explain_plan and self._run_mode in self.explain_plan_modes


class SparkWorkshopComparisonJob(SparkWorkshopJob):
    """Workshop job that runs the same contract with native and observed configs."""

    job_name: str = ""
    native_config: str = ""
    observed_config: str = ""
    native_title: str = "Native Spark run"
    native_description: str | None = None
    observed_title: str = "sparkMeasure observed run"
    observed_description: str | None = None
    completion_title: str = "Workshop comparison complete"
    completion_description: str | None = None
    native_success_marker: str | None = None
    observed_success_marker: str | None = None

    def run(self) -> int:
        self._apply_comparison_config()
        native_run = self.run_once(
            self.native_config,
            mode="native",
            log_section=True,
            title=self.native_title,
            description=self.native_description,
        )
        self.log_run_summary(native_run)
        if self.native_success_marker:
            logger.info(self.native_success_marker)

        observed_run = self.run_once(
            self.observed_config,
            mode="observed",
            log_section=True,
            title=self.observed_title,
            description=self.observed_description,
        )
        self.log_run_summary(observed_run)
        if self.observed_success_marker:
            logger.info(self.observed_success_marker)

        self.log_section(self.completion_title, self.completion_description)
        self.after_comparison(native_run, observed_run)
        if self.success_marker:
            logger.info(self.success_marker)
        self._run_mode = None
        return 0


    def _apply_comparison_config(self) -> None:
        if not self.job_name:
            return
        config = load_comparison_job_config(self.job_name, config_path=self.config_path)
        self.native_config = config.native_config
        self.observed_config = config.observed_config
        self.native_title = config.native_title
        self.native_description = config.native_description
        self.observed_title = config.observed_title
        self.observed_description = config.observed_description
        self.completion_title = config.completion_title
        self.completion_description = config.completion_description
        self.success_marker = config.success_marker
        self.native_success_marker = config.native_success_marker
        self.observed_success_marker = config.observed_success_marker
        self.explain_plan = config.explain_plan
        self.explain_plan_modes = config.explain_plan_modes
        self.explain_plan_title = config.explain_plan_title
        self.explain_plan_description = config.explain_plan_description
        self.explain_plan_mode = config.explain_plan_mode

    def after_comparison(
        self,
        native_run: ExperimentRun,
        observed_run: ExperimentRun,
    ) -> None:
        """Optional hook after both comparison runs complete."""


def _workshop_output_line(run: ExperimentRun) -> str:
    result = run.workload_result
    if isinstance(result, (str, int, float, bool)):
        rendered = f"result={result}"
    else:
        rendered = f"result_type={type(result).__name__}"
    return f"WORKSHOP_OUTPUT experiment={run.experiment_name} {rendered}"


def _log_sparkmeasure_metrics(run: ExperimentRun) -> None:
    logger.info(
        "SPARKMEASURE_METRICS "
        f"experiment={run.experiment_name} "
        f"numStages={run.metrics.get('numStages', 0)} "
        f"numTasks={run.metrics.get('numTasks', 0)} "
        f"executorRunTime={run.metrics.get('executorRunTime', 0)} "
        f"shuffleBytesWritten={run.metrics.get('shuffleBytesWritten', 0)}"
    )


def _quote(value: str) -> str:
    return '"' + value.replace('"', '\\"') + '"'
