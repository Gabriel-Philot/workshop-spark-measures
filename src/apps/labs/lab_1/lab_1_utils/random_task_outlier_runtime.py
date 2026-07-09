"""Lab-local runtime helpers for the Lab 1B task outlier lesson.

This module intentionally stays under `lab_1_utils` because it is a didactic
variation of the workshop job runtime, not a reusable platform abstraction.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

import yaml

from spark_workshop.config import load_experiment_config
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.metrics import normalize_metrics, validate_aggregate_metrics
from spark_workshop.runtime import ExperimentContext, ExperimentRun
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger, spark_job_description, terminal_box


VALID_WORKLOAD_VARIANTS = frozenset({"problematic", "fixed"})
TASK_OUTLIER_TOP_N = 5
TASK_METRICS_VIEW = "Lab1TaskMetrics"

# Compact classroom log projection only. The complete TaskMetrics DataFrame is
# still available inside log_task_outliers through create_taskmetrics_DF(...).
TASK_OUTLIER_LOG_COLUMNS = (
    "stageId",
    "index",
    "executorId",
    "host",
    "duration",
    "executorRunTime",
    "recordsRead",
    "recordsWritten",
    "shuffleTotalBytesRead",
    "shuffleBytesWritten",
    "memoryBytesSpilled",
    "diskBytesSpilled",
)


@dataclass(frozen=True)
class WorkloadSettings:
    variant: str = "problematic"
    success_marker: str = "LAB1_RANDOM_TASK_OUTLIER_OK"


class SparkMeasureDiagnosticJob(SparkWorkshopJob):
    """Single-run workshop job with lab-local task diagnostics.

    It preserves the same app contract used by the template and
    lab_1a_global_sort_diagnosis.py: subclasses implement extract, transform, load, and
    validate_result. The only local variation is that task metrics can be
    inspected before the Spark session is stopped.
    """

    workload_settings: WorkloadSettings
    collector_name: str

    def __init__(self) -> None:
        super().__init__()
        self.workload_settings = WorkloadSettings()
        self.collector_name = "stage"

    def run(self) -> int:
        if not self.config_name:
            raise ValueError("SparkMeasureDiagnosticJob requires config_name")

        config = load_experiment_config(self.config_name, config_path=self.config_path)
        logger.set_level(config.log_level)
        self.workload_settings = load_workload_settings(
            self.config_name,
            Path(self.config_path),
        )
        self.collector_name = config.observability.collector

        self.log_section(self.title, self.description)
        run = self._run_config(config)
        self.log_run_summary(run)
        logger.info(self.workload_settings.success_marker)
        self._run_mode = None
        return 0

    def _run_config(self, config: Any) -> ExperimentRun:
        logger.info(
            "WORKSHOP_EXPERIMENT_STARTED "
            f"experiment={config.name} app_name={config.app_name}"
        )
        logger.info(
            "LAB1_RANDOM_TASK_OUTLIER_CONFIG "
            f"config_name={self.config_name} collector={self.collector_name} "
            f"variant={self.workload_settings.variant}"
        )

        spark = SparkSessionSingleton.get_or_create(
            config.app_name,
            config.spark_config,
        )
        reused = SparkSessionSingleton.get_or_create(
            config.app_name,
            config.spark_config,
        )
        if reused is not spark:
            raise RuntimeError("SparkSession singleton returned different instances")
        logger.info("SPARK_SESSION_SINGLETON_OK")

        spark.sparkContext.setLogLevel(config.spark_log_level.upper())
        context = ExperimentContext(spark=spark, config=config)
        run_id = str(uuid4())
        application_id = spark.sparkContext.applicationId

        try:
            self.prepare(context)
            result, metrics = self._execute_workload(context)
            self.validate(result, context)
            logger.info(
                "WORKSHOP_EXPERIMENT_COMPLETED "
                f"experiment={config.name} run_id={run_id} "
                f"application_id={application_id}"
            )
            return ExperimentRun(
                run_id=run_id,
                experiment_name=config.name,
                application_id=application_id,
                workload_result=result,
                metrics=metrics,
                metrics_output_path=None,
            )
        finally:
            try:
                self.cleanup(context)
            finally:
                self._context = None
                SparkSessionSingleton.stop()

    def _execute_workload(
        self,
        context: ExperimentContext,
    ) -> tuple[Any, Mapping[str, int | float]]:
        observability = context.config.observability
        if not observability.enabled:
            logger.info(
                "SPARKMEASURE_ENABLED=false "
                f"experiment={context.config.name}"
            )
            return self.workload(context), {}

        logger.info(
            "SPARKMEASURE_ENABLED=true "
            f"experiment={context.config.name} "
            f"collector={observability.collector} "
            f"persist={str(observability.persist).lower()}"
        )
        if observability.persist:
            logger.warning(
                "LAB1_RANDOM_TASK_OUTLIER_PERSIST_IGNORED "
                "this lab keeps task diagnostics ephemeral"
            )

        collector = build_collector(observability.collector, context.spark)
        collector.begin()
        try:
            result = self.workload(context)
        finally:
            collector.end()

        collector.print_report()
        metrics = normalize_metrics(aggregate_metrics(observability.collector, collector))
        if observability.collector == "stage":
            validate_aggregate_metrics(metrics)
        elif not metrics:
            raise ValueError("Task metrics collection returned no numeric metrics")

        if observability.collector == "task":
            log_task_outliers(context.spark, collector)

        return result, metrics


def load_workload_settings(config_name: str, config_path: Path) -> WorkloadSettings:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    experiments = raw.get("experiments") or {}
    experiment = experiments.get(config_name) or {}
    workload = experiment.get("workload") or {}

    variant = str(workload.get("variant", "problematic")).lower()
    if variant not in VALID_WORKLOAD_VARIANTS:
        raise ValueError(
            f"Unsupported Lab 1B workload variant '{variant}'. "
            f"Expected one of {sorted(VALID_WORKLOAD_VARIANTS)}"
        )

    return WorkloadSettings(
        variant=variant,
        success_marker=str(
            workload.get("success_marker", "LAB1_RANDOM_TASK_OUTLIER_OK")
        ),
    )


def build_collector(collector_name: str, spark: Any) -> Any:
    if collector_name == "task":
        from sparkmeasure import TaskMetrics

        return TaskMetrics(spark)

    from sparkmeasure import StageMetrics

    return StageMetrics(spark)


def aggregate_metrics(collector_name: str, collector: Any) -> Mapping[str, Any]:
    if collector_name == "task":
        return dict(collector.aggregate_taskmetrics())
    return dict(collector.aggregate_stagemetrics())


def log_task_outliers(spark: Any, collector: Any) -> None:
    from pyspark.sql import functions as F

    task_metrics = collector.create_taskmetrics_DF(TASK_METRICS_VIEW)
    selected_columns = [
        column for column in TASK_OUTLIER_LOG_COLUMNS if column in task_metrics.columns
    ]
    with spark_job_description(
        spark,
        "LAB1 | random_task_outlier | inspect_task_metrics_top_runtime",
    ):
        focused_task_metrics = task_metrics.select(*selected_columns)
        if "recordsWritten" in selected_columns:
            focused_task_metrics = focused_task_metrics.filter(
                F.col("recordsWritten") > 0
            )
        rows = (
            focused_task_metrics
            .orderBy(F.desc("executorRunTime"), F.desc("duration"))
            .limit(TASK_OUTLIER_TOP_N)
            .collect()
        )

    logger.info(render_task_outlier_report([row.asDict() for row in rows]))


def render_task_outlier_line(rank: int, row: Mapping[str, Any]) -> str:
    return (
        "LAB1_TASK_OUTLIER "
        f"rank={rank} "
        f"stageId={row.get('stageId', 0)} "
        f"taskIndex={row.get('index', 0)} "
        f"executorId={row.get('executorId', '')} "
        f"duration={row.get('duration', 0)} "
        f"executorRunTime={row.get('executorRunTime', 0)} "
        f"recordsRead={row.get('recordsRead', 0)} "
        f"recordsWritten={row.get('recordsWritten', 0)} "
        f"shuffleTotalBytesRead={row.get('shuffleTotalBytesRead', 0)} "
        f"shuffleBytesWritten={row.get('shuffleBytesWritten', 0)} "
        f"memoryBytesSpilled={row.get('memoryBytesSpilled', 0)} "
        f"diskBytesSpilled={row.get('diskBytesSpilled', 0)}"
    )


def render_task_outlier_report(rows: list[Mapping[str, Any]]) -> str:
    """Render the Lab 1B task-metric projection as one classroom-friendly box."""

    lines = [
        "LAB 1B TASK OUTLIER DIAGNOSTIC REPORT",
        "SparkMeasure collector=TaskMetrics | source=create_taskmetrics_DF projection",
        "",
    ]
    if not rows:
        lines.extend(
            [
                "No task rows matched the diagnostic projection.",
                "Check whether the workload wrote records or whether the filter is too strict.",
            ]
        )
        return terminal_box(lines, width=112)

    lines.extend(
        [
            f"Top {len(rows)} task outliers by executorRunTime",
            "Read: high executorRunTime or duration identifies the long-tail task.",
            "",
        ]
    )
    lines.extend(
        render_task_outlier_line(rank, row)
        for rank, row in enumerate(rows, start=1)
    )
    lines.extend(
        [
            "",
            "This box is not a second measurement. It is a compact view of the task metrics already collected by sparkMeasure.",
        ]
    )
    return terminal_box(lines, width=112)
