"""Lab-local runtime helpers for Lab 2D empty partition diagnosis.

The main Lab 2D app should stay close to the workshop job contract. This module
owns the task-level sparkMeasure plumbing and the boxed report that translates
TaskMetrics rows into Spark UI Summary Metrics-style classroom evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from textwrap import wrap
from typing import Any, Mapping
from uuid import uuid4

import yaml

from spark_workshop.config import load_experiment_config
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.metrics import normalize_metrics
from spark_workshop.runtime import ExperimentContext, ExperimentRun
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger, spark_job_description


VALID_EMPTY_PARTITIONS_VARIANTS = frozenset({"empty"})
EMPTY_PARTITIONS_TOP_N = 5
EMPTY_PARTITIONS_METRICS_VIEW = "Lab2DEmptyPartitionsTaskMetrics"
EMPTY_PARTITIONS_MIN_STAGE_TASKS = 4
EMPTY_PARTITIONS_DATA_METRIC = "shuffleRecordsRead"
EMPTY_PARTITIONS_REPORT_WIDTH = 112

EMPTY_PARTITIONS_SUMMARY_METRICS = (
    "duration",
    "executorRunTime",
    "recordsRead",
    "bytesRead",
    "shuffleRecordsRead",
    "shuffleTotalBytesRead",
    "recordsWritten",
    "shuffleBytesWritten",
)

EMPTY_PARTITIONS_OUTLIER_COLUMNS = (
    "stageId",
    "index",
    "executorId",
    "host",
    "duration",
    "executorRunTime",
    "recordsRead",
    "bytesRead",
    "shuffleRecordsRead",
    "shuffleTotalBytesRead",
    "recordsWritten",
    "shuffleBytesWritten",
    "memoryBytesSpilled",
    "diskBytesSpilled",
)


@dataclass(frozen=True)
class EmptyPartitionsSettings:
    """Classroom settings loaded from the selected YAML experiment."""

    variant: str = "empty"
    success_marker: str = "LAB2D_EMPTY_PARTITIONS_OK"
    shuffle_partitions: int = 27
    active_buckets: int = 48


class Lab2EmptyPartitionsDiagnosticJob(SparkWorkshopJob):
    """Single-run workshop job with lab-local TaskMetrics diagnostics."""

    workload_settings: EmptyPartitionsSettings
    collector_name: str

    def __init__(self) -> None:
        super().__init__()
        self.workload_settings = EmptyPartitionsSettings()
        self.collector_name = "task"

    def run(self) -> int:
        if not self.config_name:
            raise ValueError("Lab2EmptyPartitionsDiagnosticJob requires config_name")

        config = load_experiment_config(self.config_name, config_path=self.config_path)
        logger.set_level(config.log_level)
        self.workload_settings = load_empty_partitions_settings(
            self.config_name,
            Path(self.config_path),
        )
        self.collector_name = config.observability.collector
        if self.collector_name != "task":
            raise ValueError("Lab 2D requires observability.collector=task")

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
            "LAB2D_EMPTY_PARTITIONS_CONFIG "
            f"config_name={self.config_name} "
            f"collector={self.collector_name} "
            f"variant={self.workload_settings.variant} "
            f"shuffle_partitions={self.workload_settings.shuffle_partitions} "
            f"active_buckets={self.workload_settings.active_buckets}"
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
                "LAB2D_EMPTY_PARTITIONS_PERSIST_IGNORED "
                "this lab keeps task diagnostics ephemeral"
            )

        collector = build_collector(context.spark)
        collector.begin()
        try:
            result = self.workload(context)
        finally:
            collector.end()

        collector.print_report()
        metrics = normalize_metrics(aggregate_metrics(collector))
        if not metrics:
            raise ValueError("Task metrics collection returned no numeric metrics")

        log_empty_partitions_summary(
            context.spark,
            collector,
            preferred_task_count=self.workload_settings.shuffle_partitions,
        )

        return result, metrics


def load_empty_partitions_settings(
    config_name: str,
    config_path: Path,
) -> EmptyPartitionsSettings:
    """Read Lab 2D workload settings from the local YAML config."""

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    experiments = raw.get("experiments") or {}
    experiment = experiments.get(config_name) or {}
    workload = experiment.get("workload") or {}

    variant = str(workload.get("variant", "empty")).lower()
    if variant not in VALID_EMPTY_PARTITIONS_VARIANTS:
        raise ValueError(
            f"Unsupported Lab 2D workload variant '{variant}'. "
            f"Expected one of {sorted(VALID_EMPTY_PARTITIONS_VARIANTS)}"
        )

    return EmptyPartitionsSettings(
        variant=variant,
        success_marker=str(
            workload.get("success_marker", "LAB2D_EMPTY_PARTITIONS_OK")
        ),
        shuffle_partitions=_positive_int(
            workload.get("shuffle_partitions", 27),
            "shuffle_partitions",
        ),
        active_buckets=_positive_int(
            workload.get("active_buckets", 48),
            "active_buckets",
        ),
    )


def build_collector(spark: Any) -> Any:
    from sparkmeasure import TaskMetrics

    return TaskMetrics(spark)


def aggregate_metrics(collector: Any) -> Mapping[str, Any]:
    return dict(collector.aggregate_taskmetrics())


def log_empty_partitions_summary(
    spark: Any,
    collector: Any,
    *,
    preferred_task_count: int,
) -> None:
    task_metrics = collector.create_taskmetrics_DF(EMPTY_PARTITIONS_METRICS_VIEW)
    stage = select_empty_partitions_stage(
        task_metrics,
        preferred_task_count=preferred_task_count,
    )
    if not stage:
        logger.warning("LAB2D_EMPTY_PARTITIONS_STAGE_SELECTED none")
        return

    summaries = []
    for metric in EMPTY_PARTITIONS_SUMMARY_METRICS:
        if metric not in task_metrics.columns:
            continue
        summary = summarize_empty_partition_metric(
            task_metrics,
            int(stage["stageId"]),
            metric,
        )
        if summary and float(summary.get("max", 0) or 0) > 0:
            summaries.append(summary)

    outliers = collect_empty_partition_outliers(
        spark,
        task_metrics,
        int(stage["stageId"]),
    )
    logger.info(render_empty_partitions_report(stage, summaries, outliers))


def select_empty_partitions_stage(
    task_metrics: Any,
    *,
    preferred_task_count: int | None = None,
) -> dict[str, Any] | None:
    """Select the stage where low-end task outliers are clearest."""

    from pyspark.sql import functions as F

    if "stageId" not in task_metrics.columns or "duration" not in task_metrics.columns:
        return None

    data_metric = _preferred_data_metric(task_metrics.columns)
    duration_value = F.coalesce(F.col("duration").cast("double"), F.lit(0.0))
    data_value = F.coalesce(F.col(data_metric).cast("double"), F.lit(0.0))

    stage_stats = (
        task_metrics.groupBy("stageId")
        .agg(
            F.count("*").cast("long").alias("taskCount"),
            F.min(duration_value).alias("minDuration"),
            F.expr("percentile_approx(cast(duration as double), 0.50, 10000)").alias(
                "medianDuration"
            ),
            F.expr("percentile_approx(cast(duration as double), 0.75, 10000)").alias(
                "p75Duration"
            ),
            F.max(duration_value).alias("maxDuration"),
            F.min(data_value).alias("minData"),
            F.expr(
                f"percentile_approx(cast({data_metric} as double), 0.50, 10000)"
            ).alias("medianData"),
            F.expr(
                f"percentile_approx(cast({data_metric} as double), 0.75, 10000)"
            ).alias("p75Data"),
            F.max(data_value).alias("maxData"),
            F.sum(F.when(data_value <= 0, F.lit(1)).otherwise(F.lit(0))).alias(
                "emptyTaskCount"
            ),
        )
        .withColumn(
            "durationMedianToMin",
            _low_end_ratio_expr(F.col("medianDuration"), F.col("minDuration")),
        )
        .withColumn(
            "durationMaxToP75",
            _ratio_expr(F.col("maxDuration"), F.col("p75Duration")),
        )
        .withColumn(
            "dataMedianToMin",
            _low_end_ratio_expr(F.col("medianData"), F.col("minData")),
        )
        .withColumn(
            "dataMaxToP75",
            _ratio_expr(F.col("maxData"), F.col("p75Data")),
        )
        .filter(F.col("taskCount") >= EMPTY_PARTITIONS_MIN_STAGE_TASKS)
    )

    preferred_stage_stats = (
        stage_stats.filter(F.col("taskCount") == preferred_task_count)
        if preferred_task_count
        else stage_stats
    )
    data_candidates = preferred_stage_stats.filter(
        (F.col("medianData") > 0) & (F.col("emptyTaskCount") > 0)
    )
    fallback_data_candidates = stage_stats.filter(
        (F.col("medianData") > 0) & (F.col("emptyTaskCount") > 0)
    )
    selected = (
        data_candidates.orderBy(
            F.desc("emptyTaskCount"),
            F.desc("dataMedianToMin"),
            F.asc("dataMaxToP75"),
            F.desc("taskCount"),
        ).first()
        or fallback_data_candidates.orderBy(
            F.desc("emptyTaskCount"),
            F.desc("dataMedianToMin"),
            F.asc("dataMaxToP75"),
            F.desc("taskCount"),
        ).first()
        or preferred_stage_stats.orderBy(
            F.desc("durationMedianToMin"),
            F.asc("durationMaxToP75"),
            F.desc("taskCount"),
        ).first()
        or stage_stats.orderBy(
            F.desc("durationMedianToMin"),
            F.asc("durationMaxToP75"),
            F.desc("taskCount"),
        ).first()
    )
    if not selected:
        return None

    row = selected.asDict()
    row["dataMetric"] = data_metric
    return row


def summarize_empty_partition_metric(
    task_metrics: Any,
    stage_id: int,
    metric: str,
) -> dict[str, Any] | None:
    from pyspark.sql import functions as F

    value = F.coalesce(F.col(metric).cast("double"), F.lit(0.0))
    row = (
        task_metrics.filter(F.col("stageId") == stage_id)
        .agg(
            F.count("*").cast("long").alias("taskCount"),
            F.min(value).alias("min"),
            F.expr(f"percentile_approx(cast({metric} as double), 0.25, 10000)").alias(
                "p25"
            ),
            F.expr(f"percentile_approx(cast({metric} as double), 0.50, 10000)").alias(
                "median"
            ),
            F.expr(f"percentile_approx(cast({metric} as double), 0.75, 10000)").alias(
                "p75"
            ),
            F.max(value).alias("max"),
            F.sum(F.when(value <= 0, F.lit(1)).otherwise(F.lit(0))).alias(
                "zeroTaskCount"
            ),
        )
        .first()
    )
    if not row:
        return None

    summary = row.asDict()
    summary["stageId"] = stage_id
    summary["metric"] = metric
    summary["medianToMin"] = _low_end_ratio(summary["median"], summary["min"])
    summary["maxToP75"] = _ratio(summary["max"], summary["p75"])
    return summary


def collect_empty_partition_outliers(
    spark: Any,
    task_metrics: Any,
    stage_id: int,
) -> list[Mapping[str, Any]]:
    from pyspark.sql import functions as F

    selected_columns = [
        column
        for column in EMPTY_PARTITIONS_OUTLIER_COLUMNS
        if column in task_metrics.columns
    ]
    data_metric = _preferred_data_metric(task_metrics.columns)
    order_columns = []
    if data_metric in selected_columns:
        order_columns.append(F.asc(data_metric))
    order_columns.extend([F.asc("duration"), F.asc("executorRunTime")])

    with spark_job_description(
        spark,
        "LAB2D | empty_partitions | inspect_task_metrics_summary",
    ):
        rows = (
            task_metrics.filter(F.col("stageId") == stage_id)
            .select(*selected_columns)
            .orderBy(*order_columns)
            .limit(EMPTY_PARTITIONS_TOP_N)
            .collect()
        )

    return [row.asDict() for row in rows]


def render_empty_partitions_report(
    stage: Mapping[str, Any],
    summaries: list[Mapping[str, Any]],
    outliers: list[Mapping[str, Any]],
    *,
    width: int = EMPTY_PARTITIONS_REPORT_WIDTH,
) -> str:
    """Return one classroom-friendly block with the Lab 2D TaskMetrics signal."""

    box = _ReportBox("LAB 2D TASKMETRICS EMPTY PARTITIONS REPORT", width=width)
    box.text(
        "SparkMeasure collector=TaskMetrics | purpose=min-vs-median empty partition diagnosis"
    )
    box.rule()
    box.text("Selected stage")
    box.text(
        " | ".join(
            (
                f"stageId={int(stage.get('stageId', 0))}",
                f"tasks={int(stage.get('taskCount', 0))}",
                f"emptyTasks={int(stage.get('emptyTaskCount', 0))}",
            )
        )
    )
    box.text(
        " | ".join(
            (
                f"dataMetric={stage.get('dataMetric', '')}",
                f"median/min={_format_ratio(stage.get('dataMedianToMin', 0))}",
                f"max/p75={_format_ratio(stage.get('dataMaxToP75', 0))}",
            )
        )
    )
    box.rule()
    box.text("Metric summary")
    box.text(
        f"{'metric':<24} {'min':>12} {'median':>12} {'p75':>12} "
        f"{'max':>12} {'median/min':>12} {'max/p75':>10}"
    )
    for summary in summaries:
        metric = str(summary.get("metric", ""))
        box.text(
            f"{metric:<24} "
            f"{_format_metric_value(metric, summary.get('min', 0)):>12} "
            f"{_format_metric_value(metric, summary.get('median', 0)):>12} "
            f"{_format_metric_value(metric, summary.get('p75', 0)):>12} "
            f"{_format_metric_value(metric, summary.get('max', 0)):>12} "
            f"{_format_ratio(summary.get('medianToMin', 0)):>12} "
            f"{_format_ratio(summary.get('maxToP75', 0)):>10}"
        )
    box.rule()
    box.text(f"Lowest {len(outliers)} task outliers by {stage.get('dataMetric', '')}")
    box.text(
        f"{'#':>2} {'task':>6} {'exec':>5} {'dur':>9} {'shufRows':>10} "
        f"{'shufRead':>12} {'recordsOut':>10} {'memSpill':>10}"
    )
    for rank, row in enumerate(outliers, start=1):
        box.text(
            f"{rank:>2} "
            f"{int(row.get('index', 0)):>6} "
            f"{str(row.get('executorId', '')):>5} "
            f"{_format_metric_value('duration', row.get('duration', 0)):>9} "
            f"{_format_integer(row.get('shuffleRecordsRead', 0)):>10} "
            f"{_format_metric_value('shuffleTotalBytesRead', row.get('shuffleTotalBytesRead', 0)):>12} "
            f"{_format_integer(row.get('recordsWritten', 0)):>10} "
            f"{_format_metric_value('memoryBytesSpilled', row.get('memoryBytesSpilled', 0)):>10}"
        )
    box.rule()
    box.text("Read: data min << median means some tasks have little or no useful work.")
    box.text("Use data max/p75 to separate low-end emptiness from high-end skew.")
    return "\n" + box.render()


class _ReportBox:
    """Small local box formatter to keep Lab 2D logs readable in spark-submit."""

    def __init__(self, title: str, *, width: int) -> None:
        self.width = max(width, 80)
        self.content_width = self.width - 4
        self.lines = [self._top(), self._line(title.center(self.content_width))]

    def text(self, value: str = "") -> None:
        if not value:
            self.lines.append(self._line(""))
            return
        wrapped = wrap(
            value,
            width=self.content_width,
            break_long_words=False,
            break_on_hyphens=False,
        )
        for line in wrapped or [""]:
            self.lines.append(self._line(line))

    def rule(self) -> None:
        self.lines.append(f"╠{'═' * (self.width - 2)}╣")

    def render(self) -> str:
        return "\n".join((*self.lines, self._bottom()))

    def _top(self) -> str:
        return f"╔{'═' * (self.width - 2)}╗"

    def _bottom(self) -> str:
        return f"╚{'═' * (self.width - 2)}╝"

    def _line(self, value: str) -> str:
        return f"║ {value:<{self.content_width}} ║"


def _preferred_data_metric(columns: list[str]) -> str:
    for metric in (
        EMPTY_PARTITIONS_DATA_METRIC,
        "recordsWritten",
        "recordsRead",
        "duration",
    ):
        if metric in columns:
            return metric
    return "duration"


def _ratio_expr(numerator: Any, denominator: Any) -> Any:
    from pyspark.sql import functions as F

    return (
        F.when(denominator > 0, numerator / denominator)
        .when(numerator > 0, F.lit(999999.0))
        .otherwise(F.lit(0.0))
    )


def _low_end_ratio_expr(numerator: Any, denominator: Any) -> Any:
    return _ratio_expr(numerator, denominator)


def _ratio(numerator: object, denominator: object) -> float:
    parsed_numerator = float(numerator or 0)
    parsed_denominator = float(denominator or 0)
    if parsed_denominator > 0:
        return parsed_numerator / parsed_denominator
    if parsed_numerator > 0:
        return 999999.0
    return 0.0


def _low_end_ratio(numerator: object, denominator: object) -> float:
    return _ratio(numerator, denominator)


def _format_ratio(value: object) -> str:
    parsed = float(value or 0)
    if parsed >= 999999:
        return "∞"
    if parsed.is_integer():
        return f"{int(parsed)}x"
    return f"{parsed:.4f}x"


def _format_metric_value(metric: str, value: object) -> str:
    parsed = float(value or 0)
    normalized = metric.lower()
    if "byte" in normalized or "spill" in normalized:
        return _format_bytes(parsed)
    if "duration" in normalized or "runtime" in normalized or "time" in normalized:
        return f"{int(parsed)} ms"
    return _format_integer(parsed)


def _format_integer(value: object) -> str:
    return f"{int(float(value or 0))}"


def _format_bytes(value: object) -> str:
    parsed = float(value or 0)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    unit_index = 0
    while abs(parsed) >= 1024 and unit_index < len(units) - 1:
        parsed /= 1024
        unit_index += 1
    if unit_index == 0:
        return f"{int(parsed)} {units[unit_index]}"
    return f"{parsed:.1f} {units[unit_index]}"


def _positive_int(value: object, field_name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"Lab 2D {field_name} must be >= 1")
    return parsed
