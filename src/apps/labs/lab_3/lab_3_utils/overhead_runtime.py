"""Lab-local runtime helpers for Lab 3 observability overhead benchmarks."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Mapping
from uuid import uuid4

import yaml

from spark_workshop.config import ExperimentConfig, load_experiment_config
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.metrics import SparkMeasureFactory, normalize_metrics
from spark_workshop.runtime import ExperimentContext, ExperimentRun
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger


VALID_OVERHEAD_MODES = frozenset({"none", "stage", "task"})
LAB3_METADATA_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class OverheadBenchmarkSettings:
    """Classroom benchmark settings loaded from the selected YAML experiment."""

    mode: str = "none"
    success_marker: str = "LAB3_OBSERVABILITY_OVERHEAD_OK"
    shuffle_partitions: int = 96
    benchmark_buckets: int = 256


@dataclass(frozen=True)
class OverheadRunContext:
    """Per-run benchmark identity from the bash orchestrator or environment."""

    benchmark_id: str
    run_id: str
    iteration: int
    is_warmup: bool
    mode: str
    output_suffix: str
    emit_sparkmeasure_report: bool


@dataclass(frozen=True)
class WorkloadTimings:
    """Measured runtime phases for one app execution."""

    spark_session_ms: int
    workload_wall_ms: int
    collector_begin_end_ms: int
    collector_report_ms: int
    collector_aggregate_ms: int
    validation_wall_ms: int


class Lab3ObservabilityOverheadJob(SparkWorkshopJob):
    """Single-run benchmark job with custom timing and metadata persistence."""

    workload_settings: OverheadBenchmarkSettings
    benchmark_context: OverheadRunContext
    output_row_count: int

    def __init__(self) -> None:
        super().__init__()
        self.workload_settings = OverheadBenchmarkSettings()
        self.benchmark_context = _load_run_context("none")
        self.output_row_count = 0

    def run(self) -> int:
        if not self.config_name:
            raise ValueError("Lab3ObservabilityOverheadJob requires config_name")

        config = load_experiment_config(self.config_name, config_path=self.config_path)
        logger.set_level(config.log_level)
        self.workload_settings = load_overhead_benchmark_settings(
            self.config_name,
            Path(self.config_path),
        )
        self.benchmark_context = _load_run_context(self.workload_settings.mode)
        _validate_observability_mode(config, self.workload_settings.mode)

        self.log_section(self.title, self.description)
        run = self._run_config(config)
        self.log_run_summary(run)
        logger.info(self.workload_settings.success_marker)
        self._run_mode = None
        return 0

    def _run_config(self, config: ExperimentConfig) -> ExperimentRun:
        app_started = perf_counter()
        started_at_utc = _utc_now()
        logger.info(
            "WORKSHOP_EXPERIMENT_STARTED "
            f"experiment={config.name} app_name={config.app_name}"
        )
        logger.info(
            "LAB3_OVERHEAD_CONFIG "
            f"benchmark_id={self.benchmark_context.benchmark_id} "
            f"run_id={self.benchmark_context.run_id} "
            f"iteration={self.benchmark_context.iteration} "
            f"is_warmup={str(self.benchmark_context.is_warmup).lower()} "
            f"mode={self.benchmark_context.mode} "
            f"config_name={self.config_name} "
            f"shuffle_partitions={self.workload_settings.shuffle_partitions} "
            f"benchmark_buckets={self.workload_settings.benchmark_buckets}"
        )

        spark_started = perf_counter()
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
        spark_session_ms = _elapsed_ms(spark_started)
        logger.info("SPARK_SESSION_SINGLETON_OK")

        spark.sparkContext.setLogLevel(config.spark_log_level.upper())
        context = ExperimentContext(spark=spark, config=config)
        application_id = spark.sparkContext.applicationId
        timings = WorkloadTimings(
            spark_session_ms=spark_session_ms,
            workload_wall_ms=0,
            collector_begin_end_ms=0,
            collector_report_ms=0,
            collector_aggregate_ms=0,
            validation_wall_ms=0,
        )
        metrics: Mapping[str, int | float] = {}
        result: Any = None
        metadata_output_path: str | None = None

        try:
            self.prepare(context)
            result, metrics, timings = self._execute_workload(context)
            timings = WorkloadTimings(
                spark_session_ms=spark_session_ms,
                workload_wall_ms=timings.workload_wall_ms,
                collector_begin_end_ms=timings.collector_begin_end_ms,
                collector_report_ms=timings.collector_report_ms,
                collector_aggregate_ms=timings.collector_aggregate_ms,
                validation_wall_ms=timings.validation_wall_ms,
            )
            validation_started = perf_counter()
            self.validate(result, context)
            timings = WorkloadTimings(
                spark_session_ms=timings.spark_session_ms,
                workload_wall_ms=timings.workload_wall_ms,
                collector_begin_end_ms=timings.collector_begin_end_ms,
                collector_report_ms=timings.collector_report_ms,
                collector_aggregate_ms=timings.collector_aggregate_ms,
                validation_wall_ms=_elapsed_ms(validation_started),
            )
            ended_at_utc = _utc_now()
            app_wall_ms = _elapsed_ms(app_started)
            metadata_record = self._build_metadata_record(
                config=config,
                application_id=application_id,
                result=result,
                metrics=metrics,
                timings=timings,
                started_at_utc=started_at_utc,
                ended_at_utc=ended_at_utc,
                app_wall_ms=app_wall_ms,
            )
            metadata_output_path, metadata_write_ms = persist_benchmark_metadata(
                spark,
                config,
                metadata_record,
            )
            logger.info(
                "LAB3_METADATA_WRITTEN "
                f"path={metadata_output_path} metadata_write_ms={metadata_write_ms}"
            )
            logger.info(
                "WORKSHOP_EXPERIMENT_COMPLETED "
                f"experiment={config.name} run_id={self.benchmark_context.run_id} "
                f"application_id={application_id}"
            )
            return ExperimentRun(
                run_id=self.benchmark_context.run_id,
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
    ) -> tuple[Any, Mapping[str, int | float], WorkloadTimings]:
        observability = context.config.observability
        spark_session_ms = 0
        report_ms = 0
        aggregate_ms = 0

        if not observability.enabled:
            logger.info(
                "SPARKMEASURE_ENABLED=false "
                f"experiment={context.config.name}"
            )
            workload_started = perf_counter()
            result = self.workload(context)
            workload_ms = _elapsed_ms(workload_started)
            return (
                result,
                {},
                WorkloadTimings(
                    spark_session_ms=spark_session_ms,
                    workload_wall_ms=workload_ms,
                    collector_begin_end_ms=0,
                    collector_report_ms=0,
                    collector_aggregate_ms=0,
                    validation_wall_ms=0,
                ),
            )

        logger.info(
            "SPARKMEASURE_ENABLED=true "
            f"experiment={context.config.name} "
            f"collector={observability.collector} "
            f"persist={str(observability.persist).lower()} "
            f"emit_report={str(self.benchmark_context.emit_sparkmeasure_report).lower()}"
        )
        collector = SparkMeasureFactory.create(observability.collector, context.spark)
        collector_window_started = perf_counter()
        collector.begin()
        try:
            workload_started = perf_counter()
            result = self.workload(context)
            workload_ms = _elapsed_ms(workload_started)
        finally:
            collector.end()
        collector_window_ms = _elapsed_ms(collector_window_started)

        if self.benchmark_context.emit_sparkmeasure_report:
            report_started = perf_counter()
            collector.print_report()
            report_ms = _elapsed_ms(report_started)

        aggregate_started = perf_counter()
        metrics = normalize_metrics(collector.aggregate())
        aggregate_ms = _elapsed_ms(aggregate_started)
        if not metrics:
            raise ValueError("sparkMeasure collection returned no numeric metrics")

        return (
            result,
            metrics,
            WorkloadTimings(
                spark_session_ms=spark_session_ms,
                workload_wall_ms=workload_ms,
                collector_begin_end_ms=collector_window_ms,
                collector_report_ms=report_ms,
                collector_aggregate_ms=aggregate_ms,
                validation_wall_ms=0,
            ),
        )

    def benchmark_output_path(self) -> str:
        base = self.context.config.artifacts.location("workload_output_base").rstrip("/")
        run = self.benchmark_context
        suffix = run.output_suffix.strip("/")
        if suffix:
            return f"{base}/{suffix}"
        return (
            f"{base}/benchmark_id={_path_part(run.benchmark_id)}"
            f"/mode={_path_part(run.mode)}"
            f"/iteration={run.iteration}"
            f"/run_id={_path_part(run.run_id)}"
        )

    def write_benchmark_output(self, dataframe: Any) -> str:
        output_path = self.benchmark_output_path()
        logger.info(
            "LAB3_WORKLOAD_WRITE "
            f"mode={self.benchmark_context.mode} path={output_path}"
        )
        dataframe.write.format("delta").mode("errorifexists").save(output_path)
        return output_path

    def count_output_rows(self, output_path: str) -> int:
        return int(self.context.spark.read.format("delta").load(output_path).count())

    def _build_metadata_record(
        self,
        *,
        config: ExperimentConfig,
        application_id: str,
        result: Any,
        metrics: Mapping[str, int | float],
        timings: WorkloadTimings,
        started_at_utc: str,
        ended_at_utc: str,
        app_wall_ms: int,
    ) -> dict[str, Any]:
        run = self.benchmark_context
        workload_output_path = str(result or "")
        return {
            "schema_version": LAB3_METADATA_SCHEMA_VERSION,
            "benchmark_id": run.benchmark_id,
            "run_id": run.run_id,
            "iteration": run.iteration,
            "is_warmup": run.is_warmup,
            "mode": run.mode,
            "config_name": config.name,
            "app_name": config.app_name,
            "application_id": application_id,
            "workload_output_path": workload_output_path,
            "started_at_utc": started_at_utc,
            "ended_at_utc": ended_at_utc,
            "app_wall_ms": app_wall_ms,
            "spark_session_ms": timings.spark_session_ms,
            "workload_wall_ms": timings.workload_wall_ms,
            "collector_begin_end_ms": timings.collector_begin_end_ms,
            "collector_report_ms": timings.collector_report_ms,
            "collector_aggregate_ms": timings.collector_aggregate_ms,
            "metadata_write_ms": None,
            "validation_wall_ms": timings.validation_wall_ms,
            "row_count": self.output_row_count,
            "num_stages": int(metrics.get("numStages", 0)),
            "num_tasks": int(metrics.get("numTasks", 0)),
            "executor_run_time_ms": int(metrics.get("executorRunTime", 0)),
            "shuffle_bytes_written": int(metrics.get("shuffleBytesWritten", 0)),
            "spark_version": self.context.spark.version,
        }


def load_overhead_benchmark_settings(
    config_name: str,
    config_path: Path,
) -> OverheadBenchmarkSettings:
    """Read Lab 3 workload settings from the local YAML config."""

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    experiments = raw.get("experiments") or {}
    experiment = experiments.get(config_name) or {}
    workload = experiment.get("workload") or {}

    mode = str(workload.get("mode", "none")).lower()
    if mode not in VALID_OVERHEAD_MODES:
        raise ValueError(
            f"Unsupported Lab 3 overhead mode '{mode}'. "
            f"Expected one of {sorted(VALID_OVERHEAD_MODES)}"
        )

    return OverheadBenchmarkSettings(
        mode=mode,
        success_marker=str(
            workload.get("success_marker", "LAB3_OBSERVABILITY_OVERHEAD_OK")
        ),
        shuffle_partitions=_positive_int(
            workload.get("shuffle_partitions", 96),
            "shuffle_partitions",
        ),
        benchmark_buckets=_positive_int(
            workload.get("benchmark_buckets", 256),
            "benchmark_buckets",
        ),
    )


def persist_benchmark_metadata(
    spark: Any,
    config: ExperimentConfig,
    record: Mapping[str, Any],
) -> tuple[str, int]:
    """Append one benchmark metadata row to the Lab 3 observability table."""

    from pyspark.sql import types as T

    metadata_path = config.artifacts.location("metadata_output_path")
    schema = T.StructType(
        [
            T.StructField("schema_version", T.IntegerType(), False),
            T.StructField("benchmark_id", T.StringType(), False),
            T.StructField("run_id", T.StringType(), False),
            T.StructField("iteration", T.IntegerType(), False),
            T.StructField("is_warmup", T.BooleanType(), False),
            T.StructField("mode", T.StringType(), False),
            T.StructField("config_name", T.StringType(), False),
            T.StructField("app_name", T.StringType(), False),
            T.StructField("application_id", T.StringType(), False),
            T.StructField("workload_output_path", T.StringType(), False),
            T.StructField("started_at_utc", T.StringType(), False),
            T.StructField("ended_at_utc", T.StringType(), False),
            T.StructField("app_wall_ms", T.LongType(), False),
            T.StructField("spark_session_ms", T.LongType(), False),
            T.StructField("workload_wall_ms", T.LongType(), False),
            T.StructField("collector_begin_end_ms", T.LongType(), False),
            T.StructField("collector_report_ms", T.LongType(), False),
            T.StructField("collector_aggregate_ms", T.LongType(), False),
            T.StructField("metadata_write_ms", T.LongType(), True),
            T.StructField("validation_wall_ms", T.LongType(), False),
            T.StructField("row_count", T.LongType(), False),
            T.StructField("num_stages", T.LongType(), False),
            T.StructField("num_tasks", T.LongType(), False),
            T.StructField("executor_run_time_ms", T.LongType(), False),
            T.StructField("shuffle_bytes_written", T.LongType(), False),
            T.StructField("spark_version", T.StringType(), False),
        ]
    )
    started = perf_counter()
    (
        spark.createDataFrame([dict(record)], schema=schema)
        .write.format("delta")
        .mode("append")
        .partitionBy("benchmark_id", "mode")
        .save(metadata_path)
    )
    return metadata_path, _elapsed_ms(started)


def _load_run_context(default_mode: str) -> OverheadRunContext:
    benchmark_id = os.environ.get("LAB3_BENCHMARK_ID") or _default_benchmark_id()
    run_id = os.environ.get("LAB3_RUN_ID") or str(uuid4())
    mode = (os.environ.get("LAB3_MODE") or default_mode).lower()
    if mode not in VALID_OVERHEAD_MODES:
        raise ValueError(
            f"Unsupported LAB3_MODE '{mode}'. Expected one of {sorted(VALID_OVERHEAD_MODES)}"
        )
    iteration = _non_negative_int(os.environ.get("LAB3_ITERATION", "0"), "LAB3_ITERATION")
    is_warmup = _bool_env(os.environ.get("LAB3_IS_WARMUP", "false"))
    output_suffix = os.environ.get("LAB3_OUTPUT_SUFFIX") or (
        f"benchmark_id={_path_part(benchmark_id)}"
        f"/mode={_path_part(mode)}"
        f"/iteration={iteration}"
        f"/run_id={_path_part(run_id)}"
    )
    emit_report = _bool_env(os.environ.get("LAB3_EMIT_SPARKMEASURE_REPORT", "false"))
    return OverheadRunContext(
        benchmark_id=benchmark_id,
        run_id=run_id,
        iteration=iteration,
        is_warmup=is_warmup,
        mode=mode,
        output_suffix=output_suffix,
        emit_sparkmeasure_report=emit_report,
    )


def _validate_observability_mode(
    config: ExperimentConfig,
    mode: str,
) -> None:
    if mode == "none":
        if config.observability.enabled:
            raise ValueError("Lab 3 mode=none requires observability.enabled=false")
        return

    if not config.observability.enabled:
        raise ValueError(f"Lab 3 mode={mode} requires observability.enabled=true")
    if config.observability.collector != mode:
        raise ValueError(
            f"Lab 3 mode={mode} requires observability.collector={mode}; "
            f"got {config.observability.collector}"
        )


def _elapsed_ms(started: float) -> int:
    return int((perf_counter() - started) * 1000)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_benchmark_id() -> str:
    return "lab3-overhead-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _path_part(value: str) -> str:
    return value.replace("/", "_").replace(" ", "_")


def _positive_int(value: object, field_name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"Lab 3 {field_name} must be >= 1")
    return parsed


def _non_negative_int(value: object, field_name: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return parsed


def _bool_env(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
