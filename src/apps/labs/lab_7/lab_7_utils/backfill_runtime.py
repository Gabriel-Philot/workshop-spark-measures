"""Lab-local runtime for Lab 7 daily backfill StageMetrics."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

from spark_workshop.config import ExperimentConfig, load_experiment_config
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.metrics import SparkMeasureFactory, normalize_metrics
from spark_workshop.runtime import ExperimentContext, ExperimentRun
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger, spark_job_description

from apps.labs.lab_7.lab_7_utils.backfill import (
    build_daily_backfill_metrics_record,
    expected_volume_for_date,
    load_daily_backfill_settings,
    normalize_daily_backfill_metrics,
    render_daily_backfill_block,
)


class Lab7DailyBackfillStageMetricsJob(SparkWorkshopJob):
    """Run one daily backfill and persist one StageMetrics row by date."""

    processing_date: str
    filter_strategy: str
    batch_run_id: str
    volume_plan_path: Path

    def run(self) -> int:
        if not self.config_name:
            raise ValueError("Lab7DailyBackfillStageMetricsJob requires config_name")
        if not self.processing_date:
            raise ValueError("Lab 7 daily backfill requires processing_date")

        config = load_experiment_config(self.config_name, config_path=self.config_path)
        logger.set_level(config.log_level)
        if not config.observability.enabled or config.observability.collector != "stage":
            raise ValueError("Lab 7 daily backfill requires StageMetrics observability")

        self.log_section(self.title, self.description)
        run = self._run_config(config)
        self.log_run_summary(run)
        self._run_mode = None
        return 0

    def _run_config(self, config: ExperimentConfig) -> ExperimentRun:
        date_run_id = str(uuid4())
        logger.info(
            "WORKSHOP_EXPERIMENT_STARTED "
            f"experiment={config.name} app_name={config.app_name}"
        )

        spark = SparkSessionSingleton.get_or_create(config.app_name, config.spark_config)
        reused = SparkSessionSingleton.get_or_create(config.app_name, config.spark_config)
        if reused is not spark:
            raise RuntimeError("SparkSession singleton returned different instances")
        logger.info("SPARK_SESSION_SINGLETON_OK")

        spark.sparkContext.setLogLevel(config.spark_log_level.upper())
        context = ExperimentContext(spark=spark, config=config)
        application_id = spark.sparkContext.applicationId
        metrics: dict[str, int | float] = {}
        metrics_output_path: str | None = None

        try:
            self.prepare(context)
            output_path, metrics = self._execute_stage_observed_workload(context)
            self.validate(output_path, context)
            metrics_output_path = self._persist_daily_metrics(
                context=context,
                application_id=application_id,
                date_run_id=date_run_id,
                output_path=output_path,
                metrics=metrics,
            )
            logger.info(
                "WORKSHOP_EXPERIMENT_COMPLETED "
                f"experiment={config.name} run_id={self.batch_run_id} "
                f"date_run_id={date_run_id} application_id={application_id}"
            )
            return ExperimentRun(
                run_id=self.batch_run_id,
                experiment_name=config.name,
                application_id=application_id,
                workload_result=output_path,
                metrics=metrics,
                metrics_output_path=metrics_output_path,
            )
        finally:
            try:
                self.cleanup(context)
            finally:
                self._context = None
                SparkSessionSingleton.stop()

    def _execute_stage_observed_workload(
        self,
        context: ExperimentContext,
    ) -> tuple[str, dict[str, int | float]]:
        logger.info(
            "SPARKMEASURE_ENABLED=true "
            f"experiment={context.config.name} collector=stage persist=false "
            f"processing_date={self.processing_date} "
            f"filter_strategy={self.filter_strategy}"
        )
        collector = SparkMeasureFactory.create("stage", context.spark)
        collector.begin()
        try:
            result = self.workload(context)
        finally:
            collector.end()

        metrics = normalize_metrics(collector.aggregate())
        normalized = normalize_daily_backfill_metrics(
            metrics,
            source_rows_for_date=self._date_volume.rows,
        )
        logger.info(
            "LAB7_DAILY_BACKFILL_RUN_OK "
            f"processing_date={self.processing_date} "
            f"filter_strategy={self.filter_strategy} "
            f"source_rows_for_date={self._date_volume.rows} "
            f"spike_label={self._date_volume.spike_label} "
            f"numStages={normalized.num_stages} "
            f"numTasks={normalized.num_tasks} "
            f"executorRunTime={normalized.executor_run_time_ms} "
            f"recordsRead={normalized.records_read} "
            f"shuffleBytesWritten={normalized.shuffle_bytes_written}"
        )
        return str(result), metrics

    def _persist_daily_metrics(
        self,
        *,
        context: ExperimentContext,
        application_id: str,
        date_run_id: str,
        output_path: str,
        metrics: dict[str, int | float],
    ) -> str:
        normalized = normalize_daily_backfill_metrics(
            metrics,
            source_rows_for_date=self._date_volume.rows,
        )
        settings = load_daily_backfill_settings(
            context.config.name,
            Path(self.config_path),
        )
        record = build_daily_backfill_metrics_record(
            run_id=self.batch_run_id,
            date_run_id=date_run_id,
            app_name=context.config.app_name,
            application_id=application_id,
            workload_name=settings.workload_name,
            filter_strategy=self.filter_strategy,
            processing_date=self.processing_date,
            plan=self._volume_plan,
            date_volume=self._date_volume,
            output_path=output_path,
            metrics=normalized,
        )
        with spark_job_description(
            context.spark,
            "LAB7 | daily_backfill | write_stage_metrics_by_date",
        ):
            context.write(
                "daily_backfill_stage_metrics",
                context.spark.createDataFrame([record], schema=_stage_metrics_schema()),
            )
        metrics_output_path = context.config.artifacts.output(
            "daily_backfill_stage_metrics"
        ).path
        logger.info(
            render_daily_backfill_block(
                processing_date=self.processing_date,
                filter_strategy=self.filter_strategy,
                date_volume=self._date_volume,
                output_path=output_path,
                metrics_output_path=metrics_output_path,
                metrics=normalized,
            )
        )
        logger.info(
            "LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK "
            f"processing_date={self.processing_date} "
            f"metrics_output_path={metrics_output_path}"
        )
        logger.info(
            "LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK "
            f"processing_date={self.processing_date} "
            f"source_rows_for_date={self._date_volume.rows} "
            f"volume_multiplier={self._date_volume.volume_multiplier} "
            f"spike_label={self._date_volume.spike_label}"
        )
        logger.info(settings.success_marker)
        return metrics_output_path

    def before_extract(self) -> None:
        self._volume_plan, self._date_volume = expected_volume_for_date(
            self.volume_plan_path,
            self.processing_date,
        )
        logger.info(
            "LAB7_DAILY_BACKFILL_CONFIG_OK "
            f"processing_date={self.processing_date} "
            f"filter_strategy={self.filter_strategy} "
            f"batch_run_id={self.batch_run_id} "
            f"source_rows_for_date={self._date_volume.rows} "
            f"volume_multiplier={self._date_volume.volume_multiplier} "
            f"spike_label={self._date_volume.spike_label}"
        )


def _stage_metrics_schema() -> Any:
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("date_run_id", T.StringType(), False),
            T.StructField("app_name", T.StringType(), False),
            T.StructField("application_id", T.StringType(), False),
            T.StructField("lab_id", T.StringType(), False),
            T.StructField("workload_name", T.StringType(), False),
            T.StructField("filter_strategy", T.StringType(), False),
            T.StructField("processing_date", T.StringType(), False),
            T.StructField("source_start_date", T.StringType(), False),
            T.StructField("source_end_date", T.StringType(), False),
            T.StructField("source_rows_for_date", T.LongType(), False),
            T.StructField("volume_multiplier", T.LongType(), False),
            T.StructField("spike_label", T.StringType(), False),
            T.StructField("collector_type", T.StringType(), False),
            T.StructField("executor_run_time_ms", T.LongType(), False),
            T.StructField("records_read", T.LongType(), False),
            T.StructField("records_written", T.LongType(), False),
            T.StructField("input_bytes", T.LongType(), False),
            T.StructField("shuffle_bytes_written", T.LongType(), False),
            T.StructField("shuffle_bytes_read", T.LongType(), False),
            T.StructField("shuffle_total_bytes", T.LongType(), False),
            T.StructField("memory_bytes_spilled", T.LongType(), False),
            T.StructField("disk_bytes_spilled", T.LongType(), False),
            T.StructField("jvm_gc_time_ms", T.LongType(), False),
            T.StructField("num_stages", T.LongType(), False),
            T.StructField("num_tasks", T.LongType(), False),
            T.StructField("runtime_per_million_rows", T.DoubleType(), False),
            T.StructField("shuffle_per_million_rows", T.DoubleType(), False),
            T.StructField("input_bytes_per_million_rows", T.DoubleType(), False),
            T.StructField("tasks_per_million_rows", T.DoubleType(), False),
            T.StructField("business_output_path", T.StringType(), False),
            T.StructField("created_at", T.StringType(), False),
        ]
    )
