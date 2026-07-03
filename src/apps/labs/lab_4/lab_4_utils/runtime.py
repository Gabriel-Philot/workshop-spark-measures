"""Lab-local runtime for stage-level workload fingerprinting."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

from spark_workshop.config import ExperimentConfig, load_experiment_config
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.metrics import SparkMeasureFactory, normalize_metrics
from spark_workshop.runtime import ExperimentContext, ExperimentRun
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger

from apps.labs.lab_4.lab_4_utils.fingerprint import (
    build_fingerprint_record,
    build_stage_metrics_record,
    classify_workload,
    load_fingerprint_rules,
    load_stage_workload_fingerprint_settings,
    normalize_stage_metrics,
    render_fingerprint_diagnostic_block,
)


class Lab4StageWorkloadFingerprintJob(SparkWorkshopJob):
    """Run one workload, collect StageMetrics, and write a fingerprint row."""

    def run(self) -> int:
        if not self.config_name:
            raise ValueError("Lab4StageWorkloadFingerprintJob requires config_name")

        config = load_experiment_config(self.config_name, config_path=self.config_path)
        logger.set_level(config.log_level)
        if not config.observability.enabled or config.observability.collector != "stage":
            raise ValueError("Lab 4 requires observability.enabled=true and collector=stage")

        self.log_section(self.title, self.description)
        run = self._run_config(config)
        self.log_run_summary(run)
        self._run_mode = None
        return 0

    def _run_config(self, config: ExperimentConfig) -> ExperimentRun:
        run_id = str(uuid4())
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
        result: Any = None
        metrics: dict[str, int | float] = {}
        fingerprint_output_path: str | None = None

        try:
            self.prepare(context)
            result, metrics = self._execute_stage_observed_workload(context)
            self.validate(result, context)
            fingerprint_output_path = self._persist_fingerprint_outputs(
                context=context,
                run_id=run_id,
                application_id=application_id,
                metrics=metrics,
            )
            logger.info(
                "WORKSHOP_EXPERIMENT_COMPLETED "
                f"experiment={config.name} run_id={run_id} "
                f"application_id={application_id}"
            )
            return ExperimentRun(
                run_id=run_id,
                experiment_name=config.name,
                application_id=application_id,
                workload_result=fingerprint_output_path or result,
                metrics=metrics,
                metrics_output_path=None,
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
    ) -> tuple[Any, dict[str, int | float]]:
        logger.info(
            "SPARKMEASURE_ENABLED=true "
            f"experiment={context.config.name} collector=stage persist=false"
        )
        collector = SparkMeasureFactory.create("stage", context.spark)
        collector.begin()
        try:
            result = self.workload(context)
        finally:
            collector.end()

        metrics = normalize_metrics(collector.aggregate())
        normalized = normalize_stage_metrics(metrics)
        logger.info(
            "LAB4_STAGE_METRICS_CAPTURED_OK "
            f"numStages={normalized.num_stages} "
            f"numTasks={normalized.num_tasks} "
            f"executorRunTime={normalized.executor_run_time_ms} "
            f"shuffleBytesWritten={normalized.shuffle_bytes_written} "
            f"shuffleTotalBytesRead={normalized.shuffle_bytes_read}"
        )
        return result, metrics

    def _persist_fingerprint_outputs(
        self,
        *,
        context: ExperimentContext,
        run_id: str,
        application_id: str,
        metrics: dict[str, int | float],
    ) -> str:
        settings = load_stage_workload_fingerprint_settings(
            context.config.name,
            Path(self.config_path),
        )
        rules_path = Path(self.config_path).parent / "fingerprint_rules.yaml"
        rules = load_fingerprint_rules(rules_path)
        logger.info(
            "LAB4_WORKLOAD_FINGERPRINT_RULES_OK "
            f"rules_path={rules_path} "
            f"high_shuffle_amplification_ratio={rules.high_shuffle_amplification_ratio} "
            f"high_gc_time_ratio={rules.high_gc_time_ratio}"
        )

        normalized = normalize_stage_metrics(metrics)
        decision = classify_workload(normalized, rules)
        logger.info(render_fingerprint_diagnostic_block(normalized, decision))
        logger.info(
            "LAB4_WORKLOAD_PROFILE_ASSIGNED_OK "
            f"workload_profile={decision.workload_profile} "
            f"diagnostic_flags={decision.rendered_flags}"
        )

        stage_metrics_record = build_stage_metrics_record(
            run_id=run_id,
            app_name=context.config.app_name,
            workload_name=settings.workload_name,
            workload_variant=settings.workload_variant,
            application_id=application_id,
            metrics=metrics,
            normalized=normalized,
        )
        fingerprint_record = build_fingerprint_record(
            run_id=run_id,
            app_name=context.config.app_name,
            workload_name=settings.workload_name,
            workload_variant=settings.workload_variant,
            application_id=application_id,
            normalized=normalized,
            decision=decision,
        )

        context.write(
            "stage_metrics",
            context.spark.createDataFrame(
                [stage_metrics_record],
                schema=_stage_metrics_schema(),
            ),
        )
        context.write(
            "workload_fingerprints",
            context.spark.createDataFrame(
                [fingerprint_record],
                schema=_fingerprint_schema(),
            ),
        )
        output_path = context.config.artifacts.output("workload_fingerprints").path
        logger.info(
            "LAB4_WORKLOAD_FINGERPRINT_WRITTEN_OK "
            f"output_path={output_path} "
            f"workload_profile={decision.workload_profile}"
        )
        logger.info(settings.success_marker)
        return output_path


def _stage_metrics_schema() -> Any:
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("app_name", T.StringType(), False),
            T.StructField("application_id", T.StringType(), False),
            T.StructField("workload_name", T.StringType(), False),
            T.StructField("workload_variant", T.StringType(), False),
            T.StructField("collector_type", T.StringType(), False),
            T.StructField("executor_run_time_ms", T.LongType(), False),
            T.StructField("input_bytes", T.LongType(), False),
            T.StructField("shuffle_total_bytes", T.LongType(), False),
            T.StructField("shuffle_bytes_read", T.LongType(), False),
            T.StructField("shuffle_bytes_written", T.LongType(), False),
            T.StructField("memory_bytes_spilled", T.LongType(), False),
            T.StructField("disk_bytes_spilled", T.LongType(), False),
            T.StructField("jvm_gc_time_ms", T.LongType(), False),
            T.StructField("num_stages", T.LongType(), False),
            T.StructField("num_tasks", T.LongType(), False),
            T.StructField("records_read", T.LongType(), False),
            T.StructField("records_written", T.LongType(), False),
            T.StructField("created_at", T.StringType(), False),
        ]
    )


def _fingerprint_schema() -> Any:
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("app_name", T.StringType(), False),
            T.StructField("application_id", T.StringType(), False),
            T.StructField("workload_name", T.StringType(), False),
            T.StructField("workload_variant", T.StringType(), False),
            T.StructField("executor_run_time_ms", T.LongType(), False),
            T.StructField("input_bytes", T.LongType(), False),
            T.StructField("shuffle_bytes_read", T.LongType(), False),
            T.StructField("shuffle_bytes_written", T.LongType(), False),
            T.StructField("memory_bytes_spilled", T.LongType(), False),
            T.StructField("disk_bytes_spilled", T.LongType(), False),
            T.StructField("jvm_gc_time_ms", T.LongType(), False),
            T.StructField("num_stages", T.LongType(), False),
            T.StructField("num_tasks", T.LongType(), False),
            T.StructField("shuffle_amplification_ratio", T.DoubleType(), True),
            T.StructField("gc_time_ratio", T.DoubleType(), False),
            T.StructField("spill_ratio", T.DoubleType(), False),
            T.StructField("task_density_score", T.DoubleType(), False),
            T.StructField("workload_profile", T.StringType(), False),
            T.StructField("diagnostic_flags", T.StringType(), False),
            T.StructField("recommended_next_step", T.StringType(), False),
            T.StructField("created_at", T.StringType(), False),
        ]
    )
