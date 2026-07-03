"""Lab-local runtime for the StageMetrics contract gate."""

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

from apps.labs.lab_6.lab_6_utils.contract import (
    CORRELATION,
    SCHEMA,
    SEMANTIC,
    build_invalid_demo_records,
    build_stage_metrics_record,
    layer_decision,
    load_contract_rules,
    load_stage_metrics_contract_settings,
    normalize_stage_metrics,
    render_contract_gate_block,
    validate_stage_metrics_contract,
)


class Lab6StageMetricsContractGateJob(SparkWorkshopJob):
    """Run one workload, collect StageMetrics, and validate the metrics contract."""

    inject_invalid_records: bool = False

    def run(self) -> int:
        if not self.config_name:
            raise ValueError("Lab6StageMetricsContractGateJob requires config_name")

        config = load_experiment_config(self.config_name, config_path=self.config_path)
        logger.set_level(config.log_level)
        if not config.observability.enabled or config.observability.collector != "stage":
            raise ValueError("Lab 6 requires observability.enabled=true and collector=stage")

        self.log_section(self.title, self.description)
        run = self._run_config(config)
        self.log_run_summary(run)
        self._run_mode = None
        return 0

    def _run_config(self, config: ExperimentConfig) -> ExperimentRun:
        run_id = str(uuid4())
        validation_run_id = str(uuid4())
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
        summary_output_path: str | None = None
        metrics: dict[str, int | float] = {}

        try:
            self.prepare(context)
            settings = load_stage_metrics_contract_settings(
                config.name,
                Path(self.config_path),
            )
            rules_path = Path(self.config_path).parent / "contract_rules.yaml"
            rules = load_contract_rules(rules_path)
            logger.info(
                "LAB6_CONTRACT_RULES_LOADED_OK "
                f"rules_path={rules_path} contract_version={rules.version}"
            )

            result, metrics = self._execute_stage_observed_workload(context)
            normalized = normalize_stage_metrics(metrics)
            raw_record = build_stage_metrics_record(
                run_id=run_id,
                app_name=config.app_name,
                application_id=application_id,
                settings=settings,
                contract_version=rules.version,
                metrics=normalized,
            )
            raw_dataframe = context.spark.createDataFrame(
                [raw_record],
                schema=_raw_metrics_schema(),
            )
            raw_metrics_path = self._write_raw_metrics(context, raw_dataframe)

            validation_dataframe, validation_input_path = self._build_validation_input(
                context=context,
                raw_record=raw_record,
                raw_dataframe=raw_dataframe,
                raw_metrics_path=raw_metrics_path,
            )
            input_row_count = validation_dataframe.count()
            logger.info(
                "LAB6_STAGE_METRICS_INPUT_OK "
                f"source_path={validation_input_path} rows={input_row_count} "
                f"inject_invalid_records={str(self.inject_invalid_records).lower()}"
            )

            results, summary = validate_stage_metrics_contract(
                validation_dataframe,
                rules=rules,
                validation_run_id=validation_run_id,
                source_path=validation_input_path,
            )
            logger.info(
                "LAB6_SCHEMA_CONTRACT_EVALUATED "
                f"decision={layer_decision(results, SCHEMA)}"
            )
            logger.info(
                "LAB6_SEMANTIC_CONTRACT_EVALUATED "
                f"decision={layer_decision(results, SEMANTIC)}"
            )
            logger.info(
                "LAB6_CORRELATION_CONTRACT_EVALUATED "
                f"decision={layer_decision(results, CORRELATION)}"
            )

            results_output_path, summary_output_path = self._write_contract_outputs(
                context=context,
                results=results,
                summary=summary,
            )
            logger.info(
                render_contract_gate_block(
                    summary=summary,
                    results=results,
                    raw_metrics_path=raw_metrics_path,
                    validation_input_path=validation_input_path,
                    results_output_path=results_output_path,
                    summary_output_path=summary_output_path,
                    demo_mode=self.inject_invalid_records,
                )
            )
            logger.info(
                "LAB6_CONTRACT_RESULTS_WRITTEN_OK "
                f"results_output_path={results_output_path} "
                f"summary_output_path={summary_output_path} "
                f"decision={summary.decision}"
            )
            logger.info(summary.final_marker)
            logger.info(settings.success_marker)
            logger.info(
                "WORKSHOP_EXPERIMENT_COMPLETED "
                f"experiment={config.name} run_id={run_id} "
                f"validation_run_id={validation_run_id} "
                f"application_id={application_id}"
            )
            return ExperimentRun(
                run_id=run_id,
                experiment_name=config.name,
                application_id=application_id,
                workload_result=summary_output_path or result,
                metrics=metrics,
                metrics_output_path=raw_metrics_path,
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
            "LAB6_STAGE_METRICS_CAPTURED_OK "
            f"numStages={normalized.num_stages} "
            f"numTasks={normalized.num_tasks} "
            f"executorRunTime={normalized.executor_run_time_ms} "
            f"shuffleBytesWritten={normalized.shuffle_bytes_written} "
            f"shuffleTotalBytesRead={normalized.shuffle_bytes_read}"
        )
        return result, metrics

    def _write_raw_metrics(self, context: ExperimentContext, dataframe: Any) -> str:
        with spark_job_description(
            context.spark,
            "LAB6 | stage_metrics_contract_gate | write_raw_metrics",
        ):
            context.write("stage_metrics_raw", dataframe)
        return context.config.artifacts.output("stage_metrics_raw").path

    def _build_validation_input(
        self,
        *,
        context: ExperimentContext,
        raw_record: dict[str, Any],
        raw_dataframe: Any,
        raw_metrics_path: str,
    ) -> tuple[Any, str]:
        if not self.inject_invalid_records:
            return raw_dataframe, raw_metrics_path

        demo_records = build_invalid_demo_records(raw_record)
        demo_dataframe = context.spark.createDataFrame(
            demo_records,
            schema=_raw_metrics_schema(),
        )
        with spark_job_description(
            context.spark,
            "LAB6 | stage_metrics_contract_gate | write_demo_input",
        ):
            context.write("stage_metrics_contract_demo_input", demo_dataframe)

        demo_path = context.config.artifacts.output(
            "stage_metrics_contract_demo_input"
        ).path
        return demo_dataframe, demo_path

    def _write_contract_outputs(
        self,
        *,
        context: ExperimentContext,
        results: list[Any],
        summary: Any,
    ) -> tuple[str, str]:
        result_records = [result.as_record() for result in results]
        summary_records = [summary.as_record()]

        with spark_job_description(
            context.spark,
            "LAB6 | stage_metrics_contract_gate | write_rule_results",
        ):
            context.write(
                "stage_metrics_contract_results",
                context.spark.createDataFrame(
                    result_records,
                    schema=_contract_results_schema(),
                ),
            )
        with spark_job_description(
            context.spark,
            "LAB6 | stage_metrics_contract_gate | write_contract_summary",
        ):
            context.write(
                "stage_metrics_contract_summary",
                context.spark.createDataFrame(
                    summary_records,
                    schema=_contract_summary_schema(),
                ),
            )

        return (
            context.config.artifacts.output("stage_metrics_contract_results").path,
            context.config.artifacts.output("stage_metrics_contract_summary").path,
        )


def _raw_metrics_schema() -> Any:
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), True),
            T.StructField("app_name", T.StringType(), True),
            T.StructField("application_id", T.StringType(), True),
            T.StructField("lab_id", T.StringType(), True),
            T.StructField("workload_name", T.StringType(), True),
            T.StructField("workload_variant", T.StringType(), True),
            T.StructField("collector_name", T.StringType(), True),
            T.StructField("metric_scope", T.StringType(), True),
            T.StructField("contract_version", T.StringType(), True),
            T.StructField("created_at", T.StringType(), True),
            T.StructField("num_stages", T.LongType(), True),
            T.StructField("num_tasks", T.LongType(), True),
            T.StructField("executor_run_time_ms", T.LongType(), True),
            T.StructField("shuffle_bytes_written", T.LongType(), True),
            T.StructField("shuffle_bytes_read", T.LongType(), True),
            T.StructField("jvm_gc_time_ms", T.LongType(), True),
            T.StructField("memory_bytes_spilled", T.LongType(), True),
            T.StructField("disk_bytes_spilled", T.LongType(), True),
            T.StructField("input_bytes", T.LongType(), True),
            T.StructField("shuffle_bytes_written_available", T.BooleanType(), True),
            T.StructField("shuffle_bytes_read_available", T.BooleanType(), True),
            T.StructField("jvm_gc_time_ms_available", T.BooleanType(), True),
            T.StructField("memory_bytes_spilled_available", T.BooleanType(), True),
            T.StructField("disk_bytes_spilled_available", T.BooleanType(), True),
            T.StructField("input_bytes_available", T.BooleanType(), True),
        ]
    )


def _contract_results_schema() -> Any:
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("validation_run_id", T.StringType(), False),
            T.StructField("source_path", T.StringType(), False),
            T.StructField("contract_version", T.StringType(), False),
            T.StructField("rule_id", T.StringType(), False),
            T.StructField("rule_name", T.StringType(), False),
            T.StructField("rule_type", T.StringType(), False),
            T.StructField("severity", T.StringType(), False),
            T.StructField("decision", T.StringType(), False),
            T.StructField("failed_count", T.LongType(), False),
            T.StructField("sample_failed_keys", T.StringType(), False),
            T.StructField("recommendation", T.StringType(), False),
            T.StructField("created_at", T.StringType(), False),
        ]
    )


def _contract_summary_schema() -> Any:
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("validation_run_id", T.StringType(), False),
            T.StructField("source_path", T.StringType(), False),
            T.StructField("contract_version", T.StringType(), False),
            T.StructField("total_rules", T.LongType(), False),
            T.StructField("passed_rules", T.LongType(), False),
            T.StructField("failed_rules", T.LongType(), False),
            T.StructField("decision", T.StringType(), False),
            T.StructField("created_at", T.StringType(), False),
        ]
    )
