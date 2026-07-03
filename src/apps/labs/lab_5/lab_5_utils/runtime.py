"""Lab-local runtime for stage-level runtime budget guardrails."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from spark_workshop.config import ExperimentConfig, load_experiment_config
from spark_workshop.jobs import SparkWorkshopJob
from spark_workshop.metrics import SparkMeasureFactory, normalize_metrics
from spark_workshop.runtime import ExperimentContext, ExperimentRun
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger, spark_job_description

from apps.labs.lab_5.lab_5_utils.budget import (
    RuntimeBudgetSettings,
    StageRuntimeMetrics,
    apply_runtime_budget,
    build_decision_record,
    build_stage_metrics_record,
    load_budget_rules,
    load_runtime_budget_settings,
    normalize_stage_metrics,
    render_budget_decision_block,
    validate_business_outputs,
)


@dataclass(frozen=True)
class VariantRun:
    """One measured baseline or candidate workload execution."""

    run_id: str
    variant_name: str
    output_path: str
    metrics: dict[str, int | float]
    normalized_metrics: StageRuntimeMetrics


class Lab5StageRuntimeBudgetGuardrailJob(SparkWorkshopJob):
    """Run baseline and candidate with StageMetrics and apply runtime budgets."""

    def run(self) -> int:
        if not self.config_name:
            raise ValueError("Lab5StageRuntimeBudgetGuardrailJob requires config_name")

        config = load_experiment_config(self.config_name, config_path=self.config_path)
        logger.set_level(config.log_level)
        if not config.observability.enabled or config.observability.collector != "stage":
            raise ValueError("Lab 5 requires observability.enabled=true and collector=stage")

        self.log_section(self.title, self.description)
        run = self._run_config(config)
        self.log_run_summary(run)
        self._run_mode = None
        return 0

    def build_baseline(self, inputs: Any) -> Any:
        """Return the baseline DataFrame. Implemented by the lab app."""
        raise NotImplementedError

    def build_candidate(self, inputs: Any) -> Any:
        """Return the candidate DataFrame. Implemented by the lab app."""
        raise NotImplementedError

    def load_variant(self, variant: str, data: Any) -> str:
        """Write one variant output and return its Delta path."""
        raise NotImplementedError

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
        settings = RuntimeBudgetSettings()
        decision_output_path: str | None = None
        decision_metrics: dict[str, int | float] = {}

        try:
            self.prepare(context)
            settings = load_runtime_budget_settings(config.name, Path(self.config_path))

            baseline = self._execute_variant(
                context=context,
                settings=settings,
                application_id=application_id,
                variant="baseline",
            )
            candidate = self._execute_variant(
                context=context,
                settings=settings,
                application_id=application_id,
                variant="candidate",
            )

            compatibility = validate_business_outputs(
                context.spark,
                baseline_path=baseline.output_path,
                candidate_path=candidate.output_path,
                revenue_tolerance=settings.revenue_tolerance,
            )
            logger.info(
                "LAB5_OUTPUT_COMPATIBILITY_OK "
                f"baseline_rows={compatibility.baseline_row_count} "
                f"candidate_rows={compatibility.candidate_row_count} "
                f"baseline_revenue={compatibility.baseline_total_revenue} "
                f"candidate_revenue={compatibility.candidate_total_revenue}"
            )

            rules_path = Path(self.config_path).parent / "budget_rules.yaml"
            rules = load_budget_rules(rules_path)
            logger.info(
                "LAB5_BUDGET_RULES_LOADED_OK "
                f"rules_path={rules_path} "
                f"max_executor_runtime_growth_pct="
                f"{rules.default_budget.max_executor_runtime_growth_pct}"
            )

            decision = apply_runtime_budget(
                baseline=baseline.normalized_metrics,
                candidate=candidate.normalized_metrics,
                rules=rules,
            )
            decision_output_path = self._persist_budget_outputs(
                context=context,
                run_id=run_id,
                application_id=application_id,
                settings=settings,
                baseline=baseline,
                candidate=candidate,
                decision=decision,
            )
            logger.info(
                render_budget_decision_block(
                    decision=decision,
                    baseline=baseline.normalized_metrics,
                    candidate=candidate.normalized_metrics,
                    compatibility=compatibility,
                    metrics_output_path=context.config.artifacts.output(
                        "stage_runtime_budget_runs"
                    ).path,
                    decisions_output_path=decision_output_path,
                    baseline_output_path=baseline.output_path,
                    candidate_output_path=candidate.output_path,
                )
            )
            logger.info(
                "LAB5_RUNTIME_BUDGET_DECISION_WRITTEN_OK "
                f"output_path={decision_output_path} decision={decision.decision}"
            )
            logger.info(decision.final_marker)
            logger.info(settings.success_marker)
            logger.info(
                "WORKSHOP_EXPERIMENT_COMPLETED "
                f"experiment={config.name} run_id={run_id} "
                f"application_id={application_id}"
            )
            decision_metrics = {
                "numStages": candidate.metrics.get("numStages", 0),
                "numTasks": candidate.metrics.get("numTasks", 0),
                "executorRunTime": candidate.metrics.get("executorRunTime", 0),
                "shuffleBytesWritten": candidate.metrics.get(
                    "shuffleBytesWritten",
                    0,
                ),
            }
            return ExperimentRun(
                run_id=run_id,
                experiment_name=config.name,
                application_id=application_id,
                workload_result=decision_output_path,
                metrics=decision_metrics,
                metrics_output_path=None,
            )
        finally:
            try:
                self.cleanup(context)
            finally:
                self._context = None
                SparkSessionSingleton.stop()

    def _execute_variant(
        self,
        *,
        context: ExperimentContext,
        settings: RuntimeBudgetSettings,
        application_id: str,
        variant: str,
    ) -> VariantRun:
        variant_run_id = str(uuid4())
        variant_name = (
            settings.baseline_variant if variant == "baseline" else settings.candidate_variant
        )
        logger.info(
            "SPARKMEASURE_ENABLED=true "
            f"experiment={context.config.name} collector=stage persist=false "
            f"variant={variant_name}"
        )

        collector = SparkMeasureFactory.create("stage", context.spark)
        collector.begin()
        try:
            self._context = context
            inputs = self.extract()
            if variant == "baseline":
                output = self.build_baseline(inputs)
            else:
                output = self.build_candidate(inputs)
            output_path = self.load_variant(variant, output)
        finally:
            collector.end()

        metrics = normalize_metrics(collector.aggregate())
        normalized = normalize_stage_metrics(metrics)
        marker = (
            "LAB5_BASELINE_STAGE_METRICS_OK"
            if variant == "baseline"
            else "LAB5_CANDIDATE_STAGE_METRICS_OK"
        )
        logger.info(
            f"{marker} "
            f"variant={variant_name} "
            f"run_id={variant_run_id} "
            f"application_id={application_id} "
            f"numStages={normalized.num_stages} "
            f"numTasks={normalized.num_tasks} "
            f"executorRunTime={normalized.executor_run_time_ms} "
            f"shuffleBytesWritten={normalized.shuffle_bytes_written} "
            f"shuffleTotalBytesRead={normalized.shuffle_bytes_read}"
        )
        return VariantRun(
            run_id=variant_run_id,
            variant_name=variant_name,
            output_path=output_path,
            metrics=metrics,
            normalized_metrics=normalized,
        )

    def _persist_budget_outputs(
        self,
        *,
        context: ExperimentContext,
        run_id: str,
        application_id: str,
        settings: RuntimeBudgetSettings,
        baseline: VariantRun,
        candidate: VariantRun,
        decision: Any,
    ) -> str:
        metrics_rows = [
            build_stage_metrics_record(
                run_id=baseline.run_id,
                app_name=context.config.app_name,
                application_id=application_id,
                workload_name=settings.workload_name,
                workload_variant=baseline.variant_name,
                metrics=baseline.normalized_metrics,
            ),
            build_stage_metrics_record(
                run_id=candidate.run_id,
                app_name=context.config.app_name,
                application_id=application_id,
                workload_name=settings.workload_name,
                workload_variant=candidate.variant_name,
                metrics=candidate.normalized_metrics,
            ),
        ]
        decision_record = build_decision_record(
            run_id=run_id,
            app_name=context.config.app_name,
            application_id=application_id,
            baseline_run_id=baseline.run_id,
            candidate_run_id=candidate.run_id,
            workload_name=settings.workload_name,
            baseline=baseline.normalized_metrics,
            candidate=candidate.normalized_metrics,
            decision=decision,
        )

        with spark_job_description(
            context.spark,
            "LAB5 | stage_runtime_budget | write_stage_metrics",
        ):
            context.write(
                "stage_runtime_budget_runs",
                context.spark.createDataFrame(metrics_rows, schema=_stage_metrics_schema()),
            )
        with spark_job_description(
            context.spark,
            "LAB5 | stage_runtime_budget | write_decision",
        ):
            context.write(
                "stage_runtime_budget_decisions",
                context.spark.createDataFrame(
                    [decision_record],
                    schema=_decision_schema(),
                ),
            )
        return context.config.artifacts.output("stage_runtime_budget_decisions").path


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
            T.StructField("shuffle_bytes_written", T.LongType(), False),
            T.StructField("shuffle_bytes_read", T.LongType(), False),
            T.StructField("memory_bytes_spilled", T.LongType(), False),
            T.StructField("disk_bytes_spilled", T.LongType(), False),
            T.StructField("jvm_gc_time_ms", T.LongType(), False),
            T.StructField("num_stages", T.LongType(), False),
            T.StructField("num_tasks", T.LongType(), False),
            T.StructField("input_bytes", T.LongType(), False),
            T.StructField("records_read", T.LongType(), False),
            T.StructField("records_written", T.LongType(), False),
            T.StructField("created_at", T.StringType(), False),
        ]
    )


def _decision_schema() -> Any:
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("app_name", T.StringType(), False),
            T.StructField("application_id", T.StringType(), False),
            T.StructField("baseline_run_id", T.StringType(), False),
            T.StructField("candidate_run_id", T.StringType(), False),
            T.StructField("workload_name", T.StringType(), False),
            T.StructField("workload_profile", T.StringType(), False),
            T.StructField("decision", T.StringType(), False),
            T.StructField("failed_rules", T.StringType(), False),
            T.StructField("warning_flags", T.StringType(), False),
            T.StructField("executor_run_time_delta_pct", T.DoubleType(), False),
            T.StructField("shuffle_written_delta_pct", T.DoubleType(), False),
            T.StructField("shuffle_read_delta_pct", T.DoubleType(), False),
            T.StructField("num_tasks_delta_pct", T.DoubleType(), False),
            T.StructField("num_stages_delta_pct", T.DoubleType(), False),
            T.StructField("gc_time_delta_pct", T.DoubleType(), False),
            T.StructField("spill_delta_pct", T.DoubleType(), False),
            T.StructField("baseline_metrics", T.StringType(), False),
            T.StructField("candidate_metrics", T.StringType(), False),
            T.StructField("created_at", T.StringType(), False),
        ]
    )
