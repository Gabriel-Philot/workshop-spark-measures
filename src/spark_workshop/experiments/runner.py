"""Template Method runner for workload and observability experiments."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4

from spark_workshop.config import ExperimentConfig
from spark_workshop.experiments.base import ExperimentContext, SparkExperiment
from spark_workshop.metrics import (
    SparkMeasureFactory,
    normalize_metrics,
    validate_aggregate_metrics,
)
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger


@dataclass(frozen=True)
class ExperimentRun:
    run_id: str
    experiment_name: str
    application_id: str
    workload_result: Any
    metrics: Mapping[str, int | float]
    metrics_output_path: str | None


class ExperimentRunner:
    def __init__(self, config: ExperimentConfig):
        self.config = config

    def run(self, experiment: SparkExperiment) -> ExperimentRun:
        logger.set_level(self.config.log_level)
        logger.info(
            "WORKSHOP_EXPERIMENT_STARTED "
            f"experiment={self.config.name} app_name={self.config.app_name}"
        )

        spark = SparkSessionSingleton.get_or_create(
            self.config.app_name, self.config.spark_config
        )
        reused = SparkSessionSingleton.get_or_create(
            self.config.app_name, self.config.spark_config
        )
        if reused is not spark:
            raise RuntimeError("SparkSession singleton returned different instances")
        logger.info("SPARK_SESSION_SINGLETON_OK")

        spark.sparkContext.setLogLevel(self.config.spark_log_level.upper())
        context = ExperimentContext(spark=spark, config=self.config)
        run_id = str(uuid4())
        application_id = spark.sparkContext.applicationId

        try:
            experiment.prepare(context)
            result, metrics = self._execute_workload(experiment, context)
            experiment.validate(result, context)
            output_path = self._persist_metrics(
                context=context,
                run_id=run_id,
                application_id=application_id,
                metrics=metrics,
            )
            logger.info(
                "WORKSHOP_EXPERIMENT_COMPLETED "
                f"experiment={self.config.name} run_id={run_id} "
                f"application_id={application_id}"
            )
            return ExperimentRun(
                run_id=run_id,
                experiment_name=self.config.name,
                application_id=application_id,
                workload_result=result,
                metrics=metrics,
                metrics_output_path=output_path,
            )
        finally:
            try:
                experiment.cleanup(context)
            finally:
                SparkSessionSingleton.stop()

    def _execute_workload(
        self,
        experiment: SparkExperiment,
        context: ExperimentContext,
    ) -> tuple[Any, Mapping[str, int | float]]:
        observability = self.config.observability
        if not observability.enabled:
            logger.info(
                "SPARKMEASURE_ENABLED=false "
                f"experiment={self.config.name}"
            )
            return experiment.workload(context), {}

        logger.info(
            "SPARKMEASURE_ENABLED=true "
            f"experiment={self.config.name} "
            f"collector={observability.collector} "
            f"persist={str(observability.persist).lower()}"
        )
        collector = SparkMeasureFactory.create(observability.collector, context.spark)
        collector.begin()
        try:
            result = experiment.workload(context)
        finally:
            collector.end()

        collector.print_report()
        metrics = normalize_metrics(collector.aggregate())
        if observability.collector == "stage":
            validate_aggregate_metrics(metrics)
        elif not metrics:
            raise ValueError("Task metrics collection returned no numeric metrics")
        return result, metrics

    def _persist_metrics(
        self,
        context: ExperimentContext,
        run_id: str,
        application_id: str,
        metrics: Mapping[str, int | float],
    ) -> str | None:
        observability = self.config.observability
        if not observability.enabled or not observability.persist:
            return None

        output = self.config.artifacts.output(observability.output_artifact)
        record = {
            **metrics,
            "run_id": run_id,
            "experiment_name": self.config.name,
            "application_id": application_id,
            "application_name": self.config.app_name,
            "collector_type": observability.collector,
            "collected_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": context.spark.version,
        }
        context.write(
            observability.output_artifact,
            context.spark.createDataFrame([record]),
        )
        return output.path
