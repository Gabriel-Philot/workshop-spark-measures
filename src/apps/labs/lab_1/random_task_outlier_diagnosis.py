"""# Lab 1B: random task outlier diagnosis

This script intentionally keeps the classroom interface small. Change only
`CONFIG_NAME` below to switch between stage metrics, task metrics, and the fixed
variant. The selected YAML config controls the sparkMeasure collector and the
workload variant.

Task metrics are diagnostic-only here: they are printed and inspected in-process,
not persisted as Delta artifacts.

## Classroom switch

```python
CONFIG_NAME = "lab1-random-task-outlier-stage"       # stage aggregate view
CONFIG_NAME = "lab1-random-task-outlier-task"        # task diagnostic view
CONFIG_NAME = "lab1-random-task-outlier-fixed-task"  # fixed validation view
```

## Submit command

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_1/random_task_outlier_diagnosis.py
```
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

import yaml

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_1.lab_1_utils.transformations import (
    build_random_task_outlier_fixed,
    build_random_task_outlier_problem,
    build_sales_enriched,
)
from spark_workshop.artifacts import read_artifact, write_artifact
from spark_workshop.config import load_experiment_config
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger, spark_job_description, terminal_section


CONFIG_PATH = Path(__file__).parent / "lab_1_utils" / "experiments.yaml"

# Classroom control point: change this single value before the submit.
CONFIG_NAME = "lab1-random-task-outlier-stage"

# Useful alternatives for the live demo:
# CONFIG_NAME = "lab1-random-task-outlier-task"
# CONFIG_NAME = "lab1-random-task-outlier-fixed-stage"
# CONFIG_NAME = "lab1-random-task-outlier-fixed-task"

VALID_WORKLOAD_VARIANTS = frozenset({"problematic", "fixed"})
TASK_OUTLIER_TOP_N = 5
TASK_METRICS_VIEW = "Lab1TaskMetrics"
TASK_OUTLIER_COLUMNS = (
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


def main() -> int:
    config = load_experiment_config(CONFIG_NAME, config_path=CONFIG_PATH)
    workload = _load_workload_settings(CONFIG_NAME)
    collector_name = config.observability.collector

    logger.set_level(config.log_level)
    logger.info(
        terminal_section(
            f"Lab 1B - random task outlier ({collector_name}, {workload.variant})",
            "A technical audit bucket creates a task straggler without business-key skew",
        )
    )
    logger.info(
        "WORKSHOP_EXPERIMENT_STARTED "
        f"experiment={config.name} app_name={config.app_name}"
    )
    logger.info(
        "LAB1_RANDOM_TASK_OUTLIER_CONFIG "
        f"config_name={CONFIG_NAME} collector={collector_name} "
        f"variant={workload.variant}"
    )

    spark = SparkSessionSingleton.get_or_create(config.app_name, config.spark_config)
    spark.sparkContext.setLogLevel(config.spark_log_level.upper())

    try:
        inputs = {
            name: read_artifact(spark, config.artifacts.input(name))
            for name in ("sales", "vendors", "products")
        }
        collector = _build_collector(collector_name, spark)
        logger.info(
            "SPARKMEASURE_ENABLED=true "
            f"experiment={config.name} collector={collector_name} "
            f"persist={str(config.observability.persist).lower()}"
        )
        if config.observability.persist:
            logger.warning(
                "LAB1_RANDOM_TASK_OUTLIER_PERSIST_IGNORED "
                "task/stage metrics are printed only in this script"
            )

        collector.begin()
        try:
            output_path = _run_workload(
                spark=spark,
                config=config,
                inputs=inputs,
                collector_name=collector_name,
                variant=workload.variant,
            )
        finally:
            collector.end()

        collector.print_report()
        metrics = _aggregate_metrics(collector_name, collector)
        logger.info(_metrics_line(config.name, metrics))
        if collector_name == "task":
            _log_task_outliers(spark, collector)

        logger.info(
            "LAB1_RANDOM_TASK_OUTLIER_VALIDATION_OK "
            f"experiment={config.name} collector={collector_name} "
            f"variant={workload.variant} output_path={output_path}"
        )
        logger.info(workload.success_marker)
        return 0
    finally:
        SparkSessionSingleton.stop()


def _load_workload_settings(config_name: str) -> WorkloadSettings:
    raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
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


def _build_collector(collector_name: str, spark: Any) -> Any:
    if collector_name == "task":
        from sparkmeasure import TaskMetrics

        return TaskMetrics(spark)

    from sparkmeasure import StageMetrics

    return StageMetrics(spark)


def _run_workload(
    *,
    spark: Any,
    config: Any,
    inputs: dict[str, "DataFrame"],
    collector_name: str,
    variant: str,
) -> str:
    sales_enriched = build_sales_enriched(inputs)
    if variant == "fixed":
        audit_output = sales_enriched.transform(build_random_task_outlier_fixed)
    else:
        # LIVE FIX OPTION:
        # During the workshop, replace the problematic transform below with the
        # fixed transform to spread the expensive audit bucket across more tasks:
        # audit_output = sales_enriched.transform(build_random_task_outlier_fixed)
        audit_output = sales_enriched.transform(build_random_task_outlier_problem)

    output = config.artifacts.output("audit_outlier")
    with spark_job_description(
        spark,
        "LAB1 | random_task_outlier | "
        f"collector={collector_name} | variant={variant} | write_audit_output",
    ):
        write_artifact(audit_output, output)
    return output.path


def _aggregate_metrics(collector_name: str, collector: Any) -> Mapping[str, Any]:
    if collector_name == "task":
        return dict(collector.aggregate_taskmetrics())
    return dict(collector.aggregate_stagemetrics())


def _metrics_line(experiment_name: str, metrics: Mapping[str, Any]) -> str:
    return (
        "SPARKMEASURE_METRICS "
        f"experiment={experiment_name} "
        f"numStages={metrics.get('numStages', 0)} "
        f"numTasks={metrics.get('numTasks', 0)} "
        f"executorRunTime={metrics.get('executorRunTime', 0)} "
        f"shuffleBytesWritten={metrics.get('shuffleBytesWritten', 0)}"
    )


def _log_task_outliers(spark: Any, collector: Any) -> None:
    from pyspark.sql import functions as F

    task_metrics = collector.create_taskmetrics_DF(TASK_METRICS_VIEW)
    selected_columns = [
        column for column in TASK_OUTLIER_COLUMNS if column in task_metrics.columns
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

    for rank, row in enumerate(rows, start=1):
        logger.info(render_task_outlier_line(rank, row.asDict()))


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


if __name__ == "__main__":
    raise SystemExit(main())
