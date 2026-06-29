"""# Lab 0: sparkMeasure natural API

## Submit command

Assumes the Compose stack is running and the bronze `sales`, `vendors`, and
`products` Delta tables exist at the configured input artifact paths.

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/sparkmeasure_native_api.py
```

This script intentionally calls sparkMeasure directly with `StageMetrics` so the
workshop can show the library's natural API before introducing the shared job
contract.
"""

from __future__ import annotations

from pathlib import Path

from sparkmeasure import StageMetrics

from apps.labs.lab_0.lab_0_utils.transformations import build_sales_enriched
from spark_workshop.artifacts import read_artifact
from spark_workshop.config import load_experiment_config
from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger, spark_job_description, terminal_section


CONFIG_PATH = Path(__file__).parent / "lab_0_utils" / "experiments.yaml"
EXPERIMENT_NAME = "lab0-sparkmeasure-native-api"
SAMPLE_ROWS = 10


def main() -> int:
    config = load_experiment_config(EXPERIMENT_NAME, config_path=CONFIG_PATH)
    logger.set_level(config.log_level)
    logger.info(
        terminal_section(
            "Lab 0 - sparkMeasure natural API",
            "Direct StageMetrics begin/end around a controlled action",
        )
    )
    logger.info(
        "WORKSHOP_EXPERIMENT_STARTED "
        f"experiment={config.name} app_name={config.app_name}"
    )

    spark = SparkSessionSingleton.get_or_create(config.app_name, config.spark_config)
    spark.sparkContext.setLogLevel(config.spark_log_level.upper())
    stage_metrics = StageMetrics(spark)

    try:
        inputs = {
            name: read_artifact(spark, config.artifacts.input(name))
            for name in ("sales", "vendors", "products")
        }

        logger.info("SPARKMEASURE_NATURAL_API_BEGIN collector=stage")
        stage_metrics.begin()
        try:
            sales_enriched = build_sales_enriched(inputs)
            # Intentional action for teaching the native sparkMeasure API.
            with spark_job_description(
                spark,
                "LAB0 | natural_api | show_sales_enriched_sample",
            ):
                sales_enriched.select(
                    "sale_id",
                    "sale_date",
                    "vendor_name",
                    "product_name",
                    "sale_amount",
                ).limit(SAMPLE_ROWS).show(truncate=False)
        finally:
            stage_metrics.end()
            logger.info("SPARKMEASURE_NATURAL_API_END collector=stage")

        stage_metrics.print_report()
        metrics = dict(stage_metrics.aggregate_stagemetrics())
        logger.info(
            "SPARKMEASURE_NATURAL_API_METRICS "
            f"numStages={metrics.get('numStages', 0)} "
            f"numTasks={metrics.get('numTasks', 0)} "
            f"executorRunTime={metrics.get('executorRunTime', 0)} "
            f"shuffleBytesWritten={metrics.get('shuffleBytesWritten', 0)}"
        )
        logger.info("LAB0_SPARKMEASURE_NATURAL_API_OK")
        return 0
    finally:
        SparkSessionSingleton.stop()


if __name__ == "__main__":
    raise SystemExit(main())
