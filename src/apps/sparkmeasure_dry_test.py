"""End-to-end test implemented as a workload + observability experiment."""

from typing import Any

from pyspark.sql import functions as F

from spark_workshop.config import load_experiment_config
from spark_workshop.runtime import (
    ExperimentContext,
    ExperimentRunner,
    SparkExperiment,
)


EXPERIMENT_NAME = "sparkmeasure-dry-test"


class SparkMeasureDryTest(SparkExperiment):
    def workload(self, context: ExperimentContext) -> Any:
        return (
            context.spark.range(0, 200_000, 1, numPartitions=8)
            .withColumn("bucket", F.col("id") % 32)
            .repartition(8, "bucket")
            .groupBy("bucket")
            .agg(F.count("*").alias("row_count"), F.sum("id").alias("id_sum"))
            .orderBy("bucket")
            .collect()
        )

    def validate(self, result: Any, context: ExperimentContext) -> None:
        if len(result) != 32 or sum(row.row_count for row in result) != 200_000:
            raise RuntimeError("The deterministic workload returned unexpected results")
        context.logger.info("WORKLOAD_VALIDATION_OK")


def main() -> int:
    config = load_experiment_config(EXPERIMENT_NAME)
    run = ExperimentRunner(config).run(SparkMeasureDryTest())

    print(f"SPARKMEASURE_DELTA_PATH={run.metrics_output_path}")
    print(
        "SPARKMEASURE_METRICS "
        f"numStages={run.metrics['numStages']} "
        f"numTasks={run.metrics['numTasks']} "
        f"executorRunTime={run.metrics['executorRunTime']}"
    )
    print("SPARKMEASURE_DRY_TEST_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
