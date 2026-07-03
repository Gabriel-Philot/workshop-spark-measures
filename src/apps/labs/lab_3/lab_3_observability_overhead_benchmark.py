"""# Lab 3: observability overhead benchmark

Runs one benchmark repetition for the selected observability mode.

## Submit command

Prefer the orchestrator for classroom runs:

```bash
LAB3_REPETITIONS=10 \
LAB3_WARMUP_REPETITIONS=1 \
bash src/apps/labs/lab_3/run_observability_overhead_benchmark.sh
```

For a single manual run, change `LAB3_CONFIG_NAME` to one of:

- `lab3-overhead-none`
- `lab3-overhead-stage`
- `lab3-overhead-task`

```bash
docker compose --env-file .env -f build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB3_CONFIG_NAME=lab3-overhead-stage \
    LAB3_MODE=stage \
    /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_3/lab_3_observability_overhead_benchmark.py
```

## Required configuration

This script reads Lab 3 settings from `lab_3_utils/experiments.yaml`.
The app code stays identical across modes; the YAML selects no collector,
StageMetrics, or TaskMetrics. The workload reads `sales`, `vendors`,
`products`, and `customers`, then runs the same multi-join aggregation in
all modes.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_3.lab_3_utils.overhead_runtime import (
    Lab3ObservabilityOverheadJob,
)
from apps.labs.lab_3.lab_3_utils.transformations import (
    build_observability_overhead_summary,
)
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_3_utils" / "experiments.yaml"

# Classroom control point: the bash orchestrator sets this per repetition.
CONFIG_NAME = os.environ.get("LAB3_CONFIG_NAME", "lab3-overhead-none")


class Lab3ObservabilityOverheadBenchmark(Lab3ObservabilityOverheadJob):
    """Runs the same workload with none, stage, or task observability."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 3 - observability overhead benchmark"
    description = "Compare the same workload with no collector, StageMetrics, and TaskMetrics."

    def extract(self) -> dict[str, DataFrame]:
        return {
            "sales": self.read("sales"),
            "vendors": self.read("vendors"),
            "products": self.read("products"),
            "customers": self.read("customers"),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> DataFrame:
        return build_observability_overhead_summary(
            inputs,
            shuffle_partitions=self.workload_settings.shuffle_partitions,
            benchmark_buckets=self.workload_settings.benchmark_buckets,
        )

    def load(self, sales_summary: DataFrame) -> str:
        with spark_job_description(
            self.context.spark,
            "LAB3 | observability_overhead | "
            f"mode={self.benchmark_context.mode} | "
            f"iteration={self.benchmark_context.iteration} | write_summary",
        ):
            return self.write_benchmark_output(sales_summary)

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Lab 3 workload output path was not returned")
        self.output_row_count = self.count_output_rows(output_path)
        self.logger.info(
            "LAB3_OVERHEAD_VALIDATION_OK "
            f"experiment={self.context.config.name} "
            f"mode={self.benchmark_context.mode} "
            f"iteration={self.benchmark_context.iteration} "
            f"row_count={self.output_row_count} "
            f"output_path={output_path}"
        )


def main() -> int:
    return Lab3ObservabilityOverheadBenchmark().run()


if __name__ == "__main__":
    raise SystemExit(main())
