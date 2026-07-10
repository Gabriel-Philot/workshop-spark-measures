"""# Lab 4: stage-level workload fingerprint

Runs one generated-retail Spark workload, captures sparkMeasure StageMetrics,
normalizes the aggregate metrics, and assigns a lightweight operational
fingerprint.

## Submit command

Assumes the Compose stack is running and the generated bronze retail Delta
tables exist at the configured input artifact paths.

```bash
cd src/apps/labs/lab_4
bash run_stage_workload_fingerprint.sh
```

## Required configuration

This script reads workload settings from `lab_4_utils/experiments.yaml` and
fingerprint thresholds from `lab_4_utils/fingerprint_rules.yaml`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_4.lab_4_utils.fingerprint import (
    StageWorkloadFingerprintSettings,
    load_stage_workload_fingerprint_settings,
)
from apps.labs.lab_4.lab_4_utils.runtime import Lab4StageWorkloadFingerprintJob
from apps.labs.lab_4.lab_4_utils.transformations import (
    build_stage_workload_fingerprint_summary,
)
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_4_utils" / "experiments.yaml"

# Classroom control point: change this single value only if adding variants.
CONFIG_NAME = os.environ.get("LAB4_CONFIG_NAME", "lab4-stage-workload-fingerprint")


class Lab4StageWorkloadFingerprint(Lab4StageWorkloadFingerprintJob):
    """Builds a stage-level operational fingerprint for one Spark workload."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 4 - stage-level workload fingerprint"
    description = "Classify a Spark workload using stage-level sparkMeasure aggregate metrics."

    def __init__(self) -> None:
        super().__init__()
        self.workload_settings = StageWorkloadFingerprintSettings()

    def before_extract(self) -> None:
        self.workload_settings = load_stage_workload_fingerprint_settings(
            self.context.config.name,
            CONFIG_PATH,
        )
        self.logger.info(
            "LAB4_WORKLOAD_CONFIG "
            f"config_name={self.context.config.name} "
            f"workload_name={self.workload_settings.workload_name} "
            f"variant={self.workload_settings.workload_variant} "
            f"shuffle_partitions={self.workload_settings.shuffle_partitions} "
            f"fingerprint_buckets={self.workload_settings.fingerprint_buckets}"
        )

    def extract(self) -> dict[str, "DataFrame"]:
        try:
            return {
                "sales": self.read("sales"),
                "vendors": self.read("vendors"),
                "products": self.read("products"),
                "customers": self.read("customers"),
            }
        except Exception as exc:
            raise RuntimeError(
                "Lab 4 requires generated retail Delta tables before it can run. "
                "Run `make generate SCALE=xs` after starting the platform."
            ) from exc

    def transform(self, inputs: dict[str, "DataFrame"]) -> "DataFrame":
        return build_stage_workload_fingerprint_summary(
            inputs,
            shuffle_partitions=self.workload_settings.shuffle_partitions,
            fingerprint_buckets=self.workload_settings.fingerprint_buckets,
        )

    def load(self, workload_summary: "DataFrame") -> str:
        with spark_job_description(
            self.context.spark,
            "LAB4 | stage_workload_fingerprint | write_workload_summary",
        ):
            self.write("workload_summary", workload_summary)
        return self.output_path("workload_summary")

    def validate_result(self, output_path: str) -> None:
        if not output_path:
            raise RuntimeError("Lab 4 workload summary output path was not returned")
        row_count = self.context.spark.read.format("delta").load(output_path).count()
        if row_count < 1:
            raise RuntimeError(f"Lab 4 workload summary is empty at {output_path}")
        self.logger.info(
            "LAB4_WORKLOAD_VALIDATION_OK "
            f"output_path={output_path} row_count={row_count}"
        )


def main() -> int:
    return Lab4StageWorkloadFingerprint().run()


if __name__ == "__main__":
    raise SystemExit(main())
