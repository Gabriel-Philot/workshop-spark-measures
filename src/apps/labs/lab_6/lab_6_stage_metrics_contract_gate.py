"""# Lab 6: stage metrics contract gate

Runs a small retail Spark workload, captures sparkMeasure StageMetrics, persists
the raw metrics as an observability data product, and validates the metrics
against schema, semantic, and correlation contract rules.

## Submit command

Assumes the Compose stack is running and the generated bronze retail Delta
tables exist at the configured input artifact paths.

```bash
bash src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

Failure demonstration mode:

```bash
LAB6_INJECT_INVALID_RECORDS=true \
bash src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh
```

## Required configuration

This script reads workload settings from `lab_6_utils/experiments.yaml` and
contract thresholds from `lab_6_utils/contract_rules.yaml`.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_6.lab_6_utils.contract import (
    StageMetricsContractSettings,
    load_stage_metrics_contract_settings,
)
from apps.labs.lab_6.lab_6_utils.runtime import Lab6StageMetricsContractGateJob
from apps.labs.lab_6.lab_6_utils.transformations import (
    build_stage_metrics_contract_gate_output,
)
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_6_utils" / "experiments.yaml"

# Classroom control point: change this single value only if adding variants.
CONFIG_NAME = os.environ.get(
    "LAB6_CONFIG_NAME",
    "lab6-stage-metrics-contract-gate",
)


class Lab6StageMetricsContractGate(Lab6StageMetricsContractGateJob):
    """Validate collected StageMetrics before using them for automation."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 6 - stage metrics contract gate"
    description = (
        "Treat sparkMeasure StageMetrics as an observability data product and "
        "validate schema, semantic, and correlation contracts."
    )

    def __init__(self, *, inject_invalid_records: bool = False) -> None:
        super().__init__()
        self.inject_invalid_records = inject_invalid_records
        self.workload_settings = StageMetricsContractSettings()

    def before_extract(self) -> None:
        self.workload_settings = load_stage_metrics_contract_settings(
            self.context.config.name,
            CONFIG_PATH,
        )
        self.logger.info(
            "LAB6_WORKLOAD_CONFIG "
            f"config_name={self.context.config.name} "
            f"workload_name={self.workload_settings.workload_name} "
            f"workload_variant={self.workload_settings.workload_variant} "
            f"shuffle_partitions={self.workload_settings.shuffle_partitions} "
            f"inject_invalid_records={str(self.inject_invalid_records).lower()}"
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
                "Lab 6 requires generated retail Delta tables before it can run. "
                "Run `make generate SCALE=xs` after starting the platform."
            ) from exc

    def transform(self, inputs: dict[str, "DataFrame"]) -> "DataFrame":
        return build_stage_metrics_contract_gate_output(
            inputs,
            shuffle_partitions=self.workload_settings.shuffle_partitions,
        )

    def load(self, data: "DataFrame") -> str:
        with spark_job_description(
            self.context.spark,
            "LAB6 | stage_metrics_contract_gate | write_business_output",
        ):
            self.write("business_output", data)
        return self.output_path("business_output")


def main() -> int:
    args = _parse_args()
    return Lab6StageMetricsContractGate(
        inject_invalid_records=args.inject_invalid_records,
    ).run()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Lab 6 StageMetrics contract gate.",
    )
    parser.add_argument(
        "--inject-invalid-records",
        default=os.environ.get("LAB6_INJECT_INVALID_RECORDS", "false"),
        type=_parse_bool,
        help="Inject controlled invalid metrics rows into a separate demo input.",
    )
    return parser.parse_args()


def _parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(
        f"Expected a boolean value, got '{value}'. Use true or false."
    )


if __name__ == "__main__":
    raise SystemExit(main())
