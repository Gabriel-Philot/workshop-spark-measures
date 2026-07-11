"""# Lab 5: stage-level runtime budget guardrail

Runs an approved baseline Spark workload and a functionally equivalent
candidate workload, captures sparkMeasure StageMetrics for both, validates that
the business outputs match, and applies runtime budget rules.

## Submit command

Assumes the Compose stack is running and the generated bronze retail Delta
tables exist at the configured input artifact paths.

```bash
cd src/apps/labs/lab_5
bash run_stage_runtime_budget_guardrail.sh
```

## Required configuration

This script reads workload settings from `lab_5_utils/experiments.yaml` and
budget thresholds from `lab_5_utils/budget_rules.yaml`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_5.lab_5_utils.budget import (
    RuntimeBudgetSettings,
    load_runtime_budget_settings,
)
from apps.labs.lab_5.lab_5_utils.runtime import Lab5StageRuntimeBudgetGuardrailJob
from apps.labs.lab_5.lab_5_utils.transformations import (
    build_runtime_budget_baseline,
    build_runtime_budget_candidate,
)
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_5_utils" / "experiments.yaml"

# Classroom control point: change this single value only if adding variants.
CONFIG_NAME = os.environ.get(
    "LAB5_CONFIG_NAME",
    "lab5-stage-runtime-budget-guardrail",
)


class Lab5StageRuntimeBudgetGuardrail(Lab5StageRuntimeBudgetGuardrailJob):
    """Compare baseline and candidate workloads using stage-level budgets."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    title = "Lab 5 - stage-level runtime budget guardrail"
    description = (
        "Compare a baseline workload with a candidate PR using sparkMeasure "
        "StageMetrics and YAML runtime budgets."
    )

    def __init__(self) -> None:
        super().__init__()
        self.workload_settings = RuntimeBudgetSettings()

    def before_extract(self) -> None:
        self.workload_settings = load_runtime_budget_settings(
            self.context.config.name,
            CONFIG_PATH,
        )
        self.logger.info(
            "LAB5_WORKLOAD_CONFIG "
            f"config_name={self.context.config.name} "
            f"workload_name={self.workload_settings.workload_name} "
            f"baseline_variant={self.workload_settings.baseline_variant} "
            f"candidate_variant={self.workload_settings.candidate_variant} "
            f"baseline_keyed_partitions="
            f"{self.workload_settings.baseline_keyed_partitions} "
            f"candidate_round_robin_partitions="
            f"{self.workload_settings.candidate_round_robin_partitions} "
            f"candidate_keyed_partitions="
            f"{self.workload_settings.candidate_keyed_partitions}"
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
                "Lab 5 requires generated retail Delta tables before it can run. "
                "Run `make generate SCALE=xs` after starting the platform."
            ) from exc

    def build_baseline(self, inputs: dict[str, "DataFrame"]) -> "DataFrame":
        return build_runtime_budget_baseline(
            inputs,
            keyed_partitions=self.workload_settings.baseline_keyed_partitions,
        )

    def build_candidate(self, inputs: dict[str, "DataFrame"]) -> "DataFrame":
        return build_runtime_budget_candidate(
            inputs,
            round_robin_partitions=(
                self.workload_settings.candidate_round_robin_partitions
            ),
            keyed_partitions=self.workload_settings.candidate_keyed_partitions,
            guardrail_buckets=self.workload_settings.candidate_guardrail_buckets,
        )

    def load_variant(self, variant: str, data: "DataFrame") -> str:
        artifact_name = (
            "baseline_business_output"
            if variant == "baseline"
            else "candidate_business_output"
        )
        with spark_job_description(
            self.context.spark,
            f"LAB5 | stage_runtime_budget | {variant} | write_business_output",
        ):
            self.write(artifact_name, data)
        return self.output_path(artifact_name)


def main() -> int:
    return Lab5StageRuntimeBudgetGuardrail().run()


if __name__ == "__main__":
    raise SystemExit(main())
