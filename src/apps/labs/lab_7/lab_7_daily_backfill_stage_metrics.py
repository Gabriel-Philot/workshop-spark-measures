"""# Lab 7: daily backfill StageMetrics by date

Processes one temporal source `processing_date`, writes a daily dashboard
business output, captures sparkMeasure StageMetrics, and persists one metrics
row keyed by the business date.

## Submit command

Run the default 14-date classroom batch:

```bash
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

Run a calibration subset:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

## Required configuration

This script reads experiment settings from `lab_7_utils/experiments.yaml` and
expected per-date volumes from `lab_7_utils/volume_plan.yaml`.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from uuid import uuid4

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from apps.labs.lab_7.lab_7_utils.backfill_runtime import (
    Lab7DailyBackfillStageMetricsJob,
)
from apps.labs.lab_7.lab_7_utils.transformations import (
    EARLY_PARTITION_FILTER,
    SUPPORTED_FILTER_STRATEGIES,
    build_daily_activity_dashboard,
)
from spark_workshop.utils import spark_job_description


CONFIG_PATH = Path(__file__).parent / "lab_7_utils" / "experiments.yaml"
VOLUME_PLAN_PATH = Path(__file__).parent / "lab_7_utils" / "volume_plan.yaml"

# Classroom control point: change this only if adding Lab 7 backfill variants.
CONFIG_NAME = os.environ.get("LAB7_CONFIG_NAME", "lab7-daily-backfill-stage-metrics")


class Lab7DailyBackfillStageMetrics(Lab7DailyBackfillStageMetricsJob):
    """Run one daily temporal backfill and persist stage metrics by date."""

    config_path = CONFIG_PATH
    config_name = CONFIG_NAME
    volume_plan_path = VOLUME_PLAN_PATH
    title = "Lab 7 - daily backfill StageMetrics"
    description = (
        "Process one temporal business date and persist sparkMeasure "
        "StageMetrics with expected source volume."
    )

    def __init__(
        self,
        *,
        processing_date: str,
        filter_strategy: str,
        batch_run_id: str,
    ) -> None:
        super().__init__()
        self.processing_date = processing_date
        self.filter_strategy = filter_strategy
        self.batch_run_id = batch_run_id

    def extract(self) -> "DataFrame":
        try:
            return self.read("source_events_temporal")
        except Exception as exc:
            raise RuntimeError(
                "Lab 7 daily backfill requires the temporal bronze source. "
                "Run `make generate-lab7` first."
            ) from exc

    def transform(self, data: "DataFrame") -> "DataFrame":
        return build_daily_activity_dashboard(
            data,
            processing_date=self.processing_date,
            filter_strategy=self.filter_strategy,
        )

    def load(self, data: "DataFrame") -> str:
        with spark_job_description(
            self.context.spark,
            (
                "LAB7 | daily_backfill | "
                f"processing_date={self.processing_date} | write_dashboard"
            ),
        ):
            self.write("daily_activity_dashboard", data)
        return self.output_path("daily_activity_dashboard")

    def validate_result(self, result: str) -> None:
        count = (
            self.context.spark.read.format("delta")
            .load(result)
            .count()
        )
        if count < 1:
            raise RuntimeError(
                "Lab 7 daily backfill produced no dashboard rows for "
                f"processing_date={self.processing_date}"
            )


def main() -> int:
    args = _parse_args()
    batch_run_id = args.batch_run_id or os.environ.get("LAB7_BACKFILL_RUN_ID") or str(uuid4())
    return Lab7DailyBackfillStageMetrics(
        processing_date=args.processing_date,
        filter_strategy=args.filter_strategy,
        batch_run_id=batch_run_id,
    ).run()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one Lab 7 daily backfill.")
    parser.add_argument(
        "--processing-date",
        default=os.environ.get("LAB7_PROCESSING_DATE", ""),
        required=not bool(os.environ.get("LAB7_PROCESSING_DATE", "")),
        help="Business date to process, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--filter-strategy",
        default=os.environ.get("LAB7_FILTER_STRATEGY", EARLY_PARTITION_FILTER),
        choices=tuple(sorted(SUPPORTED_FILTER_STRATEGIES)),
        help="Source filtering strategy for this backfill run.",
    )
    parser.add_argument(
        "--batch-run-id",
        default=os.environ.get("LAB7_BACKFILL_RUN_ID", ""),
        help="Shared id across all processing dates in one runner batch.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
