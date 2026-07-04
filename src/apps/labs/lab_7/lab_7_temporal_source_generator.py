"""# Lab 7: temporal source generator

Creates the Lab 7 temporal bronze source used by the later daily backfill
observability lessons. The generator is intentionally scoped to Lab 7 paths and
does not rewrite the retail bronze tables used by previous labs.

## Submit command

Run the Lab 7-only generator:

```bash
bash src/apps/labs/lab_7/lab_7_utils/runners/run_temporal_source_generator.sh
```

Append one new day:

```bash
LAB7_GENERATE_MODE=append_day \
LAB7_APPEND_DATE=2026-01-15 \
LAB7_APPEND_VOLUME_MULTIPLIER=100 \
bash src/apps/labs/lab_7/lab_7_utils/runners/run_temporal_source_generator.sh
```

## Required configuration

This script reads experiment settings from `lab_7_utils/experiments.yaml` and
the temporal volume shape from `lab_7_utils/volume_plan.yaml`.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from apps.labs.lab_7.lab_7_utils.generator import run_temporal_source_generator


CONFIG_PATH = Path(__file__).parent / "lab_7_utils" / "experiments.yaml"
VOLUME_PLAN_PATH = Path(__file__).parent / "lab_7_utils" / "volume_plan.yaml"

# Classroom control point: change this only if adding Lab 7 generator variants.
CONFIG_NAME = os.environ.get("LAB7_CONFIG_NAME", "lab7-temporal-source-generator")


def main() -> int:
    args = _parse_args()
    run_temporal_source_generator(
        config_name=CONFIG_NAME,
        config_path=CONFIG_PATH,
        volume_plan_path=VOLUME_PLAN_PATH,
        mode=args.mode,
        append_date=args.append_date,
        append_volume_multiplier=args.append_volume_multiplier,
        replace_lab7_source=args.replace_lab7_source,
    )
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the Lab 7 temporal source.")
    parser.add_argument(
        "--mode",
        default=os.environ.get("LAB7_GENERATE_MODE", "full"),
        choices=("full", "append_day"),
        help="Generation mode. Use full for the planned range or append_day for one new date.",
    )
    parser.add_argument(
        "--append-date",
        default=os.environ.get("LAB7_APPEND_DATE", ""),
        help="Date for append_day mode, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--append-volume-multiplier",
        default=os.environ.get("LAB7_APPEND_VOLUME_MULTIPLIER", "1"),
        type=int,
        help="Volume multiplier for append_day mode.",
    )
    parser.add_argument(
        "--replace-lab7-source",
        default=os.environ.get("LAB7_REPLACE_SOURCE", "false"),
        type=_parse_bool,
        help="Explicitly delete and regenerate only the scoped Lab 7 source path.",
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
