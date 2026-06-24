"""CLI entrypoint for spark-submit generator runs.

Execution flow:
1. `make generate SCALE=<preset>` or direct `spark-submit` calls this file.
2. CLI arguments select a generator YAML file and a named scale preset.
3. `load_contract(config, scale)` reads the YAML and converts it into a typed
   `GeneratorContract` with paths, scale, skew, write mode, and time settings.
4. The contract is injected into the selected materializer. The first runtime
   implementation is `SparkNativeMaterializer`, which writes related Delta
   tables to MinIO and validates row counts, FK coverage, skew, and file stats.

Example:
`make generate SCALE=xs GENERATOR_RUN_ID=manual-xs-001` becomes a spark-submit
call with `--scale xs`; the loader resolves `scales.xs` from
`generator/configs/retail_sales_skew.yaml` and passes that resolved contract to
the generator.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from spark_workshop.utils import logger
from workshop_generator.contract import load_contract
from workshop_generator.materializers import SparkNativeMaterializer


DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "retail_sales_skew.yaml"


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_id = args.run_id or _default_run_id(args.scale)
    contract = load_contract(args.config, args.scale)
    logger.set_level(args.log_level)

    if contract.engine == "dbldatagen":
        raise RuntimeError(
            "dbldatagen engine is planned but not enabled in the first implementation slice. "
            "Use materialization.engine=spark-native for the local workshop generator."
        )

    manifest = SparkNativeMaterializer(
        contract=contract,
        run_id=run_id,
        validate=not args.skip_validation,
    ).run()

    print(f"GENERATOR_RUN_ID={run_id}")
    print(f"GENERATOR_SCALE={contract.scale.name}")
    print(f"GENERATOR_ENGINE={contract.engine}")
    print(f"GENERATOR_SALES_PATH={manifest['paths']['sales']}")
    print(f"GENERATOR_MANIFEST_PATH={manifest['paths']['manifest']}")
    if not args.skip_validation:
        validation = manifest["validation"]
        for line in _volume_metric_lines(validation):
            print(line)
        print(
            "GENERATOR_VALIDATION_OK "
            f"sales_rows={validation['counts']['sales']} "
            f"hot_vendor_share={validation['hot_vendor_share']:.4f} "
            f"sales_files={validation['sales_file_stats']['file_count']}"
        )
    print("GENERATOR_OK")
    return 0


def _volume_metric_lines(validation: dict) -> list[str]:
    counts = validation.get("counts", {})
    table_file_stats = validation.get("table_file_stats", {})
    ordered_tables = ("vendors", "products", "customers", "sales")
    return [
        _format_volume_metric_line(
            table=table,
            rows=int(counts.get(table, 0)),
            file_stats=table_file_stats.get(table, {}),
        )
        for table in ordered_tables
        if table in counts or table in table_file_stats
    ]


def _format_volume_metric_line(table: str, rows: int, file_stats: dict) -> str:
    return (
        "GENERATOR_VOLUME "
        f"table={table} "
        f"rows={rows} "
        f"files={int(file_stats.get('file_count', 0))} "
        f"total_bytes={int(file_stats.get('total_bytes', 0))} "
        f"min_file_bytes={int(file_stats.get('min_file_bytes', 0))} "
        f"avg_file_bytes={float(file_stats.get('avg_file_bytes', 0.0)):.1f} "
        f"max_file_bytes={int(file_stats.get('max_file_bytes', 0))}"
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate related Delta datasets for Spark Measures labs.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Generator contract YAML path.")
    parser.add_argument("--scale", default="demo", help="Scale preset name from the contract.")
    parser.add_argument("--run-id", default="", help="Optional manifest run id. Defaults to timestamped id.")
    parser.add_argument("--skip-validation", action="store_true", help="Skip Spark validation after writing Delta tables.")
    parser.add_argument("--log-level", default="INFO", help="Application logger level.")
    return parser.parse_args(argv)


def _default_run_id(scale: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"retail-sales-skew-{scale}-{timestamp}"


if __name__ == "__main__":
    raise SystemExit(main())
