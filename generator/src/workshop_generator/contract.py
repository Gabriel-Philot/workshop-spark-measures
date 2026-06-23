"""Load and validate schema-first generator contracts."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}")
SUPPORTED_ENGINES = frozenset({"spark-native", "dbldatagen"})


@dataclass(frozen=True)
class ScalePreset:
    name: str
    vendor_rows: int
    products_per_vendor: int
    customer_rows: int
    sales_rows: int
    partitions: int
    max_records_per_file: int
    payload_columns: int = 0
    payload_width: int = 0

    @property
    def product_rows(self) -> int:
        return self.vendor_rows * self.products_per_vendor


@dataclass(frozen=True)
class VendorSkew:
    hot_vendor_id: int
    hot_vendor_share: float
    tolerance: float = 0.08


@dataclass(frozen=True)
class WriteSpec:
    mode: str
    sales_partition_by: tuple[str, ...]


@dataclass(frozen=True)
class TimeSpec:
    start_date: str
    days: int


@dataclass(frozen=True)
class GeneratorPaths:
    bronze_base: str
    observability_base: str

    def table_path(self, table: str) -> str:
        return f"{self.bronze_base.rstrip('/')}/{table}"

    def manifest_path(self, run_id: str) -> str:
        return f"{self.observability_base.rstrip('/')}/{run_id}/manifest.json"


@dataclass(frozen=True)
class GeneratorContract:
    name: str
    app_name: str
    seed: int
    engine: str
    scale: ScalePreset
    paths: GeneratorPaths
    write: WriteSpec
    vendor_skew: VendorSkew
    time: TimeSpec

    @property
    def tables(self) -> tuple[str, ...]:
        return ("vendors", "products", "customers", "sales")


def load_contract(config_path: str | Path, scale_name: str) -> GeneratorContract:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as config_file:
        raw = _expand_env(yaml.safe_load(config_file) or {})

    scenario = _as_mapping(raw.get("scenario"), "scenario")
    scales = _as_mapping(scenario.get("scales"), "scenario.scales")
    if scale_name not in scales:
        raise KeyError(
            f"Unknown generator scale '{scale_name}'. Available scales: {sorted(scales)}"
        )

    engine = str(
        _as_mapping(scenario.get("materialization") or {}, "scenario.materialization").get(
            "engine", "spark-native"
        )
    ).lower()
    if engine not in SUPPORTED_ENGINES:
        raise ValueError(
            f"Unsupported generator engine '{engine}'. Expected one of {sorted(SUPPORTED_ENGINES)}"
        )

    paths = _as_mapping(scenario.get("paths"), "scenario.paths")
    write = _as_mapping(scenario.get("write") or {}, "scenario.write")
    distributions = _as_mapping(
        scenario.get("distributions"), "scenario.distributions"
    )
    vendor_skew = _as_mapping(
        distributions.get("vendor_skew"), "scenario.distributions.vendor_skew"
    )
    time = _as_mapping(scenario.get("time"), "scenario.time")

    contract = GeneratorContract(
        name=str(scenario.get("name", "")).strip(),
        app_name=str(scenario.get("app_name", "")).strip(),
        seed=_int(scenario.get("seed", 42), "scenario.seed"),
        engine=engine,
        scale=_scale(scale_name, _as_mapping(scales[scale_name], f"scenario.scales.{scale_name}")),
        paths=GeneratorPaths(
            bronze_base=str(paths.get("bronze_base", "")).rstrip("/"),
            observability_base=str(paths.get("observability_base", "")).rstrip("/"),
        ),
        write=WriteSpec(
            mode=str(write.get("mode", "overwrite")),
            sales_partition_by=tuple(
                str(column) for column in (write.get("sales_partition_by") or ())
            ),
        ),
        vendor_skew=VendorSkew(
            hot_vendor_id=_int(vendor_skew.get("hot_vendor_id", 1), "hot_vendor_id"),
            hot_vendor_share=_float(
                vendor_skew.get("hot_vendor_share", 0.70), "hot_vendor_share"
            ),
            tolerance=_float(vendor_skew.get("tolerance", 0.08), "tolerance"),
        ),
        time=TimeSpec(
            start_date=str(time.get("start_date", "2026-01-01")),
            days=_int(time.get("days", 30), "scenario.time.days"),
        ),
    )
    _validate(contract)
    return contract


def _scale(name: str, value: Mapping[str, Any]) -> ScalePreset:
    return ScalePreset(
        name=name,
        vendor_rows=_int(value.get("vendors"), f"scale.{name}.vendors"),
        products_per_vendor=_int(
            value.get("products_per_vendor"), f"scale.{name}.products_per_vendor"
        ),
        customer_rows=_int(value.get("customers"), f"scale.{name}.customers"),
        sales_rows=_int(value.get("sales"), f"scale.{name}.sales"),
        partitions=_int(value.get("partitions"), f"scale.{name}.partitions"),
        max_records_per_file=_int(
            value.get("max_records_per_file"), f"scale.{name}.max_records_per_file"
        ),
        payload_columns=_int(value.get("payload_columns", 0), f"scale.{name}.payload_columns"),
        payload_width=_int(value.get("payload_width", 0), f"scale.{name}.payload_width"),
    )


def _validate(contract: GeneratorContract) -> None:
    if not contract.name:
        raise ValueError("Generator scenario requires scenario.name")
    if not contract.app_name:
        raise ValueError("Generator scenario requires scenario.app_name")
    if not contract.paths.bronze_base:
        raise ValueError("Generator scenario requires paths.bronze_base")
    if not contract.paths.observability_base:
        raise ValueError("Generator scenario requires paths.observability_base")
    if contract.scale.vendor_rows < 2:
        raise ValueError("Generator scale requires at least two vendors")
    if contract.scale.products_per_vendor < 1:
        raise ValueError("Generator scale requires products_per_vendor >= 1")
    if contract.scale.customer_rows < 1:
        raise ValueError("Generator scale requires customers >= 1")
    if contract.scale.sales_rows < 1:
        raise ValueError("Generator scale requires sales >= 1")
    if contract.scale.partitions < 1:
        raise ValueError("Generator scale requires partitions >= 1")
    if contract.scale.max_records_per_file < 1:
        raise ValueError("Generator scale requires max_records_per_file >= 1")
    if not 0 < contract.vendor_skew.hot_vendor_share < 1:
        raise ValueError("hot_vendor_share must be between 0 and 1")
    if not 1 <= contract.vendor_skew.hot_vendor_id <= contract.scale.vendor_rows:
        raise ValueError("hot_vendor_id must exist in vendors")
    if contract.time.days < 1:
        raise ValueError("Generator time.days must be >= 1")
    allowed_sales_partitions = {"vendor_id", "sale_date"}
    unknown_partitions = set(contract.write.sales_partition_by) - allowed_sales_partitions
    if unknown_partitions:
        raise ValueError(
            "Unsupported sales partition columns: " f"{sorted(unknown_partitions)}"
        )


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_expand_env(child) for child in value]
    if isinstance(value, str):
        return _ENV_PATTERN.sub(
            lambda match: os.environ.get(match.group(1), match.group(3) or ""), value
        )
    return value


def _as_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    return value


def _int(value: Any, name: str) -> int:
    if value is None:
        raise ValueError(f"{name} is required")
    try:
        return int(str(value).replace("_", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _float(value: Any, name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a float") from exc
