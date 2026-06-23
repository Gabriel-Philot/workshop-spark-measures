from pathlib import Path

import pytest

from workshop_generator.contract import load_contract


CONFIG = Path(__file__).resolve().parents[1] / "generator" / "configs" / "retail_sales_skew.yaml"


def test_load_retail_sales_contract_demo():
    contract = load_contract(CONFIG, "demo")

    assert contract.name == "retail_sales_skew"
    assert contract.engine == "spark-native"
    assert contract.scale.name == "demo"
    assert contract.scale.vendor_rows == 20
    assert contract.scale.product_rows == 200
    assert contract.scale.sales_rows == 200000
    assert contract.paths.table_path("sales") == "s3a://lakehouse/bronze/retail/sales"
    assert contract.paths.manifest_path("run-1") == "s3a://observability/generator-runs/run-1/manifest.json"
    assert contract.write.sales_partition_by == ("vendor_id",)
    assert contract.vendor_skew.hot_vendor_id == 1
    assert contract.vendor_skew.hot_vendor_share == 0.70


def test_load_contract_expands_environment(monkeypatch):
    monkeypatch.setenv("MINIO_LAKEHOUSE_URI", "s3a://custom-lake")
    monkeypatch.setenv("MINIO_OBSERVABILITY_URI", "s3a://custom-observability")

    contract = load_contract(CONFIG, "demo")

    assert contract.paths.table_path("vendors") == "s3a://custom-lake/bronze/retail/vendors"
    assert contract.paths.manifest_path("abc") == "s3a://custom-observability/generator-runs/abc/manifest.json"


def test_load_contract_rejects_unknown_scale():
    with pytest.raises(KeyError, match="Unknown generator scale"):
        load_contract(CONFIG, "missing")
