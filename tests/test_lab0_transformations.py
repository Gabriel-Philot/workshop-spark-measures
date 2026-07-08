from apps.labs.lab_0.lab_0_utils.transformations import SALES_ENRICHED_COLUMNS
from apps.labs.lab_0.lab_0_utils.source_inventory_summary import (
    render_inventory_summary,
)
from spark_workshop.utils.terminal import terminal_box


def test_sales_enriched_columns_are_silver_schema_order():
    assert SALES_ENRICHED_COLUMNS == (
        "sale_id",
        "sale_date",
        "sale_year_month",
        "vendor_id",
        "vendor_name",
        "vendor_region",
        "product_id",
        "product_name",
        "category_id",
        "customer_id",
        "quantity",
        "unit_price",
        "sale_amount",
    )


def test_terminal_box_renders_wrapped_multiline_content():
    rendered = terminal_box(
        [
            "## Example",
            "",
            "A long classroom line that should wrap inside the terminal box.",
        ],
        width=48,
    )

    assert rendered.startswith("\n╔")
    assert "║ ## Example" in rendered
    assert "A long classroom line" in rendered
    assert rendered.endswith("╝")


def test_lab0_inventory_summary_renders_source_profiles_and_teaching_signal():
    rendered = render_inventory_summary(
        source_profiles={
            "vendors": {
                "rows": 100,
                "files": 16,
                "total_bytes": 18190,
                "min_file_bytes": 1122,
                "avg_file_bytes": 1136.9,
                "max_file_bytes": 1151,
                "columns": 4,
            },
            "sales": {
                "rows": 5_000_000,
                "files": 114,
                "total_bytes": 76_270_652,
                "min_file_bytes": 77_242,
                "avg_file_bytes": 669_040.8,
                "max_file_bytes": 3_814_303,
                "columns": 9,
            },
        },
        relationship_checks={
            "vendor_fk_violations": 0,
            "product_fk_violations": 0,
            "customer_fk_violations": 0,
        },
        imbalance_note={
            "top_vendor_id": 1,
            "top_vendor_rows": 3_500_488,
            "top_vendor_share": 0.7001,
        },
    )

    assert "## LAB 0A SOURCE INVENTORY" in rendered
    assert "vendors rows=100 files=16 total=17.8 KiB" in rendered
    assert "sales rows=5,000,000 files=114 total=72.7 MiB" in rendered
    assert "vendor_fk_violations=0" in rendered
    assert "top_vendor_id=1 rows=3,500,488 share=70.01%" in rendered
