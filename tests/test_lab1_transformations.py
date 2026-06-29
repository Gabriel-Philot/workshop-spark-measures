from apps.labs.lab_1.lab_1_utils.transformations import (
    SALES_ENRICHED_COLUMNS,
    TOP_SALES_GLOBAL_SORT_COLUMNS,
)


def test_lab1_sales_enriched_columns_are_local_to_lab():
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


def test_lab1_global_sort_preserves_enriched_schema():
    assert TOP_SALES_GLOBAL_SORT_COLUMNS == SALES_ENRICHED_COLUMNS
