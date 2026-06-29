from apps.labs.lab_0.lab_0_utils.transformations import SALES_ENRICHED_COLUMNS


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
