from apps.labs.lab_1.lab_1_utils.transformations import (
    SALES_ENRICHED_COLUMNS,
    TOP_SALES_GLOBAL_SORT_COLUMNS,
    AUDIT_BUCKETS,
    AUDIT_OUTLIER_COLUMNS,
    FIXED_AUDIT_PARTITIONS,
    FIXED_AUDIT_SALT_BUCKETS,
    HEAVY_AUDIT_REPEAT_FACTOR,
    SLOW_AUDIT_BUCKET,
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


def test_lab1_random_task_outlier_constants_are_didactic():
    assert AUDIT_BUCKETS == 16
    assert 0 <= SLOW_AUDIT_BUCKET < AUDIT_BUCKETS
    assert FIXED_AUDIT_SALT_BUCKETS == 8
    assert FIXED_AUDIT_PARTITIONS == 64
    assert HEAVY_AUDIT_REPEAT_FACTOR == 512
    assert AUDIT_OUTLIER_COLUMNS == SALES_ENRICHED_COLUMNS + (
        "audit_bucket",
        "audit_fingerprint",
    )
