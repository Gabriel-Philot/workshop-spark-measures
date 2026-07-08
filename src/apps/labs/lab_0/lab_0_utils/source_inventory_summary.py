"""Terminal summary helpers for Lab 0 source inventory."""

from __future__ import annotations

from typing import Mapping

from spark_workshop.utils import terminal_box


SOURCE_TABLE_ORDER = ("vendors", "products", "customers", "sales")


def render_inventory_summary(
    *,
    source_profiles: Mapping[str, Mapping[str, int | float]],
    relationship_checks: Mapping[str, int],
    imbalance_note: Mapping[str, int | float],
) -> str:
    """Render the Lab 0A source inventory as a readable final log block."""

    lines = [
        "## LAB 0A SOURCE INVENTORY",
        "",
        "### Bronze source volumes",
        *_source_profile_lines(source_profiles),
        "",
        "### Relationship readiness",
        (
            "vendor_fk_violations="
            f"{relationship_checks.get('vendor_fk_violations', 0)} "
            "product_fk_violations="
            f"{relationship_checks.get('product_fk_violations', 0)} "
            "customer_fk_violations="
            f"{relationship_checks.get('customer_fk_violations', 0)}"
        ),
        "",
        "### Teaching signal",
        (
            "sales vendor imbalance: "
            f"top_vendor_id={_format_number(imbalance_note.get('top_vendor_id', 0))} "
            f"rows={_format_number(imbalance_note.get('top_vendor_rows', 0))} "
            f"share={_format_percent(imbalance_note.get('top_vendor_share', 0.0))}"
        ),
        "",
        "### Classroom next step",
        "Run Lab 0B for the native sparkMeasure API, then Lab 0C for the observed comparison.",
    ]
    return terminal_box(lines, width=104)


def _source_profile_lines(
    source_profiles: Mapping[str, Mapping[str, int | float]],
) -> list[str]:
    ordered_tables = [
        *[table for table in SOURCE_TABLE_ORDER if table in source_profiles],
        *[table for table in source_profiles if table not in SOURCE_TABLE_ORDER],
    ]
    return [
        (
            f"{table} "
            f"rows={_format_number(profile.get('rows', 0))} "
            f"files={_format_number(profile.get('files', 0))} "
            f"total={_format_bytes(profile.get('total_bytes', 0))} "
            f"avg_file={_format_bytes(profile.get('avg_file_bytes', 0))} "
            f"columns={_format_number(profile.get('columns', 0))}"
        )
        for table in ordered_tables
        for profile in [source_profiles[table]]
    ]


def _format_number(value: int | float | object) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if numeric.is_integer():
        return f"{int(numeric):,}"
    return f"{numeric:,.1f}"


def _format_percent(value: int | float | object) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    return f"{numeric * 100:.2f}%"


def _format_bytes(value: int | float | object) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = 0.0
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = max(parsed, 0.0)
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    if unit == "B":
        return f"{int(size):,} {unit}"
    return f"{size:,.1f} {unit}"
