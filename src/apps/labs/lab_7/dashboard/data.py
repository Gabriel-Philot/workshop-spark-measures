"""DuckDB data access for the Lab 7 dashboard."""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - exercised by Streamlit runtime path
    from .config import DashboardConfig
except ImportError:  # pragma: no cover - exercised when app.py runs as script
    from config import DashboardConfig


def sql_literal(value: str) -> str:
    """Return a single-quoted SQL literal with defensive escaping."""

    return "'" + value.replace("'", "''") + "'"


def build_duckdb_secret_sql(config: DashboardConfig) -> str:
    """Build the DuckDB S3 secret statement used for MinIO access."""

    return f"""
CREATE OR REPLACE SECRET lab7_minio (
    TYPE s3,
    PROVIDER config,
    KEY_ID {sql_literal(config.access_key)},
    SECRET {sql_literal(config.secret_key)},
    REGION {sql_literal(config.region)},
    ENDPOINT {sql_literal(config.duckdb_endpoint)},
    URL_STYLE 'path',
    USE_SSL false
);
""".strip()


def build_delta_scan_sql(metrics_uri: str) -> str:
    """Build the Delta scan SQL for the Lab 7 StageMetrics table."""

    if not metrics_uri.startswith("s3://"):
        raise ValueError(f"Lab 7 expects an s3:// metrics URI, got: {metrics_uri}")
    return f"SELECT * FROM delta_scan({sql_literal(metrics_uri)})"


def build_metrics_query(metrics_uri: str) -> str:
    """Build the dashboard query over the persisted Lab 7 metrics table."""

    scan = build_delta_scan_sql(metrics_uri)
    return f"""
WITH metrics AS (
    {scan}
),
typed AS (
    SELECT
        CAST(run_id AS VARCHAR) AS run_id,
        CAST(date_run_id AS VARCHAR) AS date_run_id,
        CAST(filter_strategy AS VARCHAR) AS filter_strategy,
        CAST(processing_date AS DATE) AS processing_date,
        CAST(spike_label AS VARCHAR) AS spike_label,
        CAST(volume_multiplier AS DOUBLE) AS volume_multiplier,
        CAST(source_rows_for_date AS DOUBLE) AS source_rows_for_date,
        CAST(records_read AS DOUBLE) AS records_read,
        CAST(executor_run_time_ms AS DOUBLE) AS executor_run_time_ms,
        CAST(shuffle_bytes_written AS DOUBLE) AS shuffle_bytes_written,
        CAST(shuffle_bytes_read AS DOUBLE) AS shuffle_bytes_read,
        CAST(input_bytes AS DOUBLE) AS input_bytes,
        CAST(num_stages AS DOUBLE) AS num_stages,
        CAST(num_tasks AS DOUBLE) AS num_tasks,
        CAST(memory_bytes_spilled AS DOUBLE) AS memory_bytes_spilled,
        CAST(disk_bytes_spilled AS DOUBLE) AS disk_bytes_spilled,
        CAST(jvm_gc_time_ms AS DOUBLE) AS jvm_gc_time_ms,
        CAST(runtime_per_million_rows AS DOUBLE) AS runtime_per_million_rows,
        CAST(shuffle_per_million_rows AS DOUBLE) AS shuffle_per_million_rows,
        CAST(input_bytes_per_million_rows AS DOUBLE) AS input_bytes_per_million_rows,
        CAST(tasks_per_million_rows AS DOUBLE) AS tasks_per_million_rows,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM metrics
)
SELECT
    *,
    CASE
        WHEN source_rows_for_date > 0 THEN records_read / source_rows_for_date
        ELSE NULL
    END AS records_read_to_expected_ratio,
    CASE
        WHEN source_rows_for_date > 0 THEN shuffle_bytes_written / source_rows_for_date
        ELSE NULL
    END AS shuffle_written_per_source_row
FROM typed
ORDER BY processing_date
""".strip()


def configure_duckdb_connection(connection: Any, config: DashboardConfig) -> None:
    """Load required DuckDB extensions and configure MinIO credentials."""

    for extension in ("httpfs", "delta"):
        try:
            connection.execute(f"LOAD {extension}")
        except Exception:
            connection.execute(f"INSTALL {extension}")
            connection.execute(f"LOAD {extension}")
    connection.execute(build_duckdb_secret_sql(config))


def load_metrics_dataframe(config: DashboardConfig) -> Any:
    """Read Lab 7 metrics into a pandas DataFrame through DuckDB."""

    import duckdb

    connection = duckdb.connect(":memory:")
    try:
        configure_duckdb_connection(connection, config)
        return connection.execute(build_metrics_query(config.metrics_uri)).fetch_df()
    finally:
        connection.close()
