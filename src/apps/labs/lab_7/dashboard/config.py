"""Configuration helpers for the Lab 7 Streamlit dashboard.

This module intentionally has no Streamlit or DuckDB dependency so it can be
unit-tested with the repository's normal lightweight test environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


DEFAULT_METRICS_URI = "s3://observability/lab7/daily_backfill_stage_metrics"
DEFAULT_MINIO_ENDPOINT = "http://minio:9000"
DEFAULT_REGION = "us-east-1"


@dataclass(frozen=True)
class DashboardConfig:
    """Runtime settings required to query Lab 7 metrics from MinIO."""

    metrics_uri: str
    minio_endpoint: str
    access_key: str
    secret_key: str
    region: str

    @property
    def duckdb_endpoint(self) -> str:
        """Return the endpoint format expected by DuckDB S3 secrets."""

        endpoint = self.minio_endpoint.removeprefix("http://").removeprefix("https://")
        return endpoint.rstrip("/")


def load_dashboard_config(env: Mapping[str, str] | None = None) -> DashboardConfig:
    """Load dashboard settings from environment variables."""

    source = env or os.environ
    return DashboardConfig(
        metrics_uri=source.get("LAB7_DASHBOARD_METRICS_URI", DEFAULT_METRICS_URI),
        minio_endpoint=source.get("LAB7_DASHBOARD_MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
        access_key=source.get("MINIO_ROOT_USER", source.get("AWS_ACCESS_KEY_ID", "sparkworkshop")),
        secret_key=source.get(
            "MINIO_ROOT_PASSWORD",
            source.get("AWS_SECRET_ACCESS_KEY", "sparkworkshop123"),
        ),
        region=source.get("AWS_REGION", source.get("AWS_DEFAULT_REGION", DEFAULT_REGION)),
    )
