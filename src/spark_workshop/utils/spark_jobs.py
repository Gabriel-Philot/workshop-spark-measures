"""Spark job annotation helpers for readable History Server demos."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator


@contextmanager
def spark_job_description(
    spark_or_context: Any,
    description: str,
    *,
    group_id: str | None = None,
    group_description: str | None = None,
) -> Iterator[None]:
    """Temporarily label Spark jobs triggered inside the context.

    Spark History Server uses this label in the Jobs tab, which makes workshop
    actions easier to read than the default Python/Delta callsite descriptions.
    """

    spark_context = _spark_context(spark_or_context)
    if group_id:
        spark_context.setJobGroup(group_id, group_description or description)
    spark_context.setJobDescription(description)
    try:
        yield
    finally:
        _clear_job_description(spark_context)
        if group_id:
            spark_context.clearJobGroup()


def _spark_context(spark_or_context: Any) -> Any:
    return getattr(spark_or_context, "sparkContext", spark_or_context)


def _clear_job_description(spark_context: Any) -> None:
    if hasattr(spark_context, "setLocalProperty"):
        spark_context.setLocalProperty("spark.job.description", None)
    else:
        spark_context.setJobDescription("")
