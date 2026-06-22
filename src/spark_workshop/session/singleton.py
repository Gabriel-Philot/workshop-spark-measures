"""Process-local SparkSession singleton for spark-submit workloads."""

from threading import RLock
from typing import Any, Mapping

from spark_workshop.utils import logger


DEFAULT_SPARK_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


class SparkSessionSingleton:
    _instance: Any | None = None
    _app_name: str | None = None
    _lock = RLock()

    @classmethod
    def get_or_create(
        cls,
        app_name: str,
        spark_config: Mapping[str, Any] | None = None,
    ) -> Any:
        with cls._lock:
            if cls._instance is not None:
                if cls._app_name != app_name:
                    logger.warning(
                        "Reusing SparkSession "
                        f"created for app_name={cls._app_name}; requested={app_name}"
                    )
                return cls._instance

            resolved_config = dict(DEFAULT_SPARK_CONFIG)
            resolved_config.update(spark_config or {})
            logger.info(f"Creating SparkSession app_name={app_name}")
            cls._instance = _build_session(app_name, resolved_config)
            cls._app_name = app_name
            return cls._instance

    @classmethod
    def stop(cls) -> None:
        with cls._lock:
            if cls._instance is not None:
                logger.info(f"Stopping SparkSession app_name={cls._app_name}")
                cls._instance.stop()
            cls._instance = None
            cls._app_name = None

    @classmethod
    def is_initialized(cls) -> bool:
        return cls._instance is not None


def _build_session(app_name: str, spark_config: Mapping[str, Any]) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_config.items():
        builder = builder.config(key, _spark_conf_value(value))
    return builder.getOrCreate()


def _spark_conf_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)
