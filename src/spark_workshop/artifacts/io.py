"""Named artifact IO helpers used by experiment contexts."""

from typing import Any

from spark_workshop.config import ArtifactSpec
from spark_workshop.utils import logger


def read_artifact(spark: Any, spec: ArtifactSpec) -> Any:
    reader = spark.read.format(spec.format)
    for key, value in spec.options.items():
        reader = reader.option(key, _option_value(value))
    logger.info(f"Reading artifact format={spec.format} path={spec.path}")
    return reader.load(spec.path)


def write_artifact(dataframe: Any, spec: ArtifactSpec) -> None:
    writer = dataframe.write.format(spec.format).mode(spec.mode)
    for key, value in spec.options.items():
        writer = writer.option(key, _option_value(value))
    if spec.partition_by:
        writer = writer.partitionBy(*spec.partition_by)
    logger.info(
        f"Writing artifact format={spec.format} mode={spec.mode} path={spec.path}"
    )
    writer.save(spec.path)


def _option_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)
