"""Physical file statistics for Spark artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class FileStats:
    """Summary of physical data file sizes in bytes."""

    file_count: int
    total_bytes: int
    min_file_bytes: int
    avg_file_bytes: float
    max_file_bytes: int

    def as_dict(self) -> dict[str, int | float]:
        return {
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "min_file_bytes": self.min_file_bytes,
            "avg_file_bytes": self.avg_file_bytes,
            "max_file_bytes": self.max_file_bytes,
        }


def data_file_stats_for_dataframe(dataframe: Any) -> FileStats:
    """Return physical file-size stats for the files backing a Spark DataFrame."""

    return data_file_stats_for_files(
        spark=dataframe.sparkSession,
        files=dataframe.inputFiles(),
    )


def data_file_stats_for_files(spark: Any, files: Iterable[str]) -> FileStats:
    """Return physical file-size stats for paths reachable by Spark."""

    return file_stats_from_sizes(_file_sizes(spark, files))


def file_stats_from_sizes(sizes: Iterable[int]) -> FileStats:
    """Return file-size stats from an iterable of byte sizes."""

    normalized = [int(size) for size in sizes]
    if not normalized:
        return FileStats(
            file_count=0,
            total_bytes=0,
            min_file_bytes=0,
            avg_file_bytes=0.0,
            max_file_bytes=0,
        )

    total = sum(normalized)
    return FileStats(
        file_count=len(normalized),
        total_bytes=int(total),
        min_file_bytes=int(min(normalized)),
        avg_file_bytes=float(total / len(normalized)),
        max_file_bytes=int(max(normalized)),
    )


def _file_sizes(spark: Any, files: Iterable[str]) -> list[int]:
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    sizes: list[int] = []
    for file_path in files:
        path = jvm.org.apache.hadoop.fs.Path(file_path)
        fs = path.getFileSystem(hconf)
        if fs.exists(path):
            sizes.append(int(fs.getFileStatus(path).getLen()))
    return sizes
