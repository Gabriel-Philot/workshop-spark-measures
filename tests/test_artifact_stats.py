from spark_workshop.artifacts import file_stats_from_sizes


def test_file_stats_from_sizes_empty():
    stats = file_stats_from_sizes([])

    assert stats.file_count == 0
    assert stats.total_bytes == 0
    assert stats.min_file_bytes == 0
    assert stats.avg_file_bytes == 0.0
    assert stats.max_file_bytes == 0


def test_file_stats_from_sizes_calculates_distribution():
    stats = file_stats_from_sizes([100, 250, 650])

    assert stats.file_count == 3
    assert stats.total_bytes == 1000
    assert stats.min_file_bytes == 100
    assert stats.avg_file_bytes == 1000 / 3
    assert stats.max_file_bytes == 650
    assert stats.as_dict() == {
        "file_count": 3,
        "total_bytes": 1000,
        "min_file_bytes": 100,
        "avg_file_bytes": 1000 / 3,
        "max_file_bytes": 650,
    }
