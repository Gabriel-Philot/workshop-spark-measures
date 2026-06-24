from workshop_generator.cli import _format_volume_metric_line, _volume_metric_lines


def test_format_volume_metric_line():
    line = _format_volume_metric_line(
        table="sales",
        rows=5000000,
        file_stats={
            "file_count": 114,
            "total_bytes": 762602154,
            "min_file_bytes": 78078,
            "avg_file_bytes": 6689492.5789,
            "max_file_bytes": 38138230,
        },
    )

    assert line == (
        "GENERATOR_VOLUME table=sales rows=5000000 files=114 "
        "total_bytes=762602154 min_file_bytes=78078 "
        "avg_file_bytes=6689492.6 max_file_bytes=38138230"
    )


def test_volume_metric_lines_are_emitted_in_table_order():
    lines = _volume_metric_lines(
        {
            "counts": {"sales": 10, "vendors": 2},
            "table_file_stats": {
                "sales": {"file_count": 1, "total_bytes": 100},
                "vendors": {"file_count": 1, "total_bytes": 20},
            },
        }
    )

    assert lines == [
        "GENERATOR_VOLUME table=vendors rows=2 files=1 total_bytes=20 "
        "min_file_bytes=0 avg_file_bytes=0.0 max_file_bytes=0",
        "GENERATOR_VOLUME table=sales rows=10 files=1 total_bytes=100 "
        "min_file_bytes=0 avg_file_bytes=0.0 max_file_bytes=0",
    ]
