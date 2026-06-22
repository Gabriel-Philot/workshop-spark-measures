from spark_workshop.artifacts import read_artifact, write_artifact
from spark_workshop.config import ArtifactSpec


class Fluent:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def mode(self, value):
        self.calls.append(("mode", value))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self

    def partitionBy(self, *columns):
        self.calls.append(("partitionBy", columns))
        return self

    def load(self, path):
        self.calls.append(("load", path))
        return "dataframe"

    def save(self, path):
        self.calls.append(("save", path))


def test_read_artifact_uses_named_spec():
    reader = Fluent()
    spark = type("FakeSpark", (), {"read": reader})()
    spec = ArtifactSpec(
        path="s3a://tests/input", format="delta", options={"mergeSchema": True}
    )

    assert read_artifact(spark, spec) == "dataframe"
    assert reader.calls == [
        ("format", "delta"),
        ("option", "mergeSchema", "true"),
        ("load", "s3a://tests/input"),
    ]


def test_write_artifact_applies_mode_options_and_partitions():
    writer = Fluent()
    dataframe = type("FakeDataFrame", (), {"write": writer})()
    spec = ArtifactSpec(
        path="s3a://lakehouse/bronze/output",
        format="delta",
        mode="append",
        options={"overwriteSchema": False},
        partition_by=("run_date",),
    )

    write_artifact(dataframe, spec)

    assert writer.calls == [
        ("format", "delta"),
        ("mode", "append"),
        ("option", "overwriteSchema", "false"),
        ("partitionBy", ("run_date",)),
        ("save", "s3a://lakehouse/bronze/output"),
    ]
