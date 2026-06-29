from types import SimpleNamespace

from spark_workshop.utils import spark_job_description


class FakeSparkContext:
    def __init__(self):
        self.calls = []

    def setJobDescription(self, description):
        self.calls.append(("setJobDescription", description))

    def setLocalProperty(self, key, value):
        self.calls.append(("setLocalProperty", key, value))

    def setJobGroup(self, group_id, description):
        self.calls.append(("setJobGroup", group_id, description))

    def clearJobGroup(self):
        self.calls.append(("clearJobGroup",))


def test_spark_job_description_labels_and_clears_description():
    spark_context = FakeSparkContext()
    spark = SimpleNamespace(sparkContext=spark_context)

    with spark_job_description(spark, "LAB0 | action"):
        spark_context.calls.append(("action",))

    assert spark_context.calls == [
        ("setJobDescription", "LAB0 | action"),
        ("action",),
        ("setLocalProperty", "spark.job.description", None),
    ]


def test_spark_job_description_can_group_related_actions():
    spark_context = FakeSparkContext()

    with spark_job_description(
        spark_context,
        "LAB0 | grouped_action",
        group_id="lab0-group",
        group_description="Lab 0 grouped action",
    ):
        spark_context.calls.append(("action",))

    assert spark_context.calls == [
        ("setJobGroup", "lab0-group", "Lab 0 grouped action"),
        ("setJobDescription", "LAB0 | grouped_action"),
        ("action",),
        ("setLocalProperty", "spark.job.description", None),
        ("clearJobGroup",),
    ]
