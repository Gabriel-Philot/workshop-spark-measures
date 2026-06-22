from spark_workshop.metrics import SparkMeasureFactory
from spark_workshop.metrics import factory as factory_module


class FakeDelegate:
    def __init__(self):
        self.events = []

    def begin(self):
        self.events.append("begin")

    def end(self):
        self.events.append("end")

    def print_report(self):
        self.events.append("report")

    def aggregate_stagemetrics(self):
        return {"numStages": 1}


def test_factory_wraps_sparkmeasure_delegate(monkeypatch):
    delegate = FakeDelegate()
    monkeypatch.setattr(
        factory_module,
        "_create_delegate",
        lambda collector_type, spark: (delegate, "aggregate_stagemetrics"),
    )

    collector = SparkMeasureFactory.create("stage", object())
    collector.begin()
    collector.end()
    collector.print_report()

    assert collector.aggregate() == {"numStages": 1}
    assert delegate.events == ["begin", "end", "report"]
