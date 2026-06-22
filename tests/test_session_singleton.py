from types import SimpleNamespace

import pytest

from spark_workshop.session import SparkSessionSingleton
from spark_workshop.session import singleton as singleton_module


@pytest.fixture(autouse=True)
def reset_singleton_state():
    SparkSessionSingleton._instance = None
    SparkSessionSingleton._app_name = None
    yield
    SparkSessionSingleton._instance = None
    SparkSessionSingleton._app_name = None


def test_get_or_create_builds_once_and_reuses_same_session(monkeypatch):
    built = []
    session = SimpleNamespace(stop=lambda: None)

    def fake_build(app_name, spark_config):
        built.append((app_name, spark_config))
        return session

    monkeypatch.setattr(singleton_module, "_build_session", fake_build)

    first = SparkSessionSingleton.get_or_create("experiment", {"custom": True})
    second = SparkSessionSingleton.get_or_create("experiment", {"other": "ignored"})

    assert first is session
    assert second is session
    assert len(built) == 1
    assert built[0][1]["custom"] is True
    assert "spark.sql.extensions" in built[0][1]


def test_stop_releases_cached_session(monkeypatch):
    stopped = []
    session = SimpleNamespace(stop=lambda: stopped.append(True))
    monkeypatch.setattr(singleton_module, "_build_session", lambda *_: session)
    SparkSessionSingleton.get_or_create("experiment")

    SparkSessionSingleton.stop()

    assert stopped == [True]
    assert SparkSessionSingleton.is_initialized() is False
