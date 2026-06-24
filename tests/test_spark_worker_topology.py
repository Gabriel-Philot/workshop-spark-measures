from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_compose_defines_two_default_workers_and_optional_third_worker():
    compose = yaml.safe_load((ROOT / "build/docker-compose.yml").read_text())
    services = compose["services"]

    assert "spark-worker" not in services
    assert services["spark-worker-1"]["container_name"] == "wsm-spark-worker-1"
    assert services["spark-worker-2"]["container_name"] == "wsm-spark-worker-2"
    assert services["spark-worker-3"]["container_name"] == "wsm-spark-worker-3"
    assert services["spark-worker-3"]["profiles"] == ["three-workers"]

    worker_command = services["spark-worker-1"]["command"]
    assert "${SPARK_WORKER_CORES:-2}" in worker_command
    assert "${SPARK_WORKER_MEMORY:-2g}" in worker_command


def test_makefile_exposes_three_worker_topology_target():
    makefile = (ROOT / "Makefile").read_text()

    assert "compose-three-workers:" in makefile
    assert "--profile three-workers" in makefile
    assert "SPARK_WORKER_EXPECTED_REPLICAS=3 build/scripts/wait-ready.sh" in makefile
    assert "--profile three-workers down --remove-orphans" in makefile


def test_wait_ready_counts_expected_alive_workers_from_spark_master():
    script = (ROOT / "build/scripts/wait-ready.sh").read_text()

    assert "expected_workers=\"${SPARK_WORKER_EXPECTED_REPLICAS:-${SPARK_WORKER_REPLICAS:-2}}\"" in script
    assert "Spark Workers (${expected_workers})" in script
    assert "export SPARK_WORKER_EXPECTED_REPLICAS" in script
    assert "<strong>Workers:<\\/strong>" in script
    assert "test \"${alive:-0}\" -ge \"${SPARK_WORKER_EXPECTED_REPLICAS}\"" in script
