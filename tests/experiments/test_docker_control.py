"""The fixture that breaks containers must prove the container actually broke."""

import subprocess

import pytest

from tests.experiments.conftest import compose_service_for, container_for

pytestmark = [pytest.mark.stack, pytest.mark.experiment]


def test_stop_makes_the_service_not_running_and_restore_brings_it_back(docker_control):
    assert docker_control.is_running("grafana")

    docker_control.stop("grafana", timeout=0)
    assert not docker_control.is_running("grafana"), "grafana still running after stop"

    docker_control.start("grafana")
    assert docker_control.is_running("grafana")


def test_kill_survives_the_restart_policy(docker_control):
    """restart: unless-stopped would resurrect a plain `docker kill` within seconds."""
    import time

    assert docker_control.is_running("grafana")
    docker_control.kill("grafana")
    time.sleep(8)
    assert not docker_control.is_running("grafana"), (
        "grafana was resurrected by the restart policy; kill() did not clear it"
    )
    docker_control.start("grafana")
    assert docker_control.is_running("grafana")

    docker_control.restore()
    proc = subprocess.run(
        ["docker", "inspect", "--format", "{{.HostConfig.RestartPolicy.Name}}", "iot-grafana"],
        capture_output=True, text=True,
    )
    assert proc.stdout.strip() == "unless-stopped", (
        "restore() did not re-apply the restart policy after kill() cleared it"
    )


def test_restore_recovers_a_service_left_down(docker_control):
    docker_control.stop("grafana", timeout=0)
    assert not docker_control.is_running("grafana")
    docker_control.restore()
    assert docker_control.is_running("grafana")


def test_container_for_resolves_single_container_services():
    assert container_for("grafana") == "iot-grafana"
    assert container_for("influxdb", node=1) == "iot-influxdb"


def test_container_for_resolves_cluster_nodes():
    assert container_for("rabbitmq") == "iot-rabbitmq"
    assert container_for("rabbitmq", node=1) == "iot-rabbitmq"
    assert container_for("rabbitmq", node=2) == "iot-rabbitmq2"
    assert container_for("rabbitmq", node=3) == "iot-rabbitmq3"


def test_container_for_rejects_a_node_index_a_service_does_not_have():
    with pytest.raises(ValueError):
        container_for("grafana", node=2)
    with pytest.raises(ValueError):
        container_for("rabbitmq", node=4)


def test_compose_service_for_maps_node_index_to_service_name():
    assert compose_service_for("rabbitmq") == "rabbitmq"
    assert compose_service_for("rabbitmq", node=2) == "rabbitmq2"
    assert compose_service_for("telegraf") == "telegraf"


def test_partition_detaches_the_container_and_heal_reattaches_it(docker_control):
    """Grafana stands in for a broker node: same mechanism, nothing else depends on it."""
    import json

    def networks() -> set[str]:
        proc = subprocess.run(
            ["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", "iot-grafana"],
            capture_output=True, text=True, check=True,
        )
        return set(json.loads(proc.stdout))

    before = networks()
    assert "iot-messaging_core" in before

    docker_control.partition("grafana")
    assert "iot-messaging_core" not in networks(), "partition() left the container attached"

    docker_control.heal("grafana")
    assert networks() == before, "heal() did not restore the original attachments"


def test_restore_heals_a_partition_left_behind(docker_control):
    import json

    docker_control.partition("grafana")
    docker_control.restore()
    proc = subprocess.run(
        ["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", "iot-grafana"],
        capture_output=True, text=True, check=True,
    )
    assert "iot-messaging_core" in json.loads(proc.stdout), (
        "restore() left grafana partitioned; a leaked partition poisons every later run"
    )
