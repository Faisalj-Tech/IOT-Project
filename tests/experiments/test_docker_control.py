"""The fixture that breaks containers must prove the container actually broke."""

import subprocess

import pytest

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
