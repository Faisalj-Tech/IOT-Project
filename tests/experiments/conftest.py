"""Shared machinery for the Phase 2 reliability experiments."""

import json
import subprocess
import time

import pytest

from tests.conftest import ROOT, compose

CONTAINER_NAMES = {
    "rabbitmq": "iot-rabbitmq",
    "influxdb": "iot-influxdb",
    "telegraf": "iot-telegraf",
    "grafana": "iot-grafana",
    "consumer": "iot-consumer",
}


def _docker(*args: str) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        ["docker", *args], cwd=ROOT, check=False, capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"docker {' '.join(args)} failed with exit {proc.returncode}\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    return proc


class DockerControl:
    """Stop, kill, and restart compose services, verifying each state transition.

    Every infrastructure service carries `restart: unless-stopped`. `docker kill`
    alone is therefore undone by the daemon within seconds, which would turn a
    "Telegraf was dead for 60s" experiment into "Telegraf bounced". kill() clears
    the restart policy first and restore() puts it back.
    """

    def __init__(self) -> None:
        self._downed: list[str] = []
        self._policy_cleared: list[str] = []

    def is_running(self, service: str) -> bool:
        container = CONTAINER_NAMES[service]
        proc = _docker("inspect", "--format", "{{.State.Running}}", container)
        return proc.stdout.strip() == "true"

    def _await_state(self, service: str, running: bool, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_running(service) == running:
                return
            time.sleep(0.5)
        state = "running" if running else "stopped"
        raise AssertionError(f"{service} did not reach state {state} within {timeout}s")

    def stop(self, service: str, timeout: int = 10) -> None:
        """Graceful stop. timeout=0 denies shutdown grace but still sends SIGTERM."""
        compose("stop", "-t", str(timeout), service)
        self._await_state(service, running=False)
        if service not in self._downed:
            self._downed.append(service)

    def kill(self, service: str) -> None:
        """Abrupt SIGKILL that survives the restart policy."""
        container = CONTAINER_NAMES[service]
        _docker("update", "--restart=no", container)
        if service not in self._policy_cleared:
            self._policy_cleared.append(service)
        _docker("kill", container)
        self._await_state(service, running=False)
        if service not in self._downed:
            self._downed.append(service)

    def start(self, service: str, wait: bool = True) -> None:
        args = ["up", "-d"]
        if wait:
            args.append("--wait")
        compose(*args, service)
        self._await_state(service, running=True)
        if service in self._downed:
            self._downed.remove(service)

    def restore(self) -> None:
        """Bring everything back and re-apply cleared restart policies.

        Runs on every exit path, including assertion failure and interrupt, so a
        failed experiment cannot leave a service down for the next one.
        """
        for service in list(self._downed):
            try:
                self.start(service)
            except Exception:
                pass
        for service in list(self._policy_cleared):
            try:
                _docker("update", "--restart=unless-stopped", CONTAINER_NAMES[service])
            except Exception:
                pass
        self._policy_cleared.clear()


@pytest.fixture
def docker_control(stack):
    control = DockerControl()
    try:
        yield control
    finally:
        control.restore()
