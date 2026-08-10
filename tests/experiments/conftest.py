"""Shared machinery for the Phase 2 reliability experiments."""

import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

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


RESULTS_DIR = ROOT / "docs" / "results"
POLL_INTERVAL_S = 2.0


class GaugeRecorder:
    """Poll RabbitMQ management gauges on a background thread.

    messages_ready and messages_unacknowledged are gauges, not counters, which is
    why ADR-0001's ban does not apply: they are recorded as evidence of buffering
    and as the direct read on Telegraf's in-flight batch size. They never take part
    in a delivery or loss assertion.

    Poll failures are recorded as error samples rather than raised. The broker being
    unreachable is a legitimate experimental condition, not a harness bug.
    """

    def __init__(self, rabbit_get) -> None:
        self._rabbit_get = rabbit_get
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.samples: list[dict] = []
        self.marks: list[dict] = []

    def _sample_once(self) -> dict:
        now = time.time()
        try:
            telemetry = self._rabbit_get("/queues/%2F/telemetry.q").json()
            dlq = self._rabbit_get("/queues/%2F/dlq").json()
            nodes = self._rabbit_get("/nodes").json()
            node = nodes[0] if nodes else {}
            return {
                "t": now,
                "telemetry_ready": int(telemetry.get("messages_ready", 0)),
                "telemetry_unacked": int(telemetry.get("messages_unacknowledged", 0)),
                "dlq_messages": int(dlq.get("messages", 0)),
                "alarms": {
                    "mem": bool(node.get("mem_alarm", False)),
                    "disk": bool(node.get("disk_free_alarm", False)),
                },
            }
        except Exception as exc:
            return {"t": now, "error": f"{type(exc).__name__}: {exc}"}

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.samples.append(self._sample_once())
            self._stop.wait(POLL_INTERVAL_S)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)

    def mark(self, label: str) -> None:
        self.marks.append({"label": label, "t": time.time()})

    def peak(self, key: str) -> float:
        values = [s[key] for s in self.samples if key in s]
        return max(values) if values else 0

    def latest(self, key: str) -> int:
        """Most recent successful reading of a gauge.

        Distinct from peak() and not interchangeable with it: "how deep was Telegraf's
        in-flight batch at the moment we killed it" is a latest(), and reporting a
        run-wide peak under that name would overstate it.
        """
        for sample in reversed(self.samples):
            if key in sample:
                return sample[key]
        return 0

    def timeline(self) -> dict:
        return {"samples": self.samples, "marks": self.marks}


def write_result(experiment: str, payload: dict) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = payload.get("run_id", "norun")
    path = RESULTS_DIR / f"{experiment}-{run_id}.json"
    body = {
        "experiment": experiment,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    path.write_text(json.dumps(body, indent=2), encoding="utf-8")
    return path


@pytest.fixture
def gauge_recorder(stack, rabbit_get):
    recorder = GaugeRecorder(rabbit_get)
    recorder.start()
    try:
        yield recorder
    finally:
        recorder.stop()


@pytest.fixture
def results_dir():
    """Returns a callable that writes a result JSON.

    Writing happens through this fixture rather than at the end of a test body so
    that a failing experiment still leaves its evidence behind: an experiment that
    crashed with a full timeseries is a finding, one with no data is a dead end.
    """
    return write_result
