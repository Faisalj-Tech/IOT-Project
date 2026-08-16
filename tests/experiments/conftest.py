"""Shared machinery for the Phase 2 reliability experiments."""

import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.conftest import ROOT, compose

CONTAINER_NAMES: dict[str, str | tuple[str, ...]] = {
    "rabbitmq": ("iot-rabbitmq", "iot-rabbitmq2", "iot-rabbitmq3"),
    "influxdb": "iot-influxdb",
    "telegraf": "iot-telegraf",
    "grafana": "iot-grafana",
    "consumer": "iot-consumer",
}

# `name: iot-messaging` in compose.yml prefixes every network Compose creates.
PARTITION_NETWORKS = ("iot-messaging_core", "iot-messaging_edge")


def _entry(service: str) -> tuple[str, ...]:
    entry = CONTAINER_NAMES[service]
    return (entry,) if isinstance(entry, str) else entry


def container_for(service: str, node: int = 1) -> str:
    """Container name for a service, addressed by 1-based node index.

    Single-container services accept node=1 only. Passing node=2 to one of them
    is a bug in the caller, not a request to guess.
    """
    entry = _entry(service)
    if not 1 <= node <= len(entry):
        raise ValueError(
            f"{service} has {len(entry)} container(s); node={node} is out of range"
        )
    return entry[node - 1]


def compose_service_for(service: str, node: int = 1) -> str:
    """Compose service name. Node 1 keeps the bare name so Phase 1/2 configs work."""
    _entry(service)  # validates the service exists
    if node == 1:
        return service
    container_for(service, node)  # validates the index
    return f"{service}{node}"


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


def compose_files() -> tuple[str, ...]:
    """Which compose files the current stack was brought up with. Task 6 makes this
    cluster-aware; until then the stack is always the single-node Phase 1 stack."""
    return ("compose.yml",)


class DockerControl:
    """Stop, kill, partition, and restart compose services, verifying each transition.

    Every infrastructure service carries `restart: unless-stopped`. `docker kill`
    alone is therefore undone by the daemon within seconds, which would turn a
    "Telegraf was dead for 60s" experiment into "Telegraf bounced". kill() clears
    the restart policy first and restore() puts it back.

    Ledgers key on (service, node) so a three-node cluster cannot collapse three
    downed brokers into one entry.
    """

    def __init__(self) -> None:
        self._downed: list[tuple[str, int]] = []
        self._policy_cleared: list[tuple[str, int]] = []
        self._partitioned: list[tuple[str, int]] = []
        self._original_networks: dict[tuple[str, int], set[str]] = {}

    def _files(self, service: str) -> tuple[str, ...]:
        if service == "consumer":
            return CONSUMER_FILES
        return compose_files()

    def is_running(self, service: str, node: int = 1) -> bool:
        proc = _docker("inspect", "--format", "{{.State.Running}}", container_for(service, node))
        return proc.stdout.strip() == "true"

    def _await_state(self, service: str, running: bool, node: int = 1, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_running(service, node) == running:
                return
            time.sleep(0.5)
        state = "running" if running else "stopped"
        raise AssertionError(
            f"{container_for(service, node)} did not reach state {state} within {timeout}s"
        )

    def stop(self, service: str, timeout: int = 10, node: int = 1) -> None:
        """Graceful stop. timeout=0 denies shutdown grace but still sends SIGTERM."""
        compose("stop", "-t", str(timeout), compose_service_for(service, node),
                files=self._files(service))
        self._await_state(service, running=False, node=node)
        if (service, node) not in self._downed:
            self._downed.append((service, node))

    def kill(self, service: str, node: int = 1) -> None:
        """Abrupt SIGKILL that survives the restart policy."""
        container = container_for(service, node)
        _docker("update", "--restart=no", container)
        if (service, node) not in self._policy_cleared:
            self._policy_cleared.append((service, node))
        _docker("kill", container)
        self._await_state(service, running=False, node=node)
        if (service, node) not in self._downed:
            self._downed.append((service, node))

    def start(self, service: str, wait: bool = True, node: int = 1) -> None:
        args = ["up", "-d"]
        if wait:
            args.append("--wait")
        compose(*args, compose_service_for(service, node), files=self._files(service))
        self._await_state(service, running=True, node=node)
        if (service, node) in self._downed:
            self._downed.remove((service, node))

    def partition(self, service: str, node: int = 1) -> None:
        """Detach a container from the shared networks — a real network partition.

        The container keeps running and keeps its disk state; its peers simply
        cannot resolve or reach it. Its published host ports go down with the
        detachment (measured; see spec 4.1), which is why cluster state on a
        partitioned node is read through `docker exec` rather than HTTP.
        """
        import json
        container = container_for(service, node)

        # Record the original networks before disconnecting.
        if (service, node) not in self._original_networks:
            proc = _docker("inspect", "--format", "{{json .NetworkSettings.Networks}}", container)
            self._original_networks[(service, node)] = set(json.loads(proc.stdout).keys())

        # Disconnect from the partition networks.
        for network in PARTITION_NETWORKS:
            try:
                _docker("network", "disconnect", network, container)
            except RuntimeError:
                # Container may not be connected to this network; skip it.
                pass
        if (service, node) not in self._partitioned:
            self._partitioned.append((service, node))

    def heal(self, service: str, node: int = 1) -> None:
        import json
        container = container_for(service, node)

        # Get the original networks (recorded in partition()).
        original_networks = self._original_networks.get((service, node), set())

        # Reconnect only to the networks the container was originally on.
        for network in PARTITION_NETWORKS:
            if network in original_networks:
                try:
                    _docker("network", "connect", network, container)
                except RuntimeError:
                    # Already connected or some other error; skip it.
                    pass

        if (service, node) in self._partitioned:
            self._partitioned.remove((service, node))

    def restore(self) -> None:
        """Bring everything back, reattach every network, re-apply restart policies.

        Runs on every exit path, including assertion failure and interrupt. A
        partition that outlives a crashed test silently poisons every later run
        in the session — a worse failure than the one that caused it.
        """
        for service, node in list(self._partitioned):
            try:
                self.heal(service, node)
            except Exception:
                pass
        for service, node in list(self._downed):
            try:
                self.start(service, node=node)
            except Exception:
                pass
        for service, node in list(self._policy_cleared):
            try:
                _docker("update", "--restart=unless-stopped", container_for(service, node))
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


import asyncio
import os
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor

from sim.devices.runner import run_devices

_EXECUTOR = ThreadPoolExecutor(max_workers=4)


def start_sim(specs, rate_hz: float, duration_s: float, run_id: str,
              max_reconnects: int = 12) -> Future:
    """Run the device simulator on a background thread.

    The experiment body stays synchronous so container manipulation reads as a
    straight-line script. asyncio.run in a worker thread picks up the global
    WindowsSelectorEventLoopPolicy already set by main/conftest.py.
    """

    def _run() -> dict:
        return asyncio.run(
            run_devices(
                specs,
                rate_hz=rate_hz,
                duration_s=duration_s,
                run_id=run_id,
                host="localhost",
                port=1883,
                username=os.environ.get("RABBITMQ_DEVICE_USER", "device"),
                password=os.environ.get("RABBITMQ_DEVICE_PASSWORD", "devicepass"),
                max_reconnects=max_reconnects,
            )
        )

    return _EXECUTOR.submit(_run)


def drain_and_fetch(influx_query, run_id: str, start: str, expected_total: int,
                    timeout_s: float = 180.0, stable_polls_limit: int = 6) -> list[dict]:
    """Poll until the row count settles, then return the rows.

    Two exit conditions, deliberately different. A healthy run exits as soon as it has
    reached the expected total and held steady for three polls. A lossy run never
    reaches the total, so it exits on a longer stall instead — stable_polls_limit
    stable polls (default: six, i.e. 30s with no new rows, which is three of Telegraf's
    10s flush intervals). This limit is parameterizable to handle slower post-recovery
    drains that exhibit temporary plateaus mid-backlog. Without that second condition
    an experiment that actually lost messages would burn the whole timeout before
    reporting the very number it was run to measure.
    """
    from tests.conftest import fetch_seqs

    deadline = time.time() + timeout_s
    rows: list[dict] = []
    last_count = -1
    stable_polls = 0
    while time.time() < deadline:
        rows = fetch_seqs(influx_query, run_id, start=start)
        if len(rows) == last_count:
            stable_polls += 1
        else:
            stable_polls = 0
            last_count = len(rows)
        if stable_polls >= 3 and len(rows) >= expected_total:
            break
        if stable_polls >= stable_polls_limit:
            break
        time.sleep(5)
    return rows


def sequence_report(rows: list[dict], published: dict[str, int]) -> dict:
    """Compare observed sequence numbers against what the simulator says it sent.

    Gaps are the loss proof (ADR-0001). Duplicates are counted separately because
    QoS 1 is at-least-once: a publish that lands but loses its PUBACK is retried with
    the same seq, and since build_payload regenerates `ts` and `seq` is a field rather
    than a tag, both points persist at distinct timestamps.
    """
    gaps: dict[str, list[int]] = {}
    duplicates: dict[str, int] = {}
    for device, count in published.items():
        observed = [int(r["_value"]) for r in rows if r.get("device") == device]
        counts = Counter(observed)
        missing = [n for n in range(1, count + 1) if n not in counts]
        extra = sum(v - 1 for v in counts.values() if v > 1)
        if missing:
            gaps[device] = missing
        if extra:
            duplicates[device] = extra
    return {
        "total_rows": len(rows),
        "per_device": {d: sum(1 for r in rows if r.get("device") == d) for d in published},
        "gaps": gaps,
        "duplicates": duplicates,
    }


import aiomqtt


def publish_raw(topic: str, payload: bytes, count: int = 1) -> None:
    """Publish arbitrary bytes at QoS 1, bypassing build_payload's contract.

    Experiment D needs payloads the device simulator would never produce.
    """

    async def _publish() -> None:
        async with aiomqtt.Client(
            hostname="localhost",
            port=1883,
            username=os.environ.get("RABBITMQ_DEVICE_USER", "device"),
            password=os.environ.get("RABBITMQ_DEVICE_PASSWORD", "devicepass"),
            identifier=f"rawpub-{int(time.time())}",
        ) as client:
            for _ in range(count):
                await client.publish(topic, payload=payload, qos=1)

    asyncio.run(_publish())


CONSUMER_FILES = ("compose.yml", "compose.consumer.yml")


@pytest.fixture
def consumer_stack(stack, docker_control):
    """Swap Telegraf out for the ack-after-write consumer, and swap back afterwards.

    Competing consumers on one queue would split the stream, so the arms must run in
    separate runs rather than side by side.
    """
    docker_control.stop("telegraf", timeout=10)
    compose("up", "-d", "--build", "consumer", files=CONSUMER_FILES)
    deadline = time.time() + 60
    while time.time() < deadline:
        if docker_control.is_running("consumer"):
            break
        time.sleep(1)
    else:
        raise AssertionError("consumer container did not start")
    time.sleep(5)  # let the AMQP consumer attach before the run begins
    try:
        yield
    finally:
        try:
            compose("rm", "-sf", "consumer", files=CONSUMER_FILES)
        except Exception:
            pass
        docker_control.start("telegraf")
