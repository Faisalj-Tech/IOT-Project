"""Shared machinery for the Phase 2 reliability experiments."""

import json
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.conftest import ROOT, cluster_mode, compose, compose_files

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
        """Reattach a partitioned container, and verify it actually reattached.

        The connect failures below were previously swallowed and the entry
        dropped from _partitioned regardless, which left restore() with nothing
        to retry and the container detached for the rest of the session
        (audit finding H-4).
        """
        import json
        container = container_for(service, node)
        original_networks = self._original_networks.get((service, node), set())
        wanted = {n for n in PARTITION_NETWORKS if n in original_networks}

        failures: list[str] = []
        for network in wanted:
            try:
                _docker("network", "connect", network, container)
            except RuntimeError as exc:
                failures.append(f"{network}: {exc}")

        proc = _docker("inspect", "--format", "{{json .NetworkSettings.Networks}}", container)
        attached = set(json.loads(proc.stdout).keys())
        missing = wanted - attached
        if missing:
            raise RuntimeError(
                f"heal() left {container} detached from {sorted(missing)}; "
                f"connect errors: {failures or ['none reported']}. Left in "
                f"_partitioned so restore() retries."
            )

        if (service, node) in self._partitioned:
            self._partitioned.remove((service, node))

    def restore(self) -> None:
        """Bring everything back, reattach every network, re-apply restart policies.

        Runs on every exit path, including assertion failure and interrupt. A
        partition that outlives a crashed test silently poisons every later run
        in the session — a worse failure than the one that caused it.
        """
        heal_failure: Exception | None = None
        for service, node in list(self._partitioned):
            try:
                self.heal(service, node)
            except Exception as exc:
                if heal_failure is None:
                    heal_failure = exc
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
        if heal_failure is not None:
            raise heal_failure


@pytest.fixture
def docker_control(stack):
    control = DockerControl()
    try:
        yield control
    finally:
        control.restore()


RESULTS_DIR = ROOT / "docs" / "results"
POLL_INTERVAL_S = 2.0
_EXEC_TIMEOUT_S = 8.0


def _exec_json(container: str, *args: str) -> object:
    """Run a RabbitMQ CLI tool inside a container and parse its JSON.

    Verified against RabbitMQ 4.3.4:
      rabbitmq-diagnostics -q --formatter json cluster_status
      rabbitmq-queues      -q --formatter json quorum_status <queue>
      rabbitmqctl          -q --formatter json list_queues name messages_ready messages_unacknowledged

    Note list_queues lives on rabbitmqctl, not rabbitmq-queues, and there is no
    list_node_alarms subcommand — alarms come out of cluster_status.
    """
    proc = subprocess.run(
        ["docker", "exec", container, *args],
        cwd=ROOT, check=True, capture_output=True, text=True, timeout=_EXEC_TIMEOUT_S,
    )
    return json.loads(proc.stdout)


# Raft states that mean "this peer is reachable from the node we asked".
# CONFIRMED against a live partition (plan Task 11 Step 2): a detached rabbit3
# reported Raft State "unknown", outside this set, so the discrimination holds
# on RabbitMQ 4.3.4.
# "candidate" is deliberately left out — nothing today asserts on the exec-path
# online field during a leader election, so this omission is unexercised. A
# future consumer that does should treat that as a choice, not an oversight.
LIVE_RAFT_STATES = frozenset({"leader", "follower"})


def _online_from_quorum(quorum: list[dict]) -> list[str]:
    """Which quorum members the sampled node can actually see.

    Aliasing this to the full member list made a partitioned node report every
    peer as online, which is exactly the disagreement experiments H and I exist
    to record (audit finding H-2).
    """
    return [
        row["Node Name"]
        for row in quorum
        if row.get("Raft State") in LIVE_RAFT_STATES
    ]


class GaugeRecorder:
    """Poll every cluster node's gauges and cluster view on a background thread.

    messages_ready and messages_unacknowledged are gauges, not counters, which is
    why ADR-0001's ban does not apply: they are recorded as evidence of buffering,
    as the direct read on an in-flight batch size, and (ADR-0007) as a drain-exit
    guard. They never take part in a delivery or loss assertion.

    Each node's answer is kept under its own key and never merged. Under a
    partition the nodes disagreeing IS the finding, and a merged or first-node-only
    sample cannot express it. The flat top-level keys mirror node 1 so every
    Phase 2 experiment and its committed results keep reading what they always did.

    Two read paths. HTTP while a node answers; `docker exec` when it does not,
    because a partitioned container loses its published ports (spec 4.1) and exec
    needs no networking at all. Total failure of both is recorded as an error
    sample for that node alone, leaving the other nodes intact: a broker being
    unreachable is a legitimate experimental condition, not a harness bug.
    """

    def __init__(self, rabbit_get_node, nodes: tuple[int, ...] = (1,)) -> None:
        self._get_for = rabbit_get_node
        self._nodes = nodes
        self._stop = threading.Event()
        self._new_sample = threading.Condition()
        self._thread: threading.Thread | None = None
        self._exec_only: set[int] = set()
        self.samples: list[dict] = []
        self.marks: list[dict] = []

    def expect_exec(self, node: int) -> None:
        """Sample this node through docker exec only, skipping HTTP entirely.

        Called by an experiment the moment it partitions a node. Without it the
        recorder spends the whole split waiting on a socket that will never
        answer: `rabbit_get_node`'s session uses timeout=10, three calls per
        sample, against a 2-second poll interval — a 60-second split would yield
        one or two samples of the very thing being measured.
        """
        self._exec_only.add(node)

    def expect_http(self, node: int) -> None:
        """Resume HTTP sampling. Called on heal."""
        self._exec_only.discard(node)

    def _sample_node_http(self, node: int) -> dict:
        get = self._get_for(node)
        telemetry = get("/queues/%2F/telemetry.q").json()
        dlq = get("/queues/%2F/dlq").json()
        nodes = get("/nodes").json()
        me = f"rabbit@rabbit{node}"
        entry = next((n for n in nodes if n.get("name") == me), {})
        return {
            "reachable": True,
            "source": "http",
            "telemetry_ready": int(telemetry.get("messages_ready", 0)),
            "telemetry_unacked": int(telemetry.get("messages_unacknowledged", 0)),
            "dlq_messages": int(dlq.get("messages", 0)),
            "members": list(telemetry.get("members", [])),
            "online": list(telemetry.get("online", [])),
            "leader": telemetry.get("leader"),
            "partitions": list(entry.get("partitions", [])),
            "running": bool(entry.get("running", False)),
            "alarms": {
                "mem": bool(entry.get("mem_alarm", False)),
                "disk": bool(entry.get("disk_free_alarm", False)),
            },
        }

    def _sample_node_exec(self, node: int) -> dict:
        container = container_for("rabbitmq", node)
        status = _exec_json(container, "rabbitmq-diagnostics", "-q", "--formatter", "json",
                            "cluster_status")
        quorum = _exec_json(container, "rabbitmq-queues", "-q", "--formatter", "json",
                            "quorum_status", "telemetry.q")
        queues = _exec_json(container, "rabbitmqctl", "-q", "--formatter", "json",
                            "list_queues", "name", "messages_ready", "messages_unacknowledged")
        by_name = {q["name"]: q for q in queues}
        telemetry = by_name.get("telemetry.q", {})
        dlq = by_name.get("dlq", {})
        leader = next(
            (r["Node Name"] for r in quorum if r.get("Raft State") == "leader"), None
        )
        alarms = status.get("alarms") or []
        return {
            "reachable": True,
            "source": "exec",
            "telemetry_ready": int(telemetry.get("messages_ready", 0)),
            "telemetry_unacked": int(telemetry.get("messages_unacknowledged", 0)),
            "dlq_messages": int(dlq.get("messages_ready", 0)),
            "members": [r["Node Name"] for r in quorum],
            "online": _online_from_quorum(quorum),
            "leader": leader,
            "partitions": list(status.get("partitions") or []),
            "running": True,
            "alarms": {
                "mem": any("memory" in str(a) for a in alarms),
                "disk": any("disk" in str(a) for a in alarms),
            },
        }

    def _sample_node(self, node: int) -> dict:
        http_exc: Exception | None = None
        if node not in self._exec_only:
            try:
                return self._sample_node_http(node)
            except Exception as exc:
                http_exc = exc
        try:
            return self._sample_node_exec(node)
        except Exception as exec_exc:
            return {
                "reachable": False,
                "source": None,
                "http_error": None if http_exc is None else f"{type(http_exc).__name__}: {http_exc}",
                "exec_error": f"{type(exec_exc).__name__}: {exec_exc}",
            }

    def _sample_once(self) -> dict:
        now = time.time()
        per_node = {n: self._sample_node(n) for n in self._nodes}
        sample: dict = {"t": now, "nodes": per_node}
        first = per_node.get(self._nodes[0], {})
        if first.get("reachable"):
            sample.update({
                "telemetry_ready": first["telemetry_ready"],
                "telemetry_unacked": first["telemetry_unacked"],
                "dlq_messages": first["dlq_messages"],
                "alarms": first["alarms"],
            })
        else:
            sample["error"] = first.get("http_error") or first.get("exec_error", "unreachable")
        return sample

    def _loop(self) -> None:
        while not self._stop.is_set():
            sample = self._sample_once()
            with self._new_sample:
                self.samples.append(sample)
                self._new_sample.notify_all()
            self._stop.wait(POLL_INTERVAL_S)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)

    def mark(self, label: str) -> None:
        self.marks.append({"label": label, "t": time.time()})

    def await_sample_after(self, t: float, timeout_s: float = 60.0) -> float:
        """Block until a sample newer than `t` has landed. Returns its timestamp.

        A partitioned node's first sample can land ~20 seconds late: the in-flight
        poll is still burning an HTTP timeout against a node that just lost its
        published ports, and only then falls back to exec. Reading a window
        without waiting for that sample is ADR-0018's second named cause.

        Raises rather than returning None on timeout: a recorder that has stopped
        sampling is exactly the failure the harness-integrity floor exists to
        catch, and it should be loud.
        """
        deadline = time.time() + timeout_s
        with self._new_sample:
            while True:
                for sample in reversed(self.samples):
                    if sample["t"] > t:
                        return sample["t"]
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise AssertionError(
                        f"no gauge sample landed within {timeout_s}s of t={t}; "
                        f"the recorder has stopped sampling"
                    )
                self._new_sample.wait(remaining)

    def peak(self, key: str) -> float:
        values = [s[key] for s in self.samples if key in s]
        return max(values) if values else 0

    def latest(self, key: str) -> int:
        """Most recent successful reading of a gauge on the first sampled node.

        Distinct from peak() and not interchangeable with it: "how deep was
        Telegraf's in-flight batch at the moment we killed it" is a latest(), and
        reporting a run-wide peak under that name would overstate it.
        """
        for sample in reversed(self.samples):
            if key in sample:
                return sample[key]
        return 0

    def node_latest(self, node: int, key: str):
        """Most recent reading of `key` on a named node, regardless of read path."""
        for sample in reversed(self.samples):
            entry = sample.get("nodes", {}).get(node, {})
            if not entry:
                continue
            if key == "reachable":
                return entry.get("reachable", False)
            if entry.get("reachable") and key in entry:
                return entry[key]
        return None

    def mark_time(self, label: str) -> float | None:
        """When a named mark was recorded. None if it was never marked."""
        for mark in reversed(self.marks):
            if mark["label"] == label:
                return mark["t"]
        return None

    def node_window(self, node: int, key: str, since: float,
                    until: float | None = None):
        """Latest reading of `key` on a node *within a time window*.

        node_latest scans all of history, which is wrong for anything that asks
        "what did this node look like during the split". If the partitioned node
        went unreachable, node_latest happily returns its pre-partition value and
        an assertion built on it reports the opposite of the truth.
        """
        until = float("inf") if until is None else until
        for sample in reversed(self.samples):
            if not (since <= sample["t"] <= until):
                continue
            entry = sample.get("nodes", {}).get(node, {})
            if not entry:
                continue
            if key == "reachable":
                return entry.get("reachable", False)
            if entry.get("reachable") and key in entry:
                return entry[key]
        return None

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


CLUSTER_NODE_INDICES = (1, 2, 3)


@pytest.fixture
def gauge_recorder(stack, rabbit_get_node):
    nodes = CLUSTER_NODE_INDICES if cluster_mode() else (1,)
    recorder = GaugeRecorder(rabbit_get_node, nodes=nodes)
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

# Per-node MQTT endpoints on the cluster overlay. Node 1 keeps Phase 1's port
# (1883); nodes 2 and 3 are published at 1884/1885 (compose.cluster.yml).
CLUSTER_MQTT_NODES = [("localhost", 1883), ("localhost", 1884), ("localhost", 1885)]


def start_sim(specs, rate_hz: float, duration_s: float, run_id: str,
              max_reconnects: int = 12, nodes=None) -> Future:
    """Run the device simulator on a background thread.

    The experiment body stays synchronous so container manipulation reads as a
    straight-line script. asyncio.run in a worker thread picks up the global
    WindowsSelectorEventLoopPolicy already set by main/conftest.py.

    `nodes` defaults to None so Phase 2 callers are unaffected; cluster
    experiments pass CLUSTER_MQTT_NODES to fail over across brokers.
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
                nodes=nodes,
            )
        )

    return _EXECUTOR.submit(_run)


@dataclass
class DrainResult:
    """What a drain produced and how it decided to stop.

    ADR-0012's root cause took several rounds to find because none of these were
    recorded — they had to be re-derived from timestamps and gauge timelines after
    the fact. They are nearly free to capture and make a repeat self-diagnosing.
    """

    rows: list[dict] = field(default_factory=list)
    elapsed_s: float = 0.0
    exit_condition: str = "timeout"
    ready_at_exit: int | None = None
    unacked_at_exit: int | None = None
    expected_total: int | None = None

    def as_result_fields(self) -> dict:
        return {
            "drain_elapsed_s": self.elapsed_s,
            "drain_exit_condition": self.exit_condition,
            "queue_ready_at_drain_exit": self.ready_at_exit,
            "queue_unacked_at_drain_exit": self.unacked_at_exit,
            "drain_rows_at_exit": len(self.rows),
            "drain_expected_total": self.expected_total,
        }


def drain_and_fetch(influx_query, run_id: str, start: str, expected_total: int,
                    timeout_s: float = 180.0, stable_polls_limit: int = 6,
                    gauge_recorder=None, leader_node: int = 1) -> DrainResult:
    """Wait for the pipeline to finish draining, then return the rows.

    Primary exit: the broker reports the queue empty (`messages_ready == 0` and
    `messages_unacknowledged == 0`) and the row count has stopped moving. Both
    gauges are already sampled by gauge_recorder and are permitted as buffering
    evidence and as a guard under ADR-0007; they prove nothing about delivery,
    they only decide when it is safe to stop waiting for it.

    Fallback exit: poll-count stability, the Phase 2 heuristic, used when no
    recorder was supplied or when the broker is unreachable. ADR-0012 records this
    heuristic reporting loss twice on a run that had not lost anything, which is
    why it is no longer the primary condition — a slow drain that plateaus
    mid-backlog looks identical to a finished one.

    Last resort: timeout_s. An experiment that actually lost messages never
    reaches expected_total, so without the fallback it would burn the whole
    timeout before reporting the very number it was run to measure.
    """
    from tests.conftest import fetch_seqs

    began = time.time()
    deadline = began + timeout_s
    result = DrainResult()
    result.expected_total = expected_total
    last_count = -1
    stable_polls = 0
    ready = None

    while time.time() < deadline:
        result.rows = fetch_seqs(influx_query, run_id, start=start)
        if len(result.rows) == last_count:
            stable_polls += 1
        else:
            stable_polls = 0
            last_count = len(result.rows)

        if gauge_recorder is not None:
            ready = gauge_recorder.node_latest(leader_node, "telemetry_ready")
            unacked = gauge_recorder.node_latest(leader_node, "telemetry_unacked")
            result.ready_at_exit = ready
            result.unacked_at_exit = unacked
            if ready == 0 and unacked == 0 and stable_polls >= 2:
                result.exit_condition = "broker-drained"
                break

        if stable_polls >= 3 and len(result.rows) >= expected_total:
            result.exit_condition = "row-count-complete"
            break
        if (gauge_recorder is None or ready is None) and stable_polls >= stable_polls_limit:
            result.exit_condition = "row-count-gave-up"
            break
        time.sleep(5)

    result.elapsed_s = round(time.time() - began, 2)
    return result


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
