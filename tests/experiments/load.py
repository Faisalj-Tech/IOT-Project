"""Machinery for the Phase 6 load experiments.

Three of these exist because the design probes proved the obvious approach wrong:

- stable_depth() exists because NO depth source is instantaneous. The management
  API lagged a full stats interval behind rabbitmqctl, the skew inverted on a
  later run, and a passive declare reported 0 for a queue holding 5 (spec 2.10).
  This module deliberately exposes no single-read depth function.
- apply_policy()/remove_policy() exist so no overflow policy is ever written into
  definitions.json. A shared definitions edit is carried-forward bite #22.
- host_envelope() exists because a breaking point measured on one machine, with an
  unpinned watermark of 0.6 x host RAM, is reproducible by nobody (spec 2.7).
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import requests

from tests.conftest import ROOT, compose, compose_files, rabbit_api
from tests.experiments.conftest import container_for

SIM_SERVICE = "sim-load"


def _admin_auth() -> tuple[str, str]:
    return (
        os.environ.get("RABBITMQ_ADMIN_USER", "admin"),
        os.environ.get("RABBITMQ_ADMIN_PASSWORD", "adminpass"),
    )


def _policy_body(pattern: str, definition: dict, priority: int, apply_to: str) -> dict:
    """The management API's policy payload. Split out so it is unit-testable."""
    return {
        "pattern": pattern,
        "definition": definition,
        "priority": priority,
        "apply-to": apply_to,
    }


def apply_policy(name: str, pattern: str, definition: dict, vhost: str = "/",
                 priority: int = 0, apply_to: str = "queues") -> None:
    """Apply a queue policy at runtime.

    NEVER write these into definitions.json. Spec decision 7: a shared definitions
    edit broke a distant test undetected for four tasks in Phase 5, and a runtime
    policy lets one test sweep the whole overflow matrix in a single run.

    NOTE: a 2xx here proves the API accepted the policy, NOT that the broker
    honours it. reject-publish-dlx returns 201 and is silently ignored on quorum
    queues (spec 2.3). Always assert observed behaviour afterwards.
    """
    vhost_enc = requests.utils.quote(vhost, safe="")
    response = requests.put(
        f"{rabbit_api()}/api/policies/{vhost_enc}/{name}",
        json=_policy_body(pattern, definition, priority, apply_to),
        auth=_admin_auth(), timeout=10,
    )
    response.raise_for_status()


def remove_policy(name: str, vhost: str = "/") -> None:
    """Remove a policy. 404 is success: the policy is already gone."""
    vhost_enc = requests.utils.quote(vhost, safe="")
    response = requests.delete(
        f"{rabbit_api()}/api/policies/{vhost_enc}/{name}",
        auth=_admin_auth(), timeout=10,
    )
    if response.status_code not in (204, 404):
        response.raise_for_status()


@contextmanager
def load_policy(name: str, pattern: str, definition: dict, vhost: str = "/",
                priority: int = 0, apply_to: str = "queues"):
    """Apply a policy for the duration of a block, always removing it afterwards.

    The teardown runs even when the body fails, so a failing assertion cannot leak
    an overflow policy into the next test in the matrix.
    """
    apply_policy(name, pattern, definition, vhost, priority, apply_to)
    try:
        yield
    finally:
        remove_policy(name, vhost)


def _parse_list_queues(output: str, queue: str) -> dict | None:
    """Pull one queue's counters out of `rabbitmqctl list_queues` output.

    Returns None when the queue is absent, which is deliberately distinct from a
    queue holding zero messages: a test that declared nothing must not read as a
    clean empty queue and pass.
    """
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 4 and parts[0] == queue:
            return {
                "messages": int(parts[1]),
                "ready": int(parts[2]),
                "unacked": int(parts[3]),
            }
    return None


@dataclass
class DepthReading:
    queue: str
    messages: int
    ready: int
    unacked: int
    polls: int
    elapsed_s: float
    exit_condition: str  # "stable" | "timeout" | "absent"

    def as_result_fields(self) -> dict:
        return {
            f"{self.queue}_messages": self.messages,
            f"{self.queue}_ready": self.ready,
            f"{self.queue}_unacked": self.unacked,
            f"{self.queue}_depth_polls": self.polls,
            f"{self.queue}_depth_exit": self.exit_condition,
        }


def _raw_depth(queue: str, vhost: str = "/", node: int = 1) -> dict | None:
    proc = subprocess.run(
        ["docker", "exec", container_for("rabbitmq", node), "rabbitmqctl", "-q",
         "list_queues", "-p", vhost, "--no-table-headers",
         "name", "messages", "messages_ready", "messages_unacknowledged"],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return None
    return _parse_list_queues(proc.stdout, queue)


def stable_depth(queue: str, vhost: str = "/", timeout_s: float = 60.0,
                 stable_polls: int = 3, interval_s: float = 2.0,
                 node: int = 1) -> DepthReading:
    """Poll a queue's depth until it stops moving, then report it.

    There is no single-read counterpart in this module ON PURPOSE. Spec 2.10
    measured every available source lagging, including rabbitmqctl itself, and a
    single read taken right after a publish burst reports a number that was true
    at some unspecified earlier moment.
    """
    start = time.monotonic()
    polls = 0
    streak = 0
    last: dict | None = None
    while time.monotonic() - start < timeout_s:
        current = _raw_depth(queue, vhost, node)
        polls += 1
        if current is not None and current == last:
            streak += 1
            if streak >= stable_polls:
                return DepthReading(queue=queue, **current, polls=polls,
                                    elapsed_s=round(time.monotonic() - start, 2),
                                    exit_condition="stable")
        else:
            streak = 0
        last = current
        time.sleep(interval_s)
    if last is None:
        return DepthReading(queue=queue, messages=0, ready=0, unacked=0, polls=polls,
                            elapsed_s=round(time.monotonic() - start, 2),
                            exit_condition="absent")
    return DepthReading(queue=queue, **last, polls=polls,
                        elapsed_s=round(time.monotonic() - start, 2),
                        exit_condition="timeout")


def broker_memory_bytes(node: int = 1) -> int:
    """Total Erlang memory. The figure spec 2.7's model predicts."""
    proc = subprocess.run(
        ["docker", "exec", container_for("rabbitmq", node), "rabbitmqctl", "-q",
         "eval", "erlang:memory(total)."],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    return int(proc.stdout.strip())


def broker_alarms(node: int = 1) -> list:
    """Active resource alarms. Empty list means neither watermark has been hit."""
    proc = subprocess.run(
        ["docker", "exec", container_for("rabbitmq", node), "rabbitmqctl", "-q",
         "eval", "rabbit_alarm:get_alarms()."],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    return [] if proc.stdout.strip() == "[]" else [proc.stdout.strip()]


def mnesia_megabytes(node: int = 1) -> int:
    """On-disk size. Never shrinks after a purge - spec 2.9, why the disk arm
    needs `down -v` between waves rather than a purge."""
    proc = subprocess.run(
        ["docker", "exec", container_for("rabbitmq", node), "sh", "-c",
         "du -sm /var/lib/rabbitmq/mnesia | cut -f1"],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    return int(proc.stdout.strip() or 0)


def purge_queue(queue: str, vhost: str = "/") -> None:
    vhost_enc = requests.utils.quote(vhost, safe="")
    response = requests.delete(
        f"{rabbit_api()}/api/queues/{vhost_enc}/{queue}/contents",
        auth=_admin_auth(), timeout=15,
    )
    if response.status_code not in (204, 404):
        response.raise_for_status()


def host_envelope() -> dict:
    """What machine produced these numbers, and what was pinned when they were.

    Stamped onto every result JSON. Without it "the broker breaks at N messages"
    is unreproducible: the default watermark is 0.6 x host RAM (spec 2.7).
    """
    docker_version = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        cwd=ROOT, check=False, capture_output=True, text=True,
    ).stdout.strip()
    inspect = subprocess.run(
        ["docker", "inspect", container_for("rabbitmq"), "--format",
         "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}"],
        cwd=ROOT, check=False, capture_output=True, text=True,
    ).stdout.split()
    watermark = subprocess.run(
        ["docker", "exec", container_for("rabbitmq"), "rabbitmqctl", "-q", "eval",
         "vm_memory_monitor:get_memory_limit()."],
        cwd=ROOT, check=False, capture_output=True, text=True,
    ).stdout.strip()
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "docker_version": docker_version,
        "broker_mem_limit_bytes": int(inspect[0]) if inspect else None,
        "broker_nano_cpus": int(inspect[1]) if len(inspect) > 1 else None,
        "broker_memory_watermark_bytes": int(watermark) if watermark.isdigit() else watermark,
        "compose_files": list(compose_files()),
    }


@dataclass
class Swarm:
    """Scale the sim-load service up and down and collect what each replica did.

    Deliberately does NOT assign identities. `docker compose --scale` gives every
    replica an identical command line, so a per-replica argument is impossible;
    each replica self-assigns from its container hostname instead (spec 2.13,
    4.2). There is no coordination step here to get wrong.
    """

    replicas: int = 0
    _report_dir: Path = field(default=ROOT / "docs" / "results" / "_swarm")

    def scale(self, replicas: int, devices: int | None = None,
              rate_hz: float | None = None, duration_s: float | None = None,
              run_id: str | None = None) -> None:
        """Scale the swarm, optionally changing what each replica publishes.

        compose.load.yml reads devices/rate/duration from LOAD_* environment
        variables, because `--scale` gives every replica one identical command
        line and there is nowhere else to vary them from. L1 and L2 both ramp
        devices and rate per step, so setting only `replicas` would silently hold
        those at their defaults and every step would offer the same load.
        """
        env = os.environ
        if devices is not None:
            env["LOAD_DEVICES"] = str(devices)
        if rate_hz is not None:
            env["LOAD_RATE"] = str(rate_hz)
        if duration_s is not None:
            env["LOAD_DURATION"] = str(duration_s)
        if run_id is not None:
            env["LOAD_RUN_ID"] = run_id
        compose("up", "-d", "--scale", f"{SIM_SERVICE}={replicas}", SIM_SERVICE,
                files=compose_files())
        self.replicas = replicas

    def stop(self) -> None:
        compose("rm", "-sf", SIM_SERVICE, files=compose_files())
        self.replicas = 0

    def container_ids(self) -> list[str]:
        proc = subprocess.run(
            ["docker", "compose", *sum((["-f", f] for f in compose_files()), []),
             "ps", "-q", SIM_SERVICE],
            cwd=ROOT, check=False, capture_output=True, text=True,
        )
        return [line for line in proc.stdout.split() if line]

    def connection_count(self) -> int:
        """MQTT connections the broker currently reports.

        Backed by a ~5s stats interval (carried-forward bite #20), so callers
        under churn should sample this repeatedly rather than trusting one read.
        """
        response = requests.get(f"{rabbit_api()}/api/connections",
                                auth=_admin_auth(), timeout=15)
        response.raise_for_status()
        return len(response.json())

    def collect(self) -> dict:
        """Aggregate every replica's report JSON, written to a shared volume."""
        totals = {"replicas": self.replicas, "attempted": 0, "puback": 0,
                  "rejected": 0, "timed_out": 0, "reconnects": 0,
                  "reason_codes": {}, "per_replica": []}
        for path in sorted(self._report_dir.glob("*.json")):
            report = json.loads(path.read_text(encoding="utf-8"))
            totals["per_replica"].append(report)
            for key in ("attempted", "puback", "rejected", "timed_out", "reconnects"):
                totals[key] += report.get(key, 0)
            for code, count in report.get("reason_codes", {}).items():
                totals["reason_codes"][code] = totals["reason_codes"].get(code, 0) + count
        return totals

    def clear_reports(self) -> None:
        if self._report_dir.exists():
            for path in self._report_dir.glob("*.json"):
                path.unlink()
