"""L6 - what does quorum replication cost under load?

Recorded, not asserted. The comparison is against the single-node results L2 and
L4c already wrote, so this reruns the same two shapes on the 3-node cluster and
reports the deltas.

`x-quorum-initial-group-size: 3` is already set in definitions.cluster.json
(ADR-0013: the group size must be DECLARED, not inherited), so the queues
replicate without any Phase 6 change.
"""

import json
import time
from pathlib import Path

import pytest

from tests.conftest import ROOT, cluster_mode, load_mode
from tests.experiments.conftest import write_result
from tests.experiments.load import (
    Swarm, amqp_publish_burst, broker_memory_bytes, host_envelope,
    purge_queue, stable_depth,
)

pytestmark = [pytest.mark.load, pytest.mark.cluster, pytest.mark.stack]

RESULTS = ROOT / "docs" / "results"
PAYLOAD = b"x" * 200
MESSAGES = 100_000


@pytest.fixture(autouse=True)
def _requires_load_and_cluster():
    if not (load_mode() and cluster_mode()):
        pytest.skip("needs compose.load.yml + compose.cluster.yml, IOT_LOAD=1 IOT_CLUSTER=1")


def _single_node_baseline() -> dict:
    """The L2 result this run compares against. Missing is a skip, not a failure."""
    path = RESULTS / "L2-throughput-L2.json"
    if not path.exists():
        pytest.skip("run L2 on the single-node profile first")
    return json.loads(path.read_text(encoding="utf-8"))


def test_quorum_replication_cost_under_load(stack, influx_query):
    baseline = _single_node_baseline()
    purge_queue("telemetry.q")
    purge_queue("dlq")

    memory_before = broker_memory_bytes()
    started = time.monotonic()
    acked, nacked = amqp_publish_burst("telemetry.q", count=MESSAGES, body=PAYLOAD)
    publish_seconds = time.monotonic() - started
    depth = stable_depth("telemetry.q", timeout_s=300)
    memory_after = broker_memory_bytes()

    per_message_bytes = (memory_after - memory_before) / max(acked, 1)

    write_result("L6-cluster", {
        "run_id": "L6",
        "host_envelope": host_envelope(),
        "messages": acked,
        "nacked": nacked,
        "publish_seconds": round(publish_seconds, 2),
        "publish_rate_hz": round(acked / publish_seconds, 1) if publish_seconds else None,
        "queue_depth": depth.as_result_fields(),
        "memory_before_bytes": memory_before,
        "memory_after_bytes": memory_after,
        "per_message_memory_bytes": round(per_message_bytes, 1),
        "single_node_reference": {
            "note": "compare against L2-throughput-L2.json's steps",
            "steps": baseline.get("steps", []),
        },
        "model_single_node_per_message_bytes": 850 + len(PAYLOAD),
    })
