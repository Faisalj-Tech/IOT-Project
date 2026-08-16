"""Every Phase 3 measurement is meaningless on a half-formed cluster.

This gate fails loudly rather than skipping. A skipped preflight would let a
one-member telemetry.q produce publishable numbers (ADR-0013).
"""

import pytest

from tests.conftest import RABBIT_MGMT_PORTS, cluster_mode
from tests.experiments.cluster import (
    CLUSTER_NODES, node_name, queue_leader, queue_members, telegraf_connected_node,
)

pytestmark = [pytest.mark.stack, pytest.mark.cluster]


def test_the_session_was_started_in_cluster_mode():
    assert cluster_mode(), (
        "IOT_CLUSTER=1 is not set, so the stack fixture brought up the single-node "
        "stack. Run the Phase 3 matrix as: IOT_CLUSTER=1 pytest tests/ -m cluster"
    )


def test_all_three_nodes_are_running(rabbit_get_node):
    nodes = rabbit_get_node(1)("/nodes").json()
    names = sorted(n["name"] for n in nodes)
    assert names == [node_name(n) for n in CLUSTER_NODES], names
    for entry in nodes:
        assert entry["running"] is True, entry["name"]


def test_no_node_reports_a_pre_existing_partition(rabbit_get_node):
    nodes = rabbit_get_node(1)("/nodes").json()
    for entry in nodes:
        assert entry.get("partitions") == [], (
            f"{entry['name']} is already partitioned before any experiment ran"
        )


@pytest.mark.parametrize("queue", ["telemetry.q", "dlq"])
def test_quorum_queues_have_three_voting_members(rabbit_get_node, queue):
    members = queue_members(rabbit_get_node, queue)
    assert len(members) == 3, (
        f"{queue} has {len(members)} member(s): {members}. A one-member group pinned "
        f"to node 1 would make every node-kill result a config artifact — see ADR-0013. "
        f"Remediate with: docker exec iot-rabbitmq rabbitmq-queues grow rabbit@rabbit2 all"
    )
    assert queue_leader(rabbit_get_node, queue) in members


def test_every_node_answers_on_its_own_management_port(rabbit_get_node):
    for node in CLUSTER_NODES:
        resp = rabbit_get_node(node)("/overview")
        assert resp.status_code == 200, (
            f"node {node} did not answer on port {RABBIT_MGMT_PORTS[node]}: {resp.text}"
        )


def test_telegraf_is_consuming_from_some_node(rabbit_get_node):
    node = telegraf_connected_node(rabbit_get_node)
    assert node in CLUSTER_NODES, (
        "no AMQP connection from the telegraf user on any node; the ingest path is "
        "down and every drain measurement would be meaningless"
    )
