"""Cluster-shaped questions the Phase 3 experiments ask of the broker.

Everything here reads the management API rather than parsing CLI output, because
these are all majority-side questions asked from a healthy vantage point. The
`docker exec` path in GaugeRecorder exists for the other case — reading a node
that has been partitioned away.
"""

import time

CLUSTER_NODES = (1, 2, 3)


def node_name(node: int) -> str:
    return f"rabbit@rabbit{node}"


def node_index(name: str) -> int:
    """rabbit@rabbit2 -> 2"""
    return int(name.rsplit("rabbit", 1)[-1])


def _queue(rabbit_get_node, queue: str, node: int = 1) -> dict:
    return rabbit_get_node(node)(f"/queues/%2F/{queue}").json()


def queue_members(rabbit_get_node, queue: str = "telemetry.q", node: int = 1) -> list[str]:
    """Raft group membership, as the management API reports it."""
    return list(_queue(rabbit_get_node, queue, node).get("members", []))


def queue_leader(rabbit_get_node, queue: str = "telemetry.q", node: int = 1) -> str:
    return _queue(rabbit_get_node, queue, node).get("leader")


def queue_leader_node(rabbit_get_node, queue: str = "telemetry.q", node: int = 1) -> int:
    """The 1-based index of the node currently leading this queue's Raft group."""
    return node_index(queue_leader(rabbit_get_node, queue, node))


def follower_node(rabbit_get_node, queue: str = "telemetry.q", node: int = 1) -> int:
    """Any node that is a member but not the leader.

    Experiment F needs a node whose death costs the group one replica out of three
    and nothing else. A fixed index would silently become Experiment G whenever
    the leader happened to land there.
    """
    leader = queue_leader(rabbit_get_node, queue, node)
    members = queue_members(rabbit_get_node, queue, node)
    followers = [m for m in members if m != leader]
    if not followers:
        raise AssertionError(f"{queue} has no follower; members={members}, leader={leader}")
    return node_index(followers[0])


def telegraf_connected_node(rabbit_get_node) -> int | None:
    """Which node Telegraf's AMQP connection is attached to, or None.

    Experiment G is uninterpretable without this: killing the queue leader while
    Telegraf was attached to a different node is a materially different experiment
    from killing the leader Telegraf was attached to.
    """
    for node in CLUSTER_NODES:
        try:
            connections = rabbit_get_node(node)("/connections").json()
        except Exception:
            continue
        for conn in connections:
            if conn.get("user") == "telegraf" and str(conn.get("protocol", "")).startswith("AMQP"):
                return node_index(conn["node"])
    return None


def await_leader(rabbit_get_node, exclude: str, timeout_s: float = 60.0,
                 queue: str = "telemetry.q") -> tuple[int, float]:
    """Block until a node other than `exclude` leads the queue.

    Returns (leader node index, seconds waited) — Experiment G's time-to-elect
    measurement. Polls only from surviving nodes; asking the dead one would time
    out by construction.
    """
    began = time.time()
    deadline = began + timeout_s
    survivors = [n for n in CLUSTER_NODES if node_name(n) != exclude]
    while time.time() < deadline:
        for node in survivors:
            try:
                leader = queue_leader(rabbit_get_node, queue, node=node)
            except Exception:
                continue
            if leader and leader != exclude:
                return node_index(leader), round(time.time() - began, 2)
        time.sleep(0.5)
    raise AssertionError(
        f"no new leader for {queue} within {timeout_s}s after {exclude} died"
    )
