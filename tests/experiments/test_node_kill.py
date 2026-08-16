"""Experiments F and G: what a three-member quorum queue does when a node dies.

F kills a follower, G kills the leader. They are separate experiments because
killing a random node hits a follower two times in three and never exercises
re-election — the thing a cluster exists to do.

Both are asserted rather than recorded (ADR-0015). A healthy three-member group
surviving one node loss has a correct answer, and a failure here is a real
defect. That is not a violation of ADR-0004, which ADR-0009 scoped to
exploratory experiments; H and I are the exploratory ones.
"""

import time
from datetime import datetime, timezone

import pytest

from sim.devices.payload import default_specs
from tests.conftest import flux_range_start
from tests.experiments.cluster import (
    CLUSTER_NODES, await_leader, follower_node, node_name, queue_leader,
    queue_leader_node, queue_members, telegraf_connected_node,
)
from tests.experiments.conftest import (
    CLUSTER_MQTT_NODES, drain_and_fetch, sequence_report, start_sim,
)

pytestmark = [pytest.mark.stack, pytest.mark.cluster]

DEVICES = 5
RATE_HZ = 2.0
CLEAN_S = 20.0
DOWN_S = 45.0
TAIL_S = 20.0
RECONNECT_BUDGET = 12
REJOIN_TIMEOUT_S = 120.0


def surviving_node(target: int) -> int:
    """A node to read the cluster from while `target` is down.

    Every helper in cluster.py defaults to node=1, and node 1 is a perfectly
    ordinary thing to kill: follower_node can return it, and it leads the queue
    about a third of the time. Reading the cluster from the node you just killed
    fails for the whole outage window and looks like a durability defect.
    """
    return next(n for n in CLUSTER_NODES if n != target)


def _await_three_voters(rabbit_get_node, read_node: int,
                        timeout_s: float = REJOIN_TIMEOUT_S) -> float:
    """Wait for the Raft group to be whole again. Returns seconds waited."""
    began = time.time()
    deadline = began + timeout_s
    while time.time() < deadline:
        try:
            if len(queue_members(rabbit_get_node, node=read_node)) == 3:
                return round(time.time() - began, 2)
        except Exception:
            pass
        time.sleep(2)
    raise AssertionError(
        f"telemetry.q did not return to three members within {timeout_s}s of the node "
        f"restarting; members={queue_members(rabbit_get_node, node=read_node)}"
    )


def test_follower_kill_costs_a_replica_but_not_a_message(
    docker_control, gauge_recorder, results_dir, influx_query, rabbit_get_node
):
    run_id = f"expF{int(time.time()) % 100000}"
    specs = default_specs(DEVICES, region="eu", plant="plant1")
    started_at = datetime.now(timezone.utc)
    nominal_s = CLEAN_S + DOWN_S + TAIL_S

    leader_before = queue_leader(rabbit_get_node)
    leader_node_before = queue_leader_node(rabbit_get_node)
    telegraf_node = telegraf_connected_node(rabbit_get_node)
    target = follower_node(rabbit_get_node)
    assert target != leader_node_before, "follower_node returned the leader"
    read_node = surviving_node(target)

    gauge_recorder.mark("sim_start")
    future = start_sim(
        specs, RATE_HZ, nominal_s, run_id,
        max_reconnects=RECONNECT_BUDGET, nodes=CLUSTER_MQTT_NODES,
    )

    time.sleep(CLEAN_S)
    gauge_recorder.mark(f"kill_follower_node{target}")
    kill_at = time.time()
    docker_control.kill("rabbitmq", node=target)

    time.sleep(DOWN_S)
    gauge_recorder.mark("restart_node")
    docker_control.start("rabbitmq", node=target)
    rejoin_s = _await_three_voters(rabbit_get_node, read_node)
    gauge_recorder.mark("rejoined")

    published = future.result(timeout=900)
    gauge_recorder.mark("sim_done")
    expected_total = sum(published.values())

    leader_after = queue_leader(rabbit_get_node, node=read_node)
    drain = drain_and_fetch(
        influx_query, run_id, flux_range_start(started_at), expected_total,
        timeout_s=300, gauge_recorder=gauge_recorder, leader_node=leader_node_before,
    )
    report = sequence_report(drain.rows, published)
    duplicate_total = sum(report["duplicates"].values())

    # The majority never lost quorum, so the leader must never have changed. A
    # leader move here means this run was really Experiment G and its numbers
    # answer a different question.
    leader_held = leader_after == leader_before

    results_dir(
        "F-follower-kill",
        {
            "run_id": run_id,
            "arm": "telegraf",
            "config": {
                "devices": DEVICES, "rate_hz": RATE_HZ, "clean_s": CLEAN_S,
                "down_s": DOWN_S, "tail_s": TAIL_S, "max_reconnects": RECONNECT_BUDGET,
                "partition_handling": "ignore",
            },
            "killed_node": target,
            "killed_at": kill_at,
            "read_node": read_node,
            "leader_before": leader_before,
            "leader_after": leader_after,
            "leader_held": leader_held,
            "telegraf_connected_node": telegraf_node,
            "members_after_rejoin": queue_members(rabbit_get_node, node=read_node),
            "rejoin_elapsed_s": rejoin_s,
            "published_counts": published,
            "published_total": expected_total,
            "influx_total": report["total_rows"],
            "gaps": report["gaps"],
            "duplicates": report["duplicates"],
            "duplicate_total": duplicate_total,
            "duplicate_rate": duplicate_total / expected_total if expected_total else 0,
            **drain.as_result_fields(),
            "verdict": "no-loss" if not report["gaps"] else "loss",
            "timeline": gauge_recorder.timeline(),
        },
    )

    assert report["gaps"] == {}, (
        f"sequence gaps after killing follower node {target}: {report['gaps']}. A "
        f"three-member quorum group keeps a majority when one follower dies, so a gap "
        f"here is a real durability failure"
    )
    assert report["total_rows"] >= expected_total, (
        f"lost messages: expected at least {expected_total}, got {report['total_rows']}"
    )
    assert len(queue_members(rabbit_get_node, node=read_node)) == 3, (
        "the restarted node did not rejoin"
    )
    assert leader_held, (
        f"leader moved from {leader_before} to {leader_after} while a follower was "
        f"killed; this run measured a re-election, not a follower loss"
    )


ELECTION_TIMEOUT_S = 60.0


def test_leader_kill_forces_re_election_without_losing_a_message(
    docker_control, gauge_recorder, results_dir, influx_query, rabbit_get_node
):
    run_id = f"expG{int(time.time()) % 100000}"
    specs = default_specs(DEVICES, region="eu", plant="plant1")
    started_at = datetime.now(timezone.utc)
    nominal_s = CLEAN_S + DOWN_S + TAIL_S

    leader_before = queue_leader(rabbit_get_node)
    target = queue_leader_node(rabbit_get_node)
    read_node = surviving_node(target)
    telegraf_node = telegraf_connected_node(rabbit_get_node)
    # Which node Telegraf sits on is not controlled — the brokers list decides it.
    # Killing the leader Telegraf was attached to is a materially different
    # experiment from killing a leader it was not, so the run records which it was
    # rather than pretending they are the same (spec 5.G).
    telegraf_shared_the_leader = telegraf_node == target

    unacked_before_kill = gauge_recorder.node_latest(target, "telemetry_unacked")

    gauge_recorder.mark("sim_start")
    future = start_sim(
        specs, RATE_HZ, nominal_s, run_id,
        max_reconnects=RECONNECT_BUDGET, nodes=CLUSTER_MQTT_NODES,
    )

    time.sleep(CLEAN_S)
    unacked_at_kill = gauge_recorder.node_latest(target, "telemetry_unacked")
    gauge_recorder.mark(f"kill_leader_node{target}")
    docker_control.kill("rabbitmq", node=target)

    elected_node, time_to_elect_s = await_leader(
        rabbit_get_node, exclude=leader_before, timeout_s=ELECTION_TIMEOUT_S
    )
    gauge_recorder.mark(f"elected_node{elected_node}")

    time.sleep(DOWN_S)
    gauge_recorder.mark("restart_node")
    docker_control.start("rabbitmq", node=target)
    rejoin_s = _await_three_voters(rabbit_get_node, read_node)
    gauge_recorder.mark("rejoined")

    published = future.result(timeout=900)
    gauge_recorder.mark("sim_done")
    expected_total = sum(published.values())

    drain = drain_and_fetch(
        influx_query, run_id, flux_range_start(started_at), expected_total,
        timeout_s=300, gauge_recorder=gauge_recorder, leader_node=elected_node,
    )
    report = sequence_report(drain.rows, published)
    duplicate_total = sum(report["duplicates"].values())

    results_dir(
        "G-leader-kill",
        {
            "run_id": run_id,
            "arm": "telegraf",
            "config": {
                "devices": DEVICES, "rate_hz": RATE_HZ, "clean_s": CLEAN_S,
                "down_s": DOWN_S, "tail_s": TAIL_S, "max_reconnects": RECONNECT_BUDGET,
                "partition_handling": "ignore",
            },
            "killed_node": target,
            "read_node": read_node,
            "leader_before": leader_before,
            "leader_after": queue_leader(rabbit_get_node, node=read_node),
            "elected_node": elected_node,
            "time_to_elect_s": time_to_elect_s,
            "telegraf_connected_node": telegraf_node,
            "telegraf_shared_the_leader": telegraf_shared_the_leader,
            "unacked_before_run": unacked_before_kill,
            "unacked_at_kill": unacked_at_kill,
            "members_after_rejoin": queue_members(rabbit_get_node, node=read_node),
            "rejoin_elapsed_s": rejoin_s,
            "published_counts": published,
            "published_total": expected_total,
            "influx_total": report["total_rows"],
            "gaps": report["gaps"],
            "duplicates": report["duplicates"],
            "duplicate_total": duplicate_total,
            "duplicate_rate": duplicate_total / expected_total if expected_total else 0,
            **drain.as_result_fields(),
            "verdict": "no-loss" if not report["gaps"] else "loss",
            "timeline": gauge_recorder.timeline(),
        },
    )

    assert elected_node in CLUSTER_NODES and node_name(elected_node) != leader_before, (
        f"no new leader was elected; this run did not measure a re-election"
    )
    assert report["gaps"] == {}, (
        f"sequence gaps across a leader election: {report['gaps']}. Phase 2 established "
        f"Telegraf acks on receipt, before the InfluxDB write is confirmed; a gap here "
        f"means the requeue guarantee that covered every Phase 2 arm does not hold when "
        f"queue leadership moves with messages in flight"
    )
    assert report["total_rows"] >= expected_total, (
        f"lost messages: expected at least {expected_total}, got {report['total_rows']}"
    )
    assert len(queue_members(rabbit_get_node, node=read_node)) == 3, (
        "the restarted node did not rejoin"
    )
