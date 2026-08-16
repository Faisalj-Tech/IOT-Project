"""Does amqp_consumer move to another broker when its node dies?

Spec 4.4: if Telegraf does not fail over, that is a finding about Telegraf, and
Experiment G's stall must be attributed to it rather than to the Raft election.
"""

import time

import pytest

from tests.experiments.cluster import CLUSTER_NODES, telegraf_connected_node

pytestmark = [pytest.mark.stack, pytest.mark.cluster]

FAILOVER_TIMEOUT_S = 90.0


def test_telegraf_moves_to_a_surviving_broker(docker_control, rabbit_get_node, results_dir):
    run_id = f"tfo{int(time.time()) % 100000}"
    before = telegraf_connected_node(rabbit_get_node)
    assert before in CLUSTER_NODES, "Telegraf was not connected to any node to begin with"

    killed_at = time.time()
    docker_control.kill("rabbitmq", node=before)

    after = None
    elapsed = None
    deadline = time.time() + FAILOVER_TIMEOUT_S
    while time.time() < deadline:
        candidate = telegraf_connected_node(rabbit_get_node)
        if candidate is not None and candidate != before:
            after = candidate
            elapsed = round(time.time() - killed_at, 2)
            break
        time.sleep(2)

    docker_control.start("rabbitmq", node=before)

    results_dir(
        "T-telegraf-failover",
        {
            "run_id": run_id,
            "arm": "telegraf",
            "connected_node_before": before,
            "connected_node_after": after,
            "failover_elapsed_s": elapsed,
            "failover_timeout_s": FAILOVER_TIMEOUT_S,
            "verdict": "failed-over" if after else "no-failover",
        },
    )

    assert after is not None, (
        f"Telegraf did not reconnect to a surviving broker within {FAILOVER_TIMEOUT_S}s "
        f"of node {before} dying. Experiment G's stall must then be attributed to "
        f"Telegraf's client behaviour, not to the Raft election — record this in the "
        f"findings note and re-read G's result in that light."
    )
