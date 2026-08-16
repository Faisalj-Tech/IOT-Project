"""Two mounted copies of one Telegraf config is a drift hazard; this self-polices it.

Phases 4-6 will keep editing telegraf.conf. Without this test the cluster copy
silently falls behind and the cluster arm quietly stops matching the baseline.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "config" / "telegraf" / "telegraf.conf"
CLUSTER = ROOT / "config" / "telegraf" / "telegraf.cluster.conf"

EXPECTED_BASE_BROKERS = '  brokers = ["amqp://rabbitmq:5672/"]'
EXPECTED_CLUSTER_BROKERS = (
    '  brokers = ["amqp://rabbit1:5672/", "amqp://rabbit2:5672/", "amqp://rabbit3:5672/"]'
)


def test_cluster_telegraf_config_differs_only_in_the_brokers_line():
    base = BASE.read_text(encoding="utf-8").splitlines()
    cluster = CLUSTER.read_text(encoding="utf-8").splitlines()
    assert len(base) == len(cluster), (
        f"line counts differ ({len(base)} vs {len(cluster)}); the cluster config has drifted"
    )
    differing = [
        (n, b, c) for n, (b, c) in enumerate(zip(base, cluster), start=1) if b != c
    ]
    assert len(differing) == 1, f"expected exactly one differing line, got: {differing}"
    _, base_line, cluster_line = differing[0]
    assert base_line == EXPECTED_BASE_BROKERS, base_line
    assert cluster_line == EXPECTED_CLUSTER_BROKERS, cluster_line


def test_cluster_definitions_declare_a_three_member_quorum_group():
    definitions = json.loads(
        (ROOT / "config" / "rabbitmq" / "definitions.cluster.json").read_text(encoding="utf-8")
    )
    by_name = {q["name"]: q for q in definitions["queues"]}
    for name in ("telemetry.q", "dlq"):
        args = by_name[name]["arguments"]
        assert args["x-queue-type"] == "quorum", name
        assert args["x-quorum-initial-group-size"] == 3, (
            f"{name} would be declared as a one-member group; see ADR-0013"
        )
