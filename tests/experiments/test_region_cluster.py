"""R7: region queues form real three-member Raft groups.

    IOT_REGION=1 IOT_CLUSTER=1 KEEP_STACK=1 .venv/Scripts/python.exe -m pytest \
        tests/experiments/test_region_cluster.py -m "region and cluster" -v -s

Phase 3 spec section 2's trap, transplanted: a quorum queue's member group is
fixed when the queue is declared. A region queue that lands as a one-member group
would vanish with node 1, and that failure reads as a fault-tolerance finding
when it is really a declaration bug.
"""

import json
import subprocess

import pytest

from tests.conftest import cluster_mode, region_mode

pytestmark = [pytest.mark.region, pytest.mark.cluster]

REGIONS = ("eu", "us")


@pytest.fixture(autouse=True)
def _requires_both_profiles():
    if not (region_mode() and cluster_mode()):
        pytest.fail("this test needs IOT_REGION=1 and IOT_CLUSTER=1 together")


def _quorum_members(vhost: str, queue: str) -> list[dict]:
    """One dict per Raft member.

    Shape pinned against rabbitmq:4.3.4, which returns a JSON list:
        [{"Node Name": "rabbit@rabbit1", "Raft State": "leader",
          "Membership": "voter", "Last Log Index": 2, ...}]
    """
    proc = subprocess.run(
        ["docker", "exec", "iot-rabbitmq", "rabbitmq-queues", "-q",
         "--formatter", "json", "--vhost", vhost, "quorum_status", queue],
        capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, f"{proc.stdout}\n{proc.stderr}"
    members = json.loads(proc.stdout)
    assert isinstance(members, list), members
    return members


def test_region_queues_are_three_member_quorum_groups(stack):
    for region in REGIONS:
        members = _quorum_members(region, f"telemetry.{region}.q")
        names = sorted(m["Node Name"] for m in members)
        assert names == ["rabbit@rabbit1", "rabbit@rabbit2", "rabbit@rabbit3"], (
            f"telemetry.{region}.q members are {names}, not all three nodes. The queue "
            "was declared before the peers joined; see HANDOFF bite #6 and remediate "
            f"with `rabbitmq-queues grow rabbit@rabbitN all --vhost {region}`"
        )
        assert sum(1 for m in members if m["Raft State"] == "leader") == 1, members


def test_region_dead_letter_queues_are_also_replicated(stack):
    for region in REGIONS:
        members = _quorum_members(region, "dlq")
        assert len(members) == 3, f"{region}/dlq has {len(members)} member(s), not 3"
