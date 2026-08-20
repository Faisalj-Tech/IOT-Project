"""R5: the per-region policies exist, differ, and apply only where they should.

    IOT_REGION=1 KEEP_STACK=1 .venv/Scripts/python.exe -m pytest \
        tests/experiments/test_region_rules.py -m region -v
"""

import pytest

from tests.conftest import region_mode

pytestmark = [pytest.mark.region, pytest.mark.stack]

EXPECTED = {
    "eu": {"name": "eu-limits", "message-ttl": 604800000, "max-length": 100000},
    "us": {"name": "us-limits", "message-ttl": 86400000, "max-length": 10000},
}


@pytest.fixture(autouse=True)
def _requires_region_profile():
    if not region_mode():
        pytest.fail("region tests need IOT_REGION=1")


def test_each_region_carries_its_own_policy(stack, rabbit_get):
    policies = rabbit_get("/policies").json()
    by_vhost = {p["vhost"]: p for p in policies}
    for region, expected in EXPECTED.items():
        policy = by_vhost[region]
        assert policy["name"] == expected["name"]
        assert policy["definition"]["message-ttl"] == expected["message-ttl"]
        assert policy["definition"]["max-length"] == expected["max-length"]
    assert "/" not in by_vhost, (
        "vhost / must stay policy-free or every published Phase 1-3 result changes meaning"
    )


def test_the_two_regions_rules_actually_differ(stack, rabbit_get):
    policies = {p["vhost"]: p["definition"] for p in rabbit_get("/policies").json()}
    assert policies["eu"] != policies["us"]


def test_the_policy_is_applied_to_that_regions_queue(stack, rabbit_get):
    """A declared policy that matches nothing proves nothing; read the effective
    definition back off the queue itself."""
    for region, expected in EXPECTED.items():
        queue = rabbit_get(f"/queues/{region}/telemetry.{region}.q").json()
        effective = queue.get("effective_policy_definition", {})
        assert effective.get("max-length") == expected["max-length"], queue.get("policy")
        assert queue.get("policy") == expected["name"]


def test_the_default_vhost_queue_carries_no_policy(stack, rabbit_get):
    queue = rabbit_get("/queues/%2F/telemetry.q").json()
    assert not queue.get("policy")
