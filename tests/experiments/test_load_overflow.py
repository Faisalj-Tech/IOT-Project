"""L3 - queue overflow behaviour under load.

Everything asserted here was measured on 2026-08-22 (spec 2.2-2.6). The important
one: the assignment's "drop-oldest vs reject-new vs dead-letter" is a TWO-way
choice on quorum queues. reject-publish-dlx is accepted by the management API,
echoed back in the queue's arguments, and IGNORED by the broker.
"""

import pytest

from tests.conftest import load_mode
from tests.experiments.conftest import write_result
from tests.experiments.load import (
    amqp_publish_burst, host_envelope, load_policy, mqtt_publish_burst,
    purge_queue, stable_depth,
)

pytestmark = [pytest.mark.load, pytest.mark.stack]

QUEUE = "telemetry.q"


@pytest.fixture(autouse=True)
def _requires_load_profile():
    if not load_mode():
        pytest.skip("needs the compose.load.yml overlay and IOT_LOAD=1")


@pytest.fixture(autouse=True)
def no_telegraf(stack, docker_control):
    """Stop Telegraf to prevent it from consuming test messages.

    Uses the shared docker_control fixture so that if the restart fails,
    the fixture's restore() method will handle recovery for subsequent tests.
    """
    docker_control.stop("telegraf", timeout=10)
    try:
        yield
    finally:
        docker_control.start("telegraf")


@pytest.fixture
def clean_queue():
    purge_queue(QUEUE)
    purge_queue("dlq")
    yield
    purge_queue(QUEUE)
    purge_queue("dlq")


def test_drop_head_dead_letters_the_overflow(stack, clean_queue):
    """drop-head + a DLX IS the assignment's "dead-letter" behaviour.

    Measured: 20 sent into a max-length 5 queue -> 5 retained, 15 dead-lettered,
    all 20 acked from the publisher's point of view.
    """
    with load_policy("l3-drophead", f"^{QUEUE}$",
                     {"max-length": 5, "overflow": "drop-head"}):
        acked, nacked = amqp_publish_burst(QUEUE, count=20, body=b"x" * 100)
    assert acked == 20 and nacked == 0
    assert stable_depth(QUEUE).messages == 5
    assert stable_depth("dlq").messages == 15


def test_reject_publish_nacks_and_does_not_dead_letter(stack, clean_queue):
    """Measured: 6 acked (max-length engages at maxlen+1), 14 nacked, 0 dead-lettered."""
    with load_policy("l3-reject", f"^{QUEUE}$",
                     {"max-length": 5, "overflow": "reject-publish"}):
        acked, nacked = amqp_publish_burst(QUEUE, count=20, body=b"x" * 100)
    assert acked == 6, "max-length admits maxlen+1 (spec 2.4)"
    assert nacked == 14
    assert stable_depth("dlq").messages == 0


def test_reject_publish_dlx_silently_behaves_as_drop_head(stack, clean_queue):
    """The API accepts it, echoes it back, and the broker ignores it (spec 2.3).

    This asserts OBSERVED BEHAVIOUR, never the configuration read back - which is
    precisely the trap: a test that trusted the echoed arguments would report a
    working third overflow mode that does not exist.
    """
    with load_policy("l3-rejectdlx", f"^{QUEUE}$",
                     {"max-length": 5, "overflow": "reject-publish-dlx"}):
        acked, nacked = amqp_publish_burst(QUEUE, count=20, body=b"x" * 100)
    assert (acked, nacked) == (20, 0), "behaved as drop-head, as measured"
    assert stable_depth(QUEUE).messages == 5
    assert stable_depth("dlq").messages == 15


def test_max_length_bytes_engages_exactly_at_the_limit(stack, clean_queue):
    """Unlike the count limit, the bytes limit does NOT admit one past (spec 2.4).

    This is the limit that actually bounds broker memory, because per-message
    memory scales with payload (spec 2.7).
    """
    with load_policy("l3-bytes", f"^{QUEUE}$",
                     {"max-length-bytes": 2000, "overflow": "reject-publish"}):
        acked, nacked = amqp_publish_burst(QUEUE, count=10, body=b"x" * 500)
    assert acked == 4, "4 x 500B = exactly 2000B"
    assert nacked == 6


def test_ttl_expiry_dead_letters(stack, clean_queue):
    """Measured: 10 published with a 3s TTL -> queue 0, DLQ 10 after 12s."""
    with load_policy("l3-ttl", f"^{QUEUE}$", {"message-ttl": 3000}):
        acked, _ = amqp_publish_burst(QUEUE, count=10, body=b"t" * 100)
        assert acked == 10
        expired = stable_depth("dlq", timeout_s=45, stable_polls=3)
    assert expired.messages == 10


def test_mqtt5_publisher_sees_quota_exceeded_and_311_sees_silence(stack, clean_queue):
    """The phase's first infrastructure requirement, as a test (spec 2.6).

    Same broker, same full queue, same QoS-1 publish: MQTT 5 gets 0x97 in ~0.2s,
    MQTT 3.1.1 gets no PUBACK at all - and it never arrives, even after the queue
    drains. Silent message loss versus a reportable rejection.
    """
    with load_policy("l3-mqtt", f"^{QUEUE}$",
                     {"max-length": 5, "overflow": "reject-publish"}):
        v5 = mqtt_publish_burst(version=5, count=10)
        v311 = mqtt_publish_burst(version=3, count=3, timeout_s=8)

    assert v5["rejected"] > 0
    assert "Quota exceeded" in v5["reason_codes"]
    assert v311["puback"] == 0, "3.1.1 must observe nothing at all"
    assert v311["timed_out"] == 3

    write_result("L3-overflow", {
        "run_id": v5["run_id"],
        "host_envelope": host_envelope(),
        "telegraf_stopped": True,
        "mqtt5": v5,
        "mqtt311": v311,
        "finding": (
            "reject-publish is observable only over MQTT 5 (0x97 Quota exceeded); "
            "over 3.1.1 it is silent message loss"
        ),
    })
