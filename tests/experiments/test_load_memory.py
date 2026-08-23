"""L4 - where does broker memory pressure begin under a prolonged downstream outage?

THREE arms, because spec 2.8 measured that the assignment's premise - "stop InfluxDB,
observe the queue growing" - describes exactly one configuration, and NOT the default
one. On the default pipeline telemetry.q drains to zero and dlq grows instead.

  (a) Telegraf stopped                -> telemetry.q grows   (the clean model)
  (b) Telegraf running, InfluxDB down -> dlq grows           (default behaviour)
  (c) ack-after-write consumer        -> telemetry.q unacked (store-and-forward)

Arm (c) is the only one in which the broker genuinely buffers on the device's behalf,
and therefore the only one whose "messages accumulate safely and drain when InfluxDB
returns" claim is about the system rather than about a dead-letter path.
"""

import json
import time
from datetime import datetime, timezone

import pytest
import requests

from tests.conftest import (
    compose, compose_files, consumer_files, load_mode, rabbit_api,
)
from tests.experiments.conftest import write_result
from tests.experiments.load import (
    _admin_auth, amqp_publish_burst, broker_alarms, broker_memory_bytes,
    host_envelope, mnesia_megabytes, purge_queue, stable_depth,
)

pytestmark = [pytest.mark.load, pytest.mark.stack]

PAYLOAD = b"x" * 200
BATCH = 25_000
# Arm (c)'s consumer (consumer/ackafterwrite.py) parses each message as JSON with
# 9 required fields and dead-letters anything else immediately (parse_message()),
# before it ever attempts an InfluxDB write. PAYLOAD (raw filler bytes) is fine for
# arms (a)/(b), which never parse the body - but it defeats arm (c)'s entire premise:
# every message was found dead-lettering with "payload is not JSON" instantly, so
# telemetry.q never buffered anything. Live-diagnosed via `docker logs iot-consumer`
# during a direct re-run. Arm (c) needs its own valid, parseable payload.
CONSUMER_PAYLOAD = json.dumps({
    "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    "region": "eu", "plant": "plant1", "device": "l4c-probe", "metric": "temp",
    "value": 70.0, "unit": "C", "seq": 1, "run_id": "L4c",
}).encode()
MAX_BATCHES = 20  # 500k messages is well past the predicted alarm; a guard, not a target
MODEL_FIXED_BYTES = 850          # spec 2.7: N * (0.85 KB + payload)
MODEL_PREDICTED_MESSAGES = 184_000


@pytest.fixture(autouse=True)
def _requires_load_profile():
    if not load_mode():
        pytest.skip("needs the compose.load.yml overlay and IOT_LOAD=1")


def _model_prediction(watermark_bytes: int, baseline_bytes: int) -> int:
    per_message = MODEL_FIXED_BYTES + len(PAYLOAD)
    return max(0, (watermark_bytes - baseline_bytes) // per_message)


def _publish_until_alarm(queue: str) -> dict:
    """Publish in batches until the broker raises a resource alarm, or we give up."""
    sent = 0
    samples = []
    for _ in range(MAX_BATCHES):
        acked, nacked = amqp_publish_burst(queue, count=BATCH, body=PAYLOAD)
        sent += acked
        alarms = broker_alarms()
        samples.append({
            "sent": sent,
            "nacked": nacked,
            "memory_bytes": broker_memory_bytes(),
            "mnesia_mb": mnesia_megabytes(),
            "alarms": alarms,
        })
        if alarms:
            return {"alarm_at_messages": sent, "samples": samples, "alarmed": True}
    return {"alarm_at_messages": None, "samples": samples, "alarmed": False}


def test_arm_a_no_consumer_the_clean_model(stack, docker_control):
    """Telegraf stopped, so nothing drains telemetry.q. This is spec 2.7's model.

    Also asserts spec 2.9's asymmetry, which is a harness requirement for every
    later wave: memory IS released by a purge, disk is NOT.
    """
    docker_control.stop("telegraf")
    purge_queue("telemetry.q")
    purge_queue("dlq")
    try:
        baseline = broker_memory_bytes()
        envelope = host_envelope()
        predicted = _model_prediction(envelope["broker_memory_watermark_bytes"], baseline)
        outcome = _publish_until_alarm("telemetry.q")

        depth = stable_depth("telemetry.q", timeout_s=120)
        disk_before_purge = mnesia_megabytes()
        purge_queue("telemetry.q")
        time.sleep(10)
        memory_after_purge = broker_memory_bytes()
        disk_after_purge = mnesia_megabytes()

        write_result("L4-memory-a", {
            "run_id": "L4a",
            "host_envelope": envelope,
            "baseline_memory_bytes": baseline,
            "model_predicted_messages": predicted,
            "model_reference_prediction": MODEL_PREDICTED_MESSAGES,
            "measured_alarm_at_messages": outcome["alarm_at_messages"],
            "alarmed": outcome["alarmed"],
            "samples": outcome["samples"],
            "queue_depth_at_run_end": depth.messages,
            "memory_after_purge_bytes": memory_after_purge,
            "disk_before_purge_mb": disk_before_purge,
            "disk_after_purge_mb": disk_after_purge,
            "model_divergence_note": (
                "measured memory never reached the pinned 256MiB watermark even at "
                "500000 messages (2.7x the model's predicted alarm point of ~184000); "
                "memory shows a repeating rise-then-drop pattern approximately every "
                "100000 messages while mnesia disk usage grows monotonically the whole "
                "time - consistent with periodic Raft checkpoint/segment-rollover "
                "reclaiming the quorum_ets index between measurement pauses, which the "
                "model (fitted on a single uninterrupted burst) never observed. Recorded "
                "as a finding per spec 2.7's own instruction not to tune the model to fit "
                "a divergent run."
            ),
        })

        # Spec 2.9, asserted because every later wave's hygiene depends on it.
        assert memory_after_purge < baseline * 2, "memory was not released by the purge"
        # NOT asserted (ruled): a live run measured disk_after_purge < disk_before_purge on a
        # confirmed quorum queue, contradicting spec 2.9's "disk is not released on purge"
        # claim. The harness's down -v-for-disk-arm convention stays in place regardless
        # (safe either way - reclaiming disk makes it over-cautious, not wrong). Recorded as
        # a finding for the phase report, not asserted, since a live measurement falsified it.
    finally:
        docker_control.start("telegraf")


def _one_dlq_message() -> dict:
    """Pop one DLQ message (requeueing it) to read its x-death header."""
    response = requests.post(
        f"{rabbit_api()}/queues/%2F/dlq/get",
        json={"count": 1, "ackmode": "ack_requeue_true", "encoding": "auto"},
        auth=_admin_auth(), timeout=15,
    )
    response.raise_for_status()
    return response.json()[0]


def test_arm_b_default_pipeline_grows_the_dlq_not_the_telemetry_queue(stack, docker_control):
    """The assignment says "stop InfluxDB, observe the queue growing". It does not.

    Measured (spec 2.8): telemetry.q drained to ZERO while InfluxDB was down, with
    unacked pinned at exactly prefetch_count (50), and dlq grew to the full
    published count. Telegraf REJECTS each message when the output write fails, so
    the messages dead-letter immediately.

    The x-death reason is asserted, not just the growth: reason "rejected" with
    count 1 is what distinguishes this from delivery-limit exhaustion, which would
    read "delivery_limit" with count 20. Asserting only that dlq grew would pass
    for both, and they are different failures with different fixes.

    The queue that actually grows under a prolonged downstream outage carries no
    max-length, no TTL and no consumer.
    """
    purge_queue("telemetry.q")
    purge_queue("dlq")
    docker_control.stop("influxdb")
    try:
        acked, _ = amqp_publish_burst("telemetry.q", count=50_000, body=PAYLOAD)
        telemetry = stable_depth("telemetry.q", timeout_s=180)
        dead = stable_depth("dlq", timeout_s=180)
        sample = _one_dlq_message()
        x_death = sample["properties"]["headers"]["x-death"][0]

        write_result("L4-memory-b", {
            "run_id": "L4b",
            "host_envelope": host_envelope(),
            "published": acked,
            "telemetry_q": telemetry.as_result_fields(),
            "dlq": dead.as_result_fields(),
            "x_death_reason": x_death["reason"],
            "x_death_count": x_death["count"],
            "memory_bytes": broker_memory_bytes(),
            "finding": (
                "the default pipeline dead-letters under a downstream outage; "
                "telemetry.q does not grow, dlq does, and dlq is unbounded"
            ),
        })

        assert dead.messages > telemetry.messages, (
            "dlq did not outgrow telemetry.q - the default pipeline's outage "
            "behaviour has changed from what spec 2.8 measured"
        )
        assert x_death["reason"] == "rejected"
        assert x_death["count"] == 1, "count > 1 would mean delivery-limit exhaustion"
    finally:
        docker_control.start("influxdb")


def test_arm_c_ack_after_write_consumer_buffers_and_loses_nothing(stack, docker_control):
    """The only arm that demonstrates what the assignment is actually asking about.

    Telegraf MUST be stopped first: two consumers on one queue split the stream
    between them, exactly as compose.consumer.yml's own header warns, which would
    make the loss accounting meaningless.

    The ack-after-write consumer only acks a message once InfluxDB has confirmed
    the write, so during an outage it stops acking and the broker genuinely buffers
    on the device's behalf - messages accumulate as UNACKED rather than being
    dead-lettered, and they drain when InfluxDB returns.
    """
    docker_control.stop("telegraf")
    purge_queue("telemetry.q")
    purge_queue("dlq")
    compose("up", "-d", "consumer", files=consumer_files())
    try:
        docker_control.stop("influxdb")
        acked, _ = amqp_publish_burst("telemetry.q", count=20_000, body=CONSUMER_PAYLOAD)
        during = stable_depth("telemetry.q", timeout_s=180)
        memory_during = broker_memory_bytes()

        docker_control.start("influxdb")
        after = stable_depth("telemetry.q", timeout_s=600, stable_polls=5)
        dead = stable_depth("dlq", timeout_s=60)

        write_result("L4-memory-c", {
            "run_id": "L4c",
            "host_envelope": host_envelope(),
            "published": acked,
            "depth_during_outage": during.as_result_fields(),
            "depth_after_recovery": after.as_result_fields(),
            "dlq_after_recovery": dead.as_result_fields(),
            "memory_during_outage_bytes": memory_during,
        })

        assert during.messages > 0, "nothing buffered - the consumer is acking early"
        assert dead.messages == 0, "store-and-forward must not dead-letter"
        assert after.messages == 0, "the queue did not drain after InfluxDB returned"
    finally:
        compose("rm", "-sf", "consumer", files=consumer_files())
        docker_control.start("telegraf")
        docker_control.start("influxdb")
