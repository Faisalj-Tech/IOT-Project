import asyncio
import time
import uuid

import pytest

from sim.devices.payload import default_specs
from sim.devices.runner import run_devices

pytestmark = pytest.mark.stack


def publish_count(rabbit_get, name: str) -> int:
    """Queue depth measurement for Task 2.

    Returns the `messages` field (queue depth). This is safe for Task 2 because
    no consumer (Telegraf) exists yet, so depth uniquely measures what arrived.
    From Task 3 onward, Telegraf will drain the queue, making depth unreliable
    for measuring total arrivals — at that point, this should be replaced with
    a cumulative counter from message_stats or an external monitoring system.
    """
    queue = rabbit_get(f"/queues/%2F/{name}").json()
    return queue.get("messages", 0)


def test_published_messages_land_in_telemetry_queue(rabbit_get):
    before = publish_count(rabbit_get, "telemetry.q")
    run_id = uuid.uuid4().hex[:8]
    specs = default_specs(2, region="eu", plant="plant1")

    published = asyncio.run(
        run_devices(
            specs,
            rate_hz=5.0,
            duration_s=2.0,
            run_id=run_id,
            host="localhost",
            port=1883,
            username="device",
            password="devicepass",
        )
    )

    total = sum(published.values())
    assert total >= 10, published

    after = before
    # Management stats refresh every 5s by default; poll rather than sleep once.
    for _ in range(30):
        after = publish_count(rabbit_get, "telemetry.q")
        if after - before >= total:
            break
        time.sleep(0.5)

    assert after - before == total


def test_wrong_credentials_are_rejected():
    specs = default_specs(1, region="eu", plant="plant1")
    with pytest.raises(Exception):
        asyncio.run(
            run_devices(
                specs,
                rate_hz=1.0,
                duration_s=1.0,
                run_id="badauth",
                host="localhost",
                port=1883,
                username="device",
                password="wrong-password",
                max_reconnects=0,
            )
        )
