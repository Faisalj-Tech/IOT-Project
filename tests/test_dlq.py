import asyncio
import time
import uuid

import pytest

from sim.devices.payload import default_specs
from sim.devices.runner import run_devices

pytestmark = pytest.mark.stack


def test_dlq_stays_empty_during_a_healthy_run(rabbit_get):
    """Telegraf REJECTs with requeue=false when an output write fails.

    Without the dead-letter exchange those messages would vanish silently, so
    this asserts both that the DLQ exists and that a healthy run never uses it.
    """
    run_id = uuid.uuid4().hex[:8]
    specs = default_specs(2, region="eu", plant="plant1")

    asyncio.run(
        run_devices(
            specs,
            rate_hz=5.0,
            duration_s=3.0,
            run_id=run_id,
            host="localhost",
            port=1883,
            username="device",
            password="devicepass",
        )
    )

    # Give Telegraf two flush cycles to drain the queue.
    time.sleep(25)

    dlq = rabbit_get("/queues/%2F/dlq").json()
    assert dlq["messages"] == 0, f"dead-lettered messages present: {dlq['messages']}"

    telemetry = rabbit_get("/queues/%2F/telemetry.q").json()
    assert telemetry["messages"] == 0, (
        f"queue did not drain: {telemetry['messages']} messages left"
    )
