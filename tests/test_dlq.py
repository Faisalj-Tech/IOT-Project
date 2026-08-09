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

    This test is robust to test ordering: it captures baseline queue depths before
    publishing, then asserts that THIS test's messages drain to the pre-publish state.
    This avoids order-dependent failures from other tests leaving messages in flight.
    """
    run_id = uuid.uuid4().hex[:8]
    specs = default_specs(2, region="eu", plant="plant1")

    # Capture baseline depths before this test's publish.
    dlq_baseline = rabbit_get("/queues/%2F/dlq").json()
    dlq_baseline_depth = dlq_baseline["messages"]

    telemetry_baseline = rabbit_get("/queues/%2F/telemetry.q").json()
    telemetry_baseline_depth = telemetry_baseline["messages"]

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
    # Poll until telemetry.q returns to baseline depth (proving this test's messages drained).
    deadline = time.time() + 25
    while time.time() < deadline:
        telemetry = rabbit_get("/queues/%2F/telemetry.q").json()
        if telemetry["messages"] == telemetry_baseline_depth:
            break
        time.sleep(0.5)

    # Verify final state: DLQ unchanged from baseline (nothing dead-lettered by this test)
    dlq = rabbit_get("/queues/%2F/dlq").json()
    assert dlq["messages"] == dlq_baseline_depth, (
        f"DLQ depth changed: was {dlq_baseline_depth}, now {dlq['messages']}"
    )

    # Verify telemetry queue returned to pre-publish depth.
    telemetry = rabbit_get("/queues/%2F/telemetry.q").json()
    assert telemetry["messages"] == telemetry_baseline_depth, (
        f"queue did not drain: was {telemetry_baseline_depth}, now {telemetry['messages']} messages"
    )
