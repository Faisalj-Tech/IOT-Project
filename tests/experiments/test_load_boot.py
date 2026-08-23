"""Task 5 gate: the load overlay boots, the broker is pinned, latency has two clocks."""

import pytest
import time

from tests.conftest import load_mode
from tests.experiments.load import host_envelope, stable_depth, Swarm


pytestmark = [pytest.mark.load, pytest.mark.stack]


@pytest.fixture(autouse=True)
def _requires_load_profile():
    if not load_mode():
        pytest.skip("needs the compose.load.yml overlay and IOT_LOAD=1")


@pytest.fixture
def swarm_run():
    """Run the sim-load swarm briefly so telemetry rows exist for this module's tests.

    Nothing in this stack publishes continuously; the swarm is a one-shot burst
    this gate needs to prove the two-clock latency row actually lands.
    """
    swarm = Swarm()
    swarm.clear_reports()
    swarm.scale(1, devices=5, rate_hz=2.0, duration_s=30, run_id="boot-gate")
    time.sleep(40)
    yield
    swarm.stop()


def test_broker_memory_limit_is_pinned_not_host_derived(stack):
    """An unpinned watermark is 0.6 x host RAM - 9.76 GB on the design machine.

    A breaking point measured against that is a property of the tester's RAM and
    reproducible by nobody (spec 2.7). This asserts the committed pin took effect.
    """
    envelope = host_envelope()
    assert envelope["broker_mem_limit_bytes"] == 1024 ** 3
    watermark = envelope["broker_memory_watermark_bytes"]
    assert watermark == 256 * 1024 ** 2, f"watermark not pinned: {watermark}"


def test_broker_cpu_is_pinned(stack):
    envelope = host_envelope()
    assert envelope["broker_nano_cpus"] == 2_000_000_000


def test_telemetry_queue_is_present_and_readable(stack):
    reading = stable_depth("telemetry.q", timeout_s=30)
    assert reading.exit_condition != "absent"


def test_latency_rows_carry_both_clocks(stack, influx_query, swarm_run):
    """Spec 4.4: the row's timestamp is INGEST time and `ts` is a field carrying
    PUBLISH time, so end-to-end latency is a subtraction inside one row.

    Without the overlay, telegraf.conf sets timestamp_path = "ts" and the row
    knows only when the message was produced.
    """
    rows = influx_query(
        f'from(bucket:"telemetry") |> range(start:-10m) '
        f'|> filter(fn:(r) => r._measurement == "telemetry" and r._field == "ts") '
        f'|> limit(n:5)'
    )
    assert rows, "no rows carrying a `ts` field - the load telegraf overlay is not active"
    for row in rows:
        assert isinstance(row["_value"], str), "ts should be a string field"
        assert row["_time"] is not None
