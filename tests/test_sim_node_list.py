"""The simulator must move to the next node when the one it is using dies.

resolve_nodes is pure and tested without a broker. The failover behaviour is
tested against the live single-node stack by putting a closed port first in the
list: if the client did not advance, the run would exhaust max_reconnects and
raise instead of publishing.
"""

import asyncio
import time
from datetime import datetime, timezone

import pytest

from sim.devices.payload import default_specs
from sim.devices.runner import resolve_nodes, run_devices
from tests.conftest import fetch_seqs, flux_range_start

# Port 1 is reserved (tcpmux) and nothing listens on it, so a connection there
# is refused immediately rather than hanging until a timeout.
DEAD_NODE = ("localhost", 1)
LIVE_NODE = ("localhost", 1883)


def test_resolve_nodes_defaults_to_the_single_host_port_pair():
    assert resolve_nodes("localhost", 1883, None) == [("localhost", 1883)]


def test_resolve_nodes_prefers_an_explicit_list():
    given = [("a", 1), ("b", 2)]
    assert resolve_nodes("localhost", 1883, given) == given


def test_resolve_nodes_rejects_an_empty_list():
    with pytest.raises(ValueError):
        resolve_nodes("localhost", 1883, [])


@pytest.mark.stack
def test_publisher_fails_over_to_the_next_node(stack, influx_query):
    """With a dead endpoint first in the list, publishing can only succeed if the
    client advanced to the second entry."""
    run_id = f"nodelist{int(time.time()) % 100000}"
    started_at = datetime.now(timezone.utc)
    specs = default_specs(1, region="eu", plant="plant1")

    published = asyncio.run(
        run_devices(
            specs, rate_hz=5.0, duration_s=3.0, run_id=run_id,
            nodes=[DEAD_NODE, LIVE_NODE], max_reconnects=5,
        )
    )
    expected = sum(published.values())
    assert expected >= 15, published

    start = flux_range_start(started_at)
    rows: list[dict] = []
    for _ in range(24):
        rows = fetch_seqs(influx_query, run_id, start=start)
        if len(rows) >= expected:
            break
        time.sleep(5)
    assert len(rows) >= expected, (
        f"published {expected} but only {len(rows)} reached InfluxDB"
    )


def test_cli_accepts_explicit_credentials():
    """The region simulators authenticate as device-eu / device-us, not as the
    Phase 1 `device` user, and they run inside containers where passing them on
    the command line is how compose supplies them."""
    from sim.devices.__main__ import parse_args

    args = parse_args(["--username", "device-eu", "--password", "devicepass-eu"])
    assert args.username == "device-eu"
    assert args.password == "devicepass-eu"


def test_cli_credentials_default_to_the_phase_one_device(monkeypatch):
    from sim.devices.__main__ import parse_args

    monkeypatch.delenv("RABBITMQ_DEVICE_USER", raising=False)
    monkeypatch.delenv("RABBITMQ_DEVICE_PASSWORD", raising=False)
    args = parse_args([])
    assert args.username == "device"
    assert args.password == "devicepass"
