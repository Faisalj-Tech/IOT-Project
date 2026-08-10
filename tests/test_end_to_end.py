import asyncio
import time
import uuid

import pytest

from sim.devices.payload import default_specs
from sim.devices.runner import run_devices
from tests.conftest import fetch_seqs

pytestmark = pytest.mark.stack


def test_telemetry_reaches_influxdb_with_no_sequence_gaps(influx_query):
    run_id = uuid.uuid4().hex[:8]
    specs = default_specs(2, region="eu", plant="plant1")

    published = asyncio.run(
        run_devices(
            specs,
            rate_hz=5.0,
            duration_s=5.0,
            run_id=run_id,
            host="localhost",
            port=1883,
            username="device",
            password="devicepass",
        )
    )
    expected_total = sum(published.values())
    assert expected_total >= 50, published

    rows: list[dict] = []
    # Telegraf flushes every 10s; allow three flush cycles before giving up.
    for _ in range(35):
        rows = fetch_seqs(influx_query, run_id)
        if len(rows) >= expected_total:
            break
        time.sleep(1)

    assert len(rows) == expected_total, f"expected {expected_total}, got {len(rows)}"

    for spec in specs:
        device_seqs = sorted(int(r["_value"]) for r in rows if r["device"] == spec.device)
        assert device_seqs == list(range(1, published[spec.device] + 1)), (
            f"sequence gap for {spec.device}: {device_seqs[:20]}"
        )


def test_tags_survive_the_pipeline(influx_query):
    run_id = uuid.uuid4().hex[:8]
    specs = default_specs(1, region="eu", plant="plant1")
    asyncio.run(
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

    rows: list[dict] = []
    for _ in range(35):
        rows = fetch_seqs(influx_query, run_id)
        if rows:
            break
        time.sleep(1)

    assert rows, "no telemetry reached InfluxDB"
    row = rows[0]
    assert row["region"] == "eu"
    assert row["plant"] == "plant1"
    assert row["device"] == "press-01"
    assert row["metric"] == "temp"
    assert row["unit"] == "C"
    assert row["run_id"] == run_id
