"""Experiment A: stop InfluxDB while devices publish; prove the queue buffered it all.

Predicted: zero loss. Telegraf's max_undelivered_messages = 1000 engages backpressure
after ~100s at 10 msg/s, well before metric_buffer_limit = 10000 could overflow, so
unacknowledged messages stay owned by the broker for the whole outage.
"""

import time
from datetime import datetime, timezone

import pytest

from sim.devices.payload import default_specs
from tests.conftest import flux_range_start
from tests.experiments.conftest import drain_and_fetch, sequence_report, start_sim

pytestmark = [pytest.mark.stack, pytest.mark.experiment]

DEVICES = 5
RATE_HZ = 2.0
CLEAN_S = 20.0
OUTAGE_S = 60.0
TAIL_S = 20.0


def test_influxdb_outage_loses_no_messages(
    docker_control, gauge_recorder, results_dir, influx_query, rabbit_get
):
    run_id = f"expA{int(time.time()) % 100000}"
    specs = default_specs(DEVICES, region="eu", plant="plant1")
    started_at = datetime.now(timezone.utc)
    dlq_baseline = rabbit_get("/queues/%2F/dlq").json()["messages"]

    gauge_recorder.mark("sim_start")
    future = start_sim(specs, RATE_HZ, CLEAN_S + OUTAGE_S + TAIL_S, run_id)

    time.sleep(CLEAN_S)
    gauge_recorder.mark("influxdb_stop")
    docker_control.stop("influxdb", timeout=0)

    time.sleep(OUTAGE_S)
    gauge_recorder.mark("influxdb_start")
    docker_control.start("influxdb")
    gauge_recorder.mark("influxdb_healthy")

    published = future.result(timeout=300)
    gauge_recorder.mark("sim_done")
    expected_total = sum(published.values())

    start = flux_range_start(started_at)
    drain = drain_and_fetch(influx_query, run_id, start, expected_total, gauge_recorder=gauge_recorder)
    rows = drain.rows
    gauge_recorder.mark("drained")

    report = sequence_report(rows, published)
    dlq_final = rabbit_get("/queues/%2F/dlq").json()["messages"]
    peak_ready = gauge_recorder.peak("telemetry_ready")

    results_dir(
        "A-influx-outage",
        {
            "run_id": run_id,
            "arm": "telegraf",
            "config": {
                "devices": DEVICES, "rate_hz": RATE_HZ,
                "clean_s": CLEAN_S, "outage_s": OUTAGE_S, "tail_s": TAIL_S,
            },
            "published_counts": published,
            "published_total": expected_total,
            "influx_total": report["total_rows"],
            "gaps": report["gaps"],
            "duplicates": report["duplicates"],
            "peak_messages_ready": peak_ready,
            "peak_messages_unacked": gauge_recorder.peak("telemetry_unacked"),
            "dlq_baseline": dlq_baseline,
            "dlq_final": dlq_final,
            **drain.as_result_fields(),
            "verdict": "no-loss" if not report["gaps"] else "loss",
            "timeline": gauge_recorder.timeline(),
        },
    )

    # Guard: if the queue never grew, the outage did not bite and the loss numbers
    # below prove nothing about durability.
    assert peak_ready > 0, "telemetry.q never grew; the outage did not engage"

    assert report["gaps"] == {}, f"sequence gaps: {report['gaps']}"
    assert report["total_rows"] == expected_total, (
        f"expected {expected_total} points, got {report['total_rows']}"
    )
    assert dlq_final == dlq_baseline, (
        f"DLQ grew from {dlq_baseline} to {dlq_final}; messages were dead-lettered"
    )
