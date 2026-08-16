"""Experiment C: restart RabbitMQ while devices publish.

Predicted: zero loss, but measurable time dilation and non-zero duplication. ADR-0002
guarantees seq advances only after a successful publish and that the loop runs to
ceil(rate_hz * duration_s), so an outage is made up afterwards rather than lost. QoS 1
is at-least-once, so a publish that lands but loses its PUBACK is retried under the same
seq and, because build_payload regenerates ts and seq is a field rather than a tag,
persists twice.

This experiment must NOT reuse experiment A's exact-equality assertion. Legitimate
at-least-once duplication would fail it and be misread as a defect.
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
OUTAGE_S = 45.0
TAIL_S = 20.0
# The default max_reconnects=5 with 0.5s doubling backoff tolerates only ~15.5s of
# outage, which is shorter than a RabbitMQ restart: at the default the simulator
# raises and the run dies. That inadequacy is itself a finding, recorded below.
# Verified in Task 7 Step 3: at max_reconnects=5 this experiment fails with
# aiomqtt.exceptions.MqttError: [WinError 10061] No connection could be made because the target machine actively refused it.
# Recorded as a finding in the reliability report.
DEFAULT_MAX_RECONNECTS = 5
RECONNECT_BUDGET = 12


def test_broker_restart_costs_time_and_duplicates_but_not_messages(
    docker_control, gauge_recorder, results_dir, influx_query, rabbit_get
):
    run_id = f"expC{int(time.time()) % 100000}"
    specs = default_specs(DEVICES, region="eu", plant="plant1")
    started_at = datetime.now(timezone.utc)
    nominal_s = CLEAN_S + OUTAGE_S + TAIL_S
    wall_start = time.time()

    gauge_recorder.mark("sim_start")
    future = start_sim(specs, RATE_HZ, nominal_s, run_id, max_reconnects=RECONNECT_BUDGET)

    time.sleep(CLEAN_S)
    gauge_recorder.mark("rabbitmq_stop")
    docker_control.stop("rabbitmq", timeout=0)

    time.sleep(OUTAGE_S)
    gauge_recorder.mark("rabbitmq_start")
    restart_start = time.time()
    docker_control.start("rabbitmq")
    restart_duration = time.time() - restart_start
    gauge_recorder.mark("rabbitmq_healthy")

    published = future.result(timeout=600)
    wall_clock_s = time.time() - wall_start
    gauge_recorder.mark("sim_done")
    expected_total = sum(published.values())

    start = flux_range_start(started_at)
    drain = drain_and_fetch(influx_query, run_id, start, expected_total, timeout_s=240, stable_polls_limit=18, gauge_recorder=gauge_recorder)
    rows = drain.rows
    report = sequence_report(rows, published)
    duplicate_total = sum(report["duplicates"].values())

    results_dir(
        "C-broker-restart",
        {
            "run_id": run_id,
            "arm": "telegraf",
            "config": {
                "devices": DEVICES, "rate_hz": RATE_HZ, "clean_s": CLEAN_S,
                "outage_s": OUTAGE_S, "tail_s": TAIL_S,
                "max_reconnects": RECONNECT_BUDGET,
                "default_max_reconnects": DEFAULT_MAX_RECONNECTS,
            },
            "published_counts": published,
            "published_total": expected_total,
            "influx_total": report["total_rows"],
            "gaps": report["gaps"],
            "duplicates": report["duplicates"],
            "duplicate_total": duplicate_total,
            "duplicate_rate": duplicate_total / expected_total if expected_total else 0,
            "nominal_duration_s": nominal_s,
            "wall_clock_s": wall_clock_s,
            "time_dilation_s": wall_clock_s - nominal_s,
            "broker_restart_s": restart_duration,
            **drain.as_result_fields(),
            "verdict": "no-loss" if not report["gaps"] else "loss",
            "timeline": gauge_recorder.timeline(),
        },
    )

    assert report["gaps"] == {}, (
        f"sequence gaps after broker restart: {report['gaps']}. ADR-0002's guarantees "
        "predict none; a gap here means the simulator's seq handling regressed"
    )
    assert report["total_rows"] >= expected_total, (
        f"lost messages: expected at least {expected_total}, got {report['total_rows']}"
    )
    assert wall_clock_s > nominal_s, (
        "run finished within its nominal duration despite a broker outage, so the "
        "outage did not engage"
    )
