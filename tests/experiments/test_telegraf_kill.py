"""Experiment B: kill Telegraf during an InfluxDB outage, two arms.

Predicted: zero loss in both arms. AMQP unacknowledged messages are requeued when the
consumer's channel dies, so the in-flight batch returns to the queue rather than
vanishing. The informative quantity is the difference between the arms — whether a
graceful SIGTERM flushes anything an abrupt SIGKILL does not.
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
BEFORE_KILL_S = 20.0
AFTER_KILL_S = 40.0
TAIL_S = 20.0


def _run_arm(arm, docker_control, gauge_recorder, results_dir, influx_query, rabbit_get):
    run_id = f"expB{arm[:3]}{int(time.time()) % 100000}"
    specs = default_specs(DEVICES, region="eu", plant="plant1")
    started_at = datetime.now(timezone.utc)
    dlq_baseline = rabbit_get("/queues/%2F/dlq").json()["messages"]
    total_s = CLEAN_S + BEFORE_KILL_S + AFTER_KILL_S + TAIL_S

    gauge_recorder.mark("sim_start")
    future = start_sim(specs, RATE_HZ, total_s, run_id)

    time.sleep(CLEAN_S)
    gauge_recorder.mark("influxdb_stop")
    docker_control.stop("influxdb", timeout=0)

    time.sleep(BEFORE_KILL_S)
    # latest(), not peak(): this is the in-flight batch size at the moment of
    # takedown, which is the number the assignment's ack-semantics question turns on.
    unacked_at_takedown = gauge_recorder.latest("telemetry_unacked")
    ready_at_takedown = gauge_recorder.latest("telemetry_ready")
    gauge_recorder.mark(f"telegraf_down_{arm}")
    if arm == "abrupt":
        docker_control.kill("telegraf")
    else:
        docker_control.stop("telegraf", timeout=30)

    # The requeue proof: with the consumer's channel gone, unacknowledged messages
    # must return to the ready state. Sample after two poll intervals plus slack.
    time.sleep(8)
    unacked_after_takedown = gauge_recorder.latest("telemetry_unacked")
    ready_after_takedown = gauge_recorder.latest("telemetry_ready")
    gauge_recorder.mark("requeue_sampled")

    time.sleep(AFTER_KILL_S - 8)
    gauge_recorder.mark("influxdb_start")
    docker_control.start("influxdb")
    gauge_recorder.mark("telegraf_start")
    docker_control.start("telegraf")

    published = future.result(timeout=420)
    gauge_recorder.mark("sim_done")
    expected_total = sum(published.values())

    start = flux_range_start(started_at)
    drain = drain_and_fetch(influx_query, run_id, start, expected_total, timeout_s=240, gauge_recorder=gauge_recorder)
    rows = drain.rows
    gauge_recorder.mark("drained")

    report = sequence_report(rows, published)
    dlq_final = rabbit_get("/queues/%2F/dlq").json()["messages"]

    results_dir(
        "B-telegraf-kill",
        {
            "run_id": run_id,
            "arm": arm,
            "config": {
                "devices": DEVICES, "rate_hz": RATE_HZ, "clean_s": CLEAN_S,
                "before_kill_s": BEFORE_KILL_S, "after_kill_s": AFTER_KILL_S,
                "tail_s": TAIL_S,
            },
            "published_counts": published,
            "published_total": expected_total,
            "influx_total": report["total_rows"],
            "gaps": report["gaps"],
            "duplicates": report["duplicates"],
            "unacked_at_takedown": unacked_at_takedown,
            "ready_at_takedown": ready_at_takedown,
            "unacked_after_takedown": unacked_after_takedown,
            "ready_after_takedown": ready_after_takedown,
            "requeued_messages": ready_after_takedown - ready_at_takedown,
            "peak_messages_ready": gauge_recorder.peak("telemetry_ready"),
            "peak_messages_unacked": gauge_recorder.peak("telemetry_unacked"),
            "dlq_baseline": dlq_baseline,
            "dlq_final": dlq_final,
            **drain.as_result_fields(),
            "verdict": "no-loss" if not report["gaps"] else "loss",
            "timeline": gauge_recorder.timeline(),
        },
    )

    assert gauge_recorder.peak("telemetry_ready") > 0, "telemetry.q never grew"
    assert unacked_at_takedown > 0, (
        "messages_unacknowledged never rose; Telegraf had no in-flight batch to lose, "
        "so this arm proves nothing about ack semantics"
    )
    assert unacked_after_takedown < unacked_at_takedown, (
        f"{arm} arm: unacknowledged stayed at {unacked_after_takedown} after the consumer "
        "died; the in-flight batch was not released back to the queue"
    )
    assert report["gaps"] == {}, f"sequence gaps in {arm} arm: {report['gaps']}"
    assert report["total_rows"] >= expected_total, (
        f"{arm} arm lost messages: expected {expected_total}, got {report['total_rows']}"
    )
    assert dlq_final == dlq_baseline, f"DLQ grew from {dlq_baseline} to {dlq_final}"


def test_abrupt_kill_mid_outage_loses_no_messages(
    docker_control, gauge_recorder, results_dir, influx_query, rabbit_get
):
    _run_arm("abrupt", docker_control, gauge_recorder, results_dir, influx_query, rabbit_get)


def test_graceful_stop_mid_outage_loses_no_messages(
    docker_control, gauge_recorder, results_dir, influx_query, rabbit_get
):
    _run_arm("graceful", docker_control, gauge_recorder, results_dir, influx_query, rabbit_get)
