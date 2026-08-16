"""Experiment E: the same scenarios, drained by the ack-after-write consumer.

Predicted: identical zero-loss on A and B, but a different D outcome. The consumer
decides a message's fate only once the write outcome is known, so a fatally rejected
message is dead-lettered rather than dropped after an ack.
"""

import json
import time
from datetime import datetime, timezone

import pytest

from sim.devices.payload import default_specs
from tests.conftest import fetch_seqs, flux_range_start
from tests.experiments.conftest import (
    drain_and_fetch, publish_raw, sequence_report, start_sim,
)

pytestmark = [pytest.mark.stack, pytest.mark.experiment]

DEVICES = 5
RATE_HZ = 2.0
CLEAN_S = 20.0
OUTAGE_S = 60.0
TAIL_S = 20.0


def test_consumer_arm_survives_an_influxdb_outage(
    consumer_stack, docker_control, gauge_recorder, results_dir, influx_query, rabbit_get
):
    run_id = f"expEa{int(time.time()) % 100000}"
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

    published = future.result(timeout=300)
    expected_total = sum(published.values())
    drain = drain_and_fetch(
        influx_query, run_id, flux_range_start(started_at), expected_total, timeout_s=240, gauge_recorder=gauge_recorder
    )
    rows = drain.rows
    report = sequence_report(rows, published)
    dlq_final = rabbit_get("/queues/%2F/dlq").json()["messages"]

    results_dir(
        "E-consumer-influx-outage",
        {
            "run_id": run_id,
            "arm": "ack-after-write",
            "config": {"devices": DEVICES, "rate_hz": RATE_HZ,
                       "clean_s": CLEAN_S, "outage_s": OUTAGE_S, "tail_s": TAIL_S},
            "published_counts": published,
            "published_total": expected_total,
            "influx_total": report["total_rows"],
            "gaps": report["gaps"],
            "duplicates": report["duplicates"],
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
    assert report["gaps"] == {}, f"consumer arm lost messages: {report['gaps']}"
    assert report["total_rows"] >= expected_total


def test_consumer_arm_survives_being_killed_mid_outage(
    consumer_stack, docker_control, gauge_recorder, results_dir, influx_query, rabbit_get
):
    run_id = f"expEb{int(time.time()) % 100000}"
    specs = default_specs(DEVICES, region="eu", plant="plant1")
    started_at = datetime.now(timezone.utc)

    gauge_recorder.mark("sim_start")
    future = start_sim(specs, RATE_HZ, CLEAN_S + OUTAGE_S + TAIL_S, run_id)

    time.sleep(CLEAN_S)
    docker_control.stop("influxdb", timeout=0)
    time.sleep(20)
    unacked_at_kill = gauge_recorder.peak("telemetry_unacked")
    gauge_recorder.mark("consumer_kill")
    docker_control.kill("consumer")
    time.sleep(20)
    docker_control.start("influxdb")
    docker_control.start("consumer")

    published = future.result(timeout=420)
    expected_total = sum(published.values())
    drain = drain_and_fetch(
        influx_query, run_id, flux_range_start(started_at), expected_total, timeout_s=240, gauge_recorder=gauge_recorder
    )
    rows = drain.rows
    report = sequence_report(rows, published)

    results_dir(
        "E-consumer-kill",
        {
            "run_id": run_id,
            "arm": "ack-after-write",
            "published_counts": published,
            "published_total": expected_total,
            "influx_total": report["total_rows"],
            "gaps": report["gaps"],
            "duplicates": report["duplicates"],
            "unacked_at_takedown": unacked_at_kill,
            **drain.as_result_fields(),
            "verdict": "no-loss" if not report["gaps"] else "loss",
            "timeline": gauge_recorder.timeline(),
        },
    )

    assert report["gaps"] == {}, f"consumer arm lost messages when killed: {report['gaps']}"
    assert report["total_rows"] >= expected_total


def test_consumer_arm_dead_letters_a_poison_message(
    consumer_stack, gauge_recorder, results_dir, influx_query, rabbit_get
):
    """The comparison's decisive case: the same poison message Telegraf mishandled."""
    dlq_before = rabbit_get("/queues/%2F/dlq").json()["messages"]
    payload = json.dumps(
        {
            "ts": "2026-08-10T12:00:00.000Z", "region": "eu", "plant": "plant1",
            "device": "poison-01", "metric": "temp", "value": "not-a-number",
            "unit": "C", "seq": 1, "run_id": "poisonE1",
        }
    ).encode()

    publish_raw("region/eu/plant1/poison-01/temp", payload, count=5)
    time.sleep(20)

    dlq_after = rabbit_get("/queues/%2F/dlq").json()["messages"]
    ready_after = rabbit_get("/queues/%2F/telemetry.q").json()["messages_ready"]
    landed = bool(fetch_seqs(influx_query, "poisonE1", start="-10m"))

    results_dir(
        "E-consumer-poison",
        {
            "run_id": "poisonE1",
            "arm": "ack-after-write",
            "messages_published": 5,
            "dlq_before": dlq_before,
            "dlq_after": dlq_after,
            "dlq_delta": dlq_after - dlq_before,
            "ready_after": ready_after,
            "landed_in_influx": landed,
            "outcome": "nack-to-dlq" if dlq_after > dlq_before else "not-dead-lettered",
            "verdict": "nack-to-dlq" if dlq_after > dlq_before else "not-dead-lettered",
            "timeline": gauge_recorder.timeline(),
        },
    )

    assert not landed, "a message with a string value reached InfluxDB"
    assert dlq_after - dlq_before == 5, (
        f"expected 5 messages dead-lettered, DLQ went {dlq_before} -> {dlq_after}"
    )
    assert ready_after == 0, (
        "poison messages are still queued: the consumer is requeueing what it should "
        "dead-letter, which is an infinite redelivery loop"
    )
