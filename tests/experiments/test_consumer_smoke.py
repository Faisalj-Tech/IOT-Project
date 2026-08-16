"""The consumer arm must ingest a clean run exactly as Telegraf does."""

import time
from datetime import datetime, timezone

import pytest

from sim.devices.payload import default_specs
from tests.conftest import flux_range_start
from tests.experiments.conftest import drain_and_fetch, sequence_report, start_sim

pytestmark = [pytest.mark.stack, pytest.mark.experiment]


def test_consumer_ingests_a_clean_run(consumer_stack, influx_query, docker_control):
    assert not docker_control.is_running("telegraf"), (
        "Telegraf is still running; it would compete with the consumer for the same "
        "queue and split the message stream"
    )
    assert docker_control.is_running("consumer")

    run_id = f"csmoke{int(time.time()) % 100000}"
    specs = default_specs(2, region="eu", plant="plant1")
    started_at = datetime.now(timezone.utc)

    published = start_sim(specs, 5.0, 5.0, run_id).result(timeout=120)
    expected_total = sum(published.values())

    drain = drain_and_fetch(
        influx_query, run_id, flux_range_start(started_at), expected_total, timeout_s=120
    )
    rows = drain.rows
    report = sequence_report(rows, published)

    assert report["gaps"] == {}, f"consumer lost messages: {report['gaps']}"
    assert report["total_rows"] == expected_total
