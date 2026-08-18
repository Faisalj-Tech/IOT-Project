"""drain_and_fetch must exit on broker state, not on a poll-count guess.

ADR-0012: the poll-count heuristic reported loss twice in Phase 2 when a slow
drain plateaued mid-backlog. The primary exit condition is now the broker
reporting the queue empty, which cannot plateau.
"""

import time
from datetime import datetime, timezone

import pytest

from sim.devices.payload import default_specs
from tests.conftest import flux_range_start
from tests.experiments.conftest import DrainResult, drain_and_fetch, start_sim

pytestmark = [pytest.mark.stack, pytest.mark.experiment]


def test_drain_result_reports_its_exit_condition_and_elapsed_time(
    gauge_recorder, influx_query
):
    run_id = f"drain{int(time.time()) % 100000}"
    started_at = datetime.now(timezone.utc)
    specs = default_specs(2, region="eu", plant="plant1")

    future = start_sim(specs, 5.0, 6.0, run_id)
    published = future.result(timeout=300)
    expected = sum(published.values())

    result = drain_and_fetch(
        influx_query, run_id, flux_range_start(started_at), expected,
        timeout_s=180, gauge_recorder=gauge_recorder,
    )

    assert isinstance(result, DrainResult)
    assert len(result.rows) >= expected, f"{len(result.rows)} rows for {expected} published"
    assert result.exit_condition == "broker-drained", (
        f"a healthy run must exit on broker state, not {result.exit_condition!r}"
    )
    assert result.elapsed_s > 0
    assert result.ready_at_exit == 0
    assert result.unacked_at_exit == 0


def test_as_result_fields_names_everything_a_result_json_needs():
    result = DrainResult(
        rows=[], elapsed_s=12.5, exit_condition="timeout",
        ready_at_exit=7, unacked_at_exit=3, expected_total=10,
    )
    assert result.as_result_fields() == {
        "drain_elapsed_s": 12.5,
        "drain_exit_condition": "timeout",
        "queue_ready_at_drain_exit": 7,
        "queue_unacked_at_drain_exit": 3,
        "drain_rows_at_exit": 0,
        "drain_expected_total": 10,
    }


def test_drain_falls_back_to_row_stability_without_a_recorder(influx_query):
    """No gauge access means no broker-state guard; the heuristic still applies."""
    result = drain_and_fetch(
        influx_query, "no-such-run-id", "-5m", expected_total=10,
        timeout_s=60, stable_polls_limit=2,
    )
    assert result.rows == []
    assert result.exit_condition == "row-count-gave-up"
