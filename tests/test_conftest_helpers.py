"""Unit tests for the shared query helpers. No stack required."""

from datetime import datetime, timezone

from tests.conftest import fetch_seqs, flux_range_start, query_measurement


def test_flux_range_start_is_one_minute_before_and_rfc3339():
    started = datetime(2026, 8, 10, 12, 30, 0, tzinfo=timezone.utc)
    assert flux_range_start(started) == "2026-08-10T12:29:00Z"


def test_fetch_seqs_builds_a_run_scoped_flux_query():
    captured = {}

    def fake_query(flux):
        captured["flux"] = flux
        return []

    fetch_seqs(fake_query, "abc123", start="2026-08-10T12:29:00Z")
    flux = captured["flux"]
    assert 'from(bucket: "telemetry")' in flux
    assert "range(start: 2026-08-10T12:29:00Z)" in flux
    assert 'r._measurement == "telemetry"' in flux
    assert 'r.run_id == "abc123"' in flux
    assert 'r._field == "seq"' in flux


def test_query_measurement_builds_a_measurement_scoped_flux_query():
    captured = {}

    def fake_query(flux):
        captured["flux"] = flux
        return []

    query_measurement(fake_query, "rabbitmq_queue", start="-5m")
    flux = captured["flux"]
    assert 'r._measurement == "rabbitmq_queue"' in flux
    assert "range(start: -5m)" in flux
