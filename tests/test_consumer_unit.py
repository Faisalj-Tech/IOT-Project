"""Unit tests for the ack-after-write consumer. No stack, no broker."""

import json
from datetime import datetime, timezone

import pytest

from consumer.ackafterwrite import classify_write_error, parse_message, to_point

VALID = {
    "ts": "2026-08-10T12:00:00.000Z", "region": "eu", "plant": "plant1",
    "device": "press-01", "metric": "temp", "value": 71.4, "unit": "C",
    "seq": 1423, "run_id": "a3f9c1",
}


def test_parse_message_accepts_the_phase1_contract():
    parsed = parse_message(json.dumps(VALID).encode())
    assert parsed["seq"] == 1423
    assert parsed["value"] == 71.4
    assert parsed["ts"] == datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_message_rejects_a_non_numeric_value():
    bad = dict(VALID, value="not-a-number")
    with pytest.raises(ValueError):
        parse_message(json.dumps(bad).encode())


def test_parse_message_rejects_a_missing_field():
    bad = dict(VALID)
    del bad["run_id"]
    with pytest.raises(ValueError):
        parse_message(json.dumps(bad).encode())


def test_parse_message_rejects_non_json():
    with pytest.raises(ValueError):
        parse_message(b"not json at all")


def test_point_schema_matches_telegrafs_json_v2_mapping():
    """Telegraf tags region/plant/device/metric/unit/run_id and fields value(float)+seq(int).

    A mismatch here would collide with Telegraf's field types on the same measurement,
    and the resulting InfluxDB rejection would look exactly like a silent drop inside
    experiment E's consumer arm.
    """
    point = to_point(parse_message(json.dumps(VALID).encode()))
    line = point.to_line_protocol()
    assert line.startswith("telemetry,")
    for tag in ("region=eu", "plant=plant1", "device=press-01", "metric=temp",
                "unit=C", "run_id=a3f9c1"):
        assert tag in line, f"missing tag {tag} in {line}"
    assert "value=71.4" in line
    assert "seq=1423i" in line, f"seq must be an integer field, got: {line}"


def test_classify_write_error_treats_4xx_as_fatal_and_5xx_as_retryable():
    class FakeApiException(Exception):
        def __init__(self, status):
            self.status = status

    assert classify_write_error(FakeApiException(400)) == "fatal"
    assert classify_write_error(FakeApiException(422)) == "fatal"
    assert classify_write_error(FakeApiException(413)) == "fatal"
    assert classify_write_error(FakeApiException(500)) == "retryable"
    assert classify_write_error(FakeApiException(503)) == "retryable"
    assert classify_write_error(ConnectionError("influxdb unreachable")) == "retryable"
