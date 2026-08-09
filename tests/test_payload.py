from datetime import datetime, timezone

from sim.devices.payload import (
    DeviceSpec,
    build_payload,
    default_specs,
    routing_key_for,
    topic_for,
)

SPEC = DeviceSpec(
    region="eu",
    plant="plant1",
    device="press-01",
    metric="temp",
    unit="C",
    baseline=70.0,
    jitter=2.0,
)


def test_topic_uses_slash_separated_hierarchy():
    assert topic_for(SPEC) == "region/eu/plant1/press-01/temp"


def test_routing_key_mirrors_topic_with_dots():
    assert routing_key_for(SPEC) == "region.eu.plant1.press-01.temp"


def test_payload_carries_full_identity_and_contract_fields():
    now = datetime(2026, 8, 9, 18, 0, 0, 123456, tzinfo=timezone.utc)
    payload = build_payload(SPEC, seq=1423, run_id="a3f9c1", value=71.4, now=now)

    assert payload == {
        "ts": "2026-08-09T18:00:00.123Z",
        "region": "eu",
        "plant": "plant1",
        "device": "press-01",
        "metric": "temp",
        "value": 71.4,
        "unit": "C",
        "seq": 1423,
        "run_id": "a3f9c1",
    }


def test_timestamp_always_has_three_decimals_and_z_suffix():
    now = datetime(2026, 1, 2, 3, 4, 5, 0, tzinfo=timezone.utc)
    payload = build_payload(SPEC, seq=1, run_id="r", value=1.0, now=now)
    assert payload["ts"] == "2026-01-02T03:04:05.000Z"


def test_default_specs_produces_distinct_device_names():
    specs = default_specs(3, region="eu", plant="plant1")
    assert len(specs) == 3
    assert len({s.device for s in specs}) == 3
