"""Unit tests for the Phase 6 swarm additions. No Docker required."""

import json

from sim.devices.__main__ import parse_args
from sim.devices.payload import default_specs, topic_for


def test_default_specs_names_are_unchanged_for_phases_1_to_5():
    """Phases 1-5 assert on press-01..press-NN. Changing the default breaks them."""
    specs = default_specs(3, region="eu", plant="plant1")
    assert [s.device for s in specs] == ["press-01", "press-02", "press-03"]


def test_default_specs_accepts_a_prefix_and_width():
    specs = default_specs(2, region="eu", plant="plant1",
                          prefix="0e1c13b7f13b", width=3)
    assert [s.device for s in specs] == ["0e1c13b7f13b-001", "0e1c13b7f13b-002"]


def test_wider_index_avoids_collisions_past_99_devices():
    specs = default_specs(150, region="eu", plant="plant1", prefix="host", width=3)
    assert len({s.device for s in specs}) == 150
    assert specs[-1].device == "host-150"


def test_topic_still_derives_from_the_device_name():
    spec = default_specs(1, region="eu", plant="plant1",
                         prefix="0e1c13b7f13b", width=3)[0]
    assert topic_for(spec) == "region/eu/plant1/0e1c13b7f13b-001/temp"


def test_device_prefix_defaults_to_the_container_hostname(monkeypatch):
    """--scale gives every replica an identical command line, so the prefix cannot
    be passed per replica. The hostname is the one value Docker guarantees unique."""
    monkeypatch.setattr("socket.gethostname", lambda: "0e1c13b7f13b")
    args = parse_args([])
    assert args.device_prefix == "0e1c13b7f13b"


def test_device_prefix_can_be_overridden():
    args = parse_args(["--device-prefix", "press"])
    assert args.device_prefix == "press"


def test_report_flag_defaults_to_none():
    assert parse_args([]).report is None


import aiomqtt
import pytest

from sim.devices.reasoncodes import PublishAccounting, attach_reason_code_observer


def test_accounting_starts_empty():
    acc = PublishAccounting()
    assert acc.attempted == 0 and acc.rejected == 0 and acc.reason_codes == {}


def test_accounting_separates_rejections_from_disconnects():
    """Spec 2.6: aiomqtt raises MqttError for BOTH a timed-out publish and a real
    disconnect, and runner.py's reconnect loop treats them identically. Counting
    them together would report an overflow rejection as connection churn.
    """
    acc = PublishAccounting()
    acc.record_reason_code("Quota exceeded", is_failure=True)
    acc.record_timeout()
    acc.record_reconnect()
    assert acc.rejected == 1
    assert acc.timed_out == 1
    assert acc.reconnects == 1
    assert acc.reason_codes == {"Quota exceeded": 1}


def test_accounting_does_not_count_success_as_rejection():
    acc = PublishAccounting()
    acc.record_reason_code("Success", is_failure=False)
    assert acc.rejected == 0
    assert acc.puback == 1


def test_aiomqtt_still_exposes_the_paho_client():
    """Pins the one private-API assumption this module makes.

    attach_reason_code_observer() reaches into aiomqtt's underlying paho client,
    because aiomqtt itself surfaces no reason code. If a future aiomqtt renames
    this attribute the swarm would silently stop counting rejections - so the
    assumption fails loudly here instead.
    """
    async def check_paho_client():
        # Construct a live aiomqtt.Client instance (requires event loop).
        client = aiomqtt.Client(hostname="localhost", port=1883)
        assert hasattr(client, "_client"), "aiomqtt.Client no longer exposes ._client"
        assert hasattr(client._client, "on_publish"), "paho mqtt.Client no longer has on_publish"

    import asyncio
    asyncio.run(check_paho_client())
