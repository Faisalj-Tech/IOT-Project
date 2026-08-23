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
