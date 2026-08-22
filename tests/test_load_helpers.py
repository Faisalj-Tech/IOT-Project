"""Unit tests for the pure parts of the load harness. No Docker required."""

import pytest

from tests.experiments.load import DepthReading, _parse_list_queues, _policy_body


def test_policy_body_shapes_a_management_api_payload():
    body = _policy_body(
        pattern="^telemetry\\.q$",
        definition={"max-length": 100, "overflow": "reject-publish"},
        priority=5,
        apply_to="queues",
    )
    assert body == {
        "pattern": "^telemetry\\.q$",
        "definition": {"max-length": 100, "overflow": "reject-publish"},
        "priority": 5,
        "apply-to": "queues",
    }


def test_parse_list_queues_reads_the_named_queue():
    out = "telemetry.q\t120\t118\t2\ndlq\t0\t0\t0\n"
    reading = _parse_list_queues(out, "telemetry.q")
    assert reading == {"messages": 120, "ready": 118, "unacked": 2}


def test_parse_list_queues_returns_none_for_a_missing_queue():
    """A queue that does not exist yet is not the same as a queue holding zero.

    Treating them the same would let a test that declared nothing report a clean
    empty queue and pass.
    """
    assert _parse_list_queues("dlq\t0\t0\t0\n", "telemetry.q") is None


def test_parse_list_queues_ignores_malformed_lines():
    out = "warning: something\ntelemetry.q\t5\t5\t0\n"
    assert _parse_list_queues(out, "telemetry.q") == {
        "messages": 5, "ready": 5, "unacked": 0,
    }


def test_depth_reading_records_how_it_stopped():
    reading = DepthReading(
        queue="telemetry.q", messages=10, ready=10, unacked=0,
        polls=4, elapsed_s=8.0, exit_condition="stable",
    )
    assert reading.exit_condition == "stable"
    assert reading.as_result_fields()["telemetry.q_messages"] == 10
