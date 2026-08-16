"""The recorder must survive a broker outage and still produce evidence."""

import json
import time

import pytest

pytestmark = [pytest.mark.stack, pytest.mark.experiment]


def test_recorder_samples_the_broker_gauges(gauge_recorder):
    time.sleep(5)
    samples = [s for s in gauge_recorder.samples if "error" not in s]
    assert len(samples) >= 2, f"expected at least 2 samples in 5s, got {gauge_recorder.samples}"
    first = samples[0]
    assert set(first) >= {"t", "telemetry_ready", "telemetry_unacked", "dlq_messages", "alarms"}
    assert isinstance(first["telemetry_ready"], int)
    assert first["alarms"] == {"mem": False, "disk": False}


def test_latest_returns_the_most_recent_reading_not_the_peak(gauge_recorder):
    time.sleep(5)
    gauge_recorder.samples.append(
        {"t": time.time() - 10, "telemetry_ready": 900, "telemetry_unacked": 0,
         "dlq_messages": 0, "alarms": {"mem": False, "disk": False}}
    )
    gauge_recorder.samples.append(
        {"t": time.time(), "telemetry_ready": 3, "telemetry_unacked": 0,
         "dlq_messages": 0, "alarms": {"mem": False, "disk": False}}
    )
    assert gauge_recorder.peak("telemetry_ready") == 900
    assert gauge_recorder.latest("telemetry_ready") == 3


def test_recorder_records_marks_with_timestamps(gauge_recorder):
    gauge_recorder.mark("outage_start")
    time.sleep(1)
    gauge_recorder.mark("outage_end")
    labels = [m["label"] for m in gauge_recorder.marks]
    assert labels == ["outage_start", "outage_end"]
    assert gauge_recorder.marks[1]["t"] > gauge_recorder.marks[0]["t"]


def test_recorder_does_not_raise_when_the_broker_is_down(gauge_recorder, docker_control):
    docker_control.stop("rabbitmq", timeout=0)
    time.sleep(5)
    docker_control.start("rabbitmq")
    time.sleep(5)
    assert any("error" in s for s in gauge_recorder.samples), (
        "recorder recorded no error samples while the broker was down"
    )
    assert any("error" not in s for s in gauge_recorder.samples), (
        "recorder never recovered after the broker came back"
    )


def test_record_writes_a_result_json(results_dir, tmp_path):
    path = results_dir("smoke", {"run_id": "abc123", "verdict": "pass"})
    assert path.exists()
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["run_id"] == "abc123"
    assert written["experiment"] == "smoke"
    assert "recorded_at" in written
    path.unlink()


def test_samples_carry_a_per_node_section(gauge_recorder):
    time.sleep(5)
    samples = [s for s in gauge_recorder.samples if "error" not in s]
    assert samples, gauge_recorder.samples
    first = samples[0]
    assert "nodes" in first, "recorder must keep every node's view separately"
    assert 1 in first["nodes"], first["nodes"]
    node1 = first["nodes"][1]
    assert set(node1) >= {
        "reachable", "source", "telemetry_ready", "telemetry_unacked",
        "dlq_messages", "alarms", "members", "leader", "partitions",
    }, node1
    assert node1["reachable"] is True
    assert node1["source"] == "http"
    assert node1["leader"] == "rabbit@rabbit1"
    assert node1["members"] == ["rabbit@rabbit1"]
    assert node1["partitions"] == []


def test_flat_keys_still_mirror_node_one(gauge_recorder):
    """Phase 2 experiments and their committed results read the flat keys."""
    time.sleep(5)
    samples = [s for s in gauge_recorder.samples if "error" not in s]
    first = samples[0]
    assert first["telemetry_ready"] == first["nodes"][1]["telemetry_ready"]
    assert first["telemetry_unacked"] == first["nodes"][1]["telemetry_unacked"]
    assert first["dlq_messages"] == first["nodes"][1]["dlq_messages"]
    assert first["alarms"] == first["nodes"][1]["alarms"]


def test_node_latest_reads_the_named_node(gauge_recorder):
    time.sleep(5)
    assert isinstance(gauge_recorder.node_latest(1, "telemetry_ready"), int)


def test_exec_reads_a_node_whose_published_ports_are_gone(gauge_recorder, docker_control):
    """A partitioned node loses its published ports, so exec is the only read path.

    expect_exec is called explicitly rather than relying on the HTTP attempt to
    fail first — that is exactly what Experiments H and I do, and for the same
    reason: three HTTP calls at timeout=10 against a 2-second poll interval would
    starve the recorder of samples for the whole split.
    """
    docker_control.partition("rabbitmq")
    gauge_recorder.expect_exec(1)
    time.sleep(8)
    docker_control.heal("rabbitmq")
    gauge_recorder.expect_http(1)
    time.sleep(8)

    reachable = [
        s["nodes"][1]
        for s in gauge_recorder.samples
        if "nodes" in s and s["nodes"].get(1, {}).get("reachable")
    ]
    sources = [entry["source"] for entry in reachable]
    assert "exec" in sources, (
        f"recorder never read the partitioned node through docker exec: {sources}"
    )
    assert "http" in sources, "recorder never recovered to HTTP after the partition healed"

    by_source = {entry["source"]: entry for entry in reachable}
    assert by_source["exec"]["members"], "exec path returned no Raft membership"
    assert by_source["exec"]["leader"] == "rabbit@rabbit1", by_source["exec"]


def test_node_window_ignores_readings_outside_the_window(gauge_recorder):
    """node_latest scans all history; node_window must not."""
    time.sleep(5)
    cutoff = time.time()
    time.sleep(3)
    assert gauge_recorder.node_window(1, "telemetry_ready", since=cutoff) is not None
    assert gauge_recorder.node_window(
        1, "telemetry_ready", since=cutoff - 3600, until=cutoff - 1800
    ) is None


def test_mark_time_returns_the_timestamp_of_a_named_mark(gauge_recorder):
    gauge_recorder.mark("split")
    assert gauge_recorder.mark_time("split") == gauge_recorder.marks[-1]["t"]
    assert gauge_recorder.mark_time("never-marked") is None
