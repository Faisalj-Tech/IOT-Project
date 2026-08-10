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
