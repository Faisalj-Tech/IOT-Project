"""Experiment D: does any path destroy a message without it reaching the DLQ?

Telegraf's REJECT-on-output-failure path covers retryable output failure. The open
question is what happens when failure is not retryable. Two triggers are run:

  D1 parse-stage:  a payload the json_v2 parser cannot coerce.
  D2 output-stage: a write InfluxDB rejects non-retryably after a successful parse.

The outcome is recorded rather than presumed. A parse-stage NACK *with requeue* is its
own reportable failure: the poison message is redelivered forever, never reaches the
DLQ, and consumes broker and Telegraf capacity indefinitely.
"""

import json
import time

import pytest

from tests.conftest import query_measurement
from tests.experiments.conftest import publish_raw

pytestmark = [pytest.mark.stack, pytest.mark.experiment]

SETTLE_S = 40.0  # four Telegraf flush intervals


KNOWN_OUTCOMES = {
    "accepted",
    "parse-nack-to-dlq",
    "parse-nack-requeue-loop",
    "output-ack-then-drop",
    "silently-discarded-no-counter",
}

# Telegraf's internal counters that could plausibly register a discarded metric.
# Which one actually moves is not known in advance: metrics_dropped tracks output
# *buffer overflow*, which is not necessarily the same event as "the output stage
# rejected a malformed batch". Task 2 Step 6 enumerated what this build emits;
# all of these are recorded and classification keys off whichever moves.
COUNTER_FIELDS = (
    ("internal_agent", "metrics_dropped"),
    ("internal_write", "errors"),
    ("internal_write", "metrics_written"),
    ("internal_write", "metrics_dropped"),
)


def _counter(influx_query, measurement: str, field: str) -> int:
    rows = [
        r
        for r in query_measurement(influx_query, measurement, start="-10m")
        if r["_field"] == field
    ]
    return int(rows[-1]["_value"]) if rows else 0


def _counters(influx_query) -> dict:
    return {
        f"{m}.{f}": _counter(influx_query, m, f) for m, f in COUNTER_FIELDS
    }


def landed_in_influx(influx_query, run_id: str) -> bool:
    from tests.conftest import fetch_seqs

    return bool(fetch_seqs(influx_query, run_id, start="-10m"))


def _classify(dlq_delta: int, counter_deltas: dict, ready_end: int, landed: bool) -> str:
    """Name what happened to the message. Every outcome is a legitimate result."""
    if landed:
        return "accepted"
    if dlq_delta > 0:
        return "parse-nack-to-dlq"
    if ready_end > 0:
        return "parse-nack-requeue-loop"
    # Any error or drop counter moving means the pipeline knew it discarded something,
    # even if nothing reached the DLQ. No counter moving at all is the invisible case.
    if any(v > 0 for k, v in counter_deltas.items() if not k.endswith("metrics_written")):
        return "output-ack-then-drop"
    return "silently-discarded-no-counter"


def _run_trigger(trigger, payload, run_id, gauge_recorder, results_dir, influx_query, rabbit_get):
    dlq_before = rabbit_get("/queues/%2F/dlq").json()["messages"]
    ready_before = rabbit_get("/queues/%2F/telemetry.q").json()["messages_ready"]
    counters_before = _counters(influx_query)

    gauge_recorder.mark(f"{trigger}_publish")
    publish_raw("region/eu/plant1/poison-01/temp", payload, count=5)
    time.sleep(SETTLE_S)

    dlq_after = rabbit_get("/queues/%2F/dlq").json()["messages"]
    ready_after = rabbit_get("/queues/%2F/telemetry.q").json()["messages_ready"]
    counters_after = _counters(influx_query)
    counter_deltas = {k: counters_after[k] - counters_before[k] for k in counters_before}

    landed = landed_in_influx(influx_query, run_id)
    outcome = _classify(
        dlq_after - dlq_before, counter_deltas, max(ready_after - ready_before, 0), landed
    )

    results_dir(
        f"D-write-error-{trigger}",
        {
            "run_id": run_id,
            "arm": "telegraf",
            "trigger": trigger,
            "messages_published": 5,
            "dlq_before": dlq_before,
            "dlq_after": dlq_after,
            "ready_before": ready_before,
            "ready_after": ready_after,
            "counters_before": counters_before,
            "counters_after": counters_after,
            "counter_deltas": counter_deltas,
            "landed_in_influx": landed,
            "outcome": outcome,
            "verdict": outcome,
            "timeline": gauge_recorder.timeline(),
        },
    )
    return outcome


def test_d1_parse_stage_poison_message(gauge_recorder, results_dir, influx_query, rabbit_get):
    payload = json.dumps(
        {
            "ts": "2026-08-10T12:00:00.000Z", "region": "eu", "plant": "plant1",
            "device": "poison-01", "metric": "temp", "value": "not-a-number",
            "unit": "C", "seq": 1, "run_id": "poisonD1",
        }
    ).encode()
    outcome = _run_trigger(
        "d1-parse", payload, "poisonD1", gauge_recorder, results_dir, influx_query, rabbit_get
    )
    # The test asserts only that classification succeeded. Every outcome below is a
    # legitimate experimental result, and the one this experiment hunts for
    # (parse-nack-requeue-loop) must NOT fail the test — a finding is not a defect,
    # and an assertion that fails on success would invite someone to weaken it.
    assert outcome in KNOWN_OUTCOMES, f"unclassified outcome: {outcome}"
    assert not (outcome == "accepted"), (
        "a payload with a non-numeric value was accepted into InfluxDB; the parser "
        "coerced it, so this trigger does not exercise a rejection path at all and "
        "D1 needs a different payload"
    )


def test_d2_output_stage_oversized_write(gauge_recorder, results_dir, influx_query, rabbit_get):
    """An oversized tag value survives the parser and is rejected by InfluxDB."""
    payload = json.dumps(
        {
            "ts": "2026-08-10T12:00:00.000Z", "region": "eu", "plant": "x" * 200000,
            "device": "poison-02", "metric": "temp", "value": 71.4,
            "unit": "C", "seq": 1, "run_id": "poisonD2",
        }
    ).encode()
    outcome = _run_trigger(
        "d2-output", payload, "poisonD2", gauge_recorder, results_dir, influx_query, rabbit_get
    )
    # Same rule as D1: `silently-discarded-no-counter` is the report's headline
    # finding, not a test failure. The result JSON carries it into the report.
    assert outcome in KNOWN_OUTCOMES, f"unclassified outcome: {outcome}"
    assert not landed_in_influx(influx_query, "poisonD2"), (
        "an oversized tag value reached InfluxDB, so this trigger does not exercise "
        "a rejection path and D2 needs a larger payload"
    )
