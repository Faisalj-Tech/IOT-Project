"""L2 - where does the pipeline stop keeping up? L5 - what does latency do getting there?

Recorded, not asserted (ADR-0004/0009): a throughput threshold that passes on this
host is a false guarantee on any other. What is reportable is the DIVERGENCE point -
the offered rate at which broker ingress, Telegraf ingest and InfluxDB writes stop
tracking one another. Divergence locates the bottleneck; no single number does.

ACCOUNTING WARNING. telegraf.load.d adds a SECOND consumer on telemetry.q - that is
how L5 gets an ingest-time clock at all. Two consumers split the stream between them,
exactly as compose.consumer.yml's header warns. Therefore:
  - the ingest figure below counts the base `telemetry` measurement only
  - the latency sample is a SUBSET of the stream, not all of it
Both facts belong in the report. Conflating them would overstate throughput.
"""

import statistics
import time
from datetime import datetime, timezone

import pytest

from tests.conftest import load_mode
from tests.experiments.conftest import write_result
from tests.experiments.load import (
    Swarm, broker_memory_bytes, host_envelope, purge_queue, stable_depth,
)

pytestmark = [pytest.mark.load, pytest.mark.stack]

# (replicas, devices each, rate Hz each) -> offered = replicas * devices * rate
STEPS = [(2, 25, 1.0), (2, 50, 2.0), (4, 50, 4.0), (4, 100, 8.0)]
STEP_SECONDS = 60


@pytest.fixture(autouse=True)
def _requires_load_profile():
    if not load_mode():
        pytest.skip("needs the compose.load.yml overlay and IOT_LOAD=1")


def _ingested_since(influx_query, start_iso: str) -> int:
    """Rows the BASE telegraf input wrote. Excludes telemetry_latency on purpose."""
    rows = influx_query(
        f'from(bucket:"telemetry") |> range(start:{start_iso}) '
        f'|> filter(fn:(r) => r._measurement == "telemetry" and r._field == "seq")'
    )
    return len(rows)


def _latency_percentiles(influx_query, start_iso: str) -> dict:
    """p50/p95/p99 of (ingest time - publish time), from the two-clock rows.

    _time is Telegraf's ingest time; the `ts` field is the device's publish time
    (spec 4.4). Both clocks come from containers on one host, so this is pipeline
    latency, not clock skew between machines.
    """
    rows = influx_query(
        f'from(bucket:"telemetry") |> range(start:{start_iso}) '
        f'|> filter(fn:(r) => r._measurement == "telemetry_latency" and r._field == "ts")'
    )
    deltas = []
    for row in rows:
        published = datetime.strptime(row["_value"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
        deltas.append((row["_time"] - published).total_seconds())
    if not deltas:
        return {"samples": 0}
    deltas.sort()
    return {
        "samples": len(deltas),
        "p50_s": round(statistics.median(deltas), 3),
        "p95_s": round(deltas[int(len(deltas) * 0.95) - 1], 3),
        "p99_s": round(deltas[int(len(deltas) * 0.99) - 1], 3),
        "max_s": round(deltas[-1], 3),
    }


def test_throughput_ceiling_and_latency_under_load(stack, influx_query):
    swarm = Swarm()
    steps = []
    try:
        for replicas, devices, rate in STEPS:
            purge_queue("telemetry.q")
            purge_queue("dlq")
            swarm.clear_reports()
            start_iso = f"-{STEP_SECONDS + 30}s"
            offered = replicas * devices * rate

            swarm.scale(replicas, devices=devices, rate_hz=rate,
                        duration_s=STEP_SECONDS + 30)
            time.sleep(STEP_SECONDS)
            published = swarm.collect()
            swarm.stop()

            # let the pipeline drain before counting what arrived
            depth = stable_depth("telemetry.q", timeout_s=120)
            ingested = _ingested_since(influx_query, start_iso)

            steps.append({
                "replicas": replicas,
                "devices_each": devices,
                "rate_hz_each": rate,
                "offered_rate_hz": offered,
                "published_attempted": published["attempted"],
                "published_acked": published["puback"],
                "published_rejected": published["rejected"],
                "publish_timeouts": published["timed_out"],
                "reconnects": published["reconnects"],
                "ingested_rows": ingested,
                "queue_after_drain": depth.messages,
                "queue_depth_exit": depth.exit_condition,
                "broker_memory_bytes": broker_memory_bytes(),
                "latency": _latency_percentiles(influx_query, start_iso),
            })
    finally:
        swarm.stop()

    write_result("L2-throughput", {
        "run_id": "L2",
        "host_envelope": host_envelope(),
        "steps": steps,
        "accounting_note": (
            "ingested_rows counts the base `telemetry` measurement only; "
            "telegraf.load.d runs a second consumer on the same queue, so the "
            "latency sample is a subset of the stream"
        ),
    })
    write_result("L5-latency", {
        "run_id": "L5",
        "host_envelope": host_envelope(),
        "latency_by_offered_rate": {
            str(step["offered_rate_hz"]): step["latency"] for step in steps
        },
    })
