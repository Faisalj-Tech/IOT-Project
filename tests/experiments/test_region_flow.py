"""R6: both regions flow end to end, and neither leaks into the other.

    IOT_REGION=1 KEEP_STACK=1 .venv/Scripts/python.exe -m pytest \
        tests/experiments/test_region_flow.py -m region -v -s

The assertion that matters is the cross-tagged count: zero points consumed from
one region's vhost may carry the other region's run_id. Totals use >= because
QoS-1 retries legitimately duplicate (HANDOFF bite #2), and every query anchors
on the run's own start because points carry device timestamps (bite #3).
"""

import os
import subprocess
import time
import uuid
from datetime import datetime, timezone

import pytest

from tests.conftest import (BUCKET, compose_files, flux_range_start, region_mode)
from tests.experiments.conftest import ROOT, write_result

pytestmark = [pytest.mark.region, pytest.mark.stack]

DEVICES = 3
RATE_HZ = 2.0
DURATION_S = 30.0
EXPECTED_PER_REGION = int(DEVICES * RATE_HZ * DURATION_S)

PASSWORDS = {
    "eu": os.environ.get("RABBITMQ_DEVICE_EU_PASSWORD", "devicepass-eu"),
    "us": os.environ.get("RABBITMQ_DEVICE_US_PASSWORD", "devicepass-us"),
}


@pytest.fixture(autouse=True)
def _requires_region_profile():
    if not region_mode():
        pytest.fail("region tests need IOT_REGION=1")


def _compose_args() -> list[str]:
    args: list[str] = []
    for name in compose_files():
        args += ["-f", name]
    return args


def _run_sim(region: str, run_id: str) -> subprocess.CompletedProcess:
    """Run one region's simulator to completion, overriding only the run id."""
    return subprocess.run(
        ["docker", "compose", *_compose_args(), "--profile", "sim",
         "run", "--rm", f"sim-{region}",
         "python", "-m", "sim.devices",
         f"--region={region}", "--plant=plant1",
         f"--host=172.28.{'1' if region == 'eu' else '2'}.10",
         f"--port={1893 if region == 'eu' else 1993}",
         f"--username=device-{region}",
         # The password MUST be passed explicitly. Inside the container,
         # --password defaults from RABBITMQ_DEVICE_PASSWORD, which is the Phase 1
         # `device` user's password and is wrong for device-eu / device-us.
         f"--password={PASSWORDS[region]}",
         f"--devices={DEVICES}", f"--rate={RATE_HZ}", f"--duration={DURATION_S}",
         f"--run-id={run_id}"],
        capture_output=True, text=True, check=False, cwd=ROOT,
    )


def _count_by_provenance(influx_query, run_id: str, start: str) -> dict[str, int]:
    """How many points for this run_id landed under each region_src tag."""
    flux = f'''
from(bucket: "{BUCKET}")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r._field == "seq")
  |> group(columns: ["region_src"])
  |> count()
  |> map(fn: (r) => ({{r with _field: "seq", _time: now()}}))
'''
    counts: dict[str, int] = {}
    for row in influx_query(flux):
        counts[row.get("region_src", "<none>")] = int(row["_value"])
    return counts


def test_both_regions_flow_end_to_end_without_leaking(stack, influx_query, rabbit_get):
    started_at = datetime.now(timezone.utc)
    start = flux_range_start(started_at)
    run_ids = {region: f"R{region}{uuid.uuid4().hex[:6]}" for region in ("eu", "us")}

    for region, run_id in run_ids.items():
        proc = _run_sim(region, run_id)
        assert proc.returncode == 0, f"sim-{region} failed:\n{proc.stdout}\n{proc.stderr}"

    # Telegraf's flush_interval is 10s; give both inputs room to drain and write.
    deadline = time.time() + 180
    counts: dict[str, dict[str, int]] = {}
    while time.time() < deadline:
        counts = {r: _count_by_provenance(influx_query, run_ids[r], start) for r in run_ids}
        if all(counts[r].get(r, 0) >= EXPECTED_PER_REGION for r in run_ids):
            break
        time.sleep(5)

    queues = {
        region: rabbit_get(f"/queues/{region}/telemetry.{region}.q").json()
        for region in run_ids
    }
    dlqs = {region: rabbit_get(f"/queues/{region}/dlq").json() for region in run_ids}

    write_result("R-region-flow", {
        "expected_per_region": EXPECTED_PER_REGION,
        "counts_by_provenance": counts,
        "run_ids": run_ids,
        "queue_ready": {r: q.get("messages_ready") for r, q in queues.items()},
        "dlq_ready": {r: q.get("messages_ready") for r, q in dlqs.items()},
        "started_at": started_at.isoformat(),
    })

    for region, run_id in run_ids.items():
        assert counts[region].get(region, 0) >= EXPECTED_PER_REGION, (
            f"{region} lost points: {counts[region]}"
        )
        foreign = {k: v for k, v in counts[region].items() if k != region}
        assert foreign == {}, (
            f"{region}'s run {run_id} was also consumed from {foreign} — the vhosts, "
            "the bindings, or the port mapping are not segregating traffic"
        )
        assert queues[region].get("messages_ready") == 0, region
        assert dlqs[region].get("messages_ready") == 0, (
            f"{region} dead-lettered messages; a policy limit was reached or a "
            "payload was rejected"
        )
