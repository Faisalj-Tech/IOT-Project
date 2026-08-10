"""Environment facts the rest of the matrix depends on.

These are recorded rather than assumed. A finite bucket retention would expire
outage-era points and look exactly like message loss; and experiment D's trigger
cannot be chosen without knowing which stage rejects a malformed payload.
"""

import os
import time

import pytest
import requests

from tests.conftest import query_measurement

pytestmark = [pytest.mark.stack, pytest.mark.experiment]


def test_bucket_retention_is_infinite(stack):
    """A finite retention would silently expire outage-era points."""
    token = os.environ.get("INFLUXDB_TOKEN", "dev-token-0123456789abcdef")
    bucket = os.environ.get("INFLUXDB_BUCKET", "telemetry")
    resp = requests.get(
        "http://localhost:8086/api/v2/buckets",
        params={"name": bucket},
        headers={"Authorization": f"Token {token}"},
        timeout=10,
    )
    resp.raise_for_status()
    buckets = resp.json()["buckets"]
    assert buckets, f"bucket {bucket} not found"
    rules = buckets[0]["retentionRules"]
    seconds = 0 if not rules else rules[0].get("everySeconds", 0)
    assert seconds == 0, (
        f"bucket retention is {seconds}s, not infinite; outage-era points would expire "
        "and be indistinguishable from loss"
    )


def test_telegraf_internal_metrics_are_collected(stack, influx_query):
    """internal_agent.metrics_dropped is experiment D's silent-drop detector."""
    rows: list[dict] = []
    for _ in range(40):
        rows = [
            r
            for r in query_measurement(influx_query, "internal_agent", start="-5m")
            if r["_field"] == "metrics_dropped"
        ]
        if rows:
            break
        time.sleep(1)
    assert rows, "internal_agent.metrics_dropped is not reaching InfluxDB"


# Established empirically in Task 2 Step 6 by publishing a payload with a
# non-numeric `value` into the running stack and reading `docker logs iot-telegraf`.
# Experiment D's D1 trigger targets this stage.
POISON_REJECTION_STAGE = "parse"
POISON_REJECTION_EVIDENCE = "E! [inputs.amqp_consumer] Error in plugin: unable to convert field \"value\" to type float: strconv.ParseFloat: parsing \"not-a-number\": invalid syntax"


def test_poison_rejection_stage_is_recorded():
    assert POISON_REJECTION_STAGE in {"parse", "output", "silent"}
    assert POISON_REJECTION_EVIDENCE
