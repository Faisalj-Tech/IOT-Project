import time

import pytest

pytestmark = pytest.mark.stack

BUCKET = "telemetry"


def query_measurement(influx_query, measurement: str) -> list[dict]:
    flux = f'''
from(bucket: "{BUCKET}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "{measurement}")
'''
    return influx_query(flux)


def test_broker_overview_metrics_reach_influxdb(influx_query):
    rows: list[dict] = []
    for _ in range(40):
        rows = query_measurement(influx_query, "rabbitmq_overview")
        if rows:
            break
        time.sleep(1)
    assert rows, "no rabbitmq_overview metrics in InfluxDB"


def test_telemetry_queue_depth_is_reported(influx_query):
    rows: list[dict] = []
    for _ in range(40):
        rows = [
            r
            for r in query_measurement(influx_query, "rabbitmq_queue")
            if r.get("queue") == "telemetry.q" and r["_field"] == "messages"
        ]
        if rows:
            break
        time.sleep(1)
    assert rows, "no messages field for telemetry.q in rabbitmq_queue"
    assert isinstance(rows[0]["_value"], (int, float))
