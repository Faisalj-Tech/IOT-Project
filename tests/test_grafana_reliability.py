import os

import pytest
import requests

pytestmark = pytest.mark.stack

GRAFANA = "http://localhost:3000"


def _auth():
    return (
        os.environ.get("GRAFANA_ADMIN_USER", "admin"),
        os.environ.get("GRAFANA_ADMIN_PASSWORD", "grafanapass"),
    )


def test_reliability_dashboard_is_provisioned(stack):
    resp = requests.get(f"{GRAFANA}/api/dashboards/uid/iot-reliability", auth=_auth(), timeout=10)
    assert resp.status_code == 200, resp.text
    dashboard = resp.json()["dashboard"]
    titles = {panel["title"] for panel in dashboard["panels"]}
    assert titles == {
        "telemetry.q ready vs unacknowledged",
        "DLQ depth",
        "Telegraf metrics dropped",
        "Broker alarms",
    }, titles
