import os

import pytest
import requests

pytestmark = pytest.mark.stack

GRAFANA = "http://localhost:3000"


@pytest.fixture(scope="module")
def grafana_auth(stack):
    return (
        os.environ.get("GRAFANA_ADMIN_USER", "admin"),
        os.environ.get("GRAFANA_ADMIN_PASSWORD", "grafanapass"),
    )


def test_influx_datasource_is_provisioned_and_healthy(grafana_auth):
    response = requests.get(
        f"{GRAFANA}/api/datasources/uid/influxdb-telemetry", auth=grafana_auth, timeout=10
    )
    assert response.status_code == 200, response.text
    assert response.json()["type"] == "influxdb"

    health = requests.get(
        f"{GRAFANA}/api/datasources/uid/influxdb-telemetry/health", auth=grafana_auth, timeout=30
    )
    assert health.status_code == 200, health.text
    assert health.json()["status"] == "OK", health.text


def test_telemetry_dashboard_is_provisioned(grafana_auth):
    response = requests.get(
        f"{GRAFANA}/api/dashboards/uid/iot-telemetry", auth=grafana_auth, timeout=10
    )
    assert response.status_code == 200, response.text

    dashboard = response.json()["dashboard"]
    titles = {panel["title"] for panel in dashboard["panels"]}
    assert {"Telemetry by device", "telemetry.q depth"} <= titles
