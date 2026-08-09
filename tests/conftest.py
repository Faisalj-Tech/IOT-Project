import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
import requests
from influxdb_client import InfluxDBClient

# Windows requires SelectorEventLoopPolicy for aiomqtt/paho-mqtt compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ROOT = Path(__file__).resolve().parents[1]
RABBIT_API = "http://localhost:15672/api"


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env()


def compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture(scope="session")
def stack():
    """Bring the compose stack up once per test session.

    Set KEEP_STACK=1 to skip teardown while iterating locally.
    """
    if not (ROOT / ".env").exists():
        pytest.fail("main/.env is missing. Copy .env.example to .env first.")
    compose("up", "-d", "--wait")
    yield
    if os.environ.get("KEEP_STACK") != "1":
        compose("down", "-v")


@pytest.fixture(scope="session")
def rabbit_get(stack):
    auth = (
        os.environ.get("RABBITMQ_ADMIN_USER", "admin"),
        os.environ.get("RABBITMQ_ADMIN_PASSWORD", "adminpass"),
    )
    session = requests.Session()
    session.auth = auth

    def _get(path: str) -> requests.Response:
        return session.get(f"{RABBIT_API}{path}", timeout=10)

    return _get


@pytest.fixture(scope="session")
def influx_query(stack):
    token = os.environ.get("INFLUXDB_TOKEN", "dev-token-0123456789abcdef")
    org = os.environ.get("INFLUXDB_ORG", "iot")
    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)

    def _query(flux: str) -> list[dict]:
        rows: list[dict] = []
        for table in client.query_api().query(flux):
            for record in table.records:
                row = dict(record.values)
                row["_value"] = record.get_value()
                row["_field"] = record.get_field()
                row["_time"] = record.get_time()
                rows.append(row)
        return rows

    yield _query
    client.close()
