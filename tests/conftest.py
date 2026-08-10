import asyncio
import os
import subprocess
import sys
from datetime import datetime, timedelta
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


def compose(*args: str, files: tuple[str, ...] = ("compose.yml",)) -> subprocess.CompletedProcess:
    """Run `docker compose` against the project, raising with captured output on failure.

    subprocess.CalledProcessError's default __str__ hides stdout/stderr, which makes a
    failed `up --wait` almost undiagnosable from a bare pytest failure. Handoff finding #5.
    """
    file_args: list[str] = []
    for name in files:
        file_args += ["-f", name]
    proc = subprocess.run(
        ["docker", "compose", *file_args, *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(args)} failed with exit {proc.returncode}\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    return proc


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


BUCKET = "telemetry"


def flux_range_start(started_at: datetime) -> str:
    """RFC3339 UTC, one minute before started_at.

    Points carry device-side timestamps (telegraf.conf sets timestamp_path = "ts"),
    so after an outage the drained points land at outage-era times. Verification
    queries must anchor on the run's own start, never on a relative recent window.
    """
    return (started_at - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _field_query(run_id: str, field: str, start: str) -> str:
    return f'''
from(bucket: "{BUCKET}")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r._field == "{field}")
'''


def fetch_seqs(influx_query, run_id: str, start: str = "-15m") -> list[dict]:
    return influx_query(_field_query(run_id, "seq", start))


def fetch_values(influx_query, run_id: str, start: str = "-15m") -> list[dict]:
    return influx_query(_field_query(run_id, "value", start))


def query_measurement(influx_query, measurement: str, start: str = "-5m") -> list[dict]:
    flux = f'''
from(bucket: "{BUCKET}")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "{measurement}")
'''
    return influx_query(flux)
