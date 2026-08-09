import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
import requests

# Windows requires SelectorEventLoopPolicy for aiomqtt/paho-mqtt compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ROOT = Path(__file__).resolve().parents[1]
RABBIT_API = "http://localhost:15672/api"


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
