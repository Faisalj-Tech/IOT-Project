"""S7. The security overlay ADDS TLS; it must not close anything.

Scoped to run under IOT_SECURITY=1, deliberately. "Run the default-profile suite
against a security-overlay stack" is exactly what _fail_on_profile_mismatch()
exists to refuse; the default suite's own green run is a separate gate.
"""

import asyncio
import json
import ssl
import subprocess
import sys
import time

import aiomqtt
import pytest

from tests.conftest import security_mode

pytestmark = [pytest.mark.security, pytest.mark.stack]

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(autouse=True)
def _requires_security_profile():
    if not security_mode():
        pytest.fail("security tests need IOT_SECURITY=1")


def test_plaintext_1883_still_accepts_a_credential_publish(stack):
    """Spec decision 2. Every Phase 1-3 experiment publishes exactly like this."""
    async def _publish() -> None:
        async with aiomqtt.Client(
            hostname="localhost", port=1883,
            username="device", password="devicepass",
            identifier=f"s7-plain-{int(time.time() * 1000) % 100000}", timeout=15,
        ) as client:
            await client.publish(
                "region/eu/plant1/press-01/temp",
                payload=json.dumps({
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
                    "region": "eu", "plant": "plant1", "device": "press-01",
                    "metric": "temp", "value": 70.0, "unit": "C",
                    "seq": 1, "run_id": "s7",
                }).encode(), qos=1)

    asyncio.run(_publish())


@pytest.mark.parametrize("user,password", [
    ("device", "devicepass"),
    ("telegraf", "telegrafpass"),
    ("admin", "adminpass"),
])
def test_plain_credential_logins_survive_the_oauth2_backend(stack, user, password):
    """auth_backends.1 = internal keeps these working (spec 2.16)."""
    proc = subprocess.run(
        ["docker", "exec", "iot-rabbitmq", "rabbitmqctl", "-q",
         "authenticate_user", user, password],
        capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, f"{user}: {proc.stdout}\n{proc.stderr}"


def test_the_amqp_plaintext_listener_is_still_open(stack):
    proc = subprocess.run(
        ["docker", "exec", "iot-rabbitmq", "rabbitmq-diagnostics", "-q", "listeners"],
        capture_output=True, text=True, check=True,
    )
    assert "port: 5672, protocol: amqp" in proc.stdout, proc.stdout


def test_no_amqp_tls_listener_was_added(stack):
    """Spec 9. ssl_options is node-wide, so a 5671 listener would inherit
    fail_if_no_peer_cert and crl_check and force Telegraf into mTLS as well as
    OAuth2 (spec 2.17). Deliberately absent; this test says so out loud."""
    proc = subprocess.run(
        ["docker", "exec", "iot-rabbitmq", "rabbitmq-diagnostics", "-q", "listeners"],
        capture_output=True, text=True, check=True,
    )
    assert "amqp/ssl" not in proc.stdout, proc.stdout
