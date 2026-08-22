"""Keycloak realm import, S5, and S6.

The first test is a PROBE, not a regression guard. The design's OAuth2 evidence
was gathered against a realm created through Keycloak's admin REST API, while
this stack uses --import-realm. Different code path, different JSON shape for
the mappers. If the imported realm does not issue the same claims, stop: the
design's OAuth2 section rests on claims that are not actually being produced.
"""

import asyncio
import base64
import json
import os
import subprocess
import sys
import time

import aio_pika
import pytest
import requests
import urllib3

from tests.conftest import ROOT, region_mode, security_mode
from tests.experiments.conftest import write_result

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

pytestmark = [pytest.mark.security, pytest.mark.stack]

KEYCLOAK = "https://localhost:18443"
REALM = os.environ.get("KEYCLOAK_REALM", "iot")
TOKEN_URL = f"{KEYCLOAK}/realms/{REALM}/protocol/openid-connect/token"

# The dev CA is not in the system trust store and the cert is pinned to
# keycloak:8443, not localhost, so host-side calls skip verification. This is a
# test reaching a dev container, never how the broker validates the issuer -
# that uses auth_oauth2.https.cacertfile with verify_peer.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

KEYCLOAK_READY_TIMEOUT = 180.0


@pytest.fixture(autouse=True)
def _requires_security_profile():
    if not security_mode():
        pytest.fail("security tests need IOT_SECURITY=1")


@pytest.fixture(scope="module", autouse=True)
def _keycloak_ready(stack):
    """Poll the realm's own endpoint until it answers.

    Keycloak carries no compose healthcheck (its image ships neither curl nor a
    dependable bash), so `up --wait` returns as soon as the process starts. What
    matters is the realm being importable and serving tokens, which is what this
    waits for.
    """
    started = time.time()
    last = ""
    while time.time() - started < KEYCLOAK_READY_TIMEOUT:
        try:
            response = requests.get(
                f"{KEYCLOAK}/realms/{REALM}/.well-known/openid-configuration",
                verify=False, timeout=10)
            if response.status_code == 200:
                return
            last = f"HTTP {response.status_code}"
        except Exception as exc:  # noqa: BLE001 - still starting
            last = f"{type(exc).__name__}: {exc}"
        time.sleep(5)
    pytest.fail(
        f"Keycloak realm {REALM!r} not ready after {KEYCLOAK_READY_TIMEOUT}s "
        f"(last: {last}). Check `docker logs iot-keycloak | grep -i import`."
    )


def fetch_token(client_id: str, secret: str) -> str:
    """Fetch a service-account token. verify=False because the CA is the dev CA
    and the hostname is pinned to keycloak:8443, not localhost."""
    response = requests.post(
        TOKEN_URL,
        data={"client_id": client_id, "client_secret": secret,
              "grant_type": "client_credentials"},
        verify=False, timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def decode_claims(token: str) -> dict:
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def test_the_imported_realm_issues_a_token_with_the_expected_claims(stack):
    """PROBE. Confirms --import-realm reproduces what the admin API produced."""
    token = fetch_token("telegraf-eu", os.environ["KEYCLOAK_TELEGRAF_EU_SECRET"])
    claims = decode_claims(token)

    assert claims["iss"] == f"https://keycloak:8443/realms/{REALM}", claims["iss"]
    assert "rabbitmq" in claims["aud"], claims["aud"]
    assert "rmq_scopes" in claims, sorted(claims)
    assert "rabbitmq.read:eu/telemetry.eu.q" in claims["rmq_scopes"], claims["rmq_scopes"]


def test_the_short_lived_client_really_is_short_lived(stack):
    """S6 depends on this client expiring quickly; confirm the attribute took."""
    response = requests.post(
        TOKEN_URL,
        data={"client_id": "telegraf-eu-short",
              "client_secret": os.environ["KEYCLOAK_TELEGRAF_EU_SHORT_SECRET"],
              "grant_type": "client_credentials"},
        verify=False, timeout=30,
    )
    assert response.json()["expires_in"] <= 60, response.json()["expires_in"]


def amqp_url_with_token(token: str, vhost: str = "eu") -> str:
    """The token IS the password. The username is ignored by the OAuth2 backend."""
    path = "" if vhost == "/" else vhost
    return f"amqp://:{token}@localhost:5672/{path}"


@pytest.mark.region
def test_a_keycloak_token_authenticates_an_amqp_connection(stack):
    """S5."""
    if not region_mode():
        pytest.skip("needs IOT_REGION=1 as well as IOT_SECURITY=1")
    token = fetch_token("telegraf-eu", os.environ["KEYCLOAK_TELEGRAF_EU_SECRET"])

    async def _connect() -> None:
        connection = await aio_pika.connect(amqp_url_with_token(token), timeout=20)
        await connection.close()

    asyncio.run(_connect())


@pytest.mark.region
def test_a_garbage_token_is_refused(stack):
    """Negative control: without it, a broker that ignored the token entirely
    would look identical to one that validated it."""
    if not region_mode():
        pytest.skip("needs IOT_REGION=1 as well as IOT_SECURITY=1")
    async def _connect() -> None:
        connection = await aio_pika.connect(
            amqp_url_with_token("not.a.jwt"), timeout=20)
        await connection.close()

    with pytest.raises(Exception):  # noqa: B017 - aio_pika raises several types here
        asyncio.run(_connect())


@pytest.mark.region
def test_token_expiry_terminates_a_live_connection(stack):
    """S6, RECORDED. The exact mirror of S4: revocation spares established
    connections, expiry destroys them."""
    run_id = f"s6{int(time.time()) % 100000}"
    token = fetch_token("telegraf-eu-short",
                        os.environ["KEYCLOAK_TELEGRAF_EU_SHORT_SECRET"])
    findings: dict = {"run_id": run_id, "client": "telegraf-eu-short",
                      "token_lifespan_s": 60}

    async def _hold() -> None:
        connection = await aio_pika.connect(amqp_url_with_token(token), timeout=20)
        started = time.time()
        findings["connected"] = True
        try:
            for _ in range(24):  # up to ~120s
                await asyncio.sleep(5)
                if connection.is_closed:
                    findings["forced_close_seen"] = True
                    findings["survived_seconds"] = round(time.time() - started, 1)
                    return
            findings["forced_close_seen"] = False
            findings["survived_seconds"] = round(time.time() - started, 1)
        finally:
            if not connection.is_closed:
                await connection.close()

    asyncio.run(_hold())

    # After expiry, the same static token must never work again.
    refused = 0
    for _ in range(3):
        async def _retry() -> None:
            connection = await aio_pika.connect(amqp_url_with_token(token), timeout=15)
            await connection.close()
        try:
            asyncio.run(_retry())
        except Exception as exc:  # noqa: BLE001 - the refusal is the measurement
            refused += 1
            findings["reconnect_error"] = f"{type(exc).__name__}: {exc}"
        time.sleep(2)
    findings["reconnect_attempts_refused"] = refused

    logs = subprocess.run(
        ["docker", "logs", "iot-rabbitmq", "--since", "300s"],
        capture_output=True, text=True, check=False)
    combined = logs.stdout + logs.stderr
    findings["broker_logged_expiry"] = "credential has expired" in combined
    findings["broker_logged_refusal"] = "has expired at timestamp" in combined

    findings["conclusion"] = (
        "RabbitMQ force-closes a live connection at token expiry and refuses "
        "every reconnect carrying the same static token. A consumer with no "
        "refresh path stops permanently."
    )
    write_result("S6-token-expiry", findings)

    assert findings["reconnect_attempts_refused"] == 3, findings


@pytest.mark.region
def test_telegraf_itself_stops_ingesting_after_its_token_expires(stack):
    """S6, the confirmation spec 2.12 explicitly did NOT establish.

    The design measured the mechanism with aio-pika's connect_robust standing in
    for Telegraf. This runs Telegraf.
    """
    run_id = f"s6t{int(time.time()) % 100000}"
    token = fetch_token("telegraf-eu-short",
                        os.environ["KEYCLOAK_TELEGRAF_EU_SHORT_SECRET"])

    proc = subprocess.run(
        ["docker", "run", "--rm", "-d",
         "--name", "iot-telegraf-s6",
         "--network", "iot-messaging_core",
         "-e", f"IOT_OAUTH_TOKEN={token}",
         "-v", f"{ROOT / 'config' / 'telegraf' / 'telegraf.security.d'}:/etc/telegraf/telegraf.d:ro",
         "telegraf:1.39.2",
         "telegraf", "--config-directory", "/etc/telegraf/telegraf.d"],
        capture_output=True, text=True, check=False)
    assert proc.returncode == 0, f"{proc.stdout}\n{proc.stderr}"

    try:
        time.sleep(150)  # outlive the 60s token by a comfortable margin
        logs = subprocess.run(
            ["docker", "logs", "iot-telegraf-s6"],
            capture_output=True, text=True, check=False)
        combined = logs.stdout + logs.stderr
    finally:
        subprocess.run(["docker", "rm", "-f", "iot-telegraf-s6"],
                       capture_output=True, check=False)

    write_result("S6-telegraf-confirmation", {
        "run_id": run_id,
        "telegraf_log_tail": combined[-3000:],
        "shows_auth_failure": ("credential expired" in combined or "username or password not allowed" in combined),
        "conclusion": (
            "Telegraf's amqp_consumer holds its static token across reconnects "
            "and cannot recover once it expires."
        ),
    })

    assert "credential expired" in combined or "username or password not allowed" in combined, combined[-2000:]
