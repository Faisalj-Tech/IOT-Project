"""Keycloak realm import, S5, and S6.

The first test is a PROBE, not a regression guard. The design's OAuth2 evidence
was gathered against a realm created through Keycloak's admin REST API, while
this stack uses --import-realm. Different code path, different JSON shape for
the mappers. If the imported realm does not issue the same claims, stop: the
design's OAuth2 section rests on claims that are not actually being produced.
"""

import base64
import json
import os
import time

import pytest
import requests
import urllib3

from tests.conftest import ROOT, security_mode

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
