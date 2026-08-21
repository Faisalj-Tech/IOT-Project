"""S1 and S2: certificate identity, and Phase 4's layers reached through it.

S1 runs on base+security against vhost '/' with CN=device.
S2 runs on base+region+security from inside each region network (Task 9).

The positive control is not optional. Without it, a broken TLS setup and an
enforced boundary produce the same red test.
"""

import asyncio
import json
import ssl
import sys
import time

import aiomqtt
import pytest
from aiomqtt.exceptions import MqttConnectError

from tests.conftest import ROOT, security_mode

pytestmark = [pytest.mark.security, pytest.mark.stack]

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CERTS = ROOT / "certs"
BAD_CREDENTIALS = 134


@pytest.fixture(autouse=True)
def _requires_security_profile():
    if not security_mode():
        pytest.fail("security tests need IOT_SECURITY=1; there is no TLS listener without it")


def tls_params(cert_name: str) -> aiomqtt.TLSParameters:
    return aiomqtt.TLSParameters(
        ca_certs=str(CERTS / "rootCA.crt"),
        certfile=str(CERTS / f"{cert_name}.crt"),
        keyfile=str(CERTS / f"{cert_name}.key"),
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )


def tls_client(cert_name: str, *, host: str = "localhost", port: int = 8883,
               timeout: float = 15.0) -> aiomqtt.Client:
    """A client that sends NO username and NO password. The cert is the credential."""
    return aiomqtt.Client(
        hostname=host, port=port,
        tls_params=tls_params(cert_name), tls_insecure=False,
        identifier=f"s1-{cert_name}-{int(time.time() * 1000) % 100000}",
        timeout=timeout,
    )


def _payload(seq: int, run_id: str) -> bytes:
    return json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S.", time.gmtime()) + f"{seq % 1000:03d}Z",
        "region": "eu", "plant": "plant1", "device": "press-01", "metric": "temp",
        "value": 70.0, "unit": "C", "seq": seq, "run_id": run_id,
    }).encode()


async def _connect_and_publish(cert_name: str, topic: str, **kw) -> None:
    async with tls_client(cert_name, **kw) as client:
        await client.publish(topic, payload=_payload(1, "s1"), qos=1)


def test_a_certificate_authenticates_with_no_password(stack):
    """S1 positive control. CN=device names a user that exists in the base
    definitions and holds permissions on vhost '/', so this must succeed."""
    asyncio.run(_connect_and_publish("device-a", "region/eu/plant1/press-01/temp"))


def test_a_second_certificate_with_the_same_cn_also_authenticates(stack):
    """The sibling of the pair Task 5 revokes. Both work until one is revoked;
    that is what makes the revocation certificate-scoped rather than user-scoped."""
    asyncio.run(_connect_and_publish("device-b", "region/eu/plant1/press-01/temp"))


def test_a_certificate_whose_cn_names_no_user_is_refused(stack):
    """S1 negative. The cert is validly signed by the same CA and carries a CDP,
    so TLS itself succeeds; the internal backend then finds no such user.

    Returns code 134 (bad credentials) because the CN resolves to no account at all —
    authentication itself fails. This differs from code 135 (not authorized), which is
    returned when an EXISTING user is refused vhost access (see spec §2 line 75 / §6 S2) —
    that's a different test in a later task."""
    with pytest.raises(MqttConnectError) as excinfo:
        asyncio.run(_connect_and_publish("unknown-cn", "region/eu/plant1/press-01/temp"))
    assert f"code:{BAD_CREDENTIALS}" in str(excinfo.value), str(excinfo.value)


def test_a_connection_with_no_client_certificate_is_refused(stack):
    """fail_if_no_peer_cert = true, so the handshake dies before MQTT begins."""
    async def _bare() -> None:
        params = aiomqtt.TLSParameters(
            ca_certs=str(CERTS / "rootCA.crt"),
            cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        async with aiomqtt.Client(hostname="localhost", port=8883,
                                  tls_params=params, identifier="s1-nocert",
                                  timeout=15):
            pass

    with pytest.raises(aiomqtt.MqttError):
        asyncio.run(_bare())
