"""S1 and S2: certificate identity, and Phase 4's layers reached through it.

S1 runs on base+security against vhost '/' with CN=device.
S2 runs on base+region+security from inside each region network (Task 9).

The positive control is not optional. Without it, a broken TLS setup and an
enforced boundary produce the same red test.
"""

import asyncio
import json
import ssl
import subprocess
import sys
import time

import aiomqtt
import pytest
from aiomqtt.exceptions import MqttConnectError

from tests.conftest import ROOT, compose_files, region_mode, security_mode

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


REGION_TLS = {"eu": ("172.28.1.10", 9883), "us": ("172.28.2.10", 9993)}


def run_tls_probe(region: str, host: str, port: int, cert: str,
                  publish: str | None = None) -> subprocess.CompletedProcess:
    """Run the TLS probe from inside that region's own Docker network.

    `compose run --rm` starts a throwaway container attached to exactly the
    networks the service declares, so the probe inherits the simulator's network
    position without the simulator running. certs/ is mounted at /certs by
    compose.region-security.yml.
    """
    files: list[str] = []
    for name in compose_files():
        files += ["-f", name]
    argv = ["docker", "compose", *files, "--profile", "sim", "run", "--rm",
            "--entrypoint", "python", f"sim-{region}", "-m", "sim.tlsprobe",
            "--host", host, "--port", str(port),
            "--cert", f"/certs/{cert}.crt", "--key", f"/certs/{cert}.key",
            "--region", region, "--cid", f"s2-{region}-{cert}"]
    if publish:
        argv += ["--publish", publish]
    return subprocess.run(argv, capture_output=True, text=True, check=False, cwd=ROOT)


@pytest.mark.region
def test_a_region_certificate_publishes_into_its_own_region(stack):
    """S2 positive control, on the IP-bound TLS listener."""
    if not region_mode():
        pytest.skip("needs IOT_REGION=1 as well as IOT_SECURITY=1")
    host, port = REGION_TLS["eu"]
    proc = run_tls_probe("eu", host, port, "device-eu-a",
                         publish="region/eu/plant1/press-01/temp")
    assert "PUBLISH_OK" in proc.stdout, f"{proc.stdout}\n{proc.stderr}"


@pytest.mark.region
def test_a_region_certificate_cannot_publish_into_another_region(stack):
    """S2. R3's topic-permission denial, reached by certificate. Reason code 128."""
    if not region_mode():
        pytest.skip("needs IOT_REGION=1 as well as IOT_SECURITY=1")
    host, port = REGION_TLS["eu"]
    proc = run_tls_probe("eu", host, port, "device-eu-a",
                         publish="region/us/plant1/press-01/temp")
    assert "CONNECT_OK" in proc.stdout, f"{proc.stdout}\n{proc.stderr}"
    assert "PUBLISH_OK" not in proc.stdout, f"{proc.stdout}\n{proc.stderr}"
    assert "code:128" in proc.stdout, f"{proc.stdout}\n{proc.stderr}"


@pytest.mark.region
def test_a_region_certificate_is_refused_on_the_other_regions_listener(stack):
    """S2. R2's vhost denial, reached by certificate. Reason code 135.

    device-us's cert reaching the eu listener is only possible because the probe
    runs on region-eu; the point is that the credential layer refuses it even
    when the network layer would have allowed it.
    """
    if not region_mode():
        pytest.skip("needs IOT_REGION=1 as well as IOT_SECURITY=1")
    host, port = REGION_TLS["eu"]
    proc = run_tls_probe("eu", host, port, "device-us-a")
    assert "code:135" in proc.stdout, f"{proc.stdout}\n{proc.stderr}"
