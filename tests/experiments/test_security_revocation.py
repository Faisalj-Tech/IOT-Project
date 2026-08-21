"""S3 and S4: revocation, and the thing revocation does not do.

S3 asserts that revoking one certificate refuses it while a SIBLING SHARING THE
SAME CN keeps working. That sibling is the entire test. Deleting the RabbitMQ
user would also stop the first device — and would stop the second one too.

S4 records that revocation does not disturb a connection that already exists
(spec 2.9), which is why the demo needs a force-close step.
"""

import asyncio
import socket
import ssl
import sys
import time

import aiomqtt
import pytest
import requests

from scripts import make_certs
from tests.conftest import RABBIT_API, ROOT, security_mode
from tests.experiments.conftest import write_result
from tests.experiments.test_security_identity import tls_client

pytestmark = [pytest.mark.security, pytest.mark.stack]

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CERTS = ROOT / "certs"

# Measured at ~13s with no broker restart (spec 2.7). The cache TTL itself was
# never characterised, so poll rather than sleep a fixed amount.
REFUSAL_TIMEOUT = 90.0


@pytest.fixture(autouse=True)
def _requires_security_profile():
    if not security_mode():
        pytest.fail("security tests need IOT_SECURITY=1")


@pytest.fixture(autouse=True)
def _clean_revocations():
    """One test's revocation must not leak into the next."""
    yield
    make_certs.unrevoke_all(CERTS)
    make_certs.write_crl(CERTS)


def revoke_and_publish(name: str) -> None:
    """Revoke one cert and republish the CRL the broker fetches over HTTP.

    certs/ is bind-mounted into the crl service, so writing the file publishes it.
    """
    make_certs.revoke(CERTS, name)
    make_certs.write_crl(CERTS)


def _connect(cert_name: str, **kw) -> None:
    async def _go() -> None:
        async with tls_client(cert_name, **kw):
            pass
    asyncio.run(_go())


def wait_until_refused(cert_name: str, timeout: float = REFUSAL_TIMEOUT) -> float:
    """Poll until the broker refuses this cert's TLS handshake. Returns seconds taken.

    The broker sends the certificate_revoked alert lazily (after TLS handshake
    completes, when the client first tries to read). Calling recv(1) triggers this.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=str(CERTS / "rootCA.crt"))
    ctx.load_cert_chain(
        certfile=str(CERTS / f"{cert_name}.crt"),
        keyfile=str(CERTS / f"{cert_name}.key"))
    started = time.time()
    last: Exception | None = None
    while time.time() - started < timeout:
        try:
            with socket.create_connection(("localhost", 8883), timeout=15) as sock:
                ssock = ctx.wrap_socket(sock, server_hostname="localhost")
                ssock.recv(1)  # Must read to trigger certificate_revoked alert
                ssock.close()
        except (ssl.SSLError, OSError) as exc:
            last = exc
            if "REVOKED" in str(exc).upper():
                return time.time() - started
        time.sleep(3)
    raise AssertionError(
        f"{cert_name} was still accepted after {timeout}s; last error: {last}")


def test_a_non_revoked_certificate_connects_cleanly_at_the_tls_layer(stack):
    """Negative control for wait_until_refused()/the TLS-layer test: proves recv(1)
    is necessary because a non-revoked cert's handshake succeeds and recv() does not
    raise a REVOKED error — distinguishing a genuine revocation alert from any other
    immediate-failure mode the raw-SSL probe might otherwise mask.

    Device-b is never revoked in this test file; this confirms the broker accepts it."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=str(CERTS / "rootCA.crt"))
    ctx.load_cert_chain(certfile=str(CERTS / "device-b.crt"), keyfile=str(CERTS / "device-b.key"))
    with socket.create_connection(("localhost", 8883), timeout=15) as sock:
        ssock = ctx.wrap_socket(sock, server_hostname="localhost")  # Must not raise
        # recv() on non-revoked cert must not raise ssl.SSLError with REVOKED text
        try:
            ssock.recv(1)  # May return empty (broker waits for CONNECT) but must not raise
        except ssl.SSLError as e:
            if "REVOKED" in str(e).upper():
                raise AssertionError(f"Non-revoked cert device-b wrongly rejected: {e}")
        ssock.close()


def test_revoking_one_certificate_leaves_its_sibling_working(stack):
    """S3. device-a and device-b both carry CN=device."""
    _connect("device-a")   # both work before revocation
    _connect("device-b")

    revoke_and_publish("device-a")
    elapsed = wait_until_refused("device-a")

    # The sibling is what makes this a certificate-scoped result.
    _connect("device-b")

    write_result("S3-cert-scoped-revocation", {
        "run_id": f"s3{int(time.time()) % 100000}",
        "revoked_cert": "device-a",
        "sibling_cert": "device-b",
        "shared_common_name": "device",
        "seconds_until_refused": round(elapsed, 1),
        "sibling_still_connects": True,
    })


def test_the_revoked_certificate_is_refused_at_the_tls_layer(stack):
    """The refusal must be a TLS alert, not an MQTT reason code - happens at the
    TLS layer (before any MQTT CONNECT packet is sent or received)."""
    # Prove the broker's CRL cache reflects the current (un-revoked, per _clean_revocations)
    # state before we revoke. Without this, a stale cache from a prior test could mask
    # whether the broker actually re-checks the CRL or just returns a cached decision.
    _connect("device-a")  # Must succeed; if it fails, the cache didn't clear from prior test
    revoke_and_publish("device-a")
    wait_until_refused("device-a")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=str(CERTS / "rootCA.crt"))
    ctx.load_cert_chain(certfile=str(CERTS / "device-a.crt"), keyfile=str(CERTS / "device-a.key"))
    with pytest.raises(ssl.SSLError) as excinfo:
        with socket.create_connection(("localhost", 8883), timeout=15) as sock:
            ssock = ctx.wrap_socket(sock, server_hostname="localhost")
            ssock.recv(1)  # Must read to trigger certificate_revoked alert
            ssock.close()
    assert "REVOKED" in str(excinfo.value).upper(), str(excinfo.value)
