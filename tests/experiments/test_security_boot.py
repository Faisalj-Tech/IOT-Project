"""Task 3 gate: the security overlay boots and its two preconditions hold.

If the broker cannot fetch the CRL, crl_check=peer rejects EVERY certificate
with a message that reads like a broken TLS setup rather than a networking
fault (spec 2.4, 2.8). Asserting reachability here means later failures cannot
be that.
"""

import json
import subprocess

import pytest

from tests.conftest import security_mode

pytestmark = [pytest.mark.security, pytest.mark.stack]


@pytest.fixture(autouse=True)
def _requires_security_profile():
    if not security_mode():
        pytest.fail(
            "security tests need IOT_SECURITY=1 so the stack fixture brings up "
            "compose.security.yml; without it there is no TLS listener"
        )


def _listeners() -> str:
    proc = subprocess.run(
        ["docker", "exec", "iot-rabbitmq", "rabbitmq-diagnostics", "-q", "listeners"],
        capture_output=True, text=True, check=True,
    )
    return proc.stdout


def test_the_tls_listener_is_up(stack):
    output = _listeners()
    assert "port: 8883, protocol: mqtt/ssl" in output, output


def test_the_plaintext_listener_is_untouched(stack):
    """Spec decision 2: the overlay adds TLS, it does not close 1883."""
    output = _listeners()
    assert "port: 1883, protocol: mqtt" in output, output


def test_the_broker_can_fetch_the_crl(stack):
    """The CDP URI, the crl service name and its port must all agree."""
    proc = subprocess.run(
        ["docker", "exec", "iot-rabbitmq", "bash", "-c",
         "wget -q -O /tmp/probe.crl http://crl:8080/root.crl && "
         "test -s /tmp/probe.crl && echo FETCHED"],
        capture_output=True, text=True, check=False,
    )
    assert "FETCHED" in proc.stdout, (
        f"broker cannot reach the CRL at the URI baked into every device cert.\n"
        f"stdout={proc.stdout}\nstderr={proc.stderr}"
    )


def test_ssl_options_carries_the_crl_settings(stack):
    """Guards against advanced.config being ignored or partially applied."""
    proc = subprocess.run(
        ["docker", "exec", "iot-rabbitmq", "rabbitmqctl", "-q", "environment"],
        capture_output=True, text=True, check=True,
    )
    for expected in ("{crl_check,peer}", "cacertfile", "{fail_if_no_peer_cert,true}"):
        assert expected in proc.stdout, f"{expected} missing from ssl_options"
