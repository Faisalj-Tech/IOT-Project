"""Phase 4 isolation proofs: vhost, topic, and network.

Three independent denials, three different failure modes. Run with:
    IOT_REGION=1 KEEP_STACK=1 .venv/Scripts/python.exe -m pytest \
        tests/experiments/test_region_isolation.py -m region -v -s

Host-side clients reach the region vhosts through the unmapped 1883 listener
using colon-form usernames (`eu:device-eu`). The region listeners themselves are
bound to per-network addresses and are unreachable from the host by design, which
is what Task 9's network test proves.
"""

import asyncio
import json
import os
import subprocess
import sys
import time

import aiomqtt
from aiomqtt.exceptions import MqttConnectError
import pytest

from tests.conftest import compose_files, region_mode, ROOT

pytestmark = [pytest.mark.region, pytest.mark.stack]

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PASSWORDS = {
    "device-eu": os.environ.get("RABBITMQ_DEVICE_EU_PASSWORD", "devicepass-eu"),
    "device-us": os.environ.get("RABBITMQ_DEVICE_US_PASSWORD", "devicepass-us"),
}

# MQTT 5 reason codes. 135 is returned in CONNACK when the broker refuses the
# user access to the vhost; 128 comes back in PUBACK when a topic permission
# denies the routing key. Two stages, two codes, two independent boundaries.
NOT_AUTHORIZED = 135
UNSPECIFIED_ERROR = 128


@pytest.fixture(autouse=True)
def _requires_region_profile():
    if not region_mode():
        pytest.fail(
            "region tests need IOT_REGION=1 so the stack fixture brings up "
            "compose.region.yml; without it the eu/us vhosts do not exist"
        )


def _payload(region: str, seq: int, run_id: str) -> bytes:
    return json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S.", time.gmtime()) + f"{seq % 1000:03d}Z",
        "region": region, "plant": "plant1", "device": "press-01", "metric": "temp",
        "value": 70.0, "unit": "C", "seq": seq, "run_id": run_id,
    }).encode()


async def _connect_and_publish(username: str, topic: str, region: str) -> None:
    """Connect as `vhost:user` on 1883 and publish one QoS-1 message."""
    user = username.split(":")[-1]
    async with aiomqtt.Client(
        hostname="localhost", port=1883,
        username=username, password=PASSWORDS[user],
        identifier=f"isolation-{username.replace(':', '-')}-{int(time.time() * 1000) % 100000}",
        timeout=15,
    ) as client:
        await client.publish(topic, payload=_payload(region, 1, "isolation"), qos=1)


def test_a_device_cannot_connect_to_another_regions_vhost(stack):
    """R2. device-eu holds no permission in vhost us, so the CONNECT is refused
    before any publish is attempted."""
    with pytest.raises(MqttConnectError) as excinfo:
        asyncio.run(_connect_and_publish(
            "us:device-eu", "region/us/plant1/press-01/temp", "us"))
    # aiomqtt renders the code into the message as "[code:135] Not authorized".
    # Match the rendered string; `135 in str(...)` is an int in a str and raises
    # TypeError instead of failing the assertion.
    message = str(excinfo.value)
    assert f"code:{NOT_AUTHORIZED}" in message or "Not authorized" in message, message


def test_each_device_can_connect_to_its_own_vhost(stack):
    """R2's positive control. Without this, a broken password would look
    identical to an enforced boundary."""
    asyncio.run(_connect_and_publish(
        "eu:device-eu", "region/eu/plant1/press-01/temp", "eu"))
    asyncio.run(_connect_and_publish(
        "us:device-us", "region/us/plant1/press-01/temp", "us"))


def test_a_device_cannot_publish_into_another_regions_routing_key(stack):
    """R3. An authorized eu connection publishing a us routing key is refused by
    the topic permission, at PUBACK rather than at CONNECT."""
    started = time.time()
    with pytest.raises(aiomqtt.MqttCodeError) as excinfo:
        asyncio.run(_connect_and_publish(
            "eu:device-eu", "region/us/plant1/press-01/temp", "eu"))
    message = str(excinfo.value)
    assert f"code:{UNSPECIFIED_ERROR}" in message, message

    logs = subprocess.run(
        ["docker", "logs", "iot-rabbitmq", "--since", f"{int(time.time() - started) + 30}s"],
        capture_output=True, text=True, check=False,
    )
    combined = logs.stdout + logs.stderr
    assert "MQTT topic access refused" in combined, combined[-2000:]
    assert "region.us.plant1.press-01.temp" in combined, combined[-2000:]


REGION_ADDRESSES = {"eu": ("172.28.1.10", 1893), "us": ("172.28.2.10", 1993)}


def _probe_from(region: str, address: str, port: int) -> subprocess.CompletedProcess:
    """Try one TCP connect from inside that region's sim container.

    `compose run --rm` starts a throwaway container attached to exactly the
    networks the service declares, which is the whole point: the probe inherits
    the simulator's network position without needing the simulator to be running.
    Python is already in the image, so no extra package is installed.
    """
    files: list[str] = []
    for name in compose_files():
        files += ["-f", name]
    return subprocess.run(
        ["docker", "compose", *files, "--profile", "sim", "run", "--rm",
         "--entrypoint", "python", f"sim-{region}", "-c",
         f"import socket; socket.create_connection(('{address}', {port}), timeout=5)"],
        capture_output=True, text=True, check=False, cwd=ROOT,
    )


def test_a_region_container_reaches_only_its_own_listener(stack):
    """R4. The listeners bind one interface each and the networks do not route to
    one another, so this fails twice over — which is the point of doing both."""
    for region, (address, port) in REGION_ADDRESSES.items():
        own = _probe_from(region, address, port)
        assert own.returncode == 0, (
            f"sim-{region} could not reach its own listener {address}:{port}\n"
            f"{own.stdout}\n{own.stderr}"
        )

    other = {"eu": "us", "us": "eu"}
    for region, foreign in other.items():
        address, port = REGION_ADDRESSES[foreign]
        probe = _probe_from(region, address, port)
        assert probe.returncode != 0, (
            f"sim-{region} reached the {foreign} listener at {address}:{port}; "
            "the region networks are not isolated"
        )
        assert "Error" in probe.stderr or "error" in probe.stderr, probe.stderr
