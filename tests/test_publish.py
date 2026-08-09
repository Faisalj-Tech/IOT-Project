import asyncio

import pytest

from sim.devices.payload import default_specs
from sim.devices.runner import run_devices

pytestmark = pytest.mark.stack


def test_wrong_credentials_are_rejected():
    specs = default_specs(1, region="eu", plant="plant1")
    with pytest.raises(Exception):
        asyncio.run(
            run_devices(
                specs,
                rate_hz=1.0,
                duration_s=1.0,
                run_id="badauth",
                host="localhost",
                port=1883,
                username="device",
                password="wrong-password",
                max_reconnects=0,
            )
        )
