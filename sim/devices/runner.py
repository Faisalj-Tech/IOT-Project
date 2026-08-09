"""Async MQTT publishing loop for simulated devices."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import sys
from collections.abc import Sequence

import aiomqtt

from sim.devices.payload import DeviceSpec, build_payload, topic_for

log = logging.getLogger(__name__)

# Windows requires SelectorEventLoopPolicy for aiomqtt/paho-mqtt compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def _publish_device(
    spec: DeviceSpec,
    rate_hz: float,
    duration_s: float,
    run_id: str,
    host: str,
    port: int,
    username: str,
    password: str,
    max_reconnects: int,
) -> int:
    """Publish for duration_s seconds, returning how many messages were sent.

    `seq` survives reconnects: it is owned by this coroutine, not by the
    connection, so a dropped socket never restarts the sequence.
    """
    interval = 1.0 / rate_hz
    deadline = asyncio.get_running_loop().time() + duration_s
    topic = topic_for(spec)
    seq = 0
    attempt = 0
    backoff = 0.5

    while asyncio.get_running_loop().time() < deadline:
        try:
            async with aiomqtt.Client(
                hostname=host,
                port=port,
                username=username,
                password=password,
                identifier=f"{spec.device}-{run_id}",
            ) as client:
                backoff = 0.5
                while asyncio.get_running_loop().time() < deadline:
                    seq += 1
                    value = round(spec.baseline + random.uniform(-spec.jitter, spec.jitter), 3)
                    payload = build_payload(spec, seq=seq, run_id=run_id, value=value)
                    await client.publish(topic, payload=json.dumps(payload).encode(), qos=1)
                    await asyncio.sleep(interval)
        except aiomqtt.MqttError as exc:
            if attempt >= max_reconnects:
                raise
            attempt += 1
            log.warning("%s disconnected (%s); reconnecting in %.1fs", spec.device, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    return seq


async def run_devices(
    specs: Sequence[DeviceSpec],
    rate_hz: float,
    duration_s: float,
    run_id: str,
    host: str = "localhost",
    port: int = 1883,
    username: str = "device",
    password: str = "devicepass",
    max_reconnects: int = 5,
) -> dict[str, int]:
    counts = await asyncio.gather(
        *(
            _publish_device(
                spec,
                rate_hz=rate_hz,
                duration_s=duration_s,
                run_id=run_id,
                host=host,
                port=port,
                username=username,
                password=password,
                max_reconnects=max_reconnects,
            )
            for spec in specs
        )
    )
    return {spec.device: count for spec, count in zip(specs, counts)}
