"""Async MQTT publishing loop for simulated devices."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import ssl
import sys
from collections.abc import Sequence
from pathlib import Path

import aiomqtt

from sim.devices.payload import DeviceSpec, build_payload, topic_for

log = logging.getLogger(__name__)

# Windows requires SelectorEventLoopPolicy for aiomqtt/paho-mqtt compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def resolve_nodes(
    host: str, port: int, nodes: Sequence[tuple[str, int]] | None
) -> list[tuple[str, int]]:
    """Normalise the single-endpoint and node-list forms to one list.

    Phase 1/2 callers pass host/port and get a one-element list, so their
    behaviour is bit-for-bit unchanged.
    """
    if nodes is None:
        return [(host, port)]
    resolved = [(h, int(p)) for h, p in nodes]
    if not resolved:
        raise ValueError("nodes must not be empty; omit it to use host/port instead")
    return resolved


def build_tls_params(
    cafile: str | None, certfile: str | None, keyfile: str | None
) -> aiomqtt.TLSParameters | None:
    """Build TLS parameters, or None when no TLS was asked for.

    Phase 1-4 callers pass nothing and get None, so their behaviour is unchanged —
    the same contract resolve_nodes() keeps for the node-list argument.

    A partial set raises rather than degrading to an unauthenticated connection:
    under mqtt.ssl_cert_login the client certificate IS the credential, so
    silently dropping it would produce a connection with no identity at all.
    """
    provided = [p for p in (cafile, certfile, keyfile) if p]
    if not provided:
        return None
    if len(provided) != 3:
        raise ValueError(
            "TLS needs all three of --cafile, --certfile and --keyfile; "
            f"got {len(provided)}"
        )
    for path in (cafile, certfile, keyfile):
        if not Path(path).exists():
            raise FileNotFoundError(path)
    return aiomqtt.TLSParameters(
        ca_certs=cafile,
        certfile=certfile,
        keyfile=keyfile,
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )


async def _publish_device(
    spec: DeviceSpec,
    rate_hz: float,
    duration_s: float,
    run_id: str,
    nodes: list[tuple[str, int]],
    username: str,
    password: str,
    max_reconnects: int,
    tls_params: aiomqtt.TLSParameters | None = None,
) -> int:
    """Publish for duration_s seconds, returning how many messages were sent.

    Guarantees publication of at least ceil(rate_hz * duration_s) messages,
    regardless of system load or scheduler jitter. Continues until both the
    time budget is exhausted AND the minimum count is met.

    `seq` survives reconnects: it is owned by this coroutine, not by the
    connection, so a dropped socket never restarts the sequence.

    Deadline is set after the first successful connect to exclude connection
    handshake time from the publish budget. Reconnect handshake time counts
    against remaining duration (which is correct for real outages).
    """
    interval = 1.0 / rate_hz
    minimum_count = math.ceil(rate_hz * duration_s)
    deadline: float | None = None
    topic = topic_for(spec)
    seq = 0
    attempt = 0
    backoff = 0.5
    node_index = 0

    while deadline is None or asyncio.get_running_loop().time() < deadline or seq < minimum_count:
        host, port = nodes[node_index % len(nodes)]
        try:
            # Under cert login the broker takes the identity from the certificate,
            # so username/password are omitted entirely rather than sent empty.
            client_kwargs: dict = {
                "hostname": host,
                "port": port,
                "identifier": f"{spec.device}-{run_id}",
            }
            if tls_params is not None:
                client_kwargs["tls_params"] = tls_params
                client_kwargs["tls_insecure"] = False
            else:
                client_kwargs["username"] = username
                client_kwargs["password"] = password
            async with aiomqtt.Client(**client_kwargs) as client:
                backoff = 0.5
                attempt = 0
                if deadline is None:
                    deadline = asyncio.get_running_loop().time() + duration_s
                while asyncio.get_running_loop().time() < deadline or seq < minimum_count:
                    candidate = seq + 1
                    value = round(spec.baseline + random.uniform(-spec.jitter, spec.jitter), 3)
                    payload = build_payload(spec, seq=candidate, run_id=run_id, value=value)
                    await client.publish(topic, payload=json.dumps(payload).encode(), qos=1)
                    seq = candidate
                    await asyncio.sleep(interval)
        except aiomqtt.MqttError as exc:
            if attempt >= max_reconnects:
                raise
            attempt += 1
            node_index += 1  # a dead node stays dead; try the next one
            log.warning(
                "%s disconnected from %s:%s (%s); retrying on %s:%s in %.1fs",
                spec.device, host, port, exc,
                *nodes[node_index % len(nodes)], backoff,
            )
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
    max_reconnects: int = 12,
    nodes: Sequence[tuple[str, int]] | None = None,
    tls_params: aiomqtt.TLSParameters | None = None,
) -> dict[str, int]:
    resolved = resolve_nodes(host, port, nodes)
    counts = await asyncio.gather(
        *(
            _publish_device(
                spec,
                rate_hz=rate_hz,
                duration_s=duration_s,
                run_id=run_id,
                nodes=resolved,
                username=username,
                password=password,
                max_reconnects=max_reconnects,
                tls_params=tls_params,
            )
            for spec in specs
        )
    )
    return {spec.device: count for spec, count in zip(specs, counts)}
