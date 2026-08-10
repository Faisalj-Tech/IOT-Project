"""Ack-after-write AMQP consumer.

Telegraf acknowledges a message once it has been parsed and handed to the output
plugins. This consumer acknowledges only after InfluxDB has confirmed the write, so
the message's fate is decided by the write outcome:

  confirmed write   -> ack
  retryable failure -> nack(requeue=True), the message is redelivered
  fatal failure     -> nack(requeue=False), the message is dead-lettered to dlx

Phase 2 builds this as a comparison arm for the reliability report, not as a
replacement for Telegraf: the assignment specifies Telegraf-based ingest.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from influxdb_client import Point, WritePrecision

log = logging.getLogger(__name__)

REQUIRED_FIELDS = ("ts", "region", "plant", "device", "metric", "value", "unit", "seq", "run_id")
TAG_FIELDS = ("region", "plant", "device", "metric", "unit", "run_id")
FATAL_STATUSES = {400, 401, 403, 404, 413, 422}


def parse_message(body: bytes) -> dict:
    """Validate a message against the Phase 1 contract, raising ValueError on any deviation.

    Strict on purpose: a message this consumer cannot represent must be dead-lettered,
    not silently coerced. That distinction is the whole point of experiment D.
    """
    try:
        raw = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"payload is not JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"payload is not a JSON object: {type(raw).__name__}")

    missing = [f for f in REQUIRED_FIELDS if f not in raw]
    if missing:
        raise ValueError(f"payload missing required fields: {missing}")

    try:
        value = float(raw["value"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"value is not numeric: {raw['value']!r}") from exc
    if isinstance(raw["value"], str):
        raise ValueError(f"value must be a JSON number, not a string: {raw['value']!r}")

    try:
        seq = int(raw["seq"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"seq is not an integer: {raw['seq']!r}") from exc

    try:
        ts = datetime.strptime(raw["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"ts is not RFC3339 with milliseconds: {raw['ts']!r}") from exc

    parsed = {tag: str(raw[tag]) for tag in TAG_FIELDS}
    parsed.update({"value": value, "seq": seq, "ts": ts})
    return parsed


def to_point(parsed: dict) -> Point:
    """Build the point. Schema must match telegraf.conf's json_v2 mapping exactly."""
    point = Point("telemetry")
    for tag in TAG_FIELDS:
        point = point.tag(tag, parsed[tag])
    point = point.field("value", float(parsed["value"]))
    point = point.field("seq", int(parsed["seq"]))
    return point.time(parsed["ts"], WritePrecision.MS)


def classify_write_error(exc: Exception) -> str:
    """Retryable failures come back; fatal ones are dead-lettered.

    A retryable classification for a genuinely fatal error produces an infinite
    redelivery loop, which is exactly the failure mode experiment D looks for.
    """
    status = getattr(exc, "status", None)
    if status is None:
        return "retryable"
    return "fatal" if int(status) in FATAL_STATUSES or 400 <= int(status) < 500 else "retryable"


async def run(
    amqp_url: str,
    queue_name: str,
    influx_url: str,
    influx_token: str,
    influx_org: str,
    influx_bucket: str,
    prefetch_count: int = 50,
) -> None:
    """Consume queue_name, writing each message before acknowledging it."""
    import aio_pika
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=prefetch_count)
        # ensure=False: this user has no `configure` permission and must never
        # redeclare the queue with different arguments.
        queue = await channel.get_queue(queue_name, ensure=False)
        log.info("consuming %s", queue_name)

        async with queue.iterator() as messages:
            async for message in messages:
                try:
                    parsed = parse_message(message.body)
                except ValueError as exc:
                    log.warning("dead-lettering unparseable message: %s", exc)
                    await message.nack(requeue=False)
                    continue
                try:
                    # to_thread, not a direct call: the synchronous write API blocks
                    # through influxdb-client's internal retries during an InfluxDB
                    # outage — tens of seconds — which would stall the event loop and
                    # starve aio-pika's heartbeat, dropping the AMQP connection for
                    # reasons that have nothing to do with the experiment.
                    await asyncio.to_thread(
                        write_api.write,
                        bucket=influx_bucket,
                        org=influx_org,
                        record=to_point(parsed),
                    )
                except Exception as exc:
                    kind = classify_write_error(exc)
                    log.warning("write failed (%s): %s", kind, exc)
                    await message.nack(requeue=(kind == "retryable"))
                    if kind == "retryable":
                        await asyncio.sleep(1.0)
                    continue
                await message.ack()
