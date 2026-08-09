"""CLI: python -m sim.devices --devices 5 --rate 2 --duration 30"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import uuid

from sim.devices.payload import default_specs
from sim.devices.runner import run_devices


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate MQTT telemetry devices")
    parser.add_argument("--devices", type=int, default=5, help="number of virtual devices")
    parser.add_argument("--rate", type=float, default=1.0, help="messages per second per device")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds to run")
    parser.add_argument("--region", default="eu")
    parser.add_argument("--plant", default="plant1")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--run-id", default=None, help="defaults to a random 8-char id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    run_id = args.run_id or uuid.uuid4().hex[:8]
    specs = default_specs(args.devices, region=args.region, plant=args.plant)

    logging.info("run_id=%s devices=%d rate=%.2fHz duration=%.0fs", run_id, args.devices, args.rate, args.duration)
    published = asyncio.run(
        run_devices(
            specs,
            rate_hz=args.rate,
            duration_s=args.duration,
            run_id=run_id,
            host=args.host,
            port=args.port,
            username=os.environ.get("RABBITMQ_DEVICE_USER", "device"),
            password=os.environ.get("RABBITMQ_DEVICE_PASSWORD", "devicepass"),
        )
    )
    logging.info("published %d messages total: %s", sum(published.values()), published)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
