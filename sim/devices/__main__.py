"""CLI: python -m sim.devices --devices 5 --rate 2 --duration 30"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import socket
import uuid
from pathlib import Path

from sim.devices.payload import default_specs
from sim.devices.runner import build_tls_params, run_devices


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
    parser.add_argument(
        "--username",
        default=os.environ.get("RABBITMQ_DEVICE_USER", "device"),
        help="MQTT username; use vhost:user form to select a vhost on an unmapped port",
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("RABBITMQ_DEVICE_PASSWORD", "devicepass"),
    )
    parser.add_argument("--cafile", default=None, help="CA bundle for TLS")
    parser.add_argument("--certfile", default=None, help="client certificate (PEM)")
    parser.add_argument("--keyfile", default=None, help="client private key (PEM)")
    parser.add_argument(
        "--device-prefix",
        default=socket.gethostname(),
        help="device-name prefix; defaults to the container hostname, which Docker "
             "guarantees unique per replica (a --scale'd service shares one command line)",
    )
    parser.add_argument(
        "--device-width", type=int, default=3,
        help="zero-padding width for the device index",
    )
    parser.add_argument(
        "--report", default=None,
        help="path to write this replica's JSON report to; the swarm driver aggregates these",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    run_id = args.run_id or uuid.uuid4().hex[:8]
    specs = default_specs(args.devices, region=args.region, plant=args.plant,
                          prefix=args.device_prefix, width=args.device_width)

    tls_params = build_tls_params(args.cafile, args.certfile, args.keyfile)
    logging.info("run_id=%s devices=%d rate=%.2fHz duration=%.0fs", run_id, args.devices, args.rate, args.duration)
    published = asyncio.run(
        run_devices(
            specs,
            rate_hz=args.rate,
            duration_s=args.duration,
            run_id=run_id,
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            tls_params=tls_params,
        )
    )
    logging.info("published %d messages total: %s", sum(published.values()), published)
    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps({
            "run_id": run_id,
            "device_prefix": args.device_prefix,
            "devices": args.devices,
            "rate_hz": args.rate,
            "duration_s": args.duration,
            "attempted": sum(published.values()),
            "per_device": published,
        }, indent=2), encoding="utf-8")
        logging.info("wrote report to %s", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
