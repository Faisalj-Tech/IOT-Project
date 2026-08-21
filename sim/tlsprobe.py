"""One-shot MQTT-over-TLS connect and publish, for running inside a region container.

The region TLS listeners are IP-bound and unpublished, so they are only reachable
from a container on that region's Docker network. This is what
`compose run --rm sim-eu` executes; it prints one deterministic line and sets an
exit code, so a test can assert on either.

    python -m sim.tlsprobe --host 172.28.1.10 --port 9883 \
        --cert /certs/device-eu-a.crt --key /certs/device-eu-a.key \
        --publish region/eu/plant1/press-01/temp
"""

from __future__ import annotations

import argparse
import asyncio
import json
import ssl
import sys
import time

import aiomqtt

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def probe(args: argparse.Namespace) -> int:
    params = aiomqtt.TLSParameters(
        ca_certs=args.ca, certfile=args.cert, keyfile=args.key,
        cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )
    payload = json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "region": args.region, "plant": "plant1", "device": "press-01",
        "metric": "temp", "value": 70.0, "unit": "C", "seq": 1,
        "run_id": args.run_id,
    }).encode()
    try:
        async with aiomqtt.Client(
            hostname=args.host, port=args.port, tls_params=params,
            tls_insecure=False, identifier=args.cid, timeout=args.timeout,
        ) as client:
            print("CONNECT_OK", flush=True)
            if args.publish:
                await client.publish(args.publish, payload=payload, qos=1)
                print("PUBLISH_OK", flush=True)
    except Exception as exc:  # noqa: BLE001 - the failure kind is the result
        print(f"FAIL {type(exc).__name__}: {exc}", flush=True)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="One-shot MQTT-over-TLS probe")
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--ca", default="/certs/rootCA.crt")
    p.add_argument("--cert", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--publish", default=None)
    p.add_argument("--region", default="eu")
    p.add_argument("--run-id", default="tlsprobe")
    p.add_argument("--cid", default="tlsprobe")
    p.add_argument("--timeout", type=float, default=15.0)
    return asyncio.run(probe(p.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
