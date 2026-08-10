"""CLI: python -m consumer"""

from __future__ import annotations

import asyncio
import logging
import os

from consumer.ackafterwrite import run


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    user = os.environ["RABBITMQ_TELEGRAF_USER"]
    password = os.environ["RABBITMQ_TELEGRAF_PASSWORD"]
    host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
    asyncio.run(
        run(
            amqp_url=f"amqp://{user}:{password}@{host}:5672/",
            queue_name=os.environ.get("CONSUMER_QUEUE", "telemetry.q"),
            influx_url=os.environ.get("INFLUXDB_URL", "http://influxdb:8086"),
            influx_token=os.environ["INFLUXDB_TOKEN"],
            influx_org=os.environ["INFLUXDB_ORG"],
            influx_bucket=os.environ["INFLUXDB_BUCKET"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
