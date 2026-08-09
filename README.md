# Resilient IoT Messaging Infrastructure

Phase 1: a single-node MQTT → RabbitMQ → Telegraf → InfluxDB → Grafana telemetry
pipeline with a Python device simulator and an integration test suite.

## Architecture

```
sim (asyncio, QoS 1)
  --MQTT 1883--> RabbitMQ 4.3.4
      topic  region/eu/plant1/press-01/temp
      --> amq.topic (routing key region.eu.plant1.press-01.temp)
      --> binding "region.#" --> telemetry.q  [quorum, durable, DLX -> dlq]
      --> Telegraf amqp_consumer (json_v2)
      --> InfluxDB 2.9.1, org "iot", bucket "telemetry"

Telegraf inputs.rabbitmq (management API :15672) --> same bucket
Grafana <-- provisioned Flux datasource
```

## Requirements

- Docker with Compose v2
- Python 3.12

## Bring-up

```bash
cp .env.example .env
docker compose up -d --wait
```

Services:

| Service | URL | Credentials |
|---|---|---|
| RabbitMQ management | http://localhost:15672 | `admin` / `adminpass` |
| InfluxDB | http://localhost:8086 | `admin` / `influxadminpass` |
| Grafana | http://localhost:3000 | `admin` / `grafanapass` |

## Generate telemetry

```bash
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements-dev.txt
.venv/Scripts/python.exe -m sim.devices --devices 5 --rate 2 --duration 30
```

Open Grafana → folder **IoT** → dashboard **IoT Telemetry and Broker Health**.

## Verify

```bash
.venv/Scripts/python.exe -m pytest
```

The suite brings the stack up, publishes known telemetry, and asserts it arrives
in InfluxDB with a gap-free per-device sequence. Set `KEEP_STACK=1` to skip
teardown while iterating.

## Teardown

```bash
docker compose down -v
```

## Design notes

- **Broker as buffer.** `telemetry.q` is a durable quorum queue. If InfluxDB
  stops, messages accumulate there instead of being lost. Phase 2 measures this.
- **Declarative topology.** Users, permissions, queues, the dead-letter
  exchange, and bindings all live in `config/rabbitmq/definitions.json`. Nothing
  is created through the management UI, and Telegraf has no `configure`
  permission, so it cannot redeclare anything.
- **Identity in the payload.** Telegraf's `amqp_consumer` cannot read the AMQP
  routing key, so each message repeats its identity in the JSON body. The
  routing key segregates traffic broker-side; the payload identifies the
  measurement. Two separate planes, deliberately duplicated.
- **Dead-letter exchange from day one.** Telegraf REJECTs with requeue disabled
  when an output write fails. Without a DLX those messages disappear silently;
  with one, `dlq` depth is a measurable quantity.
- **`seq` and `run_id`.** Every message carries a per-device monotonic counter
  and a per-run id. Zero-loss claims are proven by checking the sequence set has
  no gaps, not by looking at a graph. `run_id` is a high-cardinality tag —
  correct for experiments, wrong for production.
- **Untuned Telegraf.** `metric_buffer_limit` and `flush_interval` are left at
  their plan values on purpose; Phase 2 measures what the defaults do during an
  outage before anything is tuned.

## Credentials

`.env` holds dev-only credentials and is gitignored. `.env.example` is the
committed template. RabbitMQ passwords appear in `definitions.json` as salted
SHA-256 hashes; regenerate them with:

```bash
.venv/Scripts/python.exe scripts/rmq_password_hash.py newpassword
```
