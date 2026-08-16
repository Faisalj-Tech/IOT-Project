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

## Reliability experiments (Phase 2)

The reliability matrix is deselected from the default test run because each
experiment takes minutes. Run it explicitly:

```bash
pytest tests/ -m experiment -v -s        # the full matrix, ~45 minutes
pytest tests/experiments/test_influx_outage.py -m experiment -v   # one experiment
```

Every run writes raw evidence to `docs/results/<experiment>-<run_id>.json`, and
`docs/reports/phase2-reliability.md` is drafted from those files. The experiments
stop and kill containers on purpose; the harness restores every service it took
down, including when an experiment fails.

The consumer arm needs its image built once:

```bash
docker compose -f compose.yml -f compose.consumer.yml build consumer
```

## Cluster (Phase 3)

`compose.cluster.yml` overlays `compose.yml` to turn the single RabbitMQ node
into a genuine three-node cluster, with `telemetry.q` and `dlq` declared as
three-member quorum groups (`x-quorum-initial-group-size: 3`, ADR-0013).
Node 1 keeps the service name `rabbitmq`, the Phase 1 ports, and the Phase 1
DNS name (`rabbit1`), so `telegraf.d`, the simulator's default endpoint, and
every Phase 1/2 test resolve unchanged. Nodes 2 and 3 boot from fresh,
cluster-only volumes and import no definitions of their own — only node 1
imports, so formation is not a three-way race.

Bring-up:

```bash
docker compose down -v
docker compose -f compose.yml -f compose.cluster.yml up -d --wait
```

Verify formation:

```bash
docker exec iot-rabbitmq rabbitmq-diagnostics -q --formatter json cluster_status
docker exec iot-rabbitmq rabbitmq-queues -q --formatter json quorum_status telemetry.q
```

`cluster_status` should list all three nodes under `running_nodes`, and
`quorum_status` should return three rows (one `leader`, two `follower`, all
`voter`). On this implementation the cluster formed cleanly on the first
attempt with three voting members — the definitions-import-vs-formation race
described in ADR-0013 did not fire, so `rabbitmq-queues grow` was not needed.
If your `quorum_status` ever returns a single row, the race did fire; recover
with:

```bash
docker exec iot-rabbitmq rabbitmq-queues grow rabbit@rabbit2 all
docker exec iot-rabbitmq rabbitmq-queues grow rabbit@rabbit3 all
```

Per-node ports (spec §4.1):

| Node | Container | MQTT | AMQP | Management UI |
|---|---|---|---|---|
| 1 (`rabbit1`) | `iot-rabbitmq` | 1883 | 5672 | http://localhost:15672 |
| 2 (`rabbit2`) | `iot-rabbitmq2` | 1884 | 5673 | http://localhost:15673 |
| 3 (`rabbit3`) | `iot-rabbitmq3` | 1885 | 5674 | http://localhost:15674 |

Cluster tests are marked `cluster` and deselected by default (`pytest.ini`
selects `not experiment and not cluster`). Point the harness at the cluster
overlay by setting `IOT_CLUSTER=1`, which makes `tests.conftest.compose_files()`
return `("compose.yml", "compose.cluster.yml")` instead of `("compose.yml",)`:

```bash
IOT_CLUSTER=1 pytest tests/ -m cluster -v
```

Partition-handling arm (`ignore` vs `pause_minority`, spec 5.H/5.I) is read
from `RABBITMQ_PARTITION_HANDLING` in `.env` and injected into every node via
`RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS`. Switching arms is a restart, not a file
edit:

```bash
# in .env: RABBITMQ_PARTITION_HANDLING=pause_minority
docker compose -f compose.yml -f compose.cluster.yml up -d --force-recreate rabbitmq rabbitmq2 rabbitmq3
```

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
