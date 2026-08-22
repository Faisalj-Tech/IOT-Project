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

A pre-existing Phase 1/2 `.env` lacks `RABBITMQ_ERLANG_COOKIE` and
`RABBITMQ_PARTITION_HANDLING` — copy them from `.env.example` before
bringing the cluster up, or formation fails with a peer-discovery-looking
error instead of the config gap it actually is.

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
`voter`). The definitions-import-vs-formation race described in ADR-0013 is
**non-deterministic per bring-up, not per machine** — on this implementation
it did not fire on one `down -v` / `up -d --wait` cycle and did fire on a
later one, on the same machine. Treat `quorum_status` returning a single row
as a routine possibility to check after every fresh bring-up, not a rare
edge case. Recover with:

```bash
docker exec iot-rabbitmq rabbitmq-queues grow rabbit@rabbit2 all
docker exec iot-rabbitmq rabbitmq-queues grow rabbit@rabbit3 all
```

`tests/experiments/test_cluster_preflight.py` (Task 7) asserts this for you
and names the exact `grow` commands in its failure message, so a half-formed
cluster fails loudly before any experiment runs on it.

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

## Fault tolerance experiments (Phase 3)

The cluster fault-tolerance matrix (follower kill, leader kill, network partition)
runs against the three-node cluster overlay above, not the single-node stack. Bring
it up the same way, verify formation, then run the matrix under `IOT_CLUSTER=1`:

```bash
docker compose down -v
docker compose -f compose.yml -f compose.cluster.yml up -d --wait
docker exec iot-rabbitmq rabbitmq-queues -q --formatter json quorum_status telemetry.q
IOT_CLUSTER=1 .venv/Scripts/python.exe -m pytest tests/ -m cluster -v -s
```

If `quorum_status` returns fewer than three rows, the definitions-import-vs-formation
race described above has fired on this bring-up — recover with the two `grow`
commands shown above before trusting any experiment result, or run the preflight
suite alone first (`tests/experiments/test_cluster_preflight.py`), which asserts this
for you and names the exact remediation in its failure message.

This single `pytest -m cluster` run covers the preflight, the Telegraf-failover
check (T), follower kill (F), leader kill (G), and the partition experiment under
whichever mode `.env` currently has (`H` under the committed `ignore` default). The
partition experiment under the *other* mode is skipped automatically with a
mode-guard message rather than run — it measures the mode, so running both under one
`.env` value would silently duplicate one of them. To measure the other mode
(`pause_minority`), switch the arm as shown above (a `--force-recreate`, not a
`pytest -m cluster` invocation edit) and run `pytest tests/experiments/test_partition.py -m cluster -v -s`
again; the two partition results come from two separate stack bring-ups by design.

Every run writes raw evidence to `docs/results/<experiment>-<run_id>.json`, the same
convention Phase 2 established. `docs/reports/phase3-fault-tolerance.md` is drafted
from those files and cites every number back to its filename. `test_partition_under_ignore`'s
own harness-integrity-floor assertion reads a value from `GaugeRecorder.node_window()`
that is timing-sensitive by construction (see the report's Limits section) and may fail
on any individual run without that indicating a regression in the system under test —
the InfluxDB sequence accounting in the same run's result JSON (`published_total`,
`influx_total`, `gaps`) is the reliable signal for whether messages were actually lost.

## Region (Phase 4)

`compose.region.yml` overlays `compose.yml` (or `compose.yml` + `compose.cluster.yml`
under `IOT_CLUSTER=1`) to add two region tenants, `eu` and `us`: their own vhosts, users,
topic-restricted permissions, MQTT listeners bound to their own `/24` Docker network, and
per-region policies (ADR-0027, ADR-0029, ADR-0030). It is always **last** in the `-f`
order — it re-mounts `rabbitmq.conf` and `definitions.json` over whatever the cluster
overlay put there, and Compose resolves same-target mounts last-one-wins.

Single node:

```bash
docker compose -f compose.yml -f compose.region.yml up -d --wait
IOT_REGION=1 KEEP_STACK=1 .venv/Scripts/python.exe -m pytest tests/ -m region -v
docker compose -f compose.yml -f compose.region.yml down -v --remove-orphans
```

On the cluster:

```bash
docker compose -f compose.yml -f compose.cluster.yml -f compose.region.yml up -d --wait
IOT_REGION=1 IOT_CLUSTER=1 KEEP_STACK=1 .venv/Scripts/python.exe -m pytest tests/ -m "region and cluster" -v
docker compose -f compose.yml -f compose.cluster.yml -f compose.region.yml down -v --remove-orphans
```

Region tests are marked `region` and deselected by default, same as `cluster`. `-m` on
the command line **replaces** `pytest.ini`'s marker expression rather than extending it,
so the combined-profile run needs the explicit `"region and cluster"` string above — a
bare `-m cluster` silently skips the region-and-cluster-marked tests, and a bare
`-m region` runs them against whatever profile is actually up regardless of whether the
cluster overlay is part of it.

Three things worth knowing before touching this profile:

- **The region subnets are hardcoded** as `172.28.1.0/24` (eu) and `172.28.2.0/24` (us),
  with the broker statically addressed at `172.28.1.10` / `172.28.2.10` on each. If either
  collides with a host or VPN network, both `compose.region.yml`'s `ipam.config` and
  `rabbitmq.region.conf`'s `mqtt.listeners.tcp.eu`/`.us` lines need to change together — a
  static test (`tests/test_region_config.py`) pins them to each other, so an edit to only
  one file fails loudly rather than binding a listener to nothing.
- **A changed region queue argument needs a volume wipe.** Definitions import never
  modifies an existing queue, and `definitions.skip_if_unchanged = true` — so editing
  `x-quorum-initial-group-size` or similar in `definitions.region.json` does nothing on a
  plain restart. Policies and permissions **do** update on re-import; only queue arguments
  need the wipe.
- **The region MQTT ports (1893, 1993) are deliberately not published to the host.**
  Publishing them would have Compose DNAT to one network's container address, which
  defeats the per-interface listener binding the whole network boundary depends on
  (ADR-0027). Host-side tooling reaches a region vhost through the existing **1883**
  listener instead, using a colon-form username (e.g. `eu:device-eu`) — this stays open in
  Phase 4 by design and is closed by Phase 5's mTLS/OAuth2 work, not before.

## Security (Phase 5)

`compose.security.yml` overlays `compose.yml` to add mTLS device identity and OAuth2
service identity: a TLS listener on `8883`, a CRL nginx sidecar, and RabbitMQ's OAuth2
auth backend wired to a Keycloak realm (ADR-0033–0037). `compose.region-security.yml`
adds the region-bound TLS listeners (`9883`/`9993`) and is valid only in combination with
both `compose.region.yml` and `compose.security.yml` — never alone.

**Certificates first.** `main/certs/` is gitignored and required before any
security-profile bring-up:

```bash
.venv/Scripts/python.exe -m scripts.make_certs
```

The test harness's `stack` fixture also generates it automatically on a clean clone, so a
plain `pytest -m security` run doesn't need this run manually first — but a manual
`docker compose up` does.

Base + security:

```bash
docker compose -f compose.yml -f compose.security.yml up -d --wait
IOT_SECURITY=1 KEEP_STACK=1 .venv/Scripts/python.exe -m pytest tests/ -m security -v -s
docker compose -f compose.yml -f compose.security.yml down -v --remove-orphans
```

Region + security (all four overlay files, `-f` order matters — `compose.region.yml`
stays ALWAYS LAST relative to `compose.yml`/`compose.cluster.yml`, the two security files
go after it):

```bash
docker compose -f compose.yml -f compose.region.yml -f compose.security.yml -f compose.region-security.yml up -d --wait
IOT_REGION=1 IOT_SECURITY=1 KEEP_STACK=1 .venv/Scripts/python.exe -m pytest tests/ -m security -v -s
docker compose -f compose.yml -f compose.region.yml -f compose.security.yml -f compose.region-security.yml down -v --remove-orphans
```

Security tests are marked `security` and deselected by default, same as `cluster` and
`region`. Tests scoped to OAuth2's service identity (`telegraf-eu`/`telegraf-eu-short`,
both scoped to vhost `eu` only) additionally carry `region` and skip themselves under
`IOT_REGION` unset — the OAuth2 realm's identity only means anything inside the region
model, so those tests need both env vars set, not `IOT_SECURITY=1` alone.

Four things worth knowing before touching this profile:

- **Certificate CN is the identity; no password is exchanged.** A device certificate's
  Common Name resolves directly to a RabbitMQ user via `ssl_cert_login_from =
  common_name`. Every device certificate must carry a `crlDistributionPoints` extension —
  without one, `crl_check = peer` rejects it outright with `{bad_crls,no_relevant_crls}`,
  which reads like a broken TLS setup rather than a missing extension.
- **Revoking a certificate is two steps, not one.** `scripts/make_certs.py`'s `revoke()` +
  `write_crl()` republishes the CRL, which gates *new* TLS handshakes within seconds — but
  an already-established connection is completely unaffected until it is also
  force-closed via the management API (`DELETE /api/connections/:name`). Skipping the
  second step is not revocation.
- **OAuth2's token is the AMQP password; the username is ignored.** `auth_backends.1 =
  internal` is checked first, so every Phase 1-4 plain-credential and every mTLS
  certificate-CN login is unaffected by the OAuth2 backend's presence.
- **An expired OAuth2 token behaves the opposite of a revoked certificate**: it forcibly
  terminates the live connection carrying it, and a static-token consumer (Telegraf's
  `amqp_consumer` has no refresh path) can never reconnect afterward. See
  `main/docs/reports/phase5-security.md` §1 for the full contrast.

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
