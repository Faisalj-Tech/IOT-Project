# Phase 4 — Multi-Region Segregation Report

## 1. What was built

Phase 4 gives the single RabbitMQ broker two region tenants, `eu` and `us`, segregated at
four independent layers so that a device or an operator can be wrong about which region it
belongs to and still be stopped, at each layer, by a different mechanism:

**Network.** Two `/24` Docker networks (`region-eu` = `172.28.1.0/24`, `region-us` =
`172.28.2.0/24`) carry the broker at a static address on each (`172.28.1.10`,
`172.28.2.10`) and nothing else routes between them. A simulator container attached to only
one region network has no IP path to the other's listener, independent of any credential
(ADR-0027).

**Listener.** Each region gets its own MQTT listener bound to that network's broker
address (`mqtt.listeners.tcp.eu = 172.28.1.10:1893`, `.us = 172.28.2.10:1993`), declared in
`rabbitmq.region.conf`. Port 1883 stays bound to all interfaces on vhost `/`, unmapped, so
every Phase 1–3 experiment keeps working untouched.

**Vhost.** A `mqtt_port_to_vhost_mapping` global parameter routes each listener's port to
its region's vhost. Four users (`device-eu`, `device-us`, `telegraf-eu`, `telegraf-us`)
hold permissions in exactly one vhost each — a device or a Telegraf input authenticated for
`eu` is refused outright, at CONNECT, if it tries to reach `us`.

**Topic routing key.** Within a vhost a device still needs a topic permission matching
`^region\.<r>\..*` on `amq.topic` to publish — an `eu`-authenticated connection publishing
under a `us` routing key is refused at PUBACK, one layer deeper than the vhost boundary.

**Data provenance.** A per-region Telegraf `amqp_consumer` input, bound to one vhost with a
read-only credential, stamps every point it consumes with a static `region_src` tag naming
the vhost — not the payload's own self-declared `region` tag, which a device controls
(ADR-0028). Every isolation assertion in this phase reads `region_src`.

**Replication.** Every region queue (`telemetry.eu.q`, `telemetry.us.q`, and each region's
`dlq`) declares `x-quorum-initial-group-size: 3`, the same Phase 3 quorum-queue pattern
applied to the two new tenants, so a region's telemetry survives a node loss exactly like
the default vhost's does.

## 2. The identity model

| User | Vhost | Configure | Write | Read | Topic permission (`amq.topic`) |
|---|---|---|---|---|---|
| `device-eu` | `eu` | `^mqtt-subscription-.*$` | `^amq\.topic$` | `^mqtt-subscription-.*$` | write/read `^region\.eu\..*` |
| `device-us` | `us` | `^mqtt-subscription-.*$` | `^amq\.topic$` | `^mqtt-subscription-.*$` | write/read `^region\.us\..*` |
| `telegraf-eu` | `eu` | `^$` (none) | `^$` (none) | `^(telemetry\.eu\.q\|amq\.topic)$` | — |
| `telegraf-us` | `us` | `^$` (none) | `^$` (none) | `^(telemetry\.us\.q\|amq\.topic)$` | — |

The two Telegraf users hold **no configure and no write** in their vhost — genuinely
read-only. Their `amqp_consumer` inputs omit `binding_key` for exactly this reason: setting
it forces a `queue.bind` at Telegraf startup, which needs `write` on the destination queue
and would fail `403 ACCESS_REFUSED` under this permission set (ADR-0030). The
`amq.topic` → `telemetry.<r>.q` binding is declared once, in `definitions.region.json`,
and owned by the definitions file rather than by the consumer.

Per-region policies apply only to their own vhost's `telemetry.*` queues:

| Policy | Vhost | `message-ttl` | `max-length` |
|---|---|---|---|
| `eu-limits` | `eu` | 604800000 (7d) | 100000 |
| `us-limits` | `us` | 86400000 (1d) | 10000 |

Vhost `/` carries no policy — every Phase 1–3 result keeps meaning what it meant before
Phase 4 existed.

## 3. Evidence

All output below was captured live against `rabbitmq:4.3.4-management`, this session
(2026-08-21), on branch `phase-4-segregation`.

### R2 — vhost denial at CONNECT

`test_a_device_cannot_connect_to_another_regions_vhost` connects as `device-eu` on vhost
`us` and asserts `aiomqtt.MqttConnectError` carries MQTT reason code **135**. Live:

```
tests/experiments/test_region_isolation.py::test_a_device_cannot_connect_to_another_regions_vhost PASSED
tests/experiments/test_region_isolation.py::test_each_device_can_connect_to_its_own_vhost PASSED
```

Broker log, same run:

```
[error] MQTT connection failed: access refused for user 'device-eu' to vhost 'us'
```

The positive control (`test_each_device_can_connect_to_its_own_vhost`) connects as
`device-eu`→`eu` and `device-us`→`us` and passes — without it, a broken password would
look identical to an enforced boundary.

### R3 — topic-key denial at PUBACK

`test_a_device_cannot_publish_into_another_regions_routing_key` connects as `device-eu` on
its own vhost `eu` (an authorized connection), then publishes under a `region.us.*` routing
key. Reason code **128** comes back at PUBACK, one layer past the CONNECT boundary R2
proved:

```
tests/experiments/test_region_isolation.py::test_a_device_cannot_publish_into_another_regions_routing_key PASSED
```

Broker log, same run:

```
[error] MQTT topic access refused: write access to topic 'region.us.plant1.press-01.temp'
in exchange 'amq.topic' in vhost 'eu' refused for user 'device-eu'
```

### R4 — network boundary

`test_a_region_container_reaches_only_its_own_listener` runs a throwaway container on each
region's own Docker network (via `compose run --rm sim-<region>`) and TCP-probes both
listeners from inside it:

```
tests/experiments/test_region_isolation.py::test_a_region_container_reaches_only_its_own_listener PASSED
```

Each region's container reaches its own listener and fails to reach the other's — the same
result Task 7 first observed manually (`sim-eu` timing out against `172.28.2.10:1993`),
now an automated assertion independent of any credential.

### R5 — per-region policy readback

`test_region_rules.py` reads policies back through the management API and off the queues
themselves, not just the policy list:

```
tests/experiments/test_region_rules.py::test_each_region_carries_its_own_policy PASSED
tests/experiments/test_region_rules.py::test_the_two_regions_rules_actually_differ PASSED
tests/experiments/test_region_rules.py::test_the_policy_is_applied_to_that_regions_queue PASSED
tests/experiments/test_region_rules.py::test_the_default_vhost_queue_carries_no_policy PASSED
```

`telemetry.eu.q`'s `effective_policy_definition.max-length` reads `100000`, `us`'s reads
`10000` — the *effective*, applied value, not merely the declared policy object, and vhost
`/`'s `telemetry.q` carries no `policy` key at all.

### R6 — end-to-end flow, zero leakage

`test_both_regions_flow_end_to_end_without_leaking` runs both simulators (3 devices, 2Hz,
30s each, 180 points expected per region) under distinct run IDs, then counts InfluxDB
points by `region_src` for each run ID:

```
tests/experiments/test_region_flow.py::test_both_regions_flow_end_to_end_without_leaking PASSED
```

Result JSON (`docs/results/R-region-flow-norun.json`, this session):

```json
{
  "expected_per_region": 180,
  "counts_by_provenance": { "eu": {"eu": 180}, "us": {"us": 180} },
  "queue_ready": {"eu": 0, "us": 0},
  "dlq_ready": {"eu": 0, "us": 0}
}
```

Both regions met their expected count (`>=`, since QoS-1 retries legitimately duplicate),
and — the assertion that actually matters — the **cross-tagged count is exactly zero** for
both: no point published under `eu`'s run ID was ever consumed from `us`'s vhost, or vice
versa. Both queues and both dead-letter queues were empty afterward.

### R7 — three-member quorum groups under the cluster

`test_region_cluster.py` runs under the combined region+cluster profile
(`compose.yml + compose.cluster.yml + compose.region.yml`) and reads Raft membership
directly off each region queue:

```
tests/experiments/test_region_cluster.py::test_region_queues_are_three_member_quorum_groups PASSED
tests/experiments/test_region_cluster.py::test_region_dead_letter_queues_are_also_replicated PASSED
```

`cluster_status`: `running_nodes: ["rabbit@rabbit1", "rabbit@rabbit2", "rabbit@rabbit3"]` —
one cluster, not two (see §5 and ADR-0032 for why that qualifier matters).
`quorum_status telemetry.eu.q` / `.us.q`: three voters each, one leader, two followers.

## 4. What is enforced where

| Layer | Mechanism | What a wrong-region client sees |
|---|---|---|
| Network | Per-region `/24`, broker statically addressed on each | No route — TCP connect times out |
| Listener | IP-bound MQTT listener per region | Connects, but to the wrong vhost's port entirely — not reachable from the other network |
| Vhost | Per-region user permissions | MQTT CONNACK reason code **135**, "Not authorized" |
| Topic key | Per-region topic permission on `amq.topic` | MQTT PUBACK reason code **128**, broker logs "MQTT topic access refused" |
| Data provenance | `region_src` tag stamped by a read-only, single-vhost consumer | A device's self-declared `region` tag can lie; `region_src` cannot — it names the vhost the point was actually consumed from |
| Replication | `x-quorum-initial-group-size: 3` on every region queue | A region's telemetry survives one node's loss like the default vhost's does |

## 5. Known limits

- **1883 stays a host-reachable path into any vhost.** Colon-form usernames
  (`eu:device-eu`) on the unmapped, all-interfaces 1883 listener select a vhost by
  credential alone, without touching either region network. This is deliberate — it is how
  host-side test tooling and CI reach the region vhosts without publishing 1893/1993 (which
  would DNAT to one network's address and defeat the IP-bound listener design) — but it
  means a leaked region password reaches that region without crossing its network boundary
  first. A credential-strength problem, not a network one; Phase 5 (mTLS + OAuth2) is where
  it is addressed (ADR-0027).
- **The AMQP ingestion plane is shared, not partitioned.** Both regions' Telegraf inputs
  connect to the same broker over the same `core` network; there is no separate ingestion
  path per region, only separate vhosts and credentials within one broker process.
- **No per-region ingestion failover.** If `telegraf-eu`'s input dies, `eu`'s telemetry
  queues back up until it restarts (or until the `eu-limits` policy's TTL/length caps
  start dropping the oldest messages) — there is no second consumer for that vhost.
- **A changed region queue argument needs a volume wipe**, not just a re-import. Definitions
  import never modifies an existing queue and `definitions.skip_if_unchanged = true`; only
  policies and permissions update on a plain re-import.
- **Region+cluster peer discovery needed a bounded-retry fix mid-phase (ADR-0032).** The
  first attempt at making a lone region-profile node avoid hanging (ADR-0031: strip all
  `cluster_formation.*`) turned out to make the combined region+cluster profile split into
  two separate clusters instead of forming one three-node group — deterministically, not as
  a race. The corrected fix (a bounded-retry three-node list) is what R7's evidence above
  reflects; a reader of ADR-0031 alone, without ADR-0032, would expect the wrong outcome.

## 6. Recommendation

A production deployment would close the 1883 credential-only path first — Phase 5's mTLS
gives each device a certificate whose CN can be mapped to a vhost the same way the IP-bound
listener is today, removing the need for any unmapped fallback listener at all. Per-region
ingestion failover (a second Telegraf replica per vhost, or a supervisor that restarts a
dead input faster than its queue's TTL expires) is the next reliability gap worth closing,
following the same ack-after-write pattern Phase 2 already proved for the default vhost.
Finally, `rabbitmq.region.conf` and `rabbitmq.cluster.conf` still diverge by hand (no drift
test exists between them, unlike `definitions.region.json`'s ADR-0029 drift test) — worth
closing before a third profile combination is added to this codebase.
