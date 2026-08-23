# Phase 6 — Load Testing Report

## 1. What was built

Phase 6 adds four load-testing gates (L1–L7) to the pipeline built by Phases 1–5, measuring throughput, latency, queue depth, memory consumption, and connection counts as offered load increases. The measurements stop at two breaking points: **the rate at which InfluxDB stops tracking broker ingress** (L2/L5) and **the memory pressure under a prolonged downstream outage** (L4), with cluster replication cost (L6) and structured Locust profiles (L11) measured separately.

**Device swarm (asyncio, Tasks 3–5).** The Phase 6 simulator runs as a scaled Docker Compose service (`sim-load` in `compose.load.yml`, no `container_name`, replicas addressed by container hostname). The host cannot run the swarm at all — `WindowsSelectorEventLoopPolicy` on Windows with CPython's `FD_SETSIZE = 512` fails process-fatally at ~500 connections (ADR-0042). Every swarm replica self-assigns its device identity from its hostname, publishes MQTT 5 (ADR-0044), counts rejections via paho's reason codes, and writes per-device reports to a bind-mounted directory for aggregation.

**Overflow matrix (L3, Task 6).** Quorum queues on RabbitMQ 4.3.4 have **two** overflow behaviours, not three: `drop-head` and `reject-publish`. The assignment's third mode (`reject-publish-dlx`) is identical to `drop-head` — the broker logs a warning and ignores it (ADR-0043). The phase tests exact measured boundaries under `max-length`, `max-length-bytes`, and `message-ttl`, with `reject-publish` invisible to MQTT 3.1.1 devices and visible (as `0x97 Quota exceeded`) to MQTT 5 ones (ADR-0044). Because Task 5's second Telegraf consumer on `telemetry.q` (for two-clock latency) would split the stream and corrupt exact-depth assertions, L3 pauses Telegraf for its duration (ADR-0047).

**Memory pressure (L4, Task 9).** The assignment's premise — "stop InfluxDB, observe the queue growing" — does not hold on the default pipeline. Telegraf rejects each message once when the output write fails, dead-lettering it immediately, so `telemetry.q` drains to zero while `dlq` (unbounded) grows instead (ADR-0045). Phase 6 measures memory pressure in three deliberately separate arms: (a) Telegraf stopped, measuring the clean per-message model; (b) Telegraf running, InfluxDB down, measuring what the default pipeline actually does; (c) an ack-after-write consumer, measuring genuine store-and-forward. `dlq`'s unboundedness was found by this phase and is documented as a recommendation without being fixed, because bounding it would change what arm (b) measures.

**Throughput/latency (L2/L5, Task 8).** A four-step swarm ramp (50→3200 Hz offered rate) publishes messages continuously and reads broker-side observations (queue depth, memory, latency samples) from InfluxDB. The swarm's own publish-accounting fields (`published_attempted`, `published_acked`, `published_rejected`, `publish_timeouts`, `reconnects`) structurally read zero because `swarm.stop()` force-removes replicas before their 90-second `duration_s` completes — only the 60-second `STEP_SECONDS` sleep occurs. All swarm-side numbers in this report are **broker and InfluxDB-side observations** of swarm-driven runs: `ingested_rows` (a commingled upper bound from both Telegraf consumers), `queue_after_drain`, `broker_memory_bytes`, and end-to-end latency.

**Cluster cost (L6, Task 10).** A single run of the first L2 step (50 Hz offered rate, 2 replicas, 25 devices each) against a 3-node cluster vs. the single-node baseline measures replication cost in per-message memory.

**Grafana dashboard (Task 12).** Five panels showing throughput (p50 publish latency), queue depth, end-to-end latency percentiles (p50/p95/p99), broker memory against the pinned 256 MiB watermark, and connection count — populated by live-verified Flux queries corrected against the actual committed config names (telemetry, not telemetry_latency; connections, not object_totals_connections).

**Locust ramp profiles (L11, Task 11).** A custom MQTT `User` class reads paho's rejection reason codes and reports them to Locust as failures. A `StepRamp` LoadTestShape drives the ramp: 10 spawn rate, 50→500 users, 30-second steps, 240-second bounded total. Driven via REST API (the `locust-master` runs without `--headless`). One live run: 141,750 MQTT publish requests, 0 failures — success path only, since the run exercised the default unbounded-queue config, not an overflow scenario.

## 2. Measured limits

All measurements taken against the pinned broker limits in `compose.load.yml`:
- Broker memory: 1 GiB (0.6 of 1.67 GiB Docker-visible)
- Broker CPU: 2 cores (pinned; no scaling)
- Memory high-water alarm threshold: 256 MiB (0.24 of 1 GiB)

### L2/L5: Throughput ceiling and latency at increasing offered rates

**Asyncio swarm numbers** (broker-side observations of swarm-driven L2 run):

| Offered rate | Replicas | Devices each | Ingested rows | Queue at drain | Broker mem |
|---|---|---|---|---|---|
| 50 Hz | 2 | 25 | 467 | 2,050 | 83 MB |
| 200 Hz | 2 | 50 | 750 | 11,500 | 97 MB |
| 800 Hz | 4 | 50 | 800 | 47,450 | 152 MB |
| 3200 Hz | 4 | 100 | 750 | 54,127 | 93 MB |

Ingested rows flatten while queue depth grows: drain rate is pinned at ~750 rows/step (InfluxDB's ingest ceiling), and everything above it accumulates in the broker. The breaking point is **InfluxDB, not the broker or the swarm**.

**End-to-end latency under growing backlog** (two-clock latency from InfluxDB's publish-time and ingest-time fields):

| Offered rate | p50 | p95 | p99 | max |
|---|---|---|---|---|
| 50 Hz | 47.6s | 76.0s | 76.1s | 76.1s |
| 200 Hz | 39.1s | 84.0s | 84.1s | 84.1s |
| 800 Hz | 40.8s | 75.4s | 75.4s | 75.4s |
| 3200 Hz | 41.1s | 85.2s | 85.2s | 85.2s |

Latency remains flat under increasing backlog — the p50 holds ~40–48s and p95/p99 hold ~75–85s. These are end-to-end, measured under a growing queue (2k→54k messages), **not broker latency alone**. Per-message traversal time is stable; messages spend time waiting in the broker's queue.

### L4: Memory under three outage scenarios

All three arms run with Telegraf stopped between arms, `compose down -v` between waves.

**Arm (a): Telegraf stopped, no consumer** (clean per-message model, all 500,000 messages on `telemetry.q`):
- Model prediction: alarm at ~184,000 messages
- Measured: 500,000 messages, **zero alarms**
- Memory pattern: repeating rise-then-drop (~76→176 MB) every ~100,000 messages
- Disk before purge: 238 MB; after purge: 7 MB
- Finding: Memory shows sawtooth pattern (consistent with periodic Raft checkpoint/segment rollover reclaiming `quorum_ets` index between measurement pauses, which the single-burst model never observed), 2.7× past the model's predicted alarm point. Disk shrinks on purge (reverse of spec §2.9's claim). Both recorded as findings per spec §2.7's instruction, not tuned away (ADR-0048).

**Arm (b): Telegraf running, InfluxDB down** (measures the default pipeline's actual behavior, 50,000 messages published):
- `telemetry.q` depth: 0 (drained by Telegraf's consumer, then rejected)
- `dlq` depth: 50,000 (all messages dead-lettered)
- `x-death` header on samples: `reason: "rejected", count: 1` (Telegraf rejects each message once, dead-letters immediately)
- Finding: The default pipeline dead-letters under a downstream outage; `telemetry.q` does not grow, `dlq` does, and `dlq` is unbounded. This is the assignment's stated scenario applied to the real pipeline, not the single-consumer model.

**Arm (c): Ack-after-write consumer** (genuine store-and-forward, 20,000 messages published):
- During outage: `telemetry.q` depth 20,000 (19,950 ready + 50 unacked at `prefetch_count`)
- After recovery: 0/0/0 in queue, 0/0/0 in dlq (all drained and written to InfluxDB)
- Finding: Only this arm demonstrates actual buffering — messages accumulate safely on the broker and drain when the downstream service returns. This is the queuing discipline the assignment's wording assumes, but it requires a consumer (ack-after-write) that is not the default pipeline.

### L6: Replication cost (single-node baseline vs. 3-node cluster, 50 Hz step)

| Profile | Messages | Memory before | Memory after | Per-message bytes |
|---|---|---|---|---|
| Single node (L2 baseline) | 2,350 | 84.4 MB | 83.3 MB | — |
| 3-node cluster | 99,995 | 77.2 MB | 113.8 MB | 366.5 |

Per-message memory: 366.5 bytes (vs. model's 1,050-byte prediction, a real divergence, recorded not tuned).

### L11: Locust ramp profile

| Metric | Value |
|---|---|
| Total publish requests | 141,750 |
| Failures | 0 |
| Spawn rate | 10 users/step |
| Max concurrent users | 500 |
| Step duration | 30 seconds |
| Total ramp duration | 240 seconds |
| Test outcome | pass (success path only, default unbounded-queue config) |

Locust's own report.html (gitignored, not committed, 1.48 MB artifact on disk) is available in the compose environment during live runs; the numbers above are from the on-disk artifact.

## 3. Failure modes observed

1. **Silent message loss under MQTT 3.1.1 backpressure** (ADR-0044): When a quorum queue's `reject-publish` policy is active, MQTT 3.1.1 devices receive no PUBACK, no error, no disconnect — and the withheld PUBACK never arrives even 30 seconds after the queue drains. The device is never told the message was rejected. MQTT 5 devices receive an immediate `0x97 Quota exceeded` reason code (0.2s typical) and can respond accordingly.

2. **Default pipeline dead-letters instead of buffering** (ADR-0045, arm b): When InfluxDB stops, Telegraf does not hold messages on `telemetry.q` waiting for InfluxDB to return — it rejects each message once and dead-letters it immediately. Memory pressure accumulates on `dlq`, not `telemetry.q`. This contradicts the assignment's stated scenario ("observe the queue growing").

3. **Configuration surface reports success for ignored settings** (ADR-0043): The management API accepts and echoes back `x-overflow: "reject-publish-dlx"` with HTTP 201, but the broker logs a warning and treats it as `drop-head` instead. A test that trusts the API response measures a lie.

4. **Memory model diverges at scale** (ADR-0048, arm a): A 500,000-message run published within pinned broker limits (1 GiB memory, 256 MiB alarm threshold) never triggered an alarm, 2.7× past the ~184,000-message prediction. Memory shows a repeating sawtooth pattern (consistent with periodic Raft checkpoint/segment rollover reclaiming the ETS index between measurement pauses) rather than the linear growth the single-burst model fitted on.

5. **Disk not reclaimed on purge** (ADR-0048, arm a, second finding): Spec §2.9 claimed disk is not released on purge. A live arm (a) run with explicit before/after measurement showed disk dropping from 238 MB to 7 MB after purge, the reverse of the spec's claim. Not tuned away; recorded as a finding.

6. **Specification mismatch on live Flux queries** (Task 12 finding): The plan's own Task 8/12 Flux queries for latency and connection-count panels named measurements and fields that don't exist in the actual committed Telegraf config. `telemetry_latency` (planned) vs. `telemetry` (actual, field `ts`); `object_totals_connections` (planned) vs. `connections` (actual). Corrected against live-verified InfluxDB schema.

## 4. Infrastructure requirements derived from measured behavior

1. **Devices that must detect broker backpressure have to speak MQTT 5** — MQTT 3.1.1 receives no signal when a broker rejects a publish under an overflow policy (ADR-0044).

2. **The default telemetry pipeline dead-letters on InfluxDB outage, not buffer** — applications requiring guaranteed message preservation need an ack-after-write consumer or an alternative buffer, not the default Telegraf+InfluxDB arrangement (ADR-0045, arm b).

3. **`dlq` must be bounded** — found unbounded in the default topology, and recommendations for bounding are in the remarks below. Not fixed this phase because bounding would change what arm (b) measures.

4. **Every overflow test requires active observation, not trust of configuration echoes** — the management API accepts and returns configuration the broker silently ignores (ADR-0043).

## 5. Known limits

- **Swarm-side publish accounting is structurally zero** (L2/L5): `swarm.stop()` force-removes replicas before their `duration_s` (90s) completes, while the test only sleeps `STEP_SECONDS` (60s). Per-replica reports (which count publishes) are written after `duration_s`, so every swarm's own reported count is zero. This is plan-inherent, not an implementer defect. All swarm-driven numbers in this report read from broker and InfluxDB observations, not from the swarm's own accounting.

- **Ingested rows is a commingled upper bound** (L2/L5): The `telemetry` measurement receives inputs from both Telegraf's base consumer and Task 5's latency consumer (identical shape, both write `seq` field). `ingested_rows` counts rows with a `seq` field in `telemetry` from both consumers combined — an upper bound on base pipeline throughput, not an isolated base count. The two consumers cannot be distinguished with the current config.

- **Latency numbers are end-to-end under growing backlog** (L2/L5): The p50/p95/p99 latencies reported are two-clock latency (time from publish to ingest) measured under queue depths of 2k–54k messages. Per-message traversal time is stable; messages spend time waiting in the broker's queue. These are not broker-latency-alone figures.

- **Memory model fitted on single uninterrupted bursts** (L4): Spec §2.7's model was fitted on two short, single-shot, uninterrupted publish runs (100k/200k messages, no consumer). Arm (a)'s multi-batch run with pauses between batches (for `broker_alarms()`/`mnesia_megabytes()` reads) showed a repeating sawtooth memory pattern consistent with periodic Raft checkpoint/segment-rollover reclaiming the `quorum_ets` index between pauses — a behavior the single-burst model never observed. Divergence recorded per spec §2.7's instruction.

- **Python-over-MQTT is a SCADA-like abstraction** (spec §9): The asyncio swarm does not model register semantics, poll-cycle contention, or deterministic scan rate — it publishes at a target rate with no per-device cycle discipline. Genuine SCADA systems have hard real-time guarantees this abstraction does not provide. The swarm measures broker load-handling at a given offered rate, not industrial protocol behavior.

- **AMQP TLS and OAuth2 token refresh remain deferred** (decision 3, Phase 5 ADR-0037): Phase 5 deliberately does not add an AMQP TLS listener because `ssl_options` is node-wide and would force Telegraf into mTLS in addition to OAuth2. Telegraf's `amqp_consumer` has no token-refresh mechanism, so an expired token permanently stops ingestion. Both are carried as named gaps, not added this phase.

## 6. Recommendations

1. **Bound `dlq`** with `x-max-length-bytes` (bytes limit engages at exactly the limit, unlike count limit) to prevent unbounded growth during prolonged downstream outages. A reasonable starting point: 1 GiB (same as broker memory limit), monitored via Grafana and alerted on approach.

2. **Upgrade to MQTT 5 device communication** to make broker backpressure observable at the device layer, enabling graceful client-side response to queue full conditions.

3. **Document the ack-after-write pattern** as the producer-side guarantee of message preservation during InfluxDB outages (arm c), and note that the default Telegraf+InfluxDB pipeline provides best-effort delivery only.

4. **Re-validate memory model** at higher scales or under different contention patterns (e.g., dynamic consumer churn) if the system is scaled beyond the ~150 MB per million messages guideline this phase observed under the repeating-sawtooth pattern.

5. **Monitor management API responses** against live observed broker state — do not trust configuration reads as evidence that a setting took effect (ADR-0043).

## 7. Raw probe appendix

The following is the complete raw evidence from the Phase 6 design session's probe measurements, reproduced verbatim from `docs/superpowers/specs/2026-08-22-iot-messaging-phase6-probes.md` as spec §2 requires.

### Phase 6 design probes — raw evidence
Measured 2026-08-22 against `rabbitmq:4.3.4-management`, default compose profile,
host: Windows 11, 24 CPU, 15.16 GiB visible to Docker, Docker 29.6.1.
Python 3.12.10, aiomqtt 2.4.0 (MQTT 3.1.1 default), aio-pika 10.0.1, paho-mqtt 2.1.0.

#### P1 — host swarm ceiling (BLOCKING: decides harness architecture)
One Python process, aiomqtt clients held open, ramped in batches of 25:
```
live=25 ... live=500  (t=0.8s)
ValueError: too many file descriptors in select()
```
Process-FATAL, not graceful degradation: the final report line never printed because
the exception killed the event loop. `sim/devices/runner.py` sets
`WindowsSelectorEventLoopPolicy` on win32 -> select() -> FD_SETSIZE=512.
=> "thousands of devices" is unreachable from the host in one process. Swarm must run
in Linux containers (epoll). `Dockerfile.sim` already exists.

#### P2 — quorum queue x-overflow (max-length=5, 20 AMQP messages, publisher confirms)
Management API accepted ALL THREE values with HTTP 201 and echoed them back in
`arguments`. The broker disagrees:
```
[warning] Invalid overflow strategy <<"reject-publish-dlx">> for quorum queue
```
| x-overflow | publisher saw | final depth | dead-lettered |
|---|---|---|---|
| drop-head | 20 acked | 5 | 15 |
| reject-publish | 6 acked, 14 Basic.Nack (DeliveryError) | 6 | 0 |
| reject-publish-dlx | 20 acked | 5 | 15 |  <- identical to drop-head

=> Only TWO real behaviours on quorum queues. The assignment's third mode
("dead-letter") is not a mode: it is drop-head + a DLX attached.
=> `reject-publish-dlx` silently degrades. A test trusting the API response measures a lie.
=> max-length engages at maxlen+1 (depth settled at 6 for a limit of 5).

#### P3 — what an MQTT QoS-1 publisher observes under reject-publish
20 QoS-1 publishes into a full queue (MQTT 3.1.1 via aiomqtt default):
```
Counter({'TIMEOUT_no_puback': 20})
```
No PUBACK. No error. No disconnect. Silence.
Backpressure test (fill past limit, publish, wait 5s, purge, wait 30s):
```
{"t":5.0,"publish_done":false,"depth":6}
{"t":6.02,"event":"purge -> HTTP 204"}
{"t":36.03,"event":"no PUBACK 30s after drain: TimeoutError"}
```
=> STALL-FOREVER, not backpressure. The withheld PUBACK never arrives even after the
queue is fully drained. Message is lost and the device is never told.
=> Same queue, same policy: AMQP publisher gets an explicit Basic.Nack; MQTT gets silence.
=> aiomqtt's client-level default timeout is 10s, so `runner.py`'s
`await client.publish(..., qos=1)` raises MqttError after 10s, which `_publish_device`
catches as a DISCONNECT and reconnects => under overflow the swarm reconnect-storms
instead of recording a rejection.
CAVEAT (unresolved at time of writing): 3.1.1 PUBACK carries no reason code. MQTT 5
PUBACK has 0x97 Quota exceeded. Re-probe under V5 before claiming "invisible to devices".

#### P4 — memory vs disk under a downstream outage (telegraf stopped, no consumer)
200-byte bodies, 200k messages, quorum queue:
```
start  erl_mem=87.7MB  mnesia=1MB
200k   erl_mem=288.7MB mnesia=95MB   free disk -98MB
```
2000-byte bodies, 100k messages (after delete + GC, baseline 79.9MB):
```
start  erl_mem=80.4MB  mnesia=95MB
100k   erl_mem=369.9MB mnesia=314MB
```
Forced GC across all processes reclaimed only ~10MB (288.7 -> 278.3): memory is retained.
Breakdown at 200k/200B:
```
quorum_ets: 0.1315 gb (35.26%)   <- dominant
binary:     0.0649 gb (17.39%)
```
Linear fit from the two points:
  broker memory ~= N * (0.85 KB + payload_bytes)
  broker disk   ~= N * (0.20 KB + payload_bytes)
Fixed per-message overhead = the Ra log's ETS index entry.
Configured limits on this host (NOT from committed config):
```
mem_limit=9764143104 (0.6 of available = 9.76 GB)
disk_limit=50000000 (50 MB), free disk 1017 GB
```
=> Memory binds long before disk here. DO NOT publish the naive extrapolation
(~9.7M messages) as a finding: it is a ~48x extrapolation beyond the measured range and
Erlang allocator / Ra segment rollover behaviour is not guaranteed linear that far out.
Report the MODEL as measured; get the actual breaking point from a pinned-limit run
INSIDE the measured range.
=> Model derived with NO consumer. The assignment's scenario (InfluxDB down, Telegraf
running) has a different shape: messages go unacked, not ready, bounded by
`max_undelivered_messages = 1000` plus Telegraf's 10k metric_buffer_limit. Measure separately.

#### P5 — MQTT client-ID collision
Two clients, same identifier `press-01-run1`:
```
[warning] MQTT disconnecting client <<...>> with duplicate id 'press-01-run1'
```
First client's next publish: `MqttCodeError: [code:128] Unspecified error`.
=> `default_specs()` always yields press-01..press-NN and the client id is
`f"{spec.device}-{run_id}"`. N swarm containers would evict each other continuously,
and runner.py's reconnect loop turns that into an eviction storm that looks like load.
=> Swarm needs a device-index offset / unique identity per container. (Bite #23 shape.)

#### P6 — observability lag (confirms carried-forward bites #20/#21)
Same instant, live vs management API:
```
{"event":"queue filled","depth":5,"api_depth":0}      <- rabbitmqctl 5, API 0
{"t":5.0,"depth":6,"api_depth":5}
```
Also observed rabbitmqctl itself reading 0 while the API read 6 (ordering flipped on a
later run), and `declaration_result.message_count` from a PASSIVE DECLARE returned 0
while the queue actually held 5.
=> No depth source is instantaneous. Tests must poll until stable, never single-read.
=> Telegraf scrapes the management API every 10s, on top of RabbitMQ's ~5s stats
interval. Port 15692 (Prometheus) is published but nothing scrapes it.

#### P7 — MQTT 5 vs 3.1.1 PUBACK under reject-publish (RESOLVES P3's caveat)
Same broker, same full queue (depth 6, max-length 5, reject-publish), same QoS-1 publish,
read at the paho layer so reason codes are visible:
```
{"proto":"3.1.1","waited_s":8.01,"puback":"NO PUBACK RECEIVED"}
{"proto":"5","waited_s":0.2,"puback":{"reason_code":"Quota exceeded","value":151,"is_failure":true}}
```
=> reject-publish IS observable to devices, but ONLY over MQTT 5 (0x97 Quota exceeded).
   Over 3.1.1 it is silent message loss.
=> Infrastructure requirement: specify MQTT 5 for devices that must detect backpressure.
=> SECOND-ORDER: aiomqtt reported this SAME v5 publish as success ("PUBACK ok (accepted)")
   — it does not surface failure reason codes. `sim/devices/runner.py` uses aiomqtt, so
   the swarm cannot observe rejections without reading the code at the paho layer.
   Pairs with ADR-0039 (MQTT5 CONNACK 134 vs 135) and ADR-0040 (aiomqtt swallowing a
   TLS alert): this is the same library-hides-the-signal shape, a third instance.

#### P8 — broker fd / connection limits (does NOT cap the experiment)
```
ulimit -n (in container) = 1048576
connection_max = undefined (unlimited)
channel_max = 2047 (per connection; irrelevant to MQTT)
```
=> No low container default will masquerade as a broker breaking point. A measured
connection ceiling will be a real resource limit.

#### P9 — TTL and x-max-length-bytes on quorum queues (both work)
DLX attached in every scenario.
| scenario | args | sent | acked | nacked | queue | DLQ |
|---|---|---|---|---|---|---|
| ttl | x-message-ttl=3000 | 10 | 10 | 0 | 0 (after 12s) | 10 |
| bytes-drophead | x-max-length-bytes=2000, drop-head | 10x500B | 10 | 0 | 3 | 7 |
| bytes-reject | x-max-length-bytes=2000, reject-publish | 10x500B | 4 | 6 | 4 | 0 |
=> TTL expiry DOES dead-letter on quorum queues. TTL is a usable overflow control.
=> x-max-length-bytes is accepted AND enforced, no "Invalid ..." warning (unlike
   reject-publish-dlx). Given memory ~= N*(0.85KB + payload), the BYTES limit is the
   control that actually bounds broker memory; the count limit does not.
=> ASYMMETRY: the COUNT limit admits maxlen+1 (kept 6 for a limit of 5); the BYTES
   limit engages exactly at the limit (4 x 500B = 2000B, 5th rejected).

#### P10 — the assignment's actual outage scenario (InfluxDB DOWN, Telegraf RUNNING)
100k x 200B published into amq.topic -> telemetry.q, InfluxDB stopped first:
```
sent=25000  telemetry.q total=0     unacked=0    mem=123MB
sent=50000  telemetry.q total=6341  unacked=50   mem=172MB
sent=100000 telemetry.q total=7640  unacked=50   mem=264MB
settle      telemetry.q total=0                  mem=274MB
```
`unacked` pinned at exactly 50 = telegraf.conf `prefetch_count = 50`.
telemetry.q drained to 0 DESPITE InfluxDB being down. The messages went to `dlq`:
```
dlq  100000
```
x-death on a sampled DLQ message:
```
"reason": "rejected", "count": 1, "queue": "telemetry.q"
```
=> NOT delivery_limit exhaustion (that would be reason "delivery_limit", count 20).
   Telegraf REJECTS each message once when the output write fails -> immediate dead-letter.
=> **THE ASSIGNMENT'S PREMISE FAILS ON THE DEFAULT PIPELINE.** "Stop InfluxDB, observe
   the queue growing" does not happen: telemetry.q stays near-empty and `dlq` grows
   instead. Broker memory pressure builds in the DLQ, which has NO max-length, NO TTL
   and NO consumer.
=> Matches Phase 2 / ADR-0011 ("ack-after-write and Telegraf dead-letter poison
   identically"). Phase 6 must pick its memory-pressure arm deliberately:
     (a) Telegraf STOPPED      -> telemetry.q grows      (clean, = P4's model)
     (b) Telegraf RUNNING      -> dlq grows              (realistic default behaviour)
     (c) ack-after-write consumer (compose.consumer.yml, Phase 2) -> queue genuinely
         grows with unacked; the only arm that demonstrates the store-and-forward
         premise the assignment is actually asking about.

#### P11 — is memory released? does disk come back? (decides run-to-run contamination)
After purging dlq (100k messages) and forcing GC:
```
before purge      288,910,744  (288.9 MB)
after purge+GC     90,891,472  ( 90.9 MB)   baseline was 74.1 MB
quorum_ets no longer in the top categories
```
=> MEMORY IS RELEASED. No leak. Purging between load waves is sufficient for the
   memory arm.
```
mnesia dir with BOTH queues empty: 157 MB
```
=> DISK IS NOT RECLAIMED. Ra log segments persist after the messages are gone.
   The disk arm needs `docker compose down -v` between waves or readings drift upward.

#### Host envelope to stamp on every result JSON
```
Docker 29.6.1, 24 CPU, 15.16 GiB visible to Docker
Windows 11 Home 10.0.26200, Python 3.12.10
rabbitmq:4.3.4-management, influxdb:2.9.1, telegraf:1.39.2, grafana/grafana-oss:13.0.2
mem watermark 0.6 of available = 9.76 GB (host-derived -> MUST be pinned)
disk_free_limit 50 MB, free disk 1017 GB
```

#### P12 — container hostname uniqueness (validates the swarm identity scheme)
`docker compose --scale sim-load=3`, each replica printing `socket.gethostname()`:
```
sim-load-1  hostname='0e1c13b7f13b' len=12 device='0e1c13b7f13b-000' client_id_len=16
sim-load-2  hostname='93aebc45ee42' len=12 device='93aebc45ee42-000' client_id_len=16
sim-load-3  hostname='38604f725af6' len=12 device='38604f725af6-000' client_id_len=16
```
=> Distinct per replica, 12 chars. Device name / client ID = 16 chars, under MQTT 3.1.1's
   23-char client-ID floor (L3 exercises a 3.1.1 publisher).
=> A literal prefix such as `press-` would make it 22 and spend the whole margin, so the
   hostname is the prefix. Resolves the `--scale` problem: replicas share one command
   line, so a per-replica offset ARGUMENT is impossible; self-assigned identity is not.

#### P13 — does RabbitMQ enforce MQTT 3.1.1's 23-char client-ID minimum? (NO)
```
{"proto":"3.1.1","client_id_len":23,"rc":"Success","published":true}
{"proto":"3.1.1","client_id_len":25,"rc":"Success","published":true}
{"proto":"3.1.1","client_id_len":64,"rc":"Success","published":true}
{"proto":"5","client_id_len":64,"rc":"Success","published":true}
```
=> 23 is the MQTT 3.1.1 spec's MINIMUM a server must accept, not a maximum. RabbitMQ
   accepts at least 64 on both protocol versions.
=> CORRECTS the first draft of P12/§2.13, which treated 23 as a budget to defend. The
   swarm's real client ID is `{device}-{run_id}` = 25 chars and works. The hostname
   prefix is justified by uniqueness alone, not by character economy.
