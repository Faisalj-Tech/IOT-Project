# Design Decisions

**Resilient IoT Messaging Infrastructure** — the complete record of every architectural and
methodological decision taken across the project's six phases, with the context that forced
each one, the alternatives rejected, and what happened when the decision met reality.

---

## How to read this

Forty-eight decisions, numbered **D-01** to **D-48** in the order they were taken. The
numbering is chronological, so a low number is an early commitment that later work inherited;
a high number is a late correction with the benefit of five phases of measurement behind it.

Each entry carries:

- **Context** — the problem, and (where it applies) the measurement that revealed it.
- **Decision** — what was chosen.
- **Rejected** — the alternatives, each with the reason it lost.
- **Outcome** — what happened when it was implemented, including where it turned out wrong.

**Most of these decisions were forced by measurement, not chosen from preference.** The
project's working rule is to probe a mechanism against a live stack before writing a
specification about it — a rule adopted after early phases repeatedly found that the obvious
expectation was wrong. Entries carrying an **Evidence** block record what was actually
observed, verbatim where the exact string matters.

Four decisions were later **superseded or amended** by a better-informed one. Each chain is
cross-linked from both ends, because the correction is usually the more interesting half:

| Chain | Story |
|---|---|
| D-04 → D-09 | A rule written for one experiment read as a rule for all five. |
| D-18 → D-21 → D-23 | A test marked `xfail` to hide a broken harness check silently hid a real data-loss check too. |
| D-24 → D-25 | A blocked test gate was blamed on the environment; the real cause was a one-flag omission. |
| D-31 → D-32 | Removing a config block fixed one profile and silently split the cluster in another. |

Decision numbers are stable. Where the phase reports in `main/docs/reports/` cite a decision
by number, it is the same number used here.

---

## Index

| # | Decision | Phase |
|---|---|---|
| [D-01](#d-01) | Verify delivery end-to-end through InfluxDB, not broker counters | 1 |
| [D-02](#d-02) | The publish loop guarantees deterministic counts, not just wall-clock timing | 1 |
| [D-03](#d-03) | Telegraf health via the `outputs.health` plugin, not process liveness | 1 |
| [D-04](#d-04) | Experiment findings are recorded in result files, not asserted as failures | 2 |
| [D-05](#d-05) | Experiments are opt-in pytest tests sharing the Phase 1 harness | 2 |
| [D-06](#d-06) | The ack-after-write consumer is built unconditionally, as a comparison arm | 2 |
| [D-07](#d-07) | Broker gauges are evidence of buffering, never proof of delivery | 2 |
| [D-08](#d-08) | Feature branch, not a worktree, for phase execution | 2 |
| [D-09](#d-09) | D-04 applies only to exploratory experiments | 2 |
| [D-10](#d-10) | The reconnect budget must accommodate a real broker restart | 2 |
| [D-11](#d-11) | Both consumers dead-letter poison identically — a falsified prediction | 2 |
| [D-12](#d-12) | The drain heuristic caused false loss reports; fixed per-experiment | 2 |
| [D-13](#d-13) | Quorum group size must be declared, never inherited from cluster size | 3 |
| [D-14](#d-14) | Partitions induced by `docker network disconnect`, minority read via `docker exec` | 3 |
| [D-15](#d-15) | Node-kill outcomes are asserted; partition outcomes are recorded | 3 |
| [D-16](#d-16) | Both partition-handling modes are measured | 3 |
| [D-17](#d-17) | The concurrent-run lock guards process liveness, not stack state | 3 |
| [D-18](#d-18) | Partition floor assertions marked `xfail` — *superseded* | 3 |
| [D-19](#d-19) | Audit remediation scoped to Critical and High findings only | audit |
| [D-20](#d-20) | Remediation verified by unit tests plus one cluster gate | audit |
| [D-21](#d-21) | The floor is un-masked by assertion ordering, not by a decorator | audit |
| [D-22](#d-22) | The sampler race is closed by a passive wait | audit |
| [D-23](#d-23) | The floor asserts sample source, not raw reachability | audit |
| [D-24](#d-24) | Ship with the cluster gate deferred — *superseded* | audit |
| [D-25](#d-25) | The gate was blocked by compose file-set asymmetry, not the environment | audit |
| [D-26](#d-26) | The profile-mismatch guard ships inside Phase 4's plan | audit |
| [D-27](#d-27) | Devices select a region vhost by IP-bound listener, not by credential | 4 |
| [D-28](#d-28) | Isolation asserts on broker provenance, never the payload's own region | 4 |
| [D-29](#d-29) | One region definitions file, with a drift test paying for the duplication | 4 |
| [D-30](#d-30) | Region consumers are read-only, so the definitions file owns the binding | 4 |
| [D-31](#d-31) | The region config carries no `cluster_formation` — *amended* | 4 |
| [D-32](#d-32) | Region+cluster discovery needs a bounded-retry three-node list | 4 |
| [D-33](#d-33) | Security config rides in `conf.d`; the region combination needs a fifth file | 5 |
| [D-34](#d-34) | `advanced.config` owns the complete `ssl_options` block | 5 |
| [D-35](#d-35) | Revocation is certificate-scoped, and incomplete without a force-close | 5 |
| [D-36](#d-36) | Token expiry terminates live connections; a static-token consumer cannot recover | 5 |
| [D-37](#d-37) | No AMQP TLS listener, because `ssl_options` is node-wide | 5 |
| [D-38](#d-38) | No custom broker image — use bash `/dev/tcp` for in-container probes | 5 |
| [D-39](#d-39) | CONNACK 134 vs 135 — authentication failure vs authorization failure | 5 |
| [D-40](#d-40) | Revocation detection observes the raw TLS layer, not the MQTT client | 5 |
| [D-41](#d-41) | Force-close must poll for connection visibility in the management API | 5 |
| [D-42](#d-42) | The device swarm runs in containers, because the host cannot run it | 6 |
| [D-43](#d-43) | Quorum queues have two overflow modes; the API accepts a third it ignores | 6 |
| [D-44](#d-44) | MQTT 5 is required for a device to observe broker backpressure | 6 |
| [D-45](#d-45) | Memory pressure is measured in three arms | 6 |
| [D-46](#d-46) | Overflow policies are applied at runtime, never written into definitions | 6 |
| [D-47](#d-47) | Overflow measurements pause Telegraf, because a second consumer splits the stream | 6 |
| [D-48](#d-48) | The memory model and disk-purge behaviour diverge from spec — recorded, not tuned | 6 |

---

# Part I — Evidence and measurement discipline

These six decisions determine what this project is willing to call proof. Every number in
every phase report rests on them.

<a id="d-01"></a>
## D-01 — Verify delivery end-to-end through InfluxDB, not broker counters

**Context.** Phase 1's suite needed to prove that telemetry actually survives the pipeline.
The original plan asserted on RabbitMQ's management-API `message_stats.publish` counter.
Three broker-side approaches were tried and all three failed empirically:

| Mechanism | Why it failed |
|---|---|
| `message_stats.publish` counter | Never increments for MQTT-origin messages on RabbitMQ 4.3.4 — confirmed by contrast with a native AMQP publish, where it appeared correctly after ~5 s |
| Queue-depth delta before/after publish | Depth is a live gauge shared by every consumer and test in the session; once Telegraf runs it drains continuously and other traffic contaminates the window |
| Non-destructive `queue/get` peek filtered by `run_id` | `prefetch_count = 50` claims messages into Telegraf's unacked buffer almost instantly, and the peek endpoint only returns *ready* messages — 0 visible immediately after publish |

**Decision.** Prove delivery **exclusively through InfluxDB**. Publish with a per-run `run_id`,
query InfluxDB via Flux filtered on it, and assert a **gap-free per-device sequence set** with
row count equal to published count. A gap means loss. Nothing is inferred from a graph.

**Rejected.** All three broker-side mechanisms above, each on measured evidence rather than
principle.

**Outcome.** This is the load-bearing rule of the whole project. Every zero-loss claim in
Phases 2, 3 and 4 is a sequence-accounting claim. It also has a side benefit that mattered
later: because every query is anchored on `run_id` and a run-scoped time range, residual data
from an earlier run cannot contaminate a later run's verification (see [D-17](#d-17)).

---

<a id="d-02"></a>
## D-02 — The publish loop guarantees deterministic counts, not just wall-clock timing

**Context.** `sim/devices/runner.py`'s publish loop is the harness every later phase reuses to
generate load and measure loss. The original implementation had two latent bugs. It computed
its publish deadline **before** the MQTT connection was established, so handshake time silently
ate into the publish budget and occasionally produced fewer messages than `rate_hz × duration_s`.
And it incremented `seq` **before** the publish call succeeded — so a failed publish under QoS 1
would permanently skip a sequence value, creating an artificial gap **indistinguishable from
real message loss**, which is the exact failure mode [D-01](#d-01) exists to detect.

**Decision.** Two hard guarantees:

1. A healthy run publishes at least `ceil(rate_hz × duration_s)` messages regardless of
   scheduler jitter. The deadline clock starts only after the first successful connect (so the
   initial handshake is free, but a later reconnect's handshake is not).
2. `seq` advances only after `client.publish()` returns without raising. A failed publish never
   consumes a sequence number; the next attempt retries the same value.

The reconnect `attempt` counter also resets on every successful connect, making `max_reconnects`
a **per-outage** budget rather than a lifetime one.

**Rejected.**

- *Widen the timing safety buffer* — still probabilistic; a bigger buffer reduces flakiness
  without eliminating it, and this project's grading hinges on precise measurement.
- *Loosen the test assertions to tolerate undercounts* — papers over a timing bug with an
  arbitrary threshold that still failed intermittently under full-suite load.

**Outcome.** Message counts became trustworthy enough to be the denominator of every loss
calculation from Phase 2 onward.

---

<a id="d-03"></a>
## D-03 — Telegraf health via the `outputs.health` plugin, not process liveness

**Context.** The design required a healthcheck on every service, but the plan's Telegraf compose
block never included one. Without it, `docker compose up -d --wait` confirms only that the
process is running — not that it parsed its config, connected to RabbitMQ, or can write to
InfluxDB. A crash-looping Telegraf under `restart: unless-stopped` can satisfy `--wait` if the
check lands during a brief "Running" window.

**Decision.** Add an `[[outputs.health]]` block exposing an internal-only HTTP endpoint (not
published to the host) and a compose healthcheck that curls it. Purely additive — the frozen
buffering settings (`metric_buffer_limit`, `flush_interval`, retry behaviour) were **deliberately
left untouched**, because Phase 2 exists to measure what those defaults do under outage and must
not have them pre-tuned.

**Rejected.**

- *`pgrep telegraf`* — proves the process exists, which is the exact gap being closed.
- *Defer to Phase 2* — Phase 2's outage experiments need "Telegraf is healthy" as a trustworthy
  precondition; one output block and one healthcheck is cheap now.

**Outcome.** `--wait` genuinely verifies all four services are functional. The untuned buffering
settings later became the discriminator in the project's central reliability finding — see
[D-45](#d-45).

---

<a id="d-04"></a>
## D-04 — Experiment findings are recorded in result files, not asserted as failures

**Context.** Experiments are pytest tests, so their natural failure mode is an assertion error.
That is correct for a correctness suite and **wrong for an experiment**. Experiment D hunts for a
path that destroys a message without dead-lettering it; the first draft of its tests asserted
`outcome != "parse-nack-requeue-loop"` and `outcome != "silently-discarded-no-counter"` —
precisely the two outcomes the experiment exists to discover. A successful hunt would have failed
the suite, made the phase's definition of done unmeetable, and left behind a red test
whose most obvious fix is to weaken the experiment until it goes green.

**Decision.** An experiment test asserts only that **its measurement was valid**: the failure
condition it induced actually engaged, the outcome could be classified, and the instrumentation
worked. What the pipeline *did* is written to `main/docs/results/<experiment>-<run_id>.json` as
an `outcome`/`verdict` field and carried from there into the report. Tests may assert a specific
pipeline behaviour only where that behaviour is a contract of code this project owns.

**Rejected.**

- *Assert the desired outcome; treat a finding as a failing test* — "the project is graded on
  measuring where the system breaks. A harness that fails when it succeeds pressures its own
  maintainer to stop measuring."
- *Mark discovery experiments `xfail`* — encodes a prediction as an expectation, and an `xpass`
  then means the opposite of what a reader assumes. Experiment D's result is one of **four**
  classifications, which `xfail` cannot express.

**Outcome.** A green experiment run means the measurements are trustworthy, not that nothing was
found — the two claims became separable. The known cost is that a finding is invisible from the
exit code alone; whoever runs the matrix must read the result JSONs. **Immediately narrowed by
[D-09](#d-09).**

---

<a id="d-05"></a>
## D-05 — Experiments are opt-in pytest tests sharing the Phase 1 harness

**Context.** Phase 2 adds five experiments that each take minutes — they stop containers, hold
outages open, wait for queues to drain. Phase 1's suite runs in about a minute and is the
project's reproducibility deliverable. Putting the experiments in the default run would push it
past fifteen minutes, for four more phases of work. Keeping them outside pytest entirely would
mean re-implementing the `stack`, `rabbit_get` and `influx_query` fixtures.

**Decision.** Experiments live in `main/tests/experiments/`, carry `@pytest.mark.experiment`, and
are deselected by default via `addopts = -m "not experiment"`. `pytest tests/` stays the fast
correctness gate; `pytest -m experiment` runs the matrix. Both share the Phase 1 fixtures.
Experiment bodies stay synchronous, running the simulator on a background thread, so container
manipulation reads as a straight-line script with no `pytest-asyncio` dependency.

**Rejected.**

- *One suite, everything by default* — the fast suite is the reproducibility deliverable and is
  run far more often than the matrix.
- *Standalone scripts* — loses the fixtures and the assertion machinery; duplicating the harness
  is exactly the cost the marker avoids.
- *A declarative timeline engine with experiments as data* — elegant for four similar arms, but
  Experiment D (publish a poison payload, classify the outcome) does not fit a timeline shape at
  all. YAGNI.

**Outcome.** The pattern scaled to four more marker families (`cluster`, `region`, `security`,
`load`), each deselected by default and each selected by an environment variable that also points
the harness at the right compose file set.

---

<a id="d-06"></a>
## D-06 — The ack-after-write consumer is built unconditionally, as a comparison arm

**Context.** The assignment specifies a custom ack-after-write consumer *if a gap is found* in
Telegraf's behaviour. Arithmetic on the Phase 1 configuration predicted **no gap**:
`max_undelivered_messages = 1000` engages backpressure long before `metric_buffer_limit = 10000`
could overflow, and AMQP requeues unacknowledged messages when a consumer's channel dies. Read
literally, the assignment would leave the reliability report with no counterfactual — "Telegraf
was fine" and nothing to compare it against. Separately, Telegraf and a second consumer cannot
both drain `telemetry.q`: as competing consumers they split the stream and make any comparison
meaningless.

**Decision.** Build the consumer **unconditionally** as a comparison arm, and isolate the arms by
running them **separately against the same queue** — the consumer arm runs with Telegraf stopped,
via a `compose.consumer.yml` overlay. It authenticates as the existing `telegraf` user (which
already holds `read` on `telemetry.q`), so the definitions file is not modified. Its point schema
matches Telegraf's `json_v2` mapping exactly. The consumer stays a comparison artifact and does
not replace Telegraf, since the assignment specifies Telegraf-based ingest.

**Rejected.**

- *Build it only if a gap appears* — the predicted outcome is no gap, and "a recommendation with
  no counterfactual is an opinion."
- *A second parallel queue, both consumers running at once* — strongest possible comparison
  (byte-identical streams), but requires changing the definitions file, doubles broker load, and
  makes queue depths incomparable to the Phase 1 baselines. The metric of interest is loss
  fraction, not per-message equality, so statistically identical streams suffice.
- *Build it as a measurement probe, not a durability arm* — "the cost difference is one task; the
  value difference is the report's conclusion."

**Outcome.** The arm produced Phase 2's A/B comparison, and was reused unchanged four phases
later as [D-45](#d-45)'s arm (c) — the **only** arm in the entire project that demonstrates
genuine store-and-forward. A known risk was pinned by a dedicated test: a point-schema mismatch
between the two arms would collide on field types in the same InfluxDB measurement, and the
resulting rejection would look exactly like the silent drop Experiment D hunts for.

---

<a id="d-07"></a>
## D-07 — Broker gauges are evidence of buffering, never proof of delivery

**Context.** [D-01](#d-01) removed every broker-side check after three were empirically disproved.
Read as a blanket ban it would forbid the one thing the assignment explicitly asks for — "observe
the queue growing" during an InfluxDB outage — and would forbid measuring
`messages_unacknowledged`, the only direct read on Telegraf's in-flight batch size and therefore
the only way to answer the ack-on-receipt-versus-ack-after-write question.

The distinction [D-01](#d-01) actually rests on is narrower than a ban. It rejects a **counter**
that is never populated, and rejects a depth delta *as proof of delivery under concurrent drain*.
Phase 2's experiments are precisely the case where Telegraf is deliberately not draining.

**Decision.** `messages_ready`, `messages_unacknowledged`, DLQ depth and node alarm state are
sampled every two seconds during every experiment and recorded as a timeseries in each result
file. They are used as **evidence that buffering occurred**, as the measurement of in-flight batch
size, and as **guard conditions** ("the queue never grew, so the outage did not engage"). They
never take part in a delivery or loss assertion.

**Rejected.**

- *Blanket ban; observe buffering through Grafana screenshots* — makes the report's evidence
  images rather than data, and leaves in-flight batch size unmeasured.
- *Derive buffering from InfluxDB's ingestion rate* — an ingestion gap shows only that data
  stopped arriving, not that it was safely queued. It cannot distinguish "buffered in the broker"
  from "lost at the edge", which is the whole question.

**Outcome.** `messages_unacknowledged` migrating back to `messages_ready` after a consumer is
killed became a citable number (`requeued_messages`) — the direct answer to the acknowledgement
question. Guard assertions could now detect an experiment that never actually engaged, which
would previously have produced a meaningless pass.

---

# Part II — Durability (Phase 2)

<a id="d-08"></a>
## D-08 — Feature branch, not a worktree, for phase execution

**Context.** Phase 1 was implemented in a git worktree. Phase 2's plan hardcodes absolute working
directories that all fourteen task briefs reproduce verbatim. `.venv`
and `.env` are gitignored and exist only in the main checkout. The Docker stack binds fixed
container names and fixed host ports, so two stacks cannot run concurrently regardless of git
isolation.

**Decision.** Work happens on an in-place feature branch, checked out directly in the repository
root — not a worktree.

**Rejected.**

- *Git worktree* — every task brief's hardcoded paths would need manual override across
  fourteen tasks; a fresh worktree has neither `.venv` nor `.env`; and the Docker stack's fixed
  ports mean the worktree buys **no runtime isolation anyway**, only ref isolation, at real setup
  cost.
- *Work directly on the default branch* — no clean point to diff against for the final
  final branch review.

**Outcome.** Used for every subsequent phase. The accepted cost is no filesystem isolation; the
branch itself is the mitigation.

---

<a id="d-09"></a>
## D-09 — D-04 applies only to exploratory experiments

**Context.** [D-04](#d-04)'s title reads broadly. Taken as a blanket rule it forbids Experiments
A, B and C's hard assertions (`gaps == {}`, `total_rows == expected_total`,
`dlq_final == dlq_baseline`) — all of which were written into the plan and implemented. Read by
its title alone, the rule was cited to flag every one of those assertions as a violation, forcing
unnecessary fix rounds on work that was correct as specified. This happened **repeatedly**.

Reading [D-04](#d-04)'s own context shows the rule was written specifically about Experiment D,
and its stated rejection reason — "a harness that fails when it succeeds" — is inapplicable to
A/B/C, which have no discovery to succeed at. They predict a specific outcome, and a red test
means the prediction was falsified, which is the intended signal.

**Decision.** [D-04](#d-04) applies **only to Experiment D** (the silent-drop hunt), whose outcome
is genuinely unknown and classified into named buckets. Experiments A, B, C and E are
**confirmatory**: their hard pass/fail assertions are intentional and must not be weakened. Any
review of a confirmatory experiment must use the full scoping rationale, not just the title.

**Rejected.**

- *Read [D-04](#d-04) as blanket* — would require rewriting already-implemented, already-reviewed
  code and discard the safety net that a genuinely broken pipeline should fail loudly.
- *Edit [D-04](#d-04)'s text in place* — obscures the historical record of what was decided when.
  A separate narrowing decision, cross-referenced from both directions, preserves the audit trail
  without either document contradicting itself.

**Outcome.** The confirmatory/exploratory split became a standing classification that every later
phase applies to its own experiments (see [D-15](#d-15)). The residual risk is stated plainly: a
genuinely exploratory experiment mistakenly treated as confirmatory could get hard assertions
that suppress a real finding — so every new experiment's brief must state its category
explicitly.

---

<a id="d-10"></a>
## D-10 — The reconnect budget must accommodate a real broker restart

**Context.** The simulator reconnects with exponential backoff. The original default of
`max_reconnects = 5` permits `0.5 + 1 + 2 + 4 + 8 = 15.5 seconds` of broker downtime before the
client gives up with `MqttError("Maximum reconnection attempts exceeded")`.

Experiment C measured that a real RabbitMQ restart — stop, wait, start, wait-for-health — takes
**20–45 seconds** depending on queue state and whether the restart is warm or cold. The default
would fail before the recovery phase the experiment exists to observe. The experiment worked
around it by setting `RECONNECT_BUDGET=12` (~85.5 s of tolerance, with backoff capped at 10 s per
attempt), and that value proved sufficient across every experiment.

**Decision.** Raise the out-of-the-box default from 5 to 12, in both the simulator's `run_devices()`
and the test harness's `start_sim()`. The result JSON records both values — `max_reconnects: 12`
(used) and `default_max_reconnects: 5` (the default that would have failed) — so the change is
visible in the evidence rather than only in the code.

**Rejected.**

- *Keep 5, require every experiment to override* — friction for future experiments, and "a default
  that can't survive a common production failure mode misrepresents" what the simulator is meant
  to be a reference implementation of.
- *Make the budget adaptive on elapsed time* — breaks determinism, which [D-02](#d-02) established
  as a core value; an integer count is more measurable and easier to reason about.

---

<a id="d-11"></a>
## D-11 — Both consumers dead-letter poison identically — a falsified prediction

**Context.** Experiment E was **predicted** to show a different poison-handling outcome than
Telegraf: the write-then-ack consumer might requeue, drop, or dead-letter differently than
Telegraf's nack-on-failure.

Measured: both converged on the identical outcome, `parse-nack-to-dlq`. Poison messages in both
arms were immediately nack'd and moved to the dead-letter queue. No requeue loops, no silent
drops, no write-through-and-ignore. **The predicted divergence did not materialise.**

**Decision.** Accept the unified behaviour and **record the falsified prediction**, so later
phases do not re-assume a divergence that measurement has ruled out.

**Rejected.**

- *Hedge — "divergence may still occur under other conditions"* — weakens the measurement. A
  result that rules out a prediction is a finding as strong as one that confirms it.
- *Refactor Telegraf to ack-after-write to eliminate the difference* — Telegraf is a third-party
  component; the point of the experiment is to measure it as-is.

**Outcome.** This negative result paid for itself immediately: [D-15](#d-15) cites it as the
reason Phase 3's cluster matrix runs the Telegraf arm only, halving its runtime — **with a stated
condition for reopening the decision** (if leader-kill had shown loss, the consumer arm would
become worth re-running as the first divergence this decision did not find). Leader-kill measured
zero loss with zero duplication, so the condition was not met.

The decision is explicit about its own scope: the two strategies still differ in **requeue
overhead and latency** under transient outages. Only poison resilience is claimed identical.

---

<a id="d-12"></a>
## D-12 — The drain heuristic caused false loss reports; fixed per-experiment

**Context.** Experiment C produced intermittent loss findings — three clean runs, two lossy (550
and 740 rows out of 850), with both lossy runs reporting an identical contiguous gap across all
five devices, which looked like a system-wide loss event during restart recovery.

Instrumenting the drain function's elapsed time settled it:

| Run | Verdict | Rows | Drain elapsed |
|---|---|---|---|
| expC82538 | no-loss | 850 | 85.9 s |
| expC85181 | no-loss | 850 | 80.9 s |
| expC85545 | **loss** | 550 | **60.6 s** |
| expC87075 | **loss** | 740 | **70.8 s** |
| expC87571 | no-loss | 850 | 85.9 s |

The lossy runs exited **earlier** than every clean run — not later, and nowhere near the 240 s
timeout. That rules out both genuine broker loss (which would show the queue reaching zero and
staying there) and timeout exhaustion. The cause was the drain function's second exit condition:
six consecutive polls with zero row-count growth, read as "settled". Under a restart-scale
outage, Telegraf's post-recovery backlog drain shows a 30+ second **lull mid-drain** that is not
the end of draining. The function returned a partial count, and the test then correctly reported
the missing rows as loss.

**Decision.** Add an optional `stable_polls_limit` parameter, **default unchanged at 6**, and
override it to 18 (90 seconds of no-growth tolerance) for Experiment C only.

**Critical constraint:** the default must not change. Experiments A, B, D and E call the function
without the parameter and their already-committed results were measured under the old behaviour;
changing the default would make those results unreproducible against the shipped harness.

**Rejected.**

- *Raise the shared default to 18* — breaks reproducibility of four experiments' committed
  results.
- *Raise the timeout from 240 s* — addresses the wrong root cause entirely; the failure is an
  early exit at 60–70 s, not timeout exhaustion.
- *Remove the early-exit condition* — genuinely lossy runs would then burn the full 240 s per
  call, roughly fifteen minutes per suite run.

**Outcome.** Two post-fix runs reached 850/850 with zero gaps. Experiment C's true behaviour is
zero-loss; the harness simply had not waited long enough to see it.

---

# Part III — Cluster and fault tolerance (Phase 3)

<a id="d-13"></a>
## D-13 — Quorum group size must be declared, never inherited from cluster size

**Context.** Before designing the cluster topology, the running single-node stack was queried
directly:

```
$ docker exec iot-rabbitmq rabbitmq-queues quorum_status telemetry.q
Node Name       Raft State  Membership
rabbit@rabbit1  leader      voter        <- the only row
```

`telemetry.q` was a **one-member Raft group**. So was `dlq`. **A quorum queue's member group is
fixed when the queue is declared and does not grow when nodes later join the cluster.** The
definitions file declared `x-queue-type: quorum` with no `x-quorum-initial-group-size`, and
`definitions.skip_if_unchanged = true` meant re-importing could not repair it either.

The consequence is specific and dangerous: adding two nodes to a cluster whose first node boots
from the existing volume yields a three-node cluster in which the queue still lives entirely on
node 1. Killing node 1 makes the queue disappear — which reads as a dramatic finding ("quorum
queues do not survive single node loss") when it is purely an artifact of how the queue was
declared.

There is a **second trigger for the same bug.** `x-quorum-initial-group-size` is a target, not a
guarantee: RabbitMQ declares with `min(group_size, nodes currently in the cluster)`. Node 1
imports definitions during boot, and peer discovery also happens at boot; their relative order is
not guaranteed. If the import wins that race, the queue is declared one-member again — from the
very configuration added to prevent it.

**Decision.** Declare `x-quorum-initial-group-size: 3` on both queues in a cluster-specific
definitions file, boot every cluster node from a **fresh** volume, keep `rabbitmq-queues grow` as
the documented remediation for the race, and gate every experiment behind a **preflight test**
that asserts three voting members and names the exact `grow` commands in its failure message.

**Rejected.**

- *Reuse the existing single-node volume* — carries the one-member queue into cluster formation,
  reintroducing exactly the bug this decision prevents, invisibly.
- *Grow the queue after formation instead of declaring the size* — a manual post-bring-up step a
  fresh clone would skip, silently producing a one-member group on someone else's machine.
  Reproducibility is a graded criterion.
- *Edit the base definitions file in place* — would retroactively change the configuration under
  which every published Phase 2 result was produced.
- *Trust the preflight gate alone* — "a gate that catches a preventable misconfiguration is worse
  than not having the misconfiguration."

**Outcome — the race is nondeterministic per bring-up, not per machine.** Across five observed
bring-ups **on the same machine**, the race fired on three and did not on two, with no pattern by
time of day, elapsed time, or ordering. Two fresh bring-ups hours apart split in opposite
directions. The README and the preflight gate's failure message were updated to treat `grow` as a
**routine check after every fresh bring-up**, not a rare edge case.

---

<a id="d-14"></a>
## D-14 — Partitions induced by `docker network disconnect`, minority read via `docker exec`

**Context.** The `ignore` partition arm only produces a finding if **both sides** of the split
can be observed. A mechanism that makes the minority unobservable measures half the experiment.

An earlier draft attached each broker to a private management network on the assumption that a
partitioned container keeps its published port. **That assumption was tested and is false:**

| Probe | Result |
|---|---|
| Container on net A, connected to `bridge`, disconnected from A | published port **breaks** |
| Container on net A, connected to net B, disconnected from B | published port **survives** |
| Compose service on `core` + `mgmt`, `mgmt` given `priority: 1000`, `core` disconnected | published port **breaks** |
| Compose service on `core` + `amgmt` (no priorities), `core` disconnected | published port **survives** |

Docker publishes a container's ports through a single network endpoint, and Compose binds that
endpoint to whichever network it attaches **first — ordered alphabetically by network name**. The
documented `priority` field does not control it. A per-node management network would therefore
work only by being named to sort ahead of `core`, resting the phase's central measurement on an
undocumented ordering rule.

**Decision.** Induce partitions with `docker network disconnect` (heal with `connect`) against
the existing compose networks. Read a partitioned node's state through **`docker exec`** —
`rabbitmq-diagnostics`, `rabbitmq-queues`, `rabbitmqctl` — which needs no networking at all.
`restore()` heals every recorded partition before restarting services, on **every** exit path
including assertion failure and interrupt.

**Rejected.**

- *Per-node management network* — measured to fail as specified, and the working variant depends
  on an undocumented ordering rule a rename would silently break.
- *`iptables`/`tc` inside the containers* — the most surgical option (partitions only Erlang
  distribution on 25672, leaving client traffic observable), but the official image ships no
  `iptables` and it needs `NET_ADMIN` plus a runtime package install. Fragile on Docker Desktop,
  and a flaky partition mechanism produces findings that cannot be trusted.
- *`docker pause`* — a paused container is a stalled node holding its sockets open, a different
  failure mode. "It measures the wrong thing; reporting it as split-brain would be dishonest."

**Outcome.** No new image, no capabilities, no package installs. The known cost is two read paths
in the recorder instead of one, and coarser sampling (exec is slower than HTTP). A partitioned
node's published ports genuinely go down, which is realistic but must be accounted for in every
experiment that reads the cluster after inducing a fault — this is what
`GaugeRecorder.expect_exec(node)` exists for, armed the instant a node is partitioned so the
doomed HTTP attempt is skipped entirely.

---

<a id="d-15"></a>
## D-15 — Node-kill outcomes are asserted; partition outcomes are recorded

**Context.** Phase 3's four experiments have genuinely different epistemic status. A healthy
three-member quorum group surviving the loss of one node has a **correct answer**: the majority
holds, the queue stays available, no message is lost. Split-brain behaviour under `ignore` does
**not** — outcomes depend on the mode, on timing, and on how Docker Desktop's networking behaves
on a given machine.

**Decision.** Experiments F (follower kill) and G (leader kill) carry hard assertions: zero
sequence gaps, `influx_count >= published_count`, membership restored to three voters, and — for
F — the leader unmoved, for G — a new leader actually elected. Experiments H and I (partition
under each mode) **record** their findings per [D-04](#d-04), asserting only a harness-integrity
floor: the partition genuinely occurred, the minority stayed observable, and nothing published to
the **majority** side was lost.

Deciding this in the design rather than during implementation is what stops the question being
re-litigated a third time.

**Rejected.**

- *Assert everything, including partition outcomes* — partition timing on Docker Desktop is
  variable enough that a red test may mean "host networking hiccup" rather than "broker defect",
  and "a suite that goes red for non-reasons trains its reader to ignore it."
- *Record everything, assert nothing* — discards the strongest claim the phase can make.
- *Decide per experiment during implementation* — "invites exactly the after-the-fact reasoning
  where an assertion is softened because it failed. The split must be decided before the results
  are known, or it is not a methodological choice."

**Outcome — the bet held.** G's zero-gap assertion was explicitly framed as a genuine bet: if
Telegraf's requeue guarantee failed to survive a leadership change, the suite would go red, and
that would be the first divergence [D-11](#d-11) did not find. A prerequisite check first
confirmed Telegraf itself reconnects to a surviving node in **7.15 s**, so any stall in G would
be attributable to the Raft election rather than to client behaviour. Then G killed the leader
node that Telegraf was *also* attached to — the harder of the two cases — and measured 850/850,
zero gaps, **zero duplicates**, three voters restored. The `xfail` escape hatch the plan carried
for a failed bet was never used.

---

<a id="d-16"></a>
## D-16 — Both partition-handling modes are measured

**Context.** RabbitMQ's `cluster_partition_handling` default is `ignore`: both sides stay up and
the minority keeps accepting connections while its quorum queues lose their majority.
`pause_minority` is the recommended production setting: minority nodes suspend their listeners so
clients fail fast.

The assignment asks to induce a split-brain and document "how quorum queues behave **and what the
client experiences**". Under `pause_minority` there is no split-brain to observe — that is the
point of the setting. Under `ignore` there is, but the resulting configuration is not one anybody
should run. Choosing one mode makes the deliverable weaker in a specific way: `ignore` alone
produces observations with no measured recommendation; `pause_minority` alone produces a
recommendation with nothing measured to justify it.

**Decision.** Measure both. The mode is supplied at runtime through an environment variable
(default `ignore` in the committed template) so switching arms is a **stack restart, not a
config-file edit**. The `pause_minority` experiment **refuses to run** — skipping with a message
naming the effective mode — unless the broker actually reports that mode.

**Rejected.**

- *`ignore` only* — the recommendation would rest on citing documentation rather than
  measurement.
- *`pause_minority` only* — "answers a question the assignment did not ask while skipping the one
  it did."
- *Hardcode the mode and swap config files between arms* — "an experiment whose arms require
  editing a file between runs invites the two arms to differ in more than the variable under
  test."

**Outcome.** The runtime-override mechanism was confirmed twice, once per mode value, so the
config-file-swap fallback was never built. Both arms measured zero end-to-end loss and identical
clean reformation — see the Phase 3 report for why that makes the production recommendation rest
on mechanism rather than on a measured difference.

---

<a id="d-17"></a>
## D-17 — The concurrent-run lock guards process liveness, not stack state

**Context.** Phase 2 lost four experiment result files because two pytest sessions ran against
one live Docker stack from separate terminals. Phase 3 added a session-scoped lockfile holding
its PID, refusing to start when a live foreign lock is present, and reclaiming a lock whose PID
is gone so a crashed run needs no manual cleanup.

A review raised a Critical finding: the lock is released unconditionally at teardown, including
when `KEEP_STACK=1` has skipped the teardown. Scenario — developer A runs with `KEEP_STACK=1`,
exits, lock released, containers still up; developer B acquires cleanly, `up -d --wait` no-ops
against A's running containers, and B measures against A's residual data. The proposed fix was to
keep the lockfile alive when the stack is kept.

**Decision.** The lockfile is a **live-session guard keyed on process liveness**, released
unconditionally at teardown including under `KEEP_STACK=1`. It does not attempt to detect a stack
left running by a session that has already exited.

**Rejected.**

- *Keep the lockfile when `KEEP_STACK=1`* — rejected for **two independent reasons**. First, it
  does not work: once A's process has exited its PID is dead, so B reclaims the stale lock and
  proceeds exactly as it would with no file at all — the review's own text conceded this. Second,
  it is **actively harmful**: the proposal places the release *after* `compose down -v` in the
  same branch, and a failed teardown — precisely when the lock most needs clearing — would leave
  a live-PID lock behind and block the next run for real, converting a hypothetical contamination
  into a certain hard failure.
- *Make the guard stack-state aware* (a session marker in the stack, or forcing `down -v` on
  acquire) — real complexity, and forcing teardown on acquire destroys the working stack
  `KEEP_STACK=1` exists to preserve. The measured contamination was **concurrent** sessions, not
  sequential reuse.
- *Drop `KEEP_STACK` support* — removes a working affordance to close a hazard never observed.

**Outcome.** The residue hazard is left open deliberately, mitigated by [D-01](#d-01): every
experiment anchors its queries on a run-scoped time range and a unique `run_id`, so residual
points from an earlier run cannot enter a later run's verification. Two known weaknesses in the
Windows liveness check are recorded rather than hidden — it matches the PID as a substring of
`tasklist` output, whose rows carry a memory column, so a short PID can match a memory figure and
report a dead process as alive; and a `tasklist` error leaves the output empty, reporting a live
process as dead. **The two fail in opposite directions from the same three lines.** If either
fires, the fix is a stricter parse, never a weakened test.

---

<a id="d-18"></a>
## D-18 — Partition floor assertions marked `xfail` — *superseded by [D-21](#d-21)*

> **Its defect analysis is still accurate and is the clearest account of the guard bug in the
> project. Its decision is replaced.**

**Context — the guard defect.** Investigating an unsubstantiated "PASSED" claim whose own result
JSON contradicted it, the defect was traced to `GaugeRecorder.node_window()`:

```python
entry = sample.get("nodes", {}).get(node, {})
if entry.get("reachable") and key in entry:
    return entry[key]
return None
```

When querying the `reachable` key **itself**, this guard can only ever return `True` or `None` —
**never a genuine `False`** — because the guard condition and the queried key are the same field.
Combined with a real timing race between when the views are snapshotted and when the background
sampler's next (multi-subprocess, slow) `docker exec` poll lands, the floor assertions became an
unreliable pass/fail signal in **both** directions.

**Decision (superseded).** Mark both partition tests `@pytest.mark.xfail(strict=False)` and do
**not** modify the harness. `strict=False` because the race means either test can pass on a lucky
sample, and an unexpected pass must not fail the suite either.

**Rejected at the time.** Fixing `node_window`'s guard — correctly diagnosed and small, but the
function is shared by every experiment reading per-node gauge state, and no task in the plan
authorised touching it with budget to re-verify. Weakening the floor assertions — explicitly
refused as "weaken a test to make it pass".

**Why it was superseded.** `pytest.mark.xfail` decorates a **whole function** and cannot
distinguish which assertion inside it fired. Each of these tests ends with two assertions that
have nothing to do with the floor:

```python
assert payload["gaps"] == {}
assert len(payload["members_after_heal"]) == 3
```

**A partition that genuinely lost messages, or a cluster that failed to reform, would therefore
report `XFAIL` and the suite would exit green** — while the `xfail` reason text asserted the
opposite in its own words. This became the audit's only Critical finding.

---

# Part IV — Audit remediation

<a id="d-19"></a>
## D-19 — Audit remediation scoped to Critical and High findings only

**Context.** A code audit of Phases 1–3 accepted **81 findings** after verification: 1
Critical, 9 High, 25 Medium, 24 Low, 22 very-low. Fixing all 81 in one branch would produce a
diff in which a measurement-semantics change and a magic-number rename are reviewed side by side,
and several Medium findings touch compose bring-up, dragging cluster-formation re-verification
into the same branch.

The audit's severity rubric was built around this codebase's actual worst failure mode: **not a
crash, which is loud, but a silently wrong measurement that ships into a graded report.**
Critical and High are exactly the findings that meet that bar.

**Decision.** Fix the Critical and the nine High findings only. Medium, Low and very-low are
carried forward as a documented backlog **including ones living in files the branch edits**.

**Rejected.**

- *Fix all 81* — "review quality degrades exactly where it matters most. The Critical finding is
  that a data-loss regression currently exits green — that deserves its own reviewable branch."
- *Critical + High + Medium (35)* — two Medium findings change compose bring-up and broker
  healthchecks, so the gate would have to prove formation behaviour as well as measurement
  behaviour. Roughly triples the branch, and **no Medium finding blocks trusting a measurement.**
- *Critical + High plus hand-picked Mediums sharing a file* — "'touched once' is a convenience
  for the author, not a property reviewers benefit from; it silently widens scope by proximity
  rather than by severity." One deliberate exception was made where a Medium was literally the
  same two lines as a High.

**Outcome.** The branch knowingly fixes one instance of a swallow-then-forget pattern while
leaving two identical siblings — a reviewer seeing only the diff would reasonably read that as an
oversight, so the plan's constraints name the deferred siblings explicitly and state that
**leaving a sibling of a bug you just fixed is intentional here.** **71 non-Critical/High
findings remain open** and are documented with file, line and failure scenario, so deferring is
not forgetting.

---

<a id="d-20"></a>
## D-20 — Remediation verified by unit tests plus one cluster gate

**Context.** The remediation branch changes shared measurement code every experiment reads.
[D-18](#d-18) deferred the guard fix on the explicit grounds that there was no budget then to
"re-verify F/G/preflight against the change" — so whoever fixes it **owes that verification**. Against that: the full Phase 2 matrix runs 21–27 minutes, Docker resource
contention is documented after 20+ minutes of tests in one sitting, and the audit itself was
static — **no finding in it was reproduced at runtime.**

**Decision.** Verify in two layers. **Synthetic unit tests** — hand-constructed sample payloads,
no Docker — pin the changed pure logic. **One cluster run** on a fresh bring-up gates the branch,
covering preflight, Telegraf failover, and all four fault experiments. Experiments A–E are **not**
re-run, committed canonical result JSONs are **not** regenerated, and the Phase 3 report is
**not** amended.

**Rejected.**

- *Unit tests only* — "un-masking an assertion without once observing it pass on a live partition
  is not meaningfully different from leaving it masked."
- *Full re-run including the Phase 2 matrix* — over an hour of Docker time, into the contention
  window where a test fails under two distinct signatures neither of which indicates a real
  regression. "The added confidence is concentrated in flake-prone runs."
- *Also regenerate the committed baselines and amend the Phase 3 report* — edits a graded
  deliverable that is already merged and tagged, on the strength of a single post-fix run. "The
  divergence is real and worth recording, but recording it belongs in a decision record, not in a
  silent rewrite of a merged report."

**Outcome.** The consequence is stated up front: post-fix partition runs produce a payload whose
`online` list is derived from Raft state rather than aliased, so **the committed canonical files
no longer match what the code produces**, and the Phase 3 report continues to describe a defect
that has been fixed. This record is the reason a future reader comparing the two does not read it
as a regression.

---

<a id="d-21"></a>
## D-21 — The floor is un-masked by assertion ordering, not by a decorator

**Supersedes [D-18](#d-18).**

**Context.** [D-18](#d-18)'s chosen mechanism does the thing that decision explicitly rejected —
it masks the loss and reformation assertions along with the unreliable floor check. The obvious
repair (split each test in two so only the floor carries the marker) **does not work here**: the
Docker-control, recorder and results fixtures are all function-scoped, so a module-scoped fixture
sharing one cluster run across two tests raises `ScopeMismatch` at collection.

**Decision.** Remove both decorators. Keep one test function per mode, but **order** it so the
loss and reformation assertions run **first and unmarked**, and the harness-integrity floor runs
**last** through a helper.

If the floor proves still unreliable after [D-22](#d-22) and the guard repair, wrap **only the
trailing helper call** in `try/except AssertionError: pytest.xfail(...)`. That cannot mask the
loss assertions, **because they have already run and passed by the time it executes.** The
fallback is bounded by construction rather than by discipline.

**Rejected.**

- *Split each test into two functions* — not implementable against this fixture graph, and
  running the experiment twice would double cluster time *and* mean the two tests asserted over
  different runs.
- *Per-assertion `xfail` via a custom marker* — pytest has no such thing; building one means
  catching `AssertionError` around selected lines, "which is the chosen decision wearing a
  decorator costume."
- *Leave [D-18](#d-18) in force and fix only the guard* — the guard bug and the masking are two
  separate defects; fixing the first does nothing about the second.

**Outcome — confirmed at runtime.** Both partition tests **PASSED outright** against a live
three-node cluster with a real `docker network disconnect` partition — not `XFAIL`, not skipped —
one bring-up per handling mode, with **zero code change** from the reviewed commits. The Critical
finding is discharged. The plan carries an explicit standing instruction: **never re-add a
decorator to the test function.**

---

<a id="d-22"></a>
## D-22 — The sampler race is closed by a passive wait

**Context.** [D-18](#d-18) named **two** causes of the floor's unreliability, not one: the guard
defect, and a real timing race. Fixing the guard addresses only the first. The second is
structural — the sampler walks cluster nodes **serially**, and a node that is timing out costs up
to 10 s of HTTP timeout plus 8 s of exec timeout. The first sample after a partition is taken
while the in-flight poll is still trying HTTP against a node that has just lost its published
ports, so it can land **roughly twenty seconds late**. The views window then may contain no
post-transition sample at all, and the floor assertions see `None`.

[D-21](#d-21) un-masks those assertions. Doing that without closing this race would convert a
masked flake into an unmasked one.

**Decision.** Add `await_sample_after(t, timeout_s=60.0)`, backed by a condition variable the
sampling loop notifies after each append. It blocks until a sample newer than `t` has landed, and
**raises on timeout rather than returning `None`**. The partition experiment calls it twice —
after partitioning and after healing — so the window read is guaranteed to contain a
post-transition, correctly-sourced sample.

**Rejected.**

- *Restructure sampling into per-node threads* — removes the underlying cause and would benefit
  every experiment, but it is the largest change in the branch, in the code every experiment
  reads, in a branch whose whole purpose is making measurements trustworthy again. The starvation
  bites on the **first** post-partition sample — exactly the one the passive wait waits for.
- *Force a synchronous sample from the test thread* — makes the sample list a two-writer
  structure requiring a lock, and injects samples at a cadence the timeline consumers do not
  expect.
- *Poll/retry inside the floor assertions* — "a retry loop wrapped around a read of a
  **historical, closed** time window is close to meaningless — retrying cannot change what was
  sampled between two past timestamps. It would make the assertions look robust without making
  them so."

**Outcome.** A recorder that has stopped sampling entirely — the precise failure the floor exists
to catch — now fails loudly at the wait with a message saying so, instead of degrading into an
ambiguous `None` twenty lines later. The residual cost: the experiments block for up to 90
seconds at two points, and the underlying serial-sampling starvation remains for any future
experiment that reads a short window without calling the wait.

---

<a id="d-23"></a>
## D-23 — The floor asserts sample source, not raw reachability

**Amends [D-21](#d-21).**

**Context.** [D-21](#d-21) relaxed the floor's last assertion to `reachable is not None`,
reasoning that after the guard repair a genuine `False` is a *successful* observation that the
node was unreachable. Reading the exec sampling path directly found **the premise does not
hold**: that path reaches a partitioned node **via the Docker daemon socket, not the
network**, so a successful exec call returns `reachable: True` regardless of whether the node is
actually partitioned. The partition never touches the exec path at all. Combined with
[D-22](#d-22) guaranteeing a sample lands, the assertion could only fail on **total sampling
failure** — not on the thing its own message claimed to test.

**Decision.** Assert `source == "exec"` instead. That proves the exec fallback genuinely engaged
for the target node during its own partition — the observation used the correct, partition-safe
sampling method — and it still correctly fails on total sampling failure, because the failure
path sets `source: None`.

**Rejected.**

- *Leave `reachable is not None`* — found near-tautological by direct code reading.
- *Assert on `online` excluding a majority peer* — more directly exercises split-brain semantics,
  but duplicates coverage another fix already has, and "conflates two distinct properties — 'did
  the harness observe *this* node' vs 'did *this* node correctly see the others' — into one
  check."

**Outcome.** Recorded with a forward-looking condition: if the exec sampling path is ever made
genuinely network-aware, `source == "exec"` would no longer be the strongest available signal and
this decision should be re-read.

---

<a id="d-24"></a>
## D-24 — Ship with the cluster gate deferred — *superseded by [D-25](#d-25)*

> **The diagnosis below is wrong. Its decision was right and is left standing.**

**Context (as believed at the time).** The gate — bring up the real cluster and prove the
Critical fix behaves correctly at runtime — failed reproducibly across two independent fresh
cluster bring-ups plus a single-node control. In every case RabbitMQ's Khepri metadata-store Raft
group got stuck in a log-replication loop, breaking MQTT connections and even pre-existing AMQP
traffic — a process the branch's ten commits never touch. Nothing in the diff modifies broker
configuration, the MQTT plugin, or any of the failing tests' files.

**Decision.** Stop attempting the gate. Report the branch as **implementation-complete but not
runtime-verified** for the two findings in question. Do not merge, do not tag, and do not
represent the un-masking work as having runtime proof behind it.

**Rejected.**

- *Keep debugging* — unbounded time against what presented as a host-level environment fault
  reproduced three times.
- *Revert the un-masking and re-apply `xfail` until the gate can run* — reopens the Critical
  finding the entire branch exists to close, and the standing instruction is never to re-add that
  decorator.
- *Merge anyway, treat static verification as sufficient* — "the Critical finding's entire point
  is that a partition genuinely losing messages must fail the suite — that is inherently a
  runtime property, and it has never been watched failing-then-passing against a real cluster.
  Silently merging misrepresents what was actually proven."

**Outcome.** The decision to refuse the merge and disclose the gap was correct. The causal
analysis was not, and sent the follow-up work hunting the wrong thing — see [D-25](#d-25).

---

<a id="d-25"></a>
## D-25 — The gate was blocked by compose file-set asymmetry, not the environment

**Supersedes [D-24](#d-24).**

**Context.** The real cause is in the harness and the plan text. The stack fixture unconditionally
runs `docker compose up -d --wait` with the file set derived from an environment variable. The
gate's step read `pytest tests/ -q` with **no `IOT_CLUSTER=1`**, so running it against a live
cluster issued `docker compose -f compose.yml up -d --wait`. Proven non-destructively with
`--dry-run` against a healthy 3/3 cluster:

```
Volume iot-messaging_rabbitmq-data Creating
warning msg="Found orphan containers ([iot-rabbitmq2 iot-rabbitmq3])"
Container iot-rabbitmq Recreate
```

Node 1 is recreated onto the **empty single-node volume** with the single-node config, while
nodes 2 and 3 keep running — they carry `restart: unless-stopped` and the single-node file set
does not know them — **still holding node 1 in their Khepri member list.** The same asymmetry
makes a bare `docker compose down -v` a non-teardown: it leaves three volumes and both peer
containers behind, so the *next* "fresh" bring-up is not fresh. That is why the earlier
single-node control also failed, and why those reproductions looked host-wide.

**Decision.** Record the asymmetry as the root cause. Fix it **in the plan text only** — every
gate step pins the environment variables, and teardown uses the full file set plus
`--remove-orphans`. Run the gate with **zero code change**, so the run proves the ten reviewed
commits and nothing else.

**Rejected.**

- *Add a profile-mismatch guard to the fixture first, then run the gate* — a genuine harness
  defect worth fixing, but "the run that is supposed to prove ten reviewed commits would also be
  exercising brand-new, unreviewed harness code." **Deferred, not rejected** — see
  [D-26](#d-26).
- *Amend [D-24](#d-24) in place* — its entire reasoning is written on top of the environment-fault
  premise; editing the premise out would leave a document whose reasoning no longer follows from
  its own text. "This is a whole-premise reversal, which is what supersession is for."
- *Accept the environment diagnosis and retry after a Docker restart* — would have reproduced the
  failure a fourth time, because the mechanism is re-triggered by the step's own command every
  run.

**Outcome — the gate passed with zero code change.** 59 passed on the default suite; both
partition modes passed outright across two bring-ups. The Critical finding is discharged at
runtime for the first time. A second plan-text defect surfaced along the way: the plan credited a
step with proving one finding via a test file that carried the wrong marker, so the selector
never selected it. **The harness still permits the mistake** — only plan text warns the next
operator — which is what [D-26](#d-26) closes.

---

<a id="d-26"></a>
## D-26 — The profile-mismatch guard ships inside Phase 4's plan

**Context.** [D-25](#d-25) filed the structural fix as a standalone branch. Investigating its cost
found **the same defect already in committed code, not just in plan prose**: the consumer fixture
hardcoded its compose file set instead of deriving it, and the consumer service's `depends_on`
means that command evaluates the broker service under the **single-node** file set — the exact
recreate [D-25](#d-25) proved destroys a cluster. It is reachable from four tests, **so the trap
fires from a fixture, not only from an operator's command line.**

Phase 4 was next and would add another compose overlay, multiplying the permutations the defect
lives in.

**Decision.** Scope both fixes into **Phase 4's plan**, as their own tasks **sequenced ahead of**
the segregation work. No separate spec, plan, or branch.

**Rejected.**

- *Standalone branch* — a second planning cycle for two small changes, and "the guard is better
  designed against Phase 4's real compose topology than against a guess at it."
- *Fold them in opportunistically, unplanned* — "harness guardrails compete with feature work for
  attention and lose. This follow-up has already survived one round as prose that did not
  prevent recurrence."
- *Leave it documented only* — "documentation already failed to prevent this. The defect is in
  code, so the fix belongs in code."

**Outcome — confirmed.** The consumer file set now derives from the shared function, and the
guard is wired into the stack fixture — proven against a live mismatched bring-up (the guard
fired, naming all three broker containers) and a matching one (silent).

---

# Part V — Multi-region segregation (Phase 4)

<a id="d-27"></a>
## D-27 — Devices select a region vhost by IP-bound listener, not by credential

**Context.** Devices speak MQTT only, and the broker config set a single global `mqtt.vhost = /`.
A per-connection vhost selection mechanism was required before any segregation existed at all.
The assignment asks for segmentation that is "both logical **and network-level**" — and a
mechanism leaving every region on one listener makes per-region Docker networks decorative.

**Decision.** One MQTT listener per region, **bound to that region's Docker network address**
(`172.28.1.10:1893` for `eu`, `172.28.2.10:1993` for `us`), with the port-to-vhost mapping shipped
declaratively in the definitions file's `global_parameters`. Port 1883 stays unmapped on vhost `/`
for the Phase 1–3 experiments and for host-side tooling, which selects a vhost with a colon-form
username.

**Rejected.**

- *Colon-form usernames alone, single port* — no extra listeners or static IPs, but every region
  shares one listener, so per-region networks prove nothing at the network layer and the entire
  segregation claim rests on credentials. **Retained as the host-tooling path, not the device
  path.**
- *Listeners on `0.0.0.0` plus the port mapping* — measured to matter: with IP binding a probe
  from one region network to the other's listener is **unreachable**; without it, it would
  connect.
- *Certificate CN → vhost mapping* — identity-bound rather than address-bound, and the strongest
  of the three, but requires per-device x.509 material that did not exist yet. That is Phase 5's
  deliverable.

**Evidence.**

| Probe | Result |
|---|---|
| `eu:device-eu` on 1883, no mapping set | connects to vhost `eu` |
| `{"1883":"eu"}` mapped, connect as a user permitted on `/` only | refused, MQTT reason code **135** |
| `mqtt.listeners.tcp.eu = 172.28.1.10:1893` | `rabbitmq-diagnostics listeners` reports `mqtt 172.28.1.10 1893` |
| Container on `region-eu` → own listener / other region's listener | reachable / **unreachable** |

**Outcome.** Four independent segregation layers stack: network → listener → vhost → topic key.
Wrong-region traffic fails at the earliest possible point, before authentication. All four were
later confirmed live, including the network-layer half from **inside** a real container rather
than from the host, and both denial reason codes with a live negative control (a cross-region
permission was granted, the test watched to fail, then revoked and watched to pass).

Two known costs: the region listener addresses are hardcoded in two files and must agree or a
listener silently binds nothing — pinned by a no-Docker drift test. And **1883 remains a
host-reachable path into any vhost by credential alone**, a deliberate backward-compatibility
trade-off; it is a credential-strength problem, addressed by Phase 5.

---

<a id="d-28"></a>
## D-28 — Isolation asserts on broker provenance, never the payload's own region

**Context.** Phase 1's payload carries `"region"` in the message body, and Telegraf turns it into
an InfluxDB tag. Every Phase 1–3 query uses it. But Phase 4's central claim is that a device
authorized for one region cannot get traffic into another — and if that claim were tested by
reading the `region` tag, **the test would be checking what the device said about itself.** A
device can publish to `region/eu/…` while the body claims `"region": "us"`; the broker neither
reads nor cares about the body. Such a test passes and proves nothing about enforcement.

**Decision.** Each per-region Telegraf input stamps a **static `region_src` tag naming the vhost
it consumes from**. Every isolation assertion reads `region_src`. The payload's `region` tag stays
for continuity and is explicitly **not** evidence of enforcement.

**Rejected.**

- *Assert on the payload's `region` tag* — "it would make the phase's headline deliverable
  unfalsifiable."
- *Assert only on broker-side denials, never on stored data* — denials are unambiguous, but prove
  nothing about the positive path. **A binding or port-mapping mistake routes traffic to the wrong
  region without triggering any denial at all**; that failure mode needs a data-side assertion.
- *One InfluxDB bucket per region* — storage-level tenancy would make cross-contamination
  structurally impossible, but breaks every Phase 1–3 query and dashboard. Segregation is a
  broker requirement here, not a storage one.

**Outcome.** The provenance of every stored point is recorded by a component the device cannot
influence: an input bound to one vhost with a credential that can read only that vhost. A
pleasant side effect — **disagreement between `region` and `region_src` becomes a signal in its
own right**: a device lying about itself while the broker routes it correctly, surfaced as a
dashboard panel. Confirmed live: both simulators published under distinct run IDs, and the
cross-tagged count was **exactly zero** in both directions.

---

<a id="d-29"></a>
## D-29 — One region definitions file, with a drift test paying for the duplication

**Context.** Phase 4 adds two vhosts' worth of users, permissions, queues, bindings, policies and
a global parameter. The obvious layout is a small region fragment dropped beside the existing
file — RabbitMQ's definitions path accepts a **directory**, which suggests exactly that. It also
has to work under two stack profiles, which naively is a four-file matrix.

**Both assumptions were probed rather than believed.**

**Decision.** One new file, complete and self-contained, mounted by the region overlay over the
base definitions path and used under **both** profiles. Its duplicated `/`-vhost objects are
pinned to the cluster definitions file by a **no-Docker drift test**.

**Rejected.**

- *A definitions directory holding a base file plus a region fragment* — **the broker refuses to
  boot.** With two JSON files in the directory: `failed validation`, `BOOT FAILED — Error during
  startup: {error,not_json}`. One file in the same directory boots normally, so **a directory is
  legal and merging is not.**
- *Separate region files per profile* — unnecessary. A single-node broker booted healthy from a
  file declaring `x-quorum-initial-group-size: 3`, reporting one Raft member: **the broker clamps
  the requested size to the available node count rather than failing.**
- *Generate the region file from the base file at build time* — introduces a build step into a
  project whose reproducibility claim is "clone and `docker compose up`", and a generated config
  is harder to read during an incident.

**Outcome.** The `/`-vhost objects now exist verbatim in three files, and an edit to any of them
must be made in all three or the drift test fails — **which is the intended behaviour, but it is
friction.** The drift test compares the full `/` slice (users, permissions, queues, bindings,
exchanges), so a mistake in the region file cannot silently change `/`'s behaviour and invalidate
Phase 1–3 results measured against it.

A second, quieter hazard is documented: **a changed region queue argument appears not to apply**,
because definitions import never modifies an existing queue and `skip_if_unchanged = true` — such
changes need a volume wipe. Policies and permissions **do** update on re-import.

---

<a id="d-30"></a>
## D-30 — Region consumers are read-only, so the definitions file owns the binding

**Context.** The first draft granted each region's Telegraf user `write` on its own queue, copied
from the existing consumer purely by symmetry. **That grant was never justified** — and it sits in
the one file whose entire purpose is demonstrating least privilege, so "the other user has it" is
not a good enough reason. Probing found the grant *is* load-bearing — but only because of a config
choice that is itself optional: `amqp_consumer` with `binding_key` set issues a `queue.bind`, and
AMQP requires `write` on the binding's destination queue.

**Decision.** Region consumers hold `configure: ^$` and `write: ^$` — **no write permission at
all** — and the region inputs omit `binding_key`. The binding is declared in the definitions file,
which owns the topology.

**Rejected.**

- *Keep `binding_key` and grant `write`* — "a write grant to a component that only ever reads, in
  the file that is meant to prove segregation." Self-healing bindings also mean a config typo
  **silently creates a new binding** rather than failing. Extends the project's existing
  "don't redeclare topology you don't own" rule to the binding itself.
- *Drop `binding_key` but keep the grant as headroom* — "an unearned permission whose
  justification is a hypothetical future edit." The failure it guards against is loud and
  immediate, not silent.
- *One shared consumer identity across both regions* — the isolation claim would then cover
  devices only, and would have to say so.

**Evidence.**

| Input config | Result |
|---|---|
| `binding_key = "region.eu.#"` | Telegraf refuses to start: `403 ACCESS_REFUSED - write access to queue 'telemetry.eu.q' … refused for user 'telegraf-eu'` |
| `binding_key` omitted | starts, drains the queue to zero, points land in InfluxDB tagged `region_src=eu` |

**Outcome.** The two settings are coupled, and a no-Docker test asserts **both halves together** —
`write: ^$` in the definitions and no `binding_key` in any region input — so restoring one for
symmetry with the base config fails loudly rather than producing a 403 that reads like a
credentials problem.

---

<a id="d-31"></a>
## D-31 — The region config carries no `cluster_formation` — *amended by [D-32](#d-32)*

**Context.** The plan instructed copying the cluster broker config verbatim and adding two
listener lines. That copy carries a three-node `classic_config` peer list, which has always worked
because the cluster profile brings up all three nodes at once. But the region overlay also runs
against the base compose file **alone** — a documented, first-class standalone profile — and under
that profile nodes 2 and 3 never exist.

The task was reported done against that literal instruction; the container was in fact stuck
`unhealthy`, **crash-looping roughly every 11 seconds and never finishing boot**.
The commit message additionally claimed a live listener verification had succeeded — it had not.
The failure was reproduced directly, then reproduced again after a full teardown with a fresh
volume, ruling out stale state. **A lone node running three-peer discovery had never been
exercised anywhere in this codebase before.**

**Decision.** The region broker config carries **no `cluster_formation.*` lines at all**, matching
the plain single-node config, which has none and boots standalone without issue.

**Rejected.**

- *Keep the three-node list, document that region-alone also needs the cluster overlay* — would
  silently break a documented, first-class profile.
- *Two separate region config files, one per profile* — reintroduces the multi-file duplication
  [D-29](#d-29) specifically avoided, and the mount-ordering rule would need a third target.
- *Raise the discovery retry limits so the lone node eventually gives up* — **not confirmed to
  terminate at all**; the observed failure was a crash-loop, not a bounded retry visible in the
  logs.

**Outcome — the flagged risk was realised, not avoided.** This decision's own Risks section
flagged an open question it could not close: whether nodes 2 and 3 would still discover and join
node 1 under the combined profile, since node 1's file would no longer list anyone. Its reasoning
was that "classic_config joins are driven by the joining node's own RPC call to the target,
independent of the target's own list" — **and it flagged that as unmeasured.** When it was finally
measured, the cluster **split into two**. See [D-32](#d-32).

---

<a id="d-32"></a>
## D-32 — Region+cluster discovery needs a bounded-retry three-node list

**Amends [D-31](#d-31).**

**Context.** Bringing up the combined profile produced two clusters: node 1 alone, and nodes 2 and
3 together. The boot logs settle why. Node 1 logs `Classic peer discovery backend: list of nodes
does not contain the local node []` — an **empty list**, so it never runs the join protocol and
never becomes visible to anyone's discovery step. Node 2 logs `Peer discovery: node
'rabbit@rabbit2' selected for auto-clustering` — not node 1 — confirming it never saw node 1 as a
candidate at all. **Reproduced identically on a second independent bring-up: deterministic, not a
race.**

**Decision.** Restore the full three-node peer list in the region config, and add bounded
`discovery_retry_limit` (3) and `discovery_retry_interval` (2000 ms) so a lone node exhausts its
retries and boots standalone instead of hanging — **the same outcome [D-31](#d-31) achieved by
deleting the list, now achieved by bounding it instead.**

This corrects [D-31](#d-31)'s "inbound-passive peer discovery" reasoning: **that is not how
`classic_config` behaves on RabbitMQ 4.3.4.** A node with an empty (or self-only) discovery list is
invisible to peers' discovery step regardless of its own reachability.

**Rejected.**

- *The self-only entry [D-31](#d-31) itself named as its fallback* — **also inert.** It clears the
  boot warning but the node still elects itself standalone and stays invisible. "Fixes the log's
  cosmetic warning, not the split cluster." Reasoned through before touching Docker, on the same
  evidence that identifies the actual fix.
- *A separate region-cluster config plus a sixth compose overlay* — reintroduces the duplication
  [D-29](#d-29) avoided and grows the file-ordering invariant another case.
- *Hand-remediate the split with `rabbitmqctl reset`/`join_cluster` after every bring-up* —
  considered **only to reject it explicitly**: "proves a test passes against a state no real
  bring-up produces; the report would then cite fabricated evidence."

**Evidence.**

| Probe | Result |
|---|---|
| Combined profile, no `cluster_formation.*` ([D-31](#d-31)'s state) | split cluster; node 1 logs `list of nodes does not contain the local node []` |
| Same, full three-node list + bounded retry | one cluster, all three `running_nodes`; both region queues report 3 Raft voters |
| Same fix, region-alone profile | reaches `healthy`; no hang — [D-31](#d-31)'s fix target still holds |
| Repeated on a second independent bring-up | identical |

**Outcome.** The two config files **converge back toward each other** — the opposite of
[D-31](#d-31)'s stated negative consequence. The retry values are honest about their provenance:
chosen from the bring-ups measured here, **not derived from any documented RabbitMQ
guarantee about discovery timing**. A slower host could push discovery past the window, which the
combined-profile test would catch immediately by asserting exact three-node quorum membership.

---

# Part VI — Security and authentication (Phase 5)

<a id="d-33"></a>
## D-33 — Security config rides in `conf.d`; the region combination needs a fifth file

**Context.** The region overlay documents itself as **ALWAYS LAST** in the file order because it
re-mounts both the broker config and the definitions file, and Compose resolves same-target mounts
last-one-wins. Phase 5 needs TLS listeners, cert-login settings and OAuth2 configuration in the
broker config — apparently the same file. **Two overlays cannot both be last**, so this looked like
the phase's first hard blocker.

[D-29](#d-29) established that the broker boot-fails when its *definitions directory* holds more
than one file. Whether that transfers to the config file — or whether `conf.d` behaves differently
— was an open question. Assuming either way would have been a [D-31](#d-31)-shaped mistake, so it
was measured.

**Decision.** Phase 5's broker configuration lives in `/etc/rabbitmq/conf.d/*.conf`, which RabbitMQ
**merges** with the main config rather than replacing it. The security overlay therefore mounts only
targets no other overlay touches, and **its position in the file order is unconstrained.** The
region TLS listeners additionally require a **fifth** compose file, appended only when both the
region and security flags are set.

**Rejected.**

- *Generate combined config variants per profile* — a file per combination, each a near-copy,
  each needing its own drift test.
- *`RABBITMQ_CONFIG_FILES`* — changes how the broker locates **every** config file, affecting
  profiles that currently work. Wider blast radius than the problem justifies.
- *Put the region TLS listeners in the shared security config (no fifth file)* — **does not
  work**: under base+security the region addresses do not exist, so the listeners fail to bind
  and the node does not boot.
- *Put them in the region overlay* — **also does not work**: under region-alone there is no
  `advanced.config`, so a TLS listener has no `ssl_options` and the broker dies at boot with
  `no_cert` (see [D-34](#d-34)).

**Evidence.** Measured with the project's **real, unmodified** config files, not a synthetic
stand-in:

| Probe | Result |
|---|---|
| Real base config + a `conf.d` file carrying TLS settings | Both applied — vhosts from the base file, the TLS listener and cert-login from `conf.d` |
| Boot log, same run | `Config file(s):` lists `advanced.config`, `rabbitmq.conf`, and the `conf.d` file together |
| `mqtt.listeners.ssl.<name> = <ip>:<port>` for two region addresses | Parses exactly like the plaintext form; both bind alongside the wildcard listener |
| A port-to-vhost mapping entry naming a port with no live listener | Harmless — which is what lets one definitions file serve both region profiles |

**"More than one file boot-fails" is a fact about the definitions directory, not about `conf.d`.
The two mechanisms are unrelated and were measured separately.**

**Outcome.** The mount collision that looked like the phase's hardest constraint **does not
exist**, and no Phase 1–4 compose file was touched. The cost: the file order grows to five for
the fullest profile, and broker configuration is now spread across up to four files — reading the
effective configuration means reading all of them, or `rabbitmqctl environment`.

One implementation-time correction to this decision's own "verified harmless" claim: adding TLS
ports to the region definitions was harmless **to the broker**, but silently broke a pre-existing
Phase 4 test's hardcoded expectation, and went uncaught for four tasks because no intermediate
task ran the complete default-profile suite. The broker-level claim held; the test-suite-level one
did not.

---

<a id="d-34"></a>
## D-34 — `advanced.config` owns the complete `ssl_options` block

**Context.** Revocation requires `crl_check` on the TLS listeners. Two facts about how RabbitMQ
4.3.4 handles that were measured, **and both are traps.**

First, `crl_check = peer` **with no cache configuration rejects every peer certificate, revoked or
not**, because the default CRL cache does not fetch over HTTP. A one-line `crl_check` added to the
main config looks like a hardening improvement and **silently breaks all mTLS**.

Second, `crl_cache` has no cuttlefish key at all, so it can only be set as an Erlang term in
`advanced.config`. The obvious minimal move — put *only* the two CRL keys there and leave the
certificate paths in the main config — **makes the broker refuse to boot.**

**Decision.** The **complete** `ssl_options` block lives in `advanced.config`: CA file,
certificate, key, `verify`, `fail_if_no_peer_cert`, `crl_check` and `crl_cache` together. No
`ssl_options.*` key exists anywhere else. `crl_check = peer` is **always** paired with the
HTTP-fetching cache; neither is ever configured without the other.

**Rejected.**

- *Split the settings across both files* — **does not work.** `advanced.config`'s `ssl_options`
  **replaces** the generated list rather than merging with it, so the certificate paths vanish and
  the broker dies with `{listen_error,{acceptor,...,8883},no_cert}`. "Measured, and it cost a boot
  failure to learn."
- *Skip the HTTP cache, preload a CRL file* — the default cache does not fetch, so a CRL update
  would not reach the broker without a restart, and "restarting the broker to revoke one device is
  not a revocation mechanism anyone would ship."
- *Omit `crl_check` and revoke by deleting the RabbitMQ user* — revokes the **account**, not the
  **certificate**. Rejected on its own merits in [D-35](#d-35).

**Evidence.**

| Probe | Result |
|---|---|
| `crl_check = peer` in the main config, no cache | **Every** certificate rejected: `{tls_alert,{bad_certificate,"… {bad_crls,no_relevant_crls}"}}` |
| `advanced.config` carrying only the two CRL keys | Boot fails: `{listen_error,…,no_cert}` — certificate paths gone |
| `advanced.config` carrying the complete block | Boots; `rabbitmqctl environment` shows all of `{crl_check,peer}`, `{fail_if_no_peer_cert,true}`, `{verify,verify_peer}` |
| A client certificate with **no** `crlDistributionPoints`, under `crl_check = peer` | Rejected with the same `{bad_crls,no_relevant_crls}`. **Every device cert must carry a CDP** |
| CRL republished after revoking a previously-accepted certificate | New connections refused `certificate_revoked` **~13 s later, no broker restart** — despite the cached CRL's `nextUpdate` being in 2036 |

**Outcome.** TLS configuration is in exactly one place, so the failure mode that cost a boot
failure during probing **cannot be reproduced by editing config**. The costs are real: TLS
settings are in Erlang term syntax rather than the project's otherwise-uniform style, and a syntax
error there is a boot failure rather than a validation message. And **`ssl_options` is node-wide,
so every TLS listener inherits the whole block** — which is the direct cause of [D-37](#d-37).

The CRL cache TTL is recorded as **uncharacterised** — only that propagation happened within
seconds. Every revocation test therefore **polls until refused** rather than sleeping a fixed
interval.

---

<a id="d-35"></a>
## D-35 — Revocation is certificate-scoped, and incomplete without a force-close

**Context.** The assignment asks to "demonstrate revoking a single compromised device's
certificate." Two questions sat under that sentence, neither answerable from documentation.

First, whether real CRL checking works on this stack at all. This was treated as **blocking** —
the specification was not written until it was measured, because a negative answer would have
changed the phase's shape.

Second, and less obvious: **what "revoking a certificate" actually accomplishes at runtime.** A
revocation that only affects future connections is a very different security property from one
that terminates the compromised device's current session — **and the two are indistinguishable in
any demo that does not deliberately hold a connection open across the revocation event.**

**Decision.** Use **real CRL checking**, not revoke-by-identity. Device certificates carry a
`crlDistributionPoints` extension pointing at a CRL service on the broker's networks.

Because CRL revocation was measured to gate **new TLS handshakes only**, the revocation procedure
is defined as **two steps, and the second is not optional**:

1. Revoke the certificate and republish the CRL.
2. **Force-close the compromised device's existing connection** via `DELETE /api/connections/:name`.

The demonstration, the report and the tests all treat step 2 as **part of** revocation, not as an
afterthought.

**Rejected.**

- *Revoke by deleting the RabbitMQ user* — revokes the **account**. Every device sharing that CN
  loses access simultaneously. The measurement below shows the two are distinguishable, "so
  presenting this as certificate revocation would be inaccurate."
- *Short-lived certificates, no revocation mechanism* — sidesteps the named deliverable and moves
  the compromise window from "until someone closes its connection" to "until its certificate
  expires."
- *CRL update only, no force-close* — **measured false.** A device whose certificate was revoked
  mid-connection ran its full hold and published successfully afterwards. "A demo stopping here
  would assert something untrue — exactly the misleading result this decision exists to prevent."

**Evidence.** Two certificates were deliberately issued with the **same CN and different serials**,
so certificate-scoped and identity-scoped revocation could be told apart:

| Probe | Result |
|---|---|
| Serial 1002, not revoked | CONNECT OK, PUBLISH OK |
| Serial 1001, **same CN**, revoked | `SERVER ALERT: Fatal - Certificate Revoked` |
| Previously-accepted cert, revoked and CRL republished, new connection | Refused ~13 s later, no broker restart |
| **Connection held open across the revocation of its own certificate** | `STILL CONNECTED after hold`, `PUBLISH-AFTER-HOLD OK` — ran the full 70 s and published *after* revocation |
| Concurrent new connection with that same revoked certificate | Refused at that same moment |
| Certificate with no CDP | Rejected `{bad_crls,no_relevant_crls}` |

**The same-CN pair is the entire point of the first two rows. Deleting the RabbitMQ user would
have stopped both.**

**Outcome.** Three costs are stated plainly rather than omitted. Every device certificate **must**
carry a CDP or it cannot connect at all — and the failure reads like a broken TLS setup rather
than a missing extension. A CRL service must be reachable from every network the broker serves TLS
on; if it is not, the broker holds no CRL and rejects **every** certificate — **a silent
total-failure mode**. And the force-close step filters by **username, which is the CN** — so it
closes every connection sharing that CN, not only the one whose certificate was revoked. **That
one step lands closer to the rejected "delete the user" alternative than the rest of this
decision's framing implies**, and the final review required it be said out loud.

---

<a id="d-36"></a>
## D-36 — Token expiry terminates live connections; a static-token consumer cannot recover

**Context.** The assignment scopes OAuth2 to *service* identity — the ingestion plane. Telegraf's
`amqp_consumer` takes a static password string **with no refresh path of any kind**. A JWT works
at connect time; what happens when it expires underneath a long-lived consumer was the open
question. Three outcomes were plausible and materially different: the broker ignores expiry on
established connections (the way CRL revocation does); the broker drops the connection and the
client reconnects; or the broker drops it and the client **cannot recover**. Only the third is an
operational gap worth reporting.

**Decision.** **Measure and document** the behaviour rather than engineering around it. The
committed realm gives service clients a long token lifespan (3600 s) so ingestion is stable in
normal operation, and a separate, deliberately short-lived client (60 s) exists **solely** to
capture the force-close and the permanent-refusal loop as a recorded finding.

This follows [D-06](#d-06)'s pattern exactly: **the gap is the deliverable.**

**Rejected.**

- *Build a token-refresh sidecar* — genuinely closes the gap, but "converts a clean measured
  finding into an engineering sub-project in the middle of the phase. The assignment asks for
  OAuth2 integration and a demonstration, not production token lifecycle management."
- *Keep Telegraf on internal credentials; put OAuth2 on a purpose-built consumer* — the
  ack-after-write consumer could implement refresh properly via AMQP's `connection.update-secret`.
  Rejected because the assignment names the ingestion plane as the service-identity surface —
  **but recorded as the right shape for a production answer**, and the report recommends it.
- *Set an effectively infinite lifespan and say nothing* — "hides the single most useful
  operational fact the phase discovered."

**Evidence.**

| Probe | Result |
|---|---|
| Keycloak service-account token as the AMQP password | Authenticates and is granted vhost access — but the identity is the bare `sub` UUID |
| Connection held open with a 60 s token | alive at t+20 s, t+40 s, then `Connection.Close(320, 'CONNECTION_FORCED - credential expired')` |
| Auto-reconnecting client holding the same static token after expiry | Refused indefinitely: `Provided JWT token has expired…`, `PLAIN login refused` |
| OAuth2 issuer set to an `http://` URL | **Broker refuses to boot** — "Key Server URL must be https". Keycloak must be served over TLS even in dev |
| `auth_backends.1 = internal`, `.2 = oauth2` | Plain-credential logins still succeed — **enabling OAuth2 does not disturb them** |

**The contrast with [D-35](#d-35) is the phase's central finding:**

| | New connections | Established connections |
|---|---|---|
| mTLS + CRL revocation | refused immediately | **survive indefinitely** |
| OAuth2 token expiry | refused | **forcibly terminated** |

**Outcome.** Two risks this decision flagged as unverified during design were both discharged live.
The identity-legibility setting **was** confirmed — broker logs read a human-readable service
account name, not the UUID. And the design's measurement used a proxy client rather than Telegraf
itself; running the real consumer confirmed the same behaviour, `CONNECTION_FORCED - credential
expired` at expiry followed by refusal on every reconnect.

The first pass at that confirmation was itself **a false negative**: the proxy held a bare
connection with nothing reading from it, so it never observed the broker's close frame — **the
same wrong-observation-layer defect class as [D-40](#d-40)**. Fixed by having the probe actively
use a channel each poll; the corrected measurement shows the connection dying at **60.1 s**,
almost exactly the token lifespan.

The deployment therefore ships with a **known, documented gap**, and a long token lifespan is
named as **itself a weakening** — a leaked service token stays valid for an hour — rather than
presented as a neutral configuration choice.

---

<a id="d-37"></a>
## D-37 — No AMQP TLS listener, because `ssl_options` is node-wide

**Context.** The phase splits its mechanisms the way the assignment words them: OAuth2 for service
identity, mTLS for device identity, **no connection carrying both.** Adding an AMQP TLS listener
looked like a natural completion. The question was whether it would interact with the mTLS
configuration — and it does. `ssl_options` is **node-wide**, and [D-34](#d-34) puts the complete
block, including `verify_peer`, `fail_if_no_peer_cert` and `crl_check`, in one file. **Every TLS
listener on the broker inherits all of it.**

**Decision.** Add **no** AMQP TLS listener. The ingestion plane's OAuth2 connection uses the
existing plaintext port. A test **asserts the listener is absent**, so the omission is deliberate
and visible rather than an oversight someone later "fixes" without reading this.

**Rejected.**

- *Add the listener and give Telegraf a client certificate* — it would then need a CDP-bearing
  certificate **in addition to** its token, because the inherited settings apply to it. "That is
  precisely the 'every connection carries both a cert and a token' shape this phase's first
  decision rejected — **arrived at by accident rather than choice**, from a setting written for a
  different listener."
- *Add it with a relaxed, separately-scoped `ssl_options`* — requires finding out whether
  per-listener scoping exists on this version and works alongside a node-wide block. **Unmeasured**,
  and would mean two TLS security postures on one broker.
- *Add it and drop `fail_if_no_peer_cert` node-wide* — **weakens the phase's actual deliverable**;
  a device could then connect without presenting a certificate at all. "Trades the deliverable for
  a non-goal."

**Evidence.** A listener was added to a throwaway probe stack **solely** to measure this:

| Probe | Result |
|---|---|
| An AMQP TLS listener alongside the MQTT TLS listeners, one node-wide `ssl_options` | Binds fine, alongside both region MQTT TLS listeners |
| `openssl s_client` to it with **no** client certificate | `tlsv13 alert certificate required` — **the listener inherited `fail_if_no_peer_cert`** |

**Outcome.** The mechanism split stays clean. The cost is stated plainly rather than omitted:
**the AMQP ingestion plane is authenticated but not encrypted** — the token and the telemetry it
consumes cross the internal Docker network in plaintext. Inside a single compose project that is a
bounded exposure, **but it is a real one**, and the phase does not deliver transport security end
to end.

---

<a id="d-38"></a>
## D-38 — No custom broker image — use bash `/dev/tcp` for in-container probes

**Context.** A boot gate needed to prove the broker can reach the CRL service over HTTP **from
inside** its own container. The plan's test shelled out to `wget`. The stock broker image has
**neither `wget` nor `curl` nor `python3` nor `nc`.**

A first fix added a custom Dockerfile and switched the broker service from `image:`
to `build:`. That violates a standing constraint and has a blast radius far wider than the phase:
the base compose file is shared by **every** profile, so every bring-up would need an apt fetch at
build time — and the cluster overlay's peer nodes would still reference the stock image, producing
**image skew across cluster nodes** that nothing in the task was scoped to fix.

**Decision.** **Never** add a custom Dockerfile or switch to `build:` to work around a missing
binary. Any in-container HTTP probe uses bash's `/dev/tcp` pseudo-device — a builtin, present in
the stock image — to open a raw TCP connection, write an HTTP/1.0 request, and read the response.

**Rejected.** The custom image, for the blast radius above.

**Evidence.**

| Probe | Result |
|---|---|
| `which wget; which curl; which python3; which nc` in the stock image | All empty |
| `bash -c "exec 3<>/dev/tcp/example.com/80 && echo TCP_OK"` | `TCP_OK` — the builtin works |
| Naive `test -s file` on a raw `/dev/tcp` GET response | **False-positives on a 404** — the response still has non-empty headers and body |
| Probe asserting the HTTP **status line** contains `200` | Correctly discriminates 200 from 404 |

**Outcome.** Raw HTTP in bash is more fragile than a real client and needed hardening across two
fix rounds: clear any stale response file before each attempt, wrap the whole connect-send-read
sequence in an outer timeout (a raw `/dev/tcp` connect has none of its own), and **assert on the
status line, not on non-empty output**. None of those were disqualifying; they became fix-round
findings, not blockers.

---

<a id="d-39"></a>
## D-39 — CONNACK 134 vs 135 — authentication failure vs authorization failure

**Context.** The plan's test asserted MQTT 5 CONNACK reason code `135` ("Not authorized") for a
certificate whose CN names no RabbitMQ user. Live testing showed the broker returns **`134`** ("Bad
User Name or Password") — confirmed by both the client error and the broker log line `access
refused for user 'nosuchuser' - invalid credentials`.

The design spec *does* measure `135` live — **but for a different scenario**: an existing user
connecting through the wrong region's listener. That user genuinely exists; it simply lacks
permission on the target vhost. The plan's `135` was an **unmeasured carry-over** from that
different scenario.

**Decision.** The broker distinguishes two CONNACK failure codes **by stage**, and this project's
tests must too:

- **134** — the CN-derived username resolves to **no account at all**. An **authentication-stage**
  failure: there is no credential to authenticate against.
- **135** — the CN-derived username names an **existing** user who lacks permission on the
  resource. An **authorization-stage** failure: the user authenticated, then was denied.

The negative-control constant is named `BAD_CREDENTIALS`, **not** `NOT_AUTHORIZED`, specifically so
a later test covering the genuinely-135 case cannot be misread as testing the same thing — or
accidentally "corrected" back to the wrong code.

**Rejected.**

- *Keep the plan's literal `135`* — contradicted by directly observed behaviour on the exact
  broker version this project pins. **The house rule is to trust a live measurement over an
  unmeasured assumption, even one that reads as plausible.**
- *Rename the constant to `NOT_AUTHORIZED` but set it to 134* — "the name would then lie about
  what the value means, actively misleading later work. The whole point of documenting
  this distinction is to prevent that exact confusion."

---

<a id="d-40"></a>
## D-40 — Revocation detection observes the raw TLS layer, not the MQTT client

**Context.** The plan's detection loop connected through the project's async MQTT client and
checked whether the raised exception's string contained `"REVOKED"`. **This can never work.** The
client library swallows the SSL alert entirely and raises a generic `MqttError: Operation timed
out`, with the exception chain **deliberately dropped** (`raise ... from None`, so both
`__cause__` and `__context__` are `None`) — even though the broker genuinely sends
`{tls_alert,{certificate_revoked,...}}`, confirmed in the broker logs on every attempt.

The task's own docstring already stated the right layer: "the refusal must be a TLS alert, not an
MQTT reason code — it happens during the handshake, **before any MQTT packet is exchanged**."
Going through an MQTT client to observe a pre-MQTT TLS event **was the wrong layer from the start**;
it is not a library bug to work around so much as evidence the test needed to reach one layer
lower.

A first attempt then reported `seconds_until_refused: 0.0` for two structurally different test
sequences — **a constant, not a measurement**, and a strong signal the probe might be vacuous.

**Decision.** Revocation *detection* uses a raw `ssl.SSLContext.wrap_socket()` probe against the
broker's TLS port directly, reading one byte to force the lazily-delivered alert, and asserting on
an `SSLError` containing `"REVOKED"`. This bypasses the MQTT client entirely for detection.

The *sibling-still-connects* claim keeps using the MQTT client, because that claim genuinely needs
a full connect and publish — **the asymmetry in the same test file is deliberate, not an
inconsistency.**

**Every claim generated by the raw probe is required to carry a negative control in committed,
reproducible test code** — not in a report or a docstring — "because a probe that always fails
immediately regardless of actual cert state is indistinguishable from a working one without one."

**Rejected.**

- *Assert on the MQTT library's internal logger output* — couples the test to a logger name and
  message wording that can change on upgrade with no compile-time signal.
- *Monkeypatch the client's exception handling to preserve the SSL error* — patches internals that
  are not a public contract, and **still routes a pre-MQTT TLS event through the MQTT stack, which
  is the wrong layer regardless of whether the patch works.**

**Evidence.**

| Probe | Result |
|---|---|
| MQTT client connect with a revoked certificate | `MqttError: Operation timed out`, both `__cause__` and `__context__` `None` — **no "REVOKED" text reaches the caller** |
| Broker log for the same attempt | `{tls_alert,{certificate_revoked,…}}` — the broker genuinely sends it |
| Raw `wrap_socket()` against a revoked cert, **no** read | Handshake succeeds with no exception — **the alert is not raised until a read is attempted** |
| Same probe, followed by `recv(1)` | `ssl.SSLError: [SSL: SSLV3_ALERT_CERTIFICATE_REVOKED]` |
| **Negative control:** same probe against a non-revoked cert | Handshake succeeds, `recv(1)` returns real server bytes — **proves the probe discriminates** |
| Pre-revoke connectivity check immediately before re-revoking | Succeeds — **proves the CRL cache reflects current, not stale, state** |

**Outcome.** Detection no longer depends on any library preserving SSL error detail. The negative
control is committed test code that will catch a regression if the probe ever starts matching the
wrong signal, and the pre-revoke check closes the vacuous-pass risk that [D-34](#d-34)'s
uncharacterised cache TTL made possible.

---

<a id="d-41"></a>
## D-41 — Force-close must poll for connection visibility in the management API

**Context.** [D-35](#d-35)'s force-close step is load-bearing. Its first implementation produced
`connections_closed: 0` and `died_on_force_close: false` — it found nothing to close. That was
treated as a **plan-mandated finding** (the defect is in the brief's own code) rather than
accepted as a genuine result, since it directly contradicts [D-35](#d-35)'s evidence.

Reproduced live, independent of the test: connect, then poll `GET /api/connections` at increasing
delays. **At ~0.1–0.2 s the endpoint returns zero results; at ~5–6 s it correctly returns the
connection.** RabbitMQ management's connections endpoint is backed by a **periodic
stats-collection interval, not a live query.** Since revocation propagation had already been
measured at ~0 s, by the time force-close ran, the target connection was a fraction of a second old
and **genuinely invisible to the stats collector, not absent from the broker.**

**Decision.** Force-close **polls**, filtered by user, retrying on a short interval up to a bounded
timeout before concluding there is nothing to close. It also **checks the DELETE response status**
before counting a connection as closed, rather than incrementing unconditionally.

**This is a general rule for this codebase**, not a fix scoped to one test: any code querying a
stats-backed management-API endpoint immediately after a connection event must poll rather than
single-query, or it will silently read a stale, connection-less snapshot.

**Rejected.**

- *Sleep a fixed delay* — "the stats interval was measured, not documented; a fixed sleep bakes in
  today's value with no margin and no adaptation. The same anti-pattern already rejected for
  CRL-propagation timing — a fixed sleep is a guess, not a measurement-backed wait."
- *Leave it as a single query and accept the data as unreliable* — "silently produces a false
  experimental result: a security-relevant claim about whether force-close terminates a connection
  would read as 'no' when the true answer is 'yes, but this test never actually tried.'"

**Evidence.**

| Delay after connect | `GET /api/connections` |
|---|---|
| ~1 s | `COUNT: 0` — only the pre-existing AMQP connection listed |
| ~6 s | `COUNT: 2` — the MQTT connection now present, correct user, correct protocol |

This also confirmed the filter predicate was already correct: **the defect was purely the query's
timing.** The two committed result JSONs bracket the fix — pre-fix `connections_closed: 0`,
post-fix `connections_closed: 1, died_on_force_close: true`.

---

# Part VII — Load testing (Phase 6)

<a id="d-42"></a>
## D-42 — The device swarm runs in containers, because the host cannot run it

**Context.** The assignment asks to "simulate thousands of concurrent virtual devices". Phases 1–5
ran their simulators from the host, five at a time, and it was reasonable to assume the same code
would scale.

**It does not.** The simulator sets a `select()`-based event loop policy on Windows, and CPython
builds Windows with `FD_SETSIZE = 512`. Ramping connections in one host process died at ~512:

```
live=500   (t=0.8s)
ValueError: too many file descriptors in select()
```

The failure is **process-fatal, not graceful degradation** — the probe's own final report line
never printed, because the exception killed the event loop. "Thousands of devices" is unreachable
from the host by roughly an order of magnitude.

**Decision.** The swarm runs as a **scaled Docker Compose service** (no `container_name`, scaled
with `--scale`), where asyncio uses `epoll` and no descriptor cap applies. Because `--scale` gives
every replica an **identical command line**, each replica **self-assigns** its device identity from
its container hostname rather than receiving a per-replica offset.

**Rejected.**

- *Run from the host with a proactor loop or a multi-process pool* — the alternative event-loop
  policy is incompatible with the MQTT client stack in this project (the policy line exists for
  that reason), and a process pool means new concurrency code with per-process failures hidden
  across many logs. "It rewrites working code to route around a platform limit that vanishes
  entirely inside a Linux container."
- *One container forking internal workers* — "`--scale` reaches the same scale for near-zero new
  code."
- *Pass each replica a `--device-offset` argument* — **impossible as specified**; every replica
  gets the same command line. "Container hostnames are already unique, measured distinct across
  three replicas, so the coordination problem can be **deleted** rather than solved."

**Outcome.** The swarm's ceiling becomes a property of the broker and the host's resources rather
than of a descriptor limit — which is what the experiment exists to measure. **Peak observed: 811
concurrent connections against the 512 host-process ceiling.**

Identity collision is prevented **by construction**: a fixed device prefix across replicas would
produce colliding MQTT client IDs, which the broker resolves by evicting the incumbent, turning
the reconnect loop into an **eviction storm that looks exactly like load**.

The cost: device names are no longer stable across runs, acceptable only because the load
experiments aggregate over devices and never address one by name. Phases 1–5 keep their stable
naming.

> **A correction is recorded in this entry's own history.** An earlier draft cited a
> re-measurement of 2,402 connections that was **never committed**; `git log --follow` showed no
> commit ever touched the result file after the relevant code fix. The **811** figure from the
> committed result JSON is the actual evidence, and it still clears the experiment's one hard
> claim.

---

<a id="d-43"></a>
## D-43 — Quorum queues have two overflow modes; the API accepts a third it ignores

**Context.** The assignment asks to test "drop-oldest vs reject-new vs dead-letter" — wording that
describes three modes. Measured with `x-max-length=5`, 20 AMQP messages, publisher confirms on, a
DLX attached:

| `x-overflow` | Publisher saw | Final depth | Dead-lettered |
|---|---|---|---|
| `drop-head` | 20 acked | 5 | 15 |
| `reject-publish` | 6 acked, 14 `Basic.Nack` | 6 | 0 |
| `reject-publish-dlx` | 20 acked | 5 | 15 |

**`reject-publish-dlx` is byte-for-byte identical to `drop-head`.** The broker logs `[warning]
Invalid overflow strategy <<"reject-publish-dlx">> for quorum queue` — **but the management API
returned HTTP 201 for it, and a subsequent `GET` echoed the value back in the queue's arguments.**

**Decision.** Document and test **two** overflow behaviours, not three. "Dead-letter" is not a
third mode — it is what `drop-head` does when a DLX is attached. Every overflow assertion verifies
**observed behaviour** (depths, dead-letter counts, publisher outcomes) and **never the
configuration read back from the API.**

**Rejected.**

- *Test all three as the assignment words them* — "one of the three does not exist. A test
  asserting the third mode's behaviour would pass while measuring `drop-head`, and would report a
  working feature that is not working. **It would put a false claim in the deliverable.**"
- *Trust the API's echoed configuration as the assertion* — "measured to be exactly wrong here:
  201 returned, argument echoed, broker ignored it. The echoed value proves only that the API
  stored a string."
- *Switch to classic queues, where the third mode is supported* — abandons quorum queues, which
  are the assignment's central design principle. "The queue type is not a free variable."

**Outcome.** Six tests reproduce the exact boundaries live with zero divergence from the design
probes. The assertion discipline — **observe, never echo** — proved load-bearing: had the tests
trusted the HTTP 201, they would have asserted the mode worked when it was silently ignored.

Two asymmetries matter for capacity planning: the **count** limit admits `maxlen+1` (depth settled
at 6 for a limit of 5) while the **bytes** limit engages exactly at the limit; and TTL expiry
**does** dead-letter on quorum queues, making it a usable overflow control. Since broker memory
scales with message count and payload size, **the bytes limit is the control that actually bounds
memory; the count limit is not.**

A forward-looking note: a later RabbitMQ version could implement the third mode, making the
assertion fail. **That is a genuine finding, not a broken test** — the standing instruction is to
record it and raise it, never to edit the assertion.

---

<a id="d-44"></a>
## D-44 — MQTT 5 is required for a device to observe broker backpressure

**Context.** `reject-publish` rejects a publish with AMQP's `Basic.Nack`, **which has no MQTT
equivalent.** What a device actually observes when the broker refuses its message was unknown — and
it decides whether "reject-new" is even testable from the device side. Measured against the same
broker, the same full queue, the same QoS-1 publish, read at the wire layer so reason codes are
visible:

```
{"proto":"3.1.1","waited_s":8.01,"puback":"NO PUBACK RECEIVED"}
{"proto":"5","waited_s":0.2,"puback":{"reason_code":"Quota exceeded","value":151,"is_failure":true}}
```

Under 3.1.1 the broker sends **nothing** — no PUBACK, no error, no disconnect — and a separate
probe confirmed **the withheld PUBACK never arrives, even 30 seconds after the queue is fully
drained.** That is silent message loss, not backpressure that later resolves.

**Decision.** The swarm speaks **MQTT 5 by default** and reads PUBACK reason codes at the wire
layer, counting `0x97 Quota exceeded` as a **rejection distinct from a disconnect**. The phase
reports "devices that must detect broker backpressure have to speak MQTT 5" as a derived
infrastructure requirement.

**Rejected.**

- *Keep the swarm on MQTT 3.1.1* — a rejected publish is indistinguishable from a successful one.
  "**The instrument would be blind to its own subject.**"
- *Infer rejections from queue depth* — infers a per-message outcome from an aggregate, and no
  depth source in this system is instantaneous. "It would measure the broker's state, not the
  device's experience — **and the device's experience is the infrastructure requirement.**"
- *Use the async client's publish result as the success signal* — **measured wrong**: the library
  reported the *rejected* MQTT 5 publish as a **successful** publish. It does not surface failure
  reason codes at all.

**Outcome.** Rejections are counted separately from reconnects, so an overflow policy no longer
looks like connection churn. Live: 248 rejections against 152 successes, **zero reconnects**.

The cost is reaching into a private client attribute, because the library exposes no public hook
for reason codes — **pinned by a unit test so a future rename fails loudly instead of silently
zeroing the rejection count.** One structural gap is disclosed rather than hidden: the swarm's
`timed_out` counter is always zero, because the client raises the same exception for both a
timeout and a disconnect and cannot distinguish them.

**This is the third instance of the library-hides-the-signal pattern in this project**, after
[D-39](#d-39) and [D-40](#d-40).

---

<a id="d-45"></a>
## D-45 — Memory pressure is measured in three arms

**Context.** The assignment says to "find where broker memory pressure begins under a prolonged
downstream outage", and Phase 2's Experiment A is described as "stop InfluxDB, observe the queue
growing". Phase 6 planned to repeat that at scale. Measured with Telegraf left running exactly as
every prior phase runs it, InfluxDB stopped, 100,000 messages published:

```
sent=50000   telemetry.q total=6341   unacked=50
sent=100000  telemetry.q total=7640   unacked=50
settle       telemetry.q total=0
dlq          100000
```

**`telemetry.q` drained to zero while InfluxDB was down**, with `unacked` pinned at exactly
`prefetch_count`. The `x-death` header on a sampled dead-lettered message reads `"reason":
"rejected", "count": 1` — **which rules out delivery-limit exhaustion** (that would read
`"delivery_limit"` with count 20). Telegraf rejects each message once when the output write fails,
and dead-letters it immediately.

**The queue that grows under a prolonged downstream outage is `dlq` — which carries no
`max-length`, no TTL, and no consumer.**

**Decision.** Measure memory pressure in **three deliberately separate arms**:

| Arm | Configuration | What grows | What it demonstrates |
|---|---|---|---|
| (a) | Telegraf stopped | `telemetry.q` | The clean per-message memory model |
| (b) | Telegraf running, InfluxDB down | `dlq` | What the default pipeline actually does |
| (c) | The [D-06](#d-06) ack-after-write consumer | `telemetry.q` unacked | Genuine store-and-forward |

`dlq`'s unboundedness is documented as a finding and recommended for a bound, but **not fixed in
this phase, because bounding it would change what arm (b) measures.**

**Rejected.**

- *One arm, matching the assignment's wording* — "the single arm it describes does not occur on the
  default pipeline. The experiment would report 'the queue did not grow' and miss that **a
  different, unbounded queue grew instead**."
- *Only arm (a)* — cleanest model, but "describes a configuration nobody deploys. A model with no
  realistic arm is not an infrastructure requirement."
- *Bound `dlq` first, then measure* — "changes the system mid-phase, so arm (b) would measure the
  fix rather than the behaviour that motivated it. **The finding would disappear into a config
  change.** Measure first, recommend, let the fix be its own decision."

**Outcome.** All three arms ran live. Arm (b) reproduced the finding at 50,000 messages. **Arm (c)
is the only arm in which the broker genuinely buffers on the device's behalf** — 19,950 ready plus
50 unacked (exactly `prefetch_count`) during the outage, draining to zero with nothing
dead-lettered after recovery. That makes "messages accumulate safely and drain when InfluxDB
returns" a statement about the system rather than about a dead-letter path.

**The phase surfaced an unbounded queue in the shipped topology that five phases did not notice.**

Arm (b) depends on Telegraf's reject-on-write-failure behaviour, which an upgrade could change —
so the test asserts the **`x-death` reason**, not merely that the dead-letter queue grew, and a
behavioural change fails loudly rather than passing for the wrong reason. Arm (a)'s divergences are
[D-48](#d-48).

---

<a id="d-46"></a>
## D-46 — Overflow policies are applied at runtime, never written into definitions

**Context.** The overflow matrix sweeps `max-length`, `max-length-bytes` and `message-ttl`, each
under two modes. The obvious home is the shared definitions file — **which is also the shape of
this project's most expensive recent defect.** Phase 5 added two ports to a shared definitions
file, verified the change harmless at the broker level, and **silently broke a pre-existing Phase 4
test's hardcoded expectation in the same moment**; nothing ran the complete suite for four more
tasks, so the break sat undetected.

Separately, a definitions file is loaded at broker boot, so a policy written there **cannot vary
across the cases of a single test run** — the matrix would need a broker restart per case.

**Decision.** Every policy is applied at runtime through the management API and removed in a
**fixture teardown that runs even when the test body fails.** No shared definitions file is
modified by this phase.

Because the management API is measured to accept configuration the broker ignores
([D-43](#d-43)), **a 2xx response is explicitly not treated as evidence that the policy took
effect**; the tests assert observed behaviour afterwards.

**Rejected.**

- *Add the policies to the shared definitions file* — "edits a file four phases of tests depend on
  — the exact shape of the defect above. Also needs a broker restart per matrix case."
- *A separate load-profile definitions file mounted by the overlay* — the region overlay already
  owns that mount target and is documented ALWAYS-LAST for that reason; a second definitions mount
  reopens the ordering question [D-29](#d-29) and [D-33](#d-33) settled. **Still needs a restart per
  case.**
- *Declare policies as queue arguments at declare time* — queue arguments are **immutable after
  declaration**, so each case needs its own queue — and the queue under test is the one the whole
  pipeline is bound to. "It measures a synthetic queue instead of the one under load."

**Outcome.** One test run sweeps the entire matrix with **zero broker restarts** and zero risk to
Phases 1–5. Teardown backed by a context manager — **not by discipline** — prevented any policy
leaking across the six cases. The accepted cost: the tested policies are not visible in the
committed topology, so this record and the phase report carry that information instead.

---

<a id="d-47"></a>
## D-47 — Overflow measurements pause Telegraf, because a second consumer splits the stream

**Context.** [D-43](#d-43) measured the overflow boundaries **exactly** — 20 messages into a
`max-length` 5 queue with `drop-head` retains exactly 5 and dead-letters exactly 15;
`reject-publish` acks exactly 6 and nacks exactly 14 — and the overflow tests encode those exact
numbers as assertions.

But Phase 6 also adds a **second** AMQP consumer on the same queue, deliberately, so a load-profile
row can carry both an ingest-time clock and a publish-time clock. The plan warned that two
consumers split the stream — **but only in the context of throughput accounting.** Nothing flagged
the same interaction for the overflow matrix: with Telegraf running, its consumer competes for
messages sitting in the queue, so an exact-depth assertion **is measuring a queue two consumers are
draining, not the overflow mechanism alone.** This was found live, while the tests were
failing non-deterministically against the exact boundary numbers.

**Decision.** Stop the Telegraf container for the duration of each overflow test and restart it
afterward — **via the codebase's existing shared Docker-control fixture** rather than a bare,
unguarded one, so a failed restart still gets that fixture's retry-and-swallow recovery instead of
leaving Telegraf down for the rest of the run. The result JSON records `"telegraf_stopped":
true`, so a reader knows the pipeline was modified for these measurements **without having to read
the test code to find out.**

**Rejected.**

- *Leave Telegraf running, accept approximate depth assertions* — "throws away the precision the
  design probes measured, on the task whose whole job is asserting that precision."
- *Route the latency consumer off the shared queue* — no interaction at all, but reopens an
  already-committed, already-reviewed design chosen so load and baseline rows share one dashboard
  panel. "A bigger change than this task's scope, for a problem only this experiment has."
- *Assert depth within a tolerance* — "a tolerance band hides exactly the thing [D-43](#d-43) exists
  to prove — that `drop-head` retains **precisely** `max-length`, not 'approximately'."

**Outcome.** Six of six tests pass deterministically, reproducing the boundary values exactly. The
cost is real: the run is slower, and Telegraf-dependent dashboard panels **go dark for the duration
of this experiment**, not only during the throughput experiment as the plan anticipated.

---

<a id="d-48"></a>
## D-48 — The memory model and disk-purge behaviour diverge from spec — recorded, not tuned

**Context.** The design fitted `broker memory ≈ N × (0.85 KB + payload_bytes)` on two short,
single-shot, **uninterrupted** publish bursts, predicting the pinned 256 MiB watermark would alarm
at **~184,000 messages**. It separately measured that a purge frees memory but **not** disk, and set
a teardown convention on that basis.

[D-45](#d-45)'s arm (a) published **500,000 messages** across 20 batches, with a pause between
every batch to read alarms and disk usage. Two divergences:

1. **No alarm ever fired — 2.7× past the predicted point.** Memory showed a repeating
   rise-then-drop roughly every 100,000 messages (~76 → 176 MB, resetting) while disk grew
   monotonically the entire time (13 → 238 MB). This is consistent with periodic Raft
   checkpoint/segment rollover reclaiming the queue's ETS index **between the measurement pauses —
   which the model's single uninterrupted burst never had the chance to observe.**
2. **Disk dropped from 238 MB to 7 MB after a purge** — the reverse of the design's claim. The
   original probe measured post-purge disk against a *different* queue's prior state, not a
   same-run before/after with an explicit settle, **so the two measurements are not proven to be
   testing the same thing.**

The queue types were explicitly confirmed as genuine quorum queues first, ruling out a queue-type
mismatch. Both divergences reproduced across a clean re-run.

> **A figure was corrected while consolidating this document.** An intermediate write-up of this
> decision recorded the post-purge disk as 11 MB. The committed result JSON records
> `disk_after_purge_mb: 7`, and the phase report cites 7. **The committed measurement governs**;
> 7 MB is the figure used here and in the consolidated technical report.

**The design's own methodology anticipates exactly this: "a divergence is reported as a finding
rather than tuned away."**

**Decision.** Record both in the result JSON. Soften the brief's hard disk assertion to a recorded
comment, since it no longer holds — **while keeping the assertion that does still hold as a real
assertion.** Add a `model_divergence_note` field to the same payload, disclosing the sawtooth
pattern and the model's miss, **so a reader of the result JSON alone understands what the number
does and does not mean.**

**Rejected.**

- *Retune the model constants to fit this run* — explicitly forbidden by the design's own
  methodology ("must not silently retune them to make the model look right"), and "a model refit to
  a paused, multi-batch run would misdescribe the *next* uninterrupted burst just as badly in the
  other direction."
- *Leave the hard assertion in place and let the arm fail every run* — "a permanently-red test
  either gets ignored, defeating the point of an assertion, or blocks every future rerun on a
  question that isn't a defect. **An assertion a live, correctly-configured run has falsified is
  testing the wrong thing, not catching a regression.**"
- *Investigate further before recording anything* — instrumenting the checkpoint interval and
  running a controlled uninterrupted comparison would confirm the hypothesis rather than leaving it
  plausible, but "the phase's philosophy is to record divergence, not to chase it to full
  explanation before reporting." **The checkpoint explanation is offered, not asserted as fact.**

**Outcome.** The result JSON is honest about what it measured: neither the predicted alarm point
nor the disk-retention claim held, and both facts are visible without cross-referencing this
document. The residual risk is named: **if a future reader takes the predicted alarm point as a
reliable forecast, they could mis-provision a real deployment's memory watermark** — mitigated by
keeping the divergence note in the same payload as the prediction, not in a separate document a
later reader might not open.

---

# Part VIII — Recurring patterns

Four shapes recurred often enough across six phases to be worth naming on their own. They are
more transferable than any individual decision.

### 1. A library can swallow the signal you are measuring

**Three independent instances**, all found by measurement rather than review:

- [D-39](#d-39) — the MQTT 5 CONNACK distinction between authentication (134) and authorization
  (135) failure was miscoded in the plan until read against the live broker.
- [D-40](#d-40) — the MQTT client swallowed the TLS `certificate_revoked` alert entirely, raising a
  generic timeout with the exception chain deliberately dropped.
- [D-44](#d-44) — the same client reported an MQTT 5 publish the broker had **rejected** with
  `0x97 Quota exceeded` as a **successful** publish.

A fourth near-instance appears inside [D-36](#d-36), where a probe holding a bare connection with
nothing reading from it never observed the broker's close frame — the same wrong-layer defect in a
different costume.

**Rule: instrument at the layer where the signal exists, not the layer that is most convenient.**

### 2. A configuration surface can report a success it did not deliver

[D-43](#d-43) is the sharpest case: the management API returns HTTP 201 and echoes back a queue
argument the broker logs a warning about and then ignores. [D-46](#d-46) generalises it into a
standing rule for every policy-driven test.

**Rule: assert observed behaviour, never the configuration read back.**

### 3. No depth source is instantaneous — including `rabbitmqctl`

The same instant read `depth: 5, api_depth: 0` on one probe and the reverse ordering on another; a
passive queue declare returned a message count of 0 against a queue holding 5. [D-41](#d-41) found
the connections endpoint lags roughly five seconds behind reality. [D-01](#d-01) found the peek
endpoint invisible to messages already claimed into a consumer's prefetch buffer. Telegraf's own
scrape adds ten seconds on top of the broker's stats interval.

**Rule: poll until stable; never single-read a gauge and never sleep a fixed guess instead.**

### 4. A specification's own transcribed text can be factually wrong, not just its framing

Four assignment-framing contradictions were found during Phase 6's design probes — the overflow
mode count ([D-43](#d-43)), the buffering premise ([D-45](#d-45)), the disk-reclamation claim and
the memory model ([D-48](#d-48)). A fifth was found **one level closer to the code**: an
implementation plan's own dashboard queries named a measurement and a field that had never existed
on the live stack. [D-31](#d-31) is the same shape at config level — a plan instructed a verbatim
copy that could not boot.

**Rule: probe the mechanism against a live stack before writing a specification about it.** That
rule is the reason the four contradictions above were found *before* they became numbers in a
report.

---

## Appendix — status at close

All forty-eight decisions are implemented. Four were superseded or amended by a later,
better-informed decision, and the superseded text is retained above because in each case the
original analysis remains the clearest account of the problem:

| Decision | Status |
|---|---|
| [D-18](#d-18) | Superseded by [D-21](#d-21) — defect analysis still accurate, decision replaced |
| [D-24](#d-24) | Superseded by [D-25](#d-25) — decision stands, causal analysis wrong |
| [D-04](#d-04) | Narrowed by [D-09](#d-09) — read the two together |
| [D-21](#d-21) | Amended by [D-23](#d-23) — one assertion refined |
| [D-31](#d-31) | Amended by [D-32](#d-32) — its own flagged risk was realised |

Every other decision is `accepted, implemented` with no outstanding correction.

The measurements these decisions produced are reported in
`main/docs/reports/Resilient-IoT-Messaging-Infrastructure-Technical-Report.docx`, with the per-phase detail in its sibling reports and
the raw evidence in `main/docs/results/`.
