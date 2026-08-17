# Phase 3 — Fault-Tolerance Report

## 1. Method and cluster configuration

`compose.cluster.yml` overlays `compose.yml` to turn the single-node Phase 1/2 stack
into a genuine three-node RabbitMQ cluster. Node 1 keeps the Phase 1 service name
(`rabbitmq`), ports, and DNS name (`rabbit1`), so every Phase 1/2 test and the
simulator's default endpoint resolve unchanged. Nodes 2 and 3 boot from fresh,
cluster-only volumes and import no definitions of their own.

| Node | Container | Volume | MQTT | AMQP | Management UI |
|---|---|---|---|---|---|
| 1 (`rabbit1`) | `iot-rabbitmq` | `rabbitmq-data-1` | 1883 | 5672 | http://localhost:15672 |
| 2 (`rabbit2`) | `iot-rabbitmq2` | `rabbitmq-data-2` | 1884 | 5673 | http://localhost:15673 |
| 3 (`rabbit3`) | `iot-rabbitmq3` | `rabbitmq-data-3` | 1885 | 5674 | http://localhost:15674 |

Partitions were induced by `docker network disconnect` on the target node's
`PARTITION_NETWORKS` membership (ADR-0014) and healed with the matching
`docker network connect`. This uses only the Docker CLI already wired into
`DockerControl` — no new image, no `NET_ADMIN`, no extra packages. A detached
container loses its published management-API port, so the minority side is
observed through `docker exec rabbitmq-diagnostics` / `rabbitmq-queues`, not HTTP
(`GaugeRecorder.expect_exec(node)`, armed the instant the partition is created).

**The cluster was verified real, not merely three containers.** After every fresh
bring-up, `docker exec iot-rabbitmq rabbitmq-queues -q --formatter json quorum_status
telemetry.q` must return three rows — one `leader`, two `follower`, all `voter` — before
any experiment is trusted. This check exists because of a finding from earlier in Phase 3
(design spec §2): on a stack built from the Phase 1 single-node data volume,
`telemetry.q` came up as a **one-member Raft group**, because a quorum queue's member
group is fixed when the queue is first declared and does not grow automatically when
more nodes join the cluster later. A follower- or leader-kill experiment run against a
one-member group would have "passed" every assertion — the queue's only member is
always trivially "the leader," so a kill would just look like an outage with no
re-election to measure — while actually testing nothing about the three-node topology
Phase 3 is supposed to be about. The fix was `x-quorum-initial-group-size: 3` declared
explicitly in a cluster-only definitions file plus a preflight test
(`test_cluster_preflight.py::test_quorum_queues_have_three_voting_members`) that fails
loudly, with the exact `rabbitmq-queues grow` remediation in its message, if formation
ever lands short.

The definitions-import-vs-cluster-formation race is non-deterministic per bring-up
(ADR-0013): it fired on 3 of 5 observed bring-ups across this project. This session's
Step 1 bring-up formed with three voters on the first
check — no `grow` was needed — but the preflight gate that would have caught and named
the remediation ran regardless, and passed.

All cluster experiments ran the Telegraf consumer arm only, at 5 simulated devices,
2 Hz, against `RABBITMQ_PARTITION_HANDLING=ignore` (F, G, H) or `pause_minority` (I, run
separately per §4 below).

## 2. F — Follower kill

Cited: `main/docs/results/F-follower-kill-expF22920.json`.

Prediction: zero loss, no client interruption — killing a non-leader replica of a
three-member Raft group should cost the cluster a replica, not availability.

Measured: node 2 (a follower) was killed and restarted. The queue leader never moved
(`leader_before` and `leader_after` both `rabbit@rabbit1`, `leader_held: true`). Telegraf
stayed connected throughout on node 3 (`telegraf_connected_node: 3`), never touching the
killed node. All three members were voters again after rejoin
(`members_after_rejoin: ["rabbit@rabbit1", "rabbit@rabbit2", "rabbit@rabbit3"]`),
`rejoin_elapsed_s: 0.0` — RabbitMQ's Ra library reforms fast enough on a healthy
localhost cluster that the harness's first poll after `start()` already sees the
restored member, not a bug in the measurement.

Sequence accounting: 850 messages published across 5 devices, 850 landed in InfluxDB
(`published_total: 850`, `influx_total: 850`), `gaps: {}`, `duplicate_total: 0`,
`verdict: "no-loss"`. The prediction held exactly: zero loss, and the client (Telegraf)
never had to reconnect because it was never attached to the node that died.

## 3. G — Leader kill

Cited: `main/docs/results/G-leader-kill-expG23613.json` and
`main/docs/results/T-telegraf-failover-tfo22639.json`.

Prediction (design spec §5.G): zero loss, a measurable stall while the election
completes, higher duplication than F. This is the sharper experiment — it actually
exercises Raft re-election, and it asks whether Telegraf's ack-on-receipt requeue
guarantee (established in Phase 2 as the reason every single-broker arm came out at
zero loss) survives a leadership change while messages are in flight.

**Telegraf shared the killed leader.** Node 1 — both the queue's leader before the kill
and the node Telegraf's `amqp_consumer` was connected to — was the one killed
(`killed_node: 1`, `telegraf_connected_node: 1`, `telegraf_shared_the_leader: true`).
This is deliberately the harder of the two cases the design called out: the client had
to both discover its broker was gone and pick up messages from wherever the queue's new
leader ended up, rather than continuing to read from an already-connected surviving node.

Leadership moved to node 2 — a real change, not a no-op (`leader_before:
"rabbit@rabbit1"`, `leader_after: "rabbit@rabbit2"`, `elected_node: 2`).
`time_to_elect_s: 0.0` and `rejoin_elapsed_s: 0.01` — both at the harness's polling
resolution, consistent with the same fast-reformation behaviour observed in F, and with
`T-telegraf-failover-tfo22639.json`'s independent measurement that Telegraf itself
reconnects to a surviving node in 7.15s after its broker is killed
(`connected_node_before: 1`, `connected_node_after: 3`, `failover_elapsed_s: 7.15`,
`verdict: "failed-over"`) — the prerequisite Task 8 established so that any stall seen
in G could be attributed to the election rather than to Telegraf's own reconnect
behaviour.

**No separate "client stall duration" field exists in the result JSON.** The two timing
figures the harness actually recorded are `time_to_elect_s` and `rejoin_elapsed_s`,
both 0.0/0.01 — the election completed within the harness's poll interval, too fast for
the recorded gauge timeline to isolate a distinct stall window as its own number. This
report does not manufacture one; the closest independently-measured proxy for
client-side interruption is T's `failover_elapsed_s: 7.15s`, which values a materially
different case (Telegraf's own broker killed, not the queue leader specifically, though
here they are the same node).

Sequence accounting: 850 published, 850 in InfluxDB, `gaps: {}`, `duplicate_total: 0`,
`verdict: "no-loss"`. `members_after_rejoin` restored to all three nodes. The loss
prediction was wrong in the safe direction: even in the harder case — Telegraf sharing
the killed leader — the requeue guarantee held across the election with zero
duplication recorded, not just zero loss.

## 4. H vs I — partition under `ignore` vs `pause_minority`

Cited: `main/docs/results/H-partition-ignore-expH-partition-ignore98906.json` and
`main/docs/results/I-partition-pause-minority-expI-partition-pause-minority217.json`.
These come from two separate stack bring-ups by design — switching
`RABBITMQ_PARTITION_HANDLING` requires a `.env` edit and a `--force-recreate` of all
three broker services, not a live toggle (ADR-0016).

**What was measured, independent of the harness assertion's pass/fail status** (see §6):

Both experiments partitioned node 3 away from the majority (nodes 1 and 2) for a 60s
split window, with the simulator publishing throughout.

*H (`ignore`):* the majority's HTTP-sourced view (`views_during_partition`, nodes 1
and 2) lists exactly `["rabbit@rabbit1", "rabbit@rabbit2"]` under `online` throughout
the split — the majority continued to see and report only itself, the leader unchanged
(`rabbit@rabbit1`) — while the minority (node 3) is unreadable over HTTP for the
duration, as expected for a detached container. Node 3's `docker exec` path, armed
during the partition, returned real command errors while detached — the timeline
records repeated `CalledProcessError: ... rabbitmq-diagnostics ... returned non-zero
exit status 64` across roughly a dozen samples spanning the split — consistent with
genuine network isolation rather than a probe that simply found nothing to report.
1000 messages published, 1000 landed in InfluxDB (`published_total: 1000, influx_total:
1000, gaps: {}, duplicate_total: 0, verdict: "no-loss"`), and the cluster reformed to
three members after healing (`members_after_heal: ["rabbit@rabbit1", "rabbit@rabbit2",
"rabbit@rabbit3"]`).

*I (`pause_minority`):* `views_during_partition` is null for all three nodes in this
run — the harness's probe window did not land a readable sample during the split for
any node, which this report records as a probe-timing gap, not as evidence about what
the minority was actually doing (see §6). What the timeline does show is a moment,
shortly after the split began, where all three nodes were simultaneously unreachable to
the harness for one polling cycle (`ReadTimeout` on nodes 1 and 2's HTTP paths,
`TimeoutExpired` on all three `docker exec` paths), followed by a roughly 46-second gap
before the next successful sample — a different signature from H, where the majority's
2-second polling cadence continued without interruption throughout the split. This
report does not conclude the majority was actually unavailable during that gap: the
recorder polls all three nodes sequentially in one loop, so a slow or hanging probe
against the paused minority node could delay the same cycle's read of nodes 1 and 2
without either of them having actually stopped serving traffic. This ambiguity is a
methodology limitation of the probe, not a measured majority outage (see §6). Despite
the gap in `views_during_partition`, the outcome fields are unambiguous: 1000 published,
1000 in InfluxDB, `gaps: {}, duplicate_total: 0, verdict: "no-loss"`, and
`members_after_heal` again lists all three nodes as voters.

**Both modes measured zero end-to-end loss and identical clean reformation.** The
evidence available to distinguish them is therefore observability during the split, not
a loss difference: H's majority-side view was continuously confirmed reachable and
reporting for the full 60s window, with the minority's isolation independently
confirmed by real `docker exec` command failures. I's minority-side behaviour during the
split was not captured by this run's probe at all — the `pause_minority` mechanism's
specific claim (the minority stops serving rather than continuing to answer) was not
directly observed here, only inferred from the timeout signature described above.

*Harness caveat, not a correctness caveat:* `test_partition_under_ignore`'s own
integrity-floor assertions are unreliable — see §6 for why. The specific defect
described there is in the `reachable` key's guard logic, but `_views()` routes every
key (`online`, `members`, `leader`, `telemetry_ready`, and `reachable` itself) through
the same window-scoped `node_window()` call, so any of that test's floor assertions can
come back `None` when the probe simply misses the sampling window for a given node —
not only the one built specifically on `reachable`. This does not affect the
correctness of the no-loss/reformation outcome reported above, which is independently
confirmed by the InfluxDB sequence accounting in both files and, for H, by the raw
`docker exec` error timeline showing genuine isolation.

## 5. Recommendation

**`pause_minority` is the recommended mode for production**, but the measurements in
this report only partially make that case, and this section says plainly which part is
measured and which part is not.

What was measured: both modes reached an identical safe outcome in this cluster's
60-second split — zero end-to-end message loss (`gaps: {}` in both H and I),
`published_total == influx_total` in both, and a clean return to three voting members
after healing in both. Quorum queues commit writes via Raft consensus regardless of
`cluster_partition_handling`; a minority of one node out of three cannot reach quorum on
its own, so this deployment's loss-safety in a split does not actually depend on which
partition-handling mode is set. `partition_handling` is not, on this evidence, a
data-safety lever for a 3-node quorum-queue topology — both arms are equally safe on
that axis, and this report does not claim otherwise.

Where the modes differ is availability behaviour during the split, and here the
evidence is asymmetric. H's `ignore` run gives a clean, fully-observed picture: the
majority answered every 2-second poll throughout the 60s window, and the minority,
though unreachable over HTTP as expected, still had its `docker exec` probe path
confirmed live enough to report real isolation errors rather than going completely
silent. I's `pause_minority` run does not give an equally clean picture — the probe
captured no readable sample from any node for a stretch of the split (§4), so this
report cannot point to a directly-observed instance of the minority actually pausing
its own service in this run's data.

The recommendation therefore rests on the documented mechanism `pause_minority` is
built to provide (a node that determines it is in the minority stops serving rather
than continuing to answer with a view of the cluster that a human or client could
mistake for current), combined with the one thing this report *can* say from its own
data: nothing measured here makes `ignore` a better choice for this deployment. `ignore`
scored no better on loss (both zero), no better on reformation time (both returned
cleanly to three voters), and its only observed advantage — the minority remaining
reachable enough to answer diagnostics during the split — is not a property a telemetry
pipeline with no minority-side client traffic needs. Given a genuine tie on every
outcome this report could measure, the mode whose designed behaviour is to fail closed
rather than fail open is the more conservative default, and that is `pause_minority`.
A future session with a probe that reliably samples the minority mid-split under
`pause_minority` (fixing the gap noted in §4 and §6) would be able to turn "recommended
on mechanism, tied on this report's measurements" into a fully measured claim.

## 6. Limits

- **Single-host Docker Desktop.** All three nodes ran as containers on one machine.
  Findings about cross-node coordination speed (`time_to_elect_s`, `rejoin_elapsed_s`)
  reflect localhost network conditions, not a real multi-host deployment's latency.
- **Partitions were induced by container network detachment, not packet loss.** `docker
  network disconnect` is a hard, instantaneous cut — there was no way to test latency
  degradation, asymmetric partitions (A can reach B but not vice versa), or a
  partition that heals gradually. Real-world network partitions are frequently messier
  than this.
- **Three nodes only.** Every finding about quorum behaviour, majority/minority split,
  and re-election is specific to a 3-node Raft group. Larger clusters change the
  majority arithmetic and were out of scope.
- **Telegraf was the sole consumer arm tested in Phase 3's fault-tolerance experiments**,
  per ADR-0011: Phase 2 established that the ack-after-write consumer and Telegraf
  converge on identical behaviour for the failure modes tested there, so re-running the
  consumer arm through the cluster experiments would have doubled runtime for a
  contrast that has not materialised. Design spec §5.G names the condition that would
  reopen this decision: if Experiment G had shown loss, the ack-after-write consumer
  would become worth re-running as a contrast arm, because that would be the first
  divergence ADR-0011 did not find. G measured `verdict: "no-loss"` with zero
  duplication even in the harder Telegraf-shares-the-leader case, so that condition was
  not met and the consumer arm remains out of scope for this phase.
- **No memory-pressure alarms were observed in any experiment.** Every sampled gauge
  reading across F, G, H, and I recorded `alarms: {"mem": false, "disk": false}`
  throughout, including during the partition windows and the post-heal drain. This
  cluster's fault-tolerance behaviour was not confounded by resource pressure in any
  run cited in this report.
- **`GaugeRecorder.node_window()`'s guard logic cannot report a genuine `False` for
  the `reachable` key itself.** The method (`main/tests/experiments/conftest.py:416-432`,
  written in Task 4, out of scope for this task to touch) is:
  ```python
  entry = sample.get("nodes", {}).get(node, {})
  if entry.get("reachable") and key in entry:
      return entry[key]
  ```
  When `key == "reachable"`, this guard only returns a value when `entry["reachable"]`
  is already truthy — a genuinely unreachable sample (`reachable: false`, or `null`
  when the probe caught nothing) fails the guard and the loop simply continues to the
  next sample, eventually returning `None` rather than `False`. So `node_window(node,
  "reachable", ...)` can only ever come back `True` or `None`, never a real `False`,
  regardless of what actually happened on the wire. Combined with a real timing race
  between when the partition is created and when the recorder's next poll lands, this
  makes `test_partition_under_ignore`'s own integrity-floor assertion
  (`assert during[target]["reachable"]`) an unreliable pass/fail signal — it can fail on
  a run where the minority genuinely was unreachable (because the sample the assertion
  reads happened to be a `None`, not a `False`), and it says nothing trustworthy either
  way. This is a harness defect, not a system defect: the actual measured outcomes for
  H and I — zero message loss, correct 3-member reformation after healing — are
  independently verified through InfluxDB sequence accounting (`published_total ==
  influx_total`, `gaps == {}` in both result files) and, for H, the raw `docker exec`
  error timeline described in §4, neither of which depends on `node_window` at all.
