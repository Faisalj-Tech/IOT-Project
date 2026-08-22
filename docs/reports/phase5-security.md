# Phase 5 — Security Report

## 1. What was built

Phase 5 adds two independent identity mechanisms to the broker built by Phases 1–4,
scoped exactly as the assignment words them: mTLS for **device** identity (MQTT
publishers), OAuth2 for **service** identity (the Telegraf ingestion plane). No
connection is asked to carry both.

**Device identity (mTLS).** Every MQTT device certificate is issued by a dev CA
(`scripts/make_certs.py`) with a `crlDistributionPoints` extension baked in — without
one, `crl_check = peer` rejects the certificate outright with `{bad_crls,no_relevant_crls}`
(ADR-0034). A new TLS listener on `8883` (all interfaces, vhost `/`) and two region-bound
TLS listeners on `9883`/`9993` (`172.28.1.10`, `172.28.2.10`, vhosts `eu`/`us`) resolve
identity from the certificate's Common Name via `ssl_cert_login_from = common_name` — the
CN becomes the RabbitMQ username, with no password exchanged. `advanced.config` carries
the **complete** `ssl_options` block — CA file, server cert/key, `verify_peer`,
`fail_if_no_peer_cert`, `crl_check = peer`, and an HTTP-fetching CRL cache — because
`advanced.config` *replaces* rather than merges with `rabbitmq.conf`'s TLS settings
(ADR-0034). An nginx sidecar (`crl`) serves the CRL over HTTP on the `core` network.

**Revocation.** Revoking one device's certificate is a two-step procedure, not one:
republish the CRL (gates new TLS handshakes within seconds, no broker restart), then
force-close the compromised device's existing connection via the management API's
`DELETE /api/connections/:name` (ADR-0035). Skipping step 2 is not revocation — a
connection held open across a CRL update keeps working indefinitely; see §3, S3/S4.

**Service identity (OAuth2).** RabbitMQ's auth backend chain is `internal` first,
`rabbit_auth_backend_oauth2` second (`config/rabbitmq/conf.d/20-oauth2.conf`) — every
Phase 1–4 plain-credential and certificate-CN path resolves through `internal` exactly as
before; OAuth2 is additive, not a replacement. A Keycloak realm (`iot`, imported via
`--import-realm` over HTTPS — RabbitMQ refuses to boot against a plaintext OIDC issuer)
issues JWTs to two service-account clients, `telegraf-eu` and a deliberately short-lived
`telegraf-eu-short`, both scoped to vhost `eu` via a custom `rmq_scopes` claim. The token
itself is the AMQP password; the username is ignored.

**Token expiry.** Unlike certificate revocation, an expired OAuth2 token **forcibly
terminates** the connection carrying it (`CONNECTION_FORCED - credential expired`), and a
static-token consumer — Telegraf's `amqp_consumer` input has no refresh path — can never
reconnect afterward (ADR-0036). This is the phase's central contrast; see §3, S6.

**No AMQP TLS.** `ssl_options` is node-wide, so an AMQP TLS listener would inherit
`fail_if_no_peer_cert` and `crl_check` from the MQTT configuration and force Telegraf into
mTLS *in addition to* OAuth2 — accidentally, from a setting written for a different
listener. Phase 5 deliberately does not add one (ADR-0037); Telegraf's OAuth2 connection
stays on plaintext `5672`.

**The phase's headline finding** is the contrast between the two mechanisms on identical
established connections:

| | New connections | Established connections |
|---|---|---|
| mTLS + CRL revocation | refused immediately | **survive indefinitely** |
| OAuth2 token expiry | refused | **forcibly terminated** |

## 2. The identity model

| Identity | Mechanism | Resolves to | Scope |
|---|---|---|---|
| Device (MQTT) | Client certificate CN, no password | RabbitMQ user named by the CN | vhost `/` on `8883`; vhost `eu`/`us` on `9883`/`9993` |
| Service (Telegraf, OAuth2) | JWT bearer token as AMQP password | Service-account identity, legible via `preferred_username_claims` | vhost `eu`, read-only on `telemetry.eu.q` + `amq.topic` (`rmq_scopes` claim) |
| Everything else (Phases 1–4) | Plain username/password | RabbitMQ user from `definitions.json`/`definitions.region.json` | Unchanged — `auth_backends.1 = internal` resolves these first |

`auth_oauth2.additional_scopes_key = rmq_scopes` carries the RabbitMQ permission scopes
in a claim of their own, separate from Keycloak's own `scope` claim machinery.
`auth_oauth2.preferred_username_claims.1 = preferred_username` makes the identity legible
in logs and the management UI as `service-account-telegraf-eu` rather than the service
account's bare `sub` UUID — verified live (§3, S5), not assumed.

Both OAuth2 clients are scoped to vhost `eu` only, matching the region model Phase 4
established: `telegraf-eu`'s whole identity only means anything inside that model, so its
own tests run under `IOT_REGION=1 IOT_SECURITY=1`, alongside Phase 4's region+security
combination (`compose.region-security.yml`, added by Task 8 for the region TLS listeners).

## 3. Evidence

All output below was captured live against `rabbitmq:4.3.4-management` and
`quay.io/keycloak/keycloak:26.4`, this session (2026-08-21/22), on branch
`phase-5-security`, at the branch's final commit (`9880272`).

### S1 — certificate identity, unknown CN refused

`test_security_identity.py`: a certificate whose CN names no RabbitMQ user at all is
refused at the **authentication** stage, MQTT5 CONNACK **134** ("Bad user name or
password") — not 135 ("not authorized"), which is a different, authorization-stage
failure this project's Task 4 discovered was originally miscoded in the plan (ADR-0039).
Broker log:

```
access refused for user 'nosuchuser' - invalid credentials
```

Positive controls (`device-a`, `device-b` connecting with their own CN) both pass.

### S2 — Phase 4's vhost/topic layers hold under certificate identity

`test_security_identity.py`'s region tests: a certificate correctly named for its own
region publishes clean; the same certificate on the *other* region's TLS listener is
refused **135** ("not authorized" — an existing user denied vhost access, genuinely
distinct from S1's 134); a same-vhost certificate publishing under the wrong region's
routing key is refused **128** at PUBACK, one layer deeper. Broker log:

```
MQTT topic access refused: write access to topic 'region.us...' ... refused for user 'device-eu'
access refused for user 'device-us' to vhost 'eu'
```

Phase 4's vhost and topic-permission boundaries hold identically whether the connecting
identity is a password or a certificate CN.

### S3 — revocation is certificate-scoped

Two certificates were issued with the **same** CN (`device`) and different serials, so
identity-scoped and certificate-scoped revocation could be told apart. `sibling_still_
connects: true` after the other's revocation is the entire point — deleting the RabbitMQ
user `device` would have stopped both.

```json
{
  "revoked_cert": "device-a", "sibling_cert": "device-b",
  "shared_common_name": "device",
  "seconds_until_refused": 0.0,
  "sibling_still_connects": true
}
```

Detection had to move to a raw `ssl.SSLContext.wrap_socket()` probe — aiomqtt/paho swallow
the TLS `certificate_revoked` alert entirely, raising a generic timeout with no exception
chain (ADR-0040). `seconds_until_refused: 0.0` is a genuine, negative-control-verified
measurement (a non-revoked certificate's identical probe runs to its full timeout and
raises `AssertionError`, proving the probe discriminates rather than firing spuriously),
not an artifact of a broken check.

### S4 — revocation spares established connections

```json
{
  "cert": "device-b", "common_name": "device",
  "published_after_revocation": true,
  "connections_closed": 1,
  "died_on_force_close": true,
  "conclusion": "CRL revocation gates new TLS handshakes only; an established connection keeps publishing until it is explicitly closed."
}
```

A connection held open across its own certificate's revocation keeps publishing
successfully — CRL revocation gates the TLS **handshake**, not an in-progress session.
The force-close step (`DELETE /api/connections/:name`) is what actually terminates it;
without that step, revocation would be incomplete (ADR-0035). The force-close
implementation itself needed a poll/retry loop: RabbitMQ management's `/api/connections`
is backed by a ~5-second stats-collection interval, not a live query, so querying it
immediately after a near-instant revocation finds nothing to close (ADR-0041).

### S5 — OAuth2 service identity authenticates and is legible

```
user 'service-account-telegraf-eu' authenticated and granted access to vhost 'eu'
```

A Keycloak-issued JWT authenticates an AMQP connection as the `telegraf-eu` service
account, scoped to vhost `eu` (matching its `rmq_scopes` claim — neither client is scoped
to vhost `/`, so these tests run under the region+security profile). The identity in logs
reads as `service-account-telegraf-eu`, not a bare UUID — `preferred_username_claims`
took, confirmed rather than assumed (this was explicitly unverified during design). A
garbage token is refused; plain-credential logins (`device`/`telegraf`/`admin`) are
unaffected by the OAuth2 backend's presence, because `auth_backends.1 = internal` is
checked first.

### S6 — token expiry kills live connections, confirmed on Telegraf

```json
{
  "client": "telegraf-eu-short", "token_lifespan_s": 60,
  "forced_close_seen": true, "survived_seconds": 60.1,
  "close_reason": "credential expired",
  "reconnect_attempts_refused": 3,
  "telegraf_confirmed": true,
  "conclusion": "RabbitMQ force-closes a live connection at token expiry and refuses every reconnect carrying the same static token. A consumer with no refresh path stops permanently."
}
```

A connection held open with a 60-second token survives exactly to its expiry boundary
(60.1s), then is force-closed — the design's original measurement used aio-pika's
`connect_robust` as a proxy for Telegraf; this task ran **Telegraf itself** and confirmed
the same behavior on the real consumer:

```
[inputs.amqp_consumer] Connection closed: Exception (320) Reason: "CONNECTION_FORCED - credential expired"; trying to reconnect
[inputs.amqp_consumer] Error connecting to "amqp://rabbitmq:5672/eu": Exception (403) Reason: "username or password not allowed"
```

Telegraf's `amqp_consumer` retries indefinitely and is refused every time — it has no
mechanism to fetch a fresh token, so ingestion stops permanently until the container is
restarted with a new one.

### S7 — no regression in Phases 1–4

Six regression tests (`test_security_regression.py`) confirm the security overlay
**adds** without closing anything: plaintext `1883` still accepts a credential-based
publish; `device`/`telegraf`/`admin` plain-credential logins survive the OAuth2 backend's
presence; the AMQP plaintext listener (`5672`) is unchanged; no AMQP TLS listener exists
(`rabbitmq-diagnostics listeners` shows no `amqp/ssl` entry, confirming ADR-0037). The
full suite passes clean on both security profiles:

```
base+security:         20/27 security-marked tests, 7 correctly skipped (region-only)
base+region+security:  27/27 security-marked tests
default profile:       104/104 (full suite, no previously-passing test deselected)
```

The default-profile run surfaced one real, unrelated regression along the way: an earlier
task (Task 8, adding the region TLS listeners) had added two ports to
`definitions.region.json`'s `mqtt_port_to_vhost_mapping` without updating a Phase 4
consistency test's hardcoded expectation. Fixed by widening the test's expected mapping
and correctly splitting its file-consistency check across the two config files the
plaintext and TLS ports actually live in — the stale assertion was the defect, not the
Task 8 change itself, which the plan's Global Constraints explicitly sanctioned.

## 4. What is enforced where

| Layer | Mechanism | What a disallowed client sees |
|---|---|---|
| Device authentication | Certificate CN, no RabbitMQ user matching it | MQTT CONNACK **134**, "Bad user name or password" |
| Device authorization (vhost) | Certificate CN names a real user, wrong region's listener | MQTT CONNACK **135**, "Not authorized" |
| Device authorization (topic) | Per-region topic permission on `amq.topic` | MQTT PUBACK **128**, broker logs "MQTT topic access refused" |
| Certificate revocation | CRL check on the TLS handshake | New connections: TLS alert `certificate_revoked`. Established connections: **unaffected** until force-closed |
| Service authentication | OAuth2 bearer token as AMQP password | Garbage/expired token: `PLAIN login refused` |
| Token expiry | Broker-side JWT expiry check on live connections | `Connection.Close(reply_code=320, "CONNECTION_FORCED - credential expired")`, then permanent refusal on reconnect |
| Plain credentials (Phases 1–4) | `auth_backends.1 = internal`, checked first | Unaffected by any Phase 5 mechanism |

## 5. Known limits

- **`8883` is host-reachable and resolves to vhost `/`** — the TLS counterpart of Phase
  4's plaintext `1883` fallback. Phase 4's Recommendation asked for the credential-only
  path to be closed; certificates close the *credential* weakness (a leaked password no
  longer suffices), but the fallback listener itself is retained deliberately, matching
  1883's continued existence.
- **No AMQP TLS** (ADR-0037). `ssl_options` is node-wide; adding a `5671` listener would
  force Telegraf into mTLS as well as OAuth2, contradicting the phase's mechanism split.
  Telegraf's token and the telemetry it consumes cross the `core` Docker network in
  plaintext — a bounded exposure inside one compose project, but a real one.
  A test asserts the listener's absence is deliberate, not an oversight.
- **Revocation requires the force-close step; a CRL alone is not a revocation.** An
  established connection survives a CRL update indefinitely (S4) — the demonstrated
  procedure is always CRL-republish-then-force-close, never CRL-republish alone.
- **A static-token consumer cannot survive expiry.** The committed realm gives
  `telegraf-eu` a long (3600s) lifespan to keep normal operation stable; a leaked service
  token is therefore valid for up to an hour. The short-lived client (`telegraf-eu-short`,
  60s) exists solely to demonstrate the gap, not as a production pattern.
- **`certs/` is gitignored.** A clean clone must run `scripts/make_certs.py` (or let the
  test harness's `stack` fixture generate it automatically) before any security-profile
  bring-up.
- **The CRL cache TTL is uncharacterised** — only that propagation was measured within
  seconds in this environment. Tests poll until refused rather than assuming a fixed
  interval.

## 6. Recommendation

Closing the AMQP plaintext exposure would need either a second, separately-scoped
`ssl_options` block (unmeasured on RabbitMQ 4.3.4 — whether per-listener scoping exists
at all was never probed) or accepting that Telegraf's connection also carries a client
certificate, which reopens the mechanism-split question this phase deliberately closed.
Either is real work, not a config toggle, and belongs in its own phase.

A production deployment would also need a genuine token-refresh path for Telegraf — AMQP
0-9-1's `connection.update-secret` exists for exactly this, but wiring it into a
purpose-built consumer (following Phase 2's ack-after-write pattern, which already proves
out a non-default consumer) is a different piece of work than this phase's mechanism
demonstration. Shipping the long-lived token as the interim posture is a real production
gap, not a neutral choice, and should be tracked as one.

Finally, this phase's own S7 regression run found a stale test assertion from Task 8 that
had been silently broken since it landed — nothing in Tasks 8 through 12 ran the complete
default-profile suite, only security- or region-marked subsets, so the break went
undetected for four tasks. A CI step running the full default-profile suite on every
commit, not just at a phase's closing task, would have caught this immediately rather than
at the end.
