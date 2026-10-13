# reliable-webhooks: design

What the service guarantees, how it keeps each guarantee, and what it does not do. Tests compare the schema and most of the statements in this document with the code, and [behaviour.md](behaviour.md#how-the-documents-are-held-to-the-code) lists them. Code comments and tests cite the sections by number.

## Contents

1. [Guarantees](#1-guarantees): G1 to G7, and what is not guaranteed
2. [Architecture and stack](#2-architecture-and-stack): the packages and the six house rules
3. [Kill table](#3-kill-table): the six points at which a replica is killed, K1 to K6
4. [Data model](#4-data-model): the schema, the state machine, and what leased, abandoned and held mean
5. [API](#5-api): the routes, the problem types, the admin routes and suspension
6. [Ingest: idempotency and transactional-outbox fan-out](#6-ingest-idempotency-and-transactional-outbox-fan-out)
7. [Delivery worker: claim, lease, send, finalize](#7-delivery-worker-claim-lease-send-finalize): the six steps of an attempt and the timing rules
8. [Retry schedule, dead letter, replay](#8-retry-schedule-dead-letter-replay)
9. [Circuit breaker](#9-circuit-breaker): the gate, the probe and how they meet the attempt budget
10. [Observability](#10-observability): metrics, traces and logs
11. [Security](#11-security): [authentication](#111-authentication) (11.1), [tenant scoping](#112-tenant-scoping) (11.2), [signatures](#113-signatures) (11.3), [SSRF and the outbound client](#114-ssrf-and-the-outbound-client) (11.4), [limits](#115-limits) (11.5), [telemetry hygiene](#116-telemetry-hygiene) (11.6), [untrusted trace context](#117-untrusted-trace-context) (11.7)
12. [Rate limiting, tenant isolation and database time bounds](#12-rate-limiting-tenant-isolation-and-database-time-bounds)
13. [Failure table](#13-failure-table): 31 failures, each with the tests that prove what happens
14. [Limitations](#14-limitations): 38 of them
15. [Operations](#15-operations): startup invariants, liveness, shutdown, background jobs, the image, the local stack, the load tests, the Helm chart, migrations and the Terraform module
16. [Test strategy and harness](#16-test-strategy-and-harness): the rules, the harness, how each proof is forced, and the failure-table check
17. [Rejected alternatives](#17-rejected-alternatives)

## 1. Guarantees

| # | Guarantee | Enforced by |
|---|---|---|
| G1 | **Durable accept.** A `202` means the event and one delivery per matching endpoint were committed in one transaction. | Single ingest transaction (section 6) |
| G2 | **At-least-once, never silently dropped.** Every delivery ends in `succeeded`, `dead` (replayable) or `cancelled` (endpoint deleted). No path deletes a `pending` delivery. | State machine and foreign keys (section 4) |
| G3 | **Idempotent ingest.** The same tenant and `Idempotency-Key` yields one event for at least the retention period (30 days). | `UNIQUE (tenant_id, idempotency_key)` |
| G4 | **Duplicates are possible and identifiable.** Every retry carries the same `webhook-id`. One attempt is exactly one HTTP request. A duplicate is sent only when an earlier attempt's outcome was never recorded. | Stable delivery id, `unknown` attempt rows, a client without automatic retries |
| G5 | **One recorded outcome per attempt.** A replica that lost its lease cannot change anything. | Lease-token fence, `UNIQUE (delivery_id, run, attempt_no)` |
| G6 | **Isolation.** One slow endpoint, one deep backlog or one busy tenant cannot take all workers, connections or DNS threads of a replica. A claim takes at most one row per endpoint and offers every tenant its first row before any tenant gets a second. | Endpoint-first claim, per-replica bulkheads and permits (sections 7, 12) |
| G7 | **No connection to non-public addresses**, at registration or at delivery. | Connect-time resolver guard (section 11.4) |

Not guaranteed: ordering, exactly-once delivery, delivery inside a time bound, or protection against loss of the Postgres primary beyond what its own durability settings give.

## 2. Architecture and stack

- Java 21, Spring Boot 4.1.x (exact version pinned in the POM), Maven, Spring MVC on virtual threads.
- `JdbcClient` with explicit SQL, Flyway, and two HikariCP pools (`api` and `worker`) with the time bounds of section 12. Flyway uses its own short-lived data source without those session limits.
- Apache HttpClient 5 (classic) for outbound calls, because it has a per-client `DnsResolver` hook.
- Micrometer with the Prometheus registry, the OpenTelemetry starter, and Spring Boot structured logging.
- Tests use JUnit, Testcontainers PostgreSQL, Awaitility and ArchUnit.

```
producer --POST /v1/events--> [replica: api] --one tx--> event + delivery rows (Postgres)
                                                              |
[replica: poller] --claim (SKIP LOCKED)--> [worker virtual threads] --signed POST--> subscriber
                                                              |
                                        finalize tx: delivery + attempt row + circuit
```

Packages:

| Package | Contents |
|---|---|
| (root) | `ReliableWebhooksApplication`, `WebhooksProperties` (every `webhooks.*` setting), `DatabasePools`, `DatabaseFailures`, `Traceparent` |
| `api/` | Controllers, `Problems`, `ApiKeyFilter`, `TenantConcurrencyFilter`, `UntrustedTraceContextFilter`, `RequestLogFields`, `IngestMeter` |
| `tenant/` | `TenantRepository` |
| `endpoint/` | `EndpointService`, `EndpointRepository`, `UrlPolicy`, `SecretCipher` |
| `ingest/` | `IngestService`, `EventRepository`, `TokenBucket` |
| `delivery/` | `DeliveryPoller`, `PollerHeartbeat`, `DeliveryWorker`, `DeliveryRepository`, `TenantDeliveryRepository`, `BulkReplay`, `CircuitBreaker`, `RetrySchedule`, `Bulkhead` |
| `http/` | `WebhookHttpClient`, `SsrfGuardDnsResolver`, `AddressPolicy`, `DnsLookups`, `Signer` |
| `ops/` | `WorkerLifecycle`, `StartupInvariants`, `ExpiryJob`, `CancelJob`, `RetentionJob`, `QueueGauges`, `PollerHealth` |
| test `harness/` | `ReplicaProcess`, `FaultReceiver`, `Rows`, `PgWait`, `StubDns`, `TcpProxy`, `TestDatabase`, `ReplicaSettings`, `InProcessReplica`, `UnsafeLocalReplica`, `ReplicaWorker`, `SelfStartedReplica`, `TestTenant`, `OpenRequests` |
| test `tools/` | `FailureTableCheck` |

The table names the classes that carry the design. The value types and helpers next to them are not listed.

House rules, each checked by a test or a startup invariant:

1. Every statement is explicit SQL in a repository class. The SQL in this document is the SQL in the code.
2. All transactions run at READ COMMITTED. Each guarantee names the row lock or constraint that enforces it.
3. Only the database clock (`now()`) decides when something is due, and it also supplies the signature timestamp. Replicas measure durations with a monotonic clock and never compare their wall clock with a database timestamp.
4. Lock order is endpoint, then delivery, then circuit. Ingest and replay share-lock the endpoint row before creating or re-arming a delivery. Endpoint delete takes it exclusively. Finalize never touches the endpoint row. Claims and jobs use `SKIP LOCKED` and never wait on a row.
5. Nothing coordinates through memory. In-memory state (bulkhead counters, request permits, API-key cache) is per replica and harmless to lose.
6. Every database call and every outbound request has a time bound, and every outbound response has a size bound (sections 11.4, 11.5, 12).

## 3. Kill table

Each row is a SIGKILL of a real replica process, landed at a chosen point and never by timing (section 16).

| # | SIGKILL lands | State left | Recovery | Visible effect | Test |
|---|---|---|---|---|---|
| K1 | In the ingest transaction, before commit | No event and no delivery; Postgres rolls back. The bucket charge stays | Producer retries with the same key | One event, one delivery per endpoint | `IngestKillIT#killedBeforeCommit` |
| K2 | After ingest commit, before the response | Event and deliveries committed | Retry returns the original event id with `Idempotent-Replay: true` | One event | `IngestKillIT#killedAfterCommitBeforeResponse` |
| K3 | After claim commit, inside the load of an ordinary closed-circuit delivery, before any byte is sent | Row leased, attempt consumed, no request | Lease expires, another replica reclaims | Attempt 1 logged `unknown`; subscriber sees one request | `WorkerKillIT#killedAfterClaimBeforeSend` |
| K4 | Mid-request | Row leased; subscriber may have processed | Same as K3 | Up to two requests, same `webhook-id` | `WorkerKillIT#killedMidRequest` |
| K5 | After the subscriber's 2xx, before finalize commits | Row leased; subscriber has processed | Same as K3 | Exactly two requests, same `webhook-id`; one `succeeded` | `WorkerKillIT#killedAfterResponseBeforeCommit` |
| K6 | Mid-request of a probe, holding the probe slot of an open circuit | `next_probe_at` pushed 60 s ahead; probing delivery leased | Slot reopens when that time passes; no stuck state | Next probe 60 s after the gate | `EndpointOutageIT#probeSlotReopensAfterProberKilled` |

- K1 leaves the charge of the request. The bucket is charged in a statement of its own before the transaction begins, and a failed ingest is not refunded (section 12), so the retry pays again.
- K2 is state-equivalent to a lost response. The test discards the first response, kills the replica and retries on another.
- A SIGKILL during graceful drain leaves what one of K3 to K6 leaves. An attempt that is in flight then has its request ahead of it, is inside it or has its answer, and it can be a probe. The drain adds no state of its own.
- **Orphaned statements.** Killing a replica does not cancel the statement its backend is running. That statement may still execute, and an autocommit statement may still commit. Safety relies on the lease token and on every autocommit write being harmless when orphaned, not on the statement having stopped. The autocommit writes are the gate, the release, the bucket charge, the delete flip, the admin patches, the endpoint patch, the key rotation and the secret rotation. Each of the first five leaves a state another row of this table or the strict-side limiter already covers. An orphaned endpoint patch sets the event types the tenant asked for, and repeating the call changes nothing. A rotation whose response was lost leaves a key that nobody holds, and the operator repeats the call. A secret rotation whose response was lost leaves a secret that nobody holds, and the tenant repeats that call too. The repeat ends the overlap of the first rotation (section 11.3).
- **Lost response of a create.** `POST /v1/endpoints` and `POST /admin/tenants` are transactions, so one that outlives its replica does not commit by itself. A response that is lost after the commit leaves the row, and the secret or the key in it was shown to nobody. The repeat of the call creates a second row (limitation 37).

## 4. Data model

```sql
CREATE TABLE tenant (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name          text NOT NULL,
  api_key_hash  bytea NOT NULL UNIQUE,                 -- SHA-256 of the key
  status        text NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended')),
  rate_per_sec  double precision NOT NULL DEFAULT 50  CHECK (rate_per_sec >= 0),
  burst         integer NOT NULL DEFAULT 100          CHECK (burst >= 0),
  max_endpoints integer NOT NULL DEFAULT 20           CHECK (max_endpoints BETWEEN 0 AND 50),
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE rate_bucket (                              -- inserted with the tenant (tokens = burst);
  tenant_id   uuid PRIMARY KEY REFERENCES tenant,       -- written on every ingest, kept off the tenant row
  tokens      double precision NOT NULL,
  refilled_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE endpoint (                                 -- configuration; written only by the tenant
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenant,
  url         text NOT NULL,
  event_types text[] NOT NULL,                          -- exact names, or '*'
  secret      bytea NOT NULL,                           -- AES-256-GCM ciphertext
  prev_secret bytea,
  prev_secret_expires_at timestamptz,
  status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active','deleted')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);
CREATE INDEX endpoint_active ON endpoint (tenant_id) WHERE status = 'active';   -- ingest match, cap, list

CREATE TABLE endpoint_circuit (                         -- health; inserted with the endpoint,
  endpoint_id uuid PRIMARY KEY REFERENCES endpoint ON DELETE CASCADE,   -- afterwards written only by workers
  state       text NOT NULL DEFAULT 'closed' CHECK (state IN ('closed','open')),
  consecutive_failures integer NOT NULL DEFAULT 0,
  probe_failures       integer NOT NULL DEFAULT 0,
  opened_at     timestamptz,
  next_probe_at timestamptz
);
CREATE TABLE event (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL REFERENCES tenant,
  type            text NOT NULL,
  body            bytea NOT NULL,                       -- rendered once at ingest, sent byte-identical
  idempotency_key text NOT NULL,
  request_hash    bytea NOT NULL,
  trace_context   text,                                 -- this service's ingest span, not the caller's
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX event_created ON event (created_at);       -- retention

CREATE TABLE delivery (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),   -- sent as webhook-id
  tenant_id   uuid NOT NULL REFERENCES tenant,
  event_id    uuid NOT NULL REFERENCES event,               -- RESTRICT: an event cannot vanish under a delivery
  endpoint_id uuid NOT NULL REFERENCES endpoint,
  state       text NOT NULL DEFAULT 'pending'
              CHECK (state IN ('pending','succeeded','dead','cancelled')),
  attempts    integer NOT NULL DEFAULT 0,                   -- claims in the current run
  replays     integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL,                     -- due time; while leased, the lease expiry
  lease_token uuid,                                         -- set by a claim, cleared by every other writer
  expires_at  timestamptz NOT NULL,                         -- created_at + 72 h, reset by replay
  dead_reason text,                                         -- exhausted | expired | endpoint_deleted
  created_at  timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (event_id, endpoint_id)
);
CREATE INDEX delivery_head   ON delivery (endpoint_id, next_attempt_at) WHERE state = 'pending';  -- the claim
CREATE INDEX delivery_expiry ON delivery (expires_at)   WHERE state = 'pending';
CREATE INDEX delivery_done   ON delivery (completed_at) WHERE state <> 'pending';
CREATE INDEX delivery_by_endpoint ON delivery (endpoint_id, state, created_at, id);               -- list, bulk replay

CREATE TABLE delivery_attempt (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  delivery_id uuid NOT NULL REFERENCES delivery ON DELETE CASCADE,
  run         integer NOT NULL,                             -- delivery.replays at the time
  attempt_no  integer NOT NULL,
  probe       boolean NOT NULL DEFAULT false,
  outcome     text NOT NULL CHECK (outcome IN ('success','failure','unknown')),
  http_status integer,
  error       text,   -- timeout | connect | tls | deadline | response_too_large | destination_not_allowed | lease_expired
  remote_addr inet,
  started_at  timestamptz NOT NULL DEFAULT now(),
  duration_ms integer,
  replica     text,
  UNIQUE (delivery_id, run, attempt_no)
);
```

State machine. There is no `in_flight` or `held` state to get stuck in.

```
pending --2xx recorded-----------------------> succeeded
pending --attempt 12 failed, or 13th claim---> dead (exhausted)
pending --expires_at passed------------------> dead (expired)
pending --endpoint deleted, then CancelJob---> cancelled
dead    --replay-----------------------------> pending (attempts = 0, replays + 1)
```

Three derived terms, each defined once:

- **Leased** means `state = 'pending' AND lease_token IS NOT NULL AND next_attempt_at > now()`. The lease is the retry timer: if the holder dies, the row becomes due again by itself, so there is no reaper.
- **Abandoned** means a `pending` row with a non-null `lease_token` whose `next_attempt_at` has passed. Its holder recorded nothing. A claim or `ExpiryJob` that touches it next logs that attempt as `unknown`. `CancelJob` does not: it cancels the row of a deleted endpoint as it is, with the attempt counted and no attempt row (limitation 15).
- **Held** means `pending` and due, but not claimable because the endpoint's circuit is open or the tenant is suspended. It is derived by the claim predicate and costs zero writes.

Every writer other than the claim sets `lease_token = NULL`: finalize, release, expiry, cancel and replay. A `dead` or `cancelled` row therefore never carries a stale token into its next run.

## 5. API

[api.md](api.md) is the reference for callers: every route with its request, its response and its status codes. This section says what the routes are for.

Tenant routes take `Authorization: Bearer <api key>`. A request body is JSON in UTF-8 (section 11.5). Errors are RFC 9457 problem documents. An id that belongs to another tenant returns 404.

A named problem type such as `rate_limited` is sent as the `type` member, as a relative reference. Any other error (401, 404, a malformed request, 500) carries no `type`, which RFC 9457 reads as `about:blank`.

| Route | Behaviour |
|---|---|
| `POST /v1/events` | Body `{"type": "...", "data": {...}}`, header `Idempotency-Key` (required, 1 to 255 printable ASCII). Returns `202 {"id"}`. |
| `GET /v1/events/{id}` | Event with its deliveries (at most 50). Its `created_at` is the `now()` of the ingest transaction, the database time that its deliveries are due from. The `created_at` in the body that subscribers receive is the clock of a replica (section 6), so the two can differ. |
| `POST /v1/endpoints` | `{url, event_types}`. Returns `201 {"id", "secret"}`. The signing secret is shown this once. At the endpoint cap it answers `409` (section 11.5). |
| `GET /v1/endpoints`, `GET /v1/endpoints/{id}` | Includes derived circuit state. At most 50 rows by the endpoint cap. |
| `PATCH /v1/endpoints/{id}` | `event_types` only. Returns `204`. A new URL is a new endpoint. |
| `DELETE /v1/endpoints/{id}` | Soft delete in one statement. Returns `204`. Delivery to the endpoint stops at commit; its pending deliveries become `cancelled` in batches (section 15). |
| `POST /v1/endpoints/{id}/secret/rotate` | New secret; the old one stays valid for 24 h (section 11.3). Returns `200 {"secret"}`. The new secret is shown this once. |
| `POST /v1/endpoints/{id}/replay` | `{limit}`; re-arms up to `limit` dead deliveries (section 8). Returns `200 {replayed, remaining}`. |
| `GET /v1/deliveries?endpoint_id=&state=` | Both filters are required. Keyset pages on `(created_at, id)`, `limit` at most 100, with a `next` cursor. The endpoint is resolved under the tenant first (404 otherwise). `limit` is 1 to 100, and 100 when it is absent. A page that another page follows carries `next`, and the request for that page sends it back as `cursor`. A deleted endpoint still resolves, so its deliveries can be listed. |
| `GET /v1/deliveries/{id}` | The delivery and the attempt log of its current run (at most 13 rows). `?run=n` selects an earlier run, and a run that the delivery does not have answers `404`. An attempt is shown with its number, its outcome, its status or error, its start and its duration. The address it went to and the replica that made it are not shown. An `unknown` attempt has no duration, and its start is the time at which it was logged. |
| `POST /v1/deliveries/{id}/replay` | Re-arms one dead delivery. Returns `204`. A delivery that is not dead answers `409`, and one whose endpoint is deleted answers `404`. |

**Derived circuit state.** An endpoint reads as `closed`, `open` or `half_open`. Only `closed` and `open` are stored (section 9). `half_open` is an open circuit whose `next_probe_at` has passed, which means the next claim may take one delivery to the endpoint as a probe. The read statement decides it with the database clock.

Problem types used for rejection:

| Status | Type | When |
|---|---|---|
| 429 | `concurrency_limit` | More than 8 concurrent requests for the tenant on this replica. `Retry-After: 1`. |
| 429 | `rate_limited` | Bucket empty. `Retry-After: ceil(cost / rate)`. |
| 429 | `rate_limit_unsatisfiable` | The tenant's rate is 0 or the cost exceeds its burst. No `Retry-After`, because waiting cannot help. |
| 429 | `lookup_busy` | Registration could not get a DNS slot. `Retry-After: 2`. |
| 403 | `tenant_suspended` | The tenant is suspended. |
| 503 | `database_busy` | A transient database error, as section 7 lists them: among them a lock timeout, a statement timeout, no connection inside the borrow wait and a connection that failed, as it does when the database restarts. `Retry-After: 1`. |

Admin routes take `Authorization: Bearer <admin token>`:

- `POST /admin/tenants` takes `{"name"}` (1 to 200 characters) and returns `201 {"id", "api_key"}`. The API key is shown this once. The tenant and its bucket are created together:

```sql
-- one transaction
INSERT INTO tenant (name, api_key_hash) VALUES (:name, :hash) RETURNING id, burst;
INSERT INTO rate_bucket (tenant_id, tokens) VALUES (:id, :burst);
```

- `PATCH /admin/tenants/{id}` takes any of `rate_per_sec`, `burst`, `max_endpoints` and `status` (`active` or `suspended`) and returns `204`. A member that is absent stays as it is. A body that names none of them answers `400`, and so does a value outside the limits of the schema (section 4) and a number with a fraction where a whole number belongs. A rate is 0 or lies between 0.000001 and 1,000,000: the bucket statement multiplies it by the seconds since the last charge, and Postgres fails a product that a double cannot hold. The patch is one autocommit statement:

```sql
UPDATE tenant
SET rate_per_sec  = coalesce(:rate_per_sec, rate_per_sec),
    burst         = coalesce(:burst, burst),
    max_endpoints = coalesce(:max_endpoints, max_endpoints),
    status        = coalesce(:status, status)
WHERE id = :id;
-- 0 rows: 404
```

  The bucket statement (section 12), the claim (section 7) and the endpoint cap (section 11.5) read the tenant row, so they go by the new values from their next statement on, on every replica. The patch evicts the tenant from the local key cache; other replicas follow within 30 s.
- `POST /admin/tenants/{id}/api-key/rotate` replaces the key immediately and returns the new one as `{"api_key"}`. Like the patch, it evicts the tenant from the local key cache; other replicas follow within 30 s.

**Suspension** is the operator's kill switch. `status = 'suspended'` does two things: `ApiKeyFilter` rejects the tenant's key, and the claim stops taking the tenant's deliveries (section 7). The rows are not written to. They are held, expire as `dead (expired)` after 72 h like any held row, and deliver after `status = 'active'` is set again.

## 6. Ingest: idempotency and transactional-outbox fan-out

The delivery rows are the outbox. They are written in the same transaction as the event, so there is no relay process and no window between "event stored" and "deliveries exist".

Order of work for `POST /v1/events`:

1. **Authenticate**, then take a per-tenant request permit (section 12). A request that is refused a permit runs no statement after that. Authentication comes first because it names the tenant, so the key lookup is the one statement that is not behind a permit. A replica caches that lookup for 30 s (section 11.1), and a request whose key is cached is refused without acquiring a database connection.
2. **Validate** charset, size, JSON, type and key against the limits of section 11.5. Render the body `{"id","type","created_at","data"}` once.
   - The body is read as UTF-8 and as nothing else. A `Content-Type` that names another charset answers `415`, before the size of the body is looked at. A body in UTF-16 or UTF-32, or one that starts with a byte order mark, is not JSON here and answers `400`.
   - `data` must be a JSON object. Its numbers keep their exact value. None is read into a double.
   - Of members with the same name in one object, the last one counts.
   - `created_at` is the replica's wall clock at this step, in milliseconds and always with three fraction digits. It is information for the subscriber. Nothing is scheduled from it, and it is never compared with a database timestamp.
3. **Idempotency lookup**: `SELECT id, request_hash FROM event WHERE tenant_id = :t AND idempotency_key = :k`. The request hash is the SHA-256 of the request body as received.
   - Same hash: return `202` with the original id and `Idempotent-Replay: true`, not charged.
   - Different hash: `422 idempotency_key_reused`.
4. **Match**: `SELECT id FROM endpoint WHERE tenant_id = :t AND status = 'active' AND (event_types @> ARRAY['*'] OR event_types @> ARRAY[:type::text])`. Call the result `matched`. The cast is there because the driver sends a string parameter as `varchar`, and Postgres has no `text[] @> varchar[]`.
5. **Charge** `max(1, |matched|) + floor(body_bytes / 4096)` units to the token bucket in one autocommit statement (section 12). `body_bytes` is the size of the body rendered in step 2, which is the body that is stored. No tokens means `429`.
6. **One transaction** on the `api` pool:

```sql
INSERT INTO event (id, tenant_id, type, body, idempotency_key, request_hash, trace_context)
VALUES (...) ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING id;
-- no row: a concurrent request with the same key won; re-run step 3 and answer from it

INSERT INTO delivery (tenant_id, event_id, endpoint_id, next_attempt_at, expires_at)
SELECT e.tenant_id, :event_id, e.id, now(), now() + interval '72 hours'
FROM endpoint e
WHERE e.id = ANY(:matched) AND e.tenant_id = :t AND e.status = 'active'
FOR SHARE OF e;
```

Why this is correct with several replicas:

- **Concurrent identical requests.** The second `INSERT ... ON CONFLICT DO NOTHING` blocks on the first one's uncommitted unique-index entry. If the first commits, the second returns no row, and its follow-up lookup is a new READ COMMITTED statement that sees the committed row. If the first aborts, the second inserts.
- **Endpoint delete racing ingest.** `FOR SHARE` conflicts with the delete's row lock. If the delete goes first, the fan-out re-checks `status = 'active'` after the wait and creates no delivery. If ingest goes first, the delivery exists before the delete commits and `CancelJob` cancels it. Once a delete has committed, no delivery is created or re-armed for that endpoint.
- **Exact charge.** The fan-out only uses ids from step 4, so deliveries created never exceed units charged. An endpoint created between steps 4 and 6 misses this event, as if the event had arrived a moment earlier.
- An event with no matching endpoint is accepted, stored and charged for its size. Its idempotency key still holds.

## 7. Delivery worker: claim, lease, send, finalize

Each replica runs one `DeliveryPoller` thread and up to 64 attempts on virtual threads. The poller claims only as many rows as it has free permits, so every claimed row starts immediately and nothing queues in memory.

The poller loop:
- It claims, hands each returned row to a virtual thread, and claims again at once if the claim returned a row and a permit is free.
- An empty claim starts an idle wait of 50 ms that doubles to 1 s (`ClaimerBackoff`). A finished attempt ends the wait. The wait doubles with every claim that brings no row, also with one that was made early because an attempt had finished, and only a claim that brings a row starts it over (limitation 35).
- Every iteration, successful or failed, updates the heartbeat that liveness reads (section 15).

**Step 1: claim.** One transaction on the `worker` pool. The statement picks endpoints first and deliveries second.

```sql
WITH eligible AS MATERIALIZED (            -- 1. endpoints this replica may serve right now
  SELECT e.id AS endpoint_id, e.tenant_id, c.state AS circuit
  FROM endpoint e
  JOIN tenant t           ON t.id = e.tenant_id
  JOIN endpoint_circuit c ON c.endpoint_id = e.id
  WHERE e.status = 'active'
    AND t.status = 'active'                                -- suspended tenants are held
    AND (c.state = 'closed' OR c.next_probe_at <= now())   -- open circuits are held
    AND e.id        <> ALL (:full_endpoints)               -- bulkheads: skip what this
    AND e.tenant_id <> ALL (:full_tenants)                 -- replica could not run anyway
), head AS (                               -- 2. the oldest lockable due row of each one
  SELECT g.endpoint_id, g.tenant_id, g.circuit, h.id, h.next_attempt_at, h.abandoned
  FROM eligible g
  CROSS JOIN LATERAL (
    SELECT d.id, d.next_attempt_at, d.lease_token IS NOT NULL AS abandoned
    FROM delivery d
    WHERE d.endpoint_id = g.endpoint_id
      AND d.state = 'pending'
      AND d.next_attempt_at <= now()
    ORDER BY d.next_attempt_at
    LIMIT 1
    FOR UPDATE OF d SKIP LOCKED
  ) h
), ranked AS (                             -- 3. tenant fairness
  SELECT *, row_number() OVER (PARTITION BY tenant_id ORDER BY next_attempt_at) AS nth_for_tenant
  FROM head
), picked AS (
  SELECT * FROM ranked
  WHERE nth_for_tenant <= 4
  ORDER BY nth_for_tenant, next_attempt_at                 -- every tenant's first row before any second
  LIMIT :free
)
UPDATE delivery d                          -- 4. lease
SET attempts = d.attempts + 1,
    lease_token = gen_random_uuid(),
    next_attempt_at = now() + :lease                       -- the lease
FROM picked p
WHERE d.id = p.id
RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.attempts, d.replays,
          d.lease_token, p.abandoned, p.circuit;
```

- **`:lease`** is the configured lease, `webhooks.delivery.lease`, which is 60 s. It is a parameter so that the statement and the startup invariant on the lease read the same value.
- **Two replicas racing.** `FOR UPDATE SKIP LOCKED` gives each due row to at most one claimer. After commit the row is no longer due, and a claimer whose snapshot predates that commit re-checks the predicate under the lock and skips the row.
- **Backlog depth does not change what a claim takes.** Each eligible endpoint contributes at most one row, whether it has one due delivery or a million. An endpoint with a deep backlog and a fast receiver gets one row per claim, like every other endpoint. A held row is never taken, because its endpoint is not in `eligible`.
- **Cost.** The statement is written for a plan that probes `delivery_head` once per eligible endpoint and locks one head row per endpoint with due work. Rows locked but not picked unlock at commit. With that plan a held row is never read, and the cost grows with the number of active endpoints, not with the queue. The statement cannot demand that plan. PostgreSQL has been seen to choose another one, with which a claim reads every pending delivery once per eligible endpoint (limitation 4).
- **Bulkheads without a release path.** A claim takes at most one delivery per endpoint and four per tenant. `Bulkhead` lists an endpoint as full at 4 in flight and a tenant as full above 12, so the per-replica caps (4 per endpoint, 16 per tenant) are never exceeded.
- **Abandoned attempts are logged.** For each returned row with `abandoned = true`, the same transaction inserts `delivery_attempt (delivery_id, run, attempt_no = attempts - 1, outcome 'unknown', error 'lease_expired')`. It means the holder recorded no result before its lease ran out, and the request may or may not have arrived. The row has `probe = false` also when the attempt was a probe, because the claim that logs it cannot know.
- **Poison deliveries.** `attempts` is counted at claim, so a delivery that kills its worker every time still runs out. On the 13th claim the worker dead-letters it (`exhausted`) before the gate and without sending: the finalize transaction with F1 only, and no attempt row.

**Release.** Two steps below give a row back without consuming an attempt. They use one statement:

```sql
-- release, one autocommit statement
UPDATE delivery SET lease_token = NULL, attempts = attempts - 1, next_attempt_at = now() + :delay
WHERE id = :id AND lease_token = :token;
```

A release writes no attempt row and does not touch the circuit. It is for decisions this replica made about itself, never for something the endpoint did.

**Step 2: gate.** Runs only for rows claimed with `circuit = 'open'`, which means a probe was due (section 9). The loser of the gate releases its row with delay 0.

**Step 3: load.** One `SELECT` on every path:

```sql
SELECT e.url, e.secret,
       CASE WHEN e.prev_secret_expires_at > now() THEN e.prev_secret END AS prev_secret,
       ev.body, ev.trace_context,
       extract(epoch FROM now())::bigint AS signed_at,       -- the signature timestamp
       extract(epoch FROM now() - d.created_at) AS age       -- of the delivery, in seconds
FROM delivery d
JOIN endpoint e ON e.id = d.endpoint_id
JOIN event ev   ON ev.id = d.event_id
WHERE d.id = :id AND d.lease_token = :token AND e.status = 'active';
```

No row means the delivery was cancelled or reclaimed, or its endpoint was deleted. The worker stops. A row of a deleted endpoint stays leased until `CancelJob` cancels it.

The trace context is the parent of the span of the attempt, and the age is what the end-to-end meter starts from (section 10). Neither is part of the request.

**Step 4: sign and send** (sections 11.3 and 11.4), under the hard attempt deadline below. If `DnsLookups` answers `busy`, this replica could not admit the lookup. The worker releases the row with a 2 s delay and stops.

**Step 5: classify.**
- 2xx is success. The status line decides; the body is not part of the outcome.
- Everything else is a failure and is retried on the schedule: any other status (3xx included, redirects are not followed), any I/O error, the deadline, a response head over the size limits, a DNS timeout, a blocked or unresolvable destination.
- Once the head of a response has arrived, the attempt has its outcome. A body that is too long, breaks off or is still arriving at the deadline costs the connection, which is closed and not used again. It does not change the outcome.
- This step picks the next state, the delay and which F3 statement applies.

An attempt that got a response is recorded with its status and no error. An attempt without a response is recorded with one of these errors and no status:

| What happened | `error` in the attempt row | `reason` on the meter |
|---|---|---|
| The host resolves to an address that the policy denies | `destination_not_allowed` | `blocked` |
| The host does not resolve | `destination_not_allowed` | `dns` |
| The lookup did not answer inside its 2 s | `timeout` | `timeout` |
| No connection could be made, or the connection ended before it had carried a response head | `connect` | `connect` |
| The TLS handshake failed | `tls` | `tls` |
| The attempt deadline passed before a response head had arrived | `deadline` | `deadline` |
| The response head is over the size limits | `response_too_large` | `response_too_large` |

On the meter a response counts by the class of its status. A status outside 2xx, 3xx and 4xx counts as `http_5xx`.

**Step 6: finalize.** One transaction:

```sql
-- F1: fence and state change
UPDATE delivery
SET state = :state, lease_token = NULL, dead_reason = :dead_reason,
    next_attempt_at = CASE WHEN :state = 'pending' THEN now() + :delay ELSE next_attempt_at END,
    completed_at    = CASE WHEN :state = 'pending' THEN NULL ELSE now() END
WHERE id = :id AND lease_token = :token;
-- 0 rows: lease lost or already finalized. Roll back, count webhooks_finalize_lost_lease_total, stop.

-- F2: the attempt row
INSERT INTO delivery_attempt (delivery_id, run, attempt_no, probe, outcome, http_status, error,
                              remote_addr, started_at, duration_ms, replica)
VALUES (:id, :run, :attempt_no, :probe, :outcome, :http_status, :error, :remote_addr::inet,
        now() - :duration_ms * interval '1 millisecond', :duration_ms, :replica);

-- F3: one of three circuit statements (section 9)
```

- `:duration_ms` is how long the request took on the monotonic clock of the replica. `started_at` is that long before the `now()` of the finalize transaction, so `started_at` plus `duration_ms` is the database time at which the attempt was recorded.
- `:replica` is `webhooks.replica-id`. It defaults to the `HOSTNAME` of the container, and to a random id where there is none.
- `:probe` is true for a delivery that was claimed with `circuit = 'open'`. It is sent only after it has won the gate, so its attempt is the probe (section 9).

Timing rules:

| Rule | Value | Why | Proven by |
|---|---|---|---|
| Attempt deadline | 15 s, hard | Covers DNS (2 s wait), connect (3 s), TLS, request and the whole response. A watchdog aborts the request at the deadline, so a drip-fed response cannot outlive it. The client connects to one address of the host, so the abort leaves it no other address to go on to (section 11.4). | `SlowEndpointIsolationIT#drippingResponseIsCutAtDeadline`, `WebhookHttpClientIT#hostWithTwoAddressesIsConnectedAtOneOfThemAndCutAtTheDeadline` |
| Lease | 60 s | `StartupInvariants` requires lease >= deadline + 30 s. | `StartupInvariantsTest` |
| Start cutoff | 35 s after the claim call began | A worker that could not finish inside its lease does not send. The row stays leased and is reclaimed. `StartupInvariants` requires start cutoff + deadline <= finalize cutoff, and < 60 s, the probe slot of section 9. | `DeliveryWorkerTest#doesNotSendAfterStartCutoff` (injected monotonic time source), `StartupInvariantsTest` |
| Finalize retry | Until 55 s after the claim call began, transient errors only | Covers a database restart; after that the row is left for reclaim. `StartupInvariants` requires finalize cutoff < lease. | `DatabaseRestartIT` (one request, one `success` row across the restart), `FinalizeRetryIT`, `FinalizeRetryTest`, `DatabaseFailuresTest`, `StartupInvariantsTest` |

- **The lease is liveness, the token is safety.** If a replica stalls past its lease, another replica reclaims and both may send. That is a duplicate with the same `webhook-id`, which at-least-once allows. Only the current token holder can pass F1, so the stale replica's result, attempt row and circuit update all roll back.
- **Clocks.** Lease expiry is decided by Postgres. The replica's cutoffs use `System.nanoTime()` measured from before the claim was sent, so its view of the lease is always the shorter one. Wall-clock skew between replica and database has no effect, including on the signature timestamp.
- **Commit-unknown.** If the connection dies during finalize commit, the retry's F1 matches 0 rows when the first commit landed, and applies the result when it did not. Either way there is one outcome.
- **What finalize retries.** Only transient errors:
  - connection failures and socket timeouts (SQLSTATE class 08, `57P01` to `57P03`);
  - no connection inside the borrow wait;
  - deadlock (`40P01`), lock timeout (`55P03`) and statement timeout (`57014`).

  Anything else, such as a data or constraint error (classes 22, 23), is a bug. It is not retried, it is counted on `webhooks_finalize_errors_total` and logged at error, and the row is left for reclaim.

  A statement that fails because its connection is gone is followed by a rollback that fails as well. What the transaction then throws is the failure of the rollback, which the pool reports without an SQLSTATE. In that case the failure of the statement decides. `FinalizeRetryIT` ends the backend of a finalize that waits for its row, as a database that shuts down does, and the result is recorded by the next try from the one request. `IngestMeterIT#requestWhoseConnectionTheDatabaseEndedIsCountedAsBusy` does the same to an ingest, which answers `503`.

  `DatabaseFailures` holds this rule. The API uses it too: a request that fails with a transient error answers `503 database_busy` (section 5).

  A retry starts 200 ms after the failure before it, and none starts after the 55 s.

## 8. Retry schedule, dead letter, replay

`RetrySchedule.delay(n, random)` is a pure function. After failed attempt `n`, with `d(n) = min(4 h, 10 s * 2^(n-1))`, the wait is `d(n)/2 + random(0, d(n)/2)` (equal jitter).

- Twelve attempts give eleven waits, from 5 to 10 s after attempt 1 up to 85 to 171 min after attempt 11.
- The whole schedule spans between 2 h 51 min and 5 h 41 min.
- The 4 h cap is not reached at twelve attempts. It exists so that raising `max-attempts` cannot produce multi-day waits.
- `Retry-After` on any failure response is honoured as `max(own delay, min(Retry-After, 1 h))`. It can lengthen a wait up to one hour and can never shorten it.
- Only a `Retry-After` that is a number of seconds is read. One that is a date counts as absent, because a date would have to be compared with the wall clock of the replica.
- The schedule is a lower bound. Attempt `n+1` never starts sooner than `d(n)/2` after attempt `n` was recorded. An open circuit can make it later, never earlier.

Dead letter. `dead` is a state of the delivery row, not a second table.

| Reason | When |
|---|---|
| `exhausted` | Attempt 12 was recorded as a failure, or the row was claimed a 13th time |
| `expired` | Still pending and not leased at `expires_at`; `ExpiryJob` moves it (section 15) |

Replay. Only `dead` deliveries are replayable. `succeeded` and `cancelled` are not, so replay cannot be used as a loop.

```sql
-- single replay, one transaction, after charging 1 unit
SELECT e.id FROM endpoint e JOIN delivery d ON d.endpoint_id = e.id
WHERE d.id = :id AND d.tenant_id = :t AND e.status = 'active' FOR SHARE OF e;   -- no row: 404
UPDATE delivery
SET state = 'pending', attempts = 0, replays = replays + 1, dead_reason = NULL, completed_at = NULL,
    lease_token = NULL,
    next_attempt_at = now() + random() * interval '60 seconds',
    expires_at = now() + interval '72 hours'
WHERE id = :id AND tenant_id = :t AND state = 'dead';                           -- 0 rows: 409
```

The unit is charged before the transaction and is not refunded, also when the replay then answers `404` or `409`.

Bulk replay (`POST /v1/endpoints/{id}/replay`) is bounded and resumable:

- `limit` is at most `min(500, burst)`. A larger one counts as that bound, so a call never costs more than the bucket can hold. A `limit` that is missing or below 1 answers `400`. So does a number with a fraction, and one above 2,147,483,647, the largest that an `int` holds.
  - The burst is the one that authentication loaded with the tenant, which a replica caches for 30 s (section 11.1).
  - With a burst of 0 the bound is 1. The bucket then refuses the call, where a bound of 0 would answer that nothing is left.
- The service resolves the endpoint under the tenant (404 otherwise), counts dead rows up to `limit` and charges that many units. Nothing dead means nothing charged and no transaction.
  - The count reads one row beyond `limit`. That row is neither charged nor re-armed. It is how the call knows that more are left.
- It then runs one transaction, with `:k` the number of rows that were charged:

```sql
SELECT id FROM endpoint WHERE id = :e AND tenant_id = :t AND status = 'active' FOR SHARE;   -- no row: 404
WITH picked AS (
  SELECT id FROM delivery
  WHERE endpoint_id = :e AND tenant_id = :t AND state = 'dead'
  ORDER BY created_at, id LIMIT :k
  FOR UPDATE SKIP LOCKED
)
UPDATE delivery d
SET (the same columns as the single replay)
FROM picked
WHERE d.id = picked.id;
```

- The rows are picked in a CTE, which is what makes `LIMIT :k` a bound. A CTE that locks rows is run once. Written as `WHERE id IN (SELECT ... LIMIT :k FOR UPDATE SKIP LOCKED)`, the subquery would be one side of a join, and the planner may run such a subquery again for each row of the other side. It does so, for example, when the statistics say that the table holds a single row. Each further run passes over the rows that the statement has re-armed already, so its `LIMIT` fills up with further dead rows, and the call re-arms more rows than it charged. The claim (section 7) and the job statements (section 15) pick their rows in a CTE as well.
- Re-armed rows are spread over 60 s, and the oldest go first.
- `replayed` is the number of rows that the transaction re-armed. `remaining` is true when the count found more rows than that.
- The caller repeats until `remaining` is false. No cursor is needed, because replayed rows leave the dead set.
- The charge is not refunded, as for the single replay. A call whose endpoint is deleted between the count and the transaction has paid and answers `404`. A call that meets another replay of the same rows has paid for rows that it then passes over: those are re-armed by the other call, and `remaining` says that this one left some.

## 9. Circuit breaker

State lives in `endpoint_circuit`, so it is fleet-wide and survives restarts.

- **Opens** after 5 consecutive failed attempts on the endpoint, across deliveries. Any recorded success resets the count and closes the circuit.
- **While open**, the endpoint is not in the claim's `eligible` set. Held deliveries are not written to and consume no attempts.
- **Probe.** When `next_probe_at` passes, the endpoint is eligible again. A claim takes at most one of its rows (one per endpoint per claim), and the worker runs the gate for that row. Several claims can get this far before the first gate commits, on one replica or on many:

```sql
-- gate, one autocommit statement
UPDATE endpoint_circuit SET next_probe_at = now() + interval '60 seconds'
WHERE endpoint_id = :e AND state = 'open' AND next_probe_at <= now();
```

One row updated means this delivery is the probe. Zero rows means another replica took the slot, and the delivery is released with delay 0 (section 7).

- **Exactly one probe fleet-wide.** The gate is a single-row `UPDATE` whose predicate Postgres re-evaluates after the row-lock wait. The second claimer sees the pushed `next_probe_at` and updates nothing.
- **No stuck probe.** Taking the slot is pushing a timestamp. A killed prober leaves nothing to clean up (K6).
- **The slot outlasts its probe.** The 60 s are part of the gate and not a setting. The request of a probe starts before the start cutoff of its claim and ends at the attempt deadline at the latest, so `StartupInvariants` requires start cutoff + attempt deadline < 60 s. With the shipped values a probe is over 50 s after its claim began, and its slot is free again 60 s after the gate, which came later.

The three F3 statements:

```sql
-- F3 success: any success closes the circuit; touches no row in the healthy case
UPDATE endpoint_circuit SET state = 'closed', consecutive_failures = 0, probe_failures = 0,
       opened_at = NULL, next_probe_at = NULL
WHERE endpoint_id = :e AND (state = 'open' OR consecutive_failures > 0);

-- F3 failure, circuit closed at claim time
UPDATE endpoint_circuit
SET consecutive_failures = consecutive_failures + 1,
    state         = CASE WHEN consecutive_failures + 1 >= 5 THEN 'open' ELSE 'closed' END,
    opened_at     = CASE WHEN consecutive_failures + 1 >= 5 THEN now() END,
    next_probe_at = CASE WHEN consecutive_failures + 1 >= 5 THEN now() + interval '30 seconds' END
WHERE endpoint_id = :e AND state = 'closed';

-- F3 probe failure: back off the next probe, 30 s doubling to 5 min.
-- The exponent is capped before the multiplication, so the interval cannot overflow
-- however long the outage lasts.
UPDATE endpoint_circuit
SET probe_failures = probe_failures + 1,
    next_probe_at  = now() + least(interval '5 minutes',
                                   interval '30 seconds' * power(2, least(probe_failures + 1, 4)))
WHERE endpoint_id = :e AND state = 'open';
```

**How the breaker and the attempt budget interact.** This is one rule:

1. Held time never consumes attempts.
2. A probe is a real attempt of the probing delivery. It consumes one attempt, writes an attempt row with `probe = true`, and on failure the delivery waits its own backoff as usual.
3. Deliveries already in flight when the circuit opens run to completion. At most 5 + 3 requests per replica reach a hard-down endpoint before it is quiet.

Consequences, each with its own test:

- **Healthy endpoint, one event always rejected.** The circuit stays closed because other deliveries succeed. That delivery walks the twelve-step schedule and ends `dead (exhausted)`.
- **Hard-down endpoint, few deliveries.** The circuit opens at the fifth failure. Each later attempt is a probe. A single delivery ends `dead (exhausted)` with twelve attempt rows: five ordinary, seven probes.
- **Hard-down endpoint, many deliveries.** Probes rotate among held deliveries, one per probe interval fleet-wide. Deliveries still pending at 72 h end `dead (expired)` with however many attempts they used.
- **Recovery.** The first successful probe closes the circuit. Held deliveries drain at one row per claim and no more than 4 concurrent requests per replica, while other endpoints keep their place in every claim.

Circuit state is shared, but what feeds it is observed by one replica at a time. Limitation 25 states what that costs.

## 10. Observability

**Metrics** (Micrometer, Prometheus). A replica serves them at `/actuator/prometheus`, which takes no key, like the health endpoints. They are scraped and pushed nowhere. No meter of the service carries a tenant, endpoint, delivery, host or URL label. Each meter ships with the code it counts.

| Metric | Labels |
|---|---|
| `webhooks_ingest_requests_total` | `result` = accepted, replayed, rate_limited, unsatisfiable, busy, invalid |
| `webhooks_delivery_attempts_total` | `outcome`, `reason` = http_2xx, http_3xx, http_4xx, http_5xx, timeout, connect, tls, dns, blocked, deadline, response_too_large, lease_expired |
| `webhooks_delivery_released_total` | `reason` = gate_lost, dns_busy |
| `webhooks_delivery_attempt_seconds` (histogram) | `outcome` = success, failure |
| `webhooks_delivery_terminal_total` | `state`, `reason` = succeeded and delivered, dead and exhausted, dead and expired, cancelled and endpoint_deleted |
| `webhooks_delivery_end_to_end_seconds` (histogram) | none |
| `webhooks_claim_seconds`, `webhooks_claim_rows`, `webhooks_claim_errors_total` | none |
| `webhooks_finalize_lost_lease_total`, `webhooks_finalize_errors_total` | none |
| `webhooks_poller_last_iteration_age_seconds` (gauge) | none |
| `webhooks_deliveries` (gauge) | `state` = due, scheduled, leased, held |
| `webhooks_oldest_due_age_seconds`, `webhooks_circuits_open` (gauges) | none |
| `webhooks_dns_lookups_total` | `result` = ok, timeout, busy, failed, blocked |
| `webhooks_ssrf_allowlist_entries` (gauge) | none; must be 0 in production |

`webhooks_delivery_attempts_total` counts attempt rows: an attempt is counted when its finalize has committed, and an abandoned attempt (`unknown`, `lease_expired`) when the claim or the expiry run that found it has. An attempt whose finalize lost its lease is counted on `webhooks_finalize_lost_lease_total` only.

`webhooks_delivery_attempt_seconds` holds how long the request of such an attempt took, on the monotonic clock of the replica. An abandoned attempt has no duration and is not in it. Its buckets end at 15 s, which is the attempt deadline that ships. They are not read from `webhooks.delivery.attempt-deadline`.

`webhooks_delivery_terminal_total` counts a delivery when it reaches a final state. The worker counts `succeeded` and `dead` with `exhausted` once the finalize has committed, `ExpiryJob` counts `dead` with `expired`, and `CancelJob` counts `cancelled`. A delivery that is replayed is counted again when its new run ends.

`webhooks_delivery_end_to_end_seconds` holds, for each delivery that succeeded, the time from its creation to the end of the request that was answered with a 2xx. That is the age of the delivery when the attempt was loaded, on the database clock (section 7), plus the time that the request took, on the monotonic clock of the replica. A replayed delivery counts from its creation and not from its replay. The buckets end at the 72 h of a delivery.

One finalize that committed is counted as if it had not. When the answer to its commit is lost with the connection, the next try finds the lease gone (section 7), and a replica cannot tell that from a lease that another replica took over. The attempt row is written and the delivery may have reached its final state, but the replica counts a lost lease on `webhooks_finalize_lost_lease_total` and nothing on the four meters above.

`webhooks_claim_seconds` holds how long a claim took that the database answered, and `webhooks_claim_rows` how many rows it returned. Both hold a count, a sum and the largest value, without buckets. A claim that failed is counted on `webhooks_claim_errors_total` only.

`webhooks_poller_last_iteration_age_seconds` is the time since the heartbeat that liveness reads (section 15).

`webhooks_ingest_requests_total` counts a request that the ingest route answered with one of these results, once, when the request is over. `invalid` is a `400`, a `413` or a `422`, and `busy` is a `503`. Nothing else is counted:
- a request that is refused before it reaches the route, by authentication, by the request permits or for its media type;
- a request to any other route;
- a request that the ingest route answered with any other status. A `500` is such an answer, so a failure that the service maps to none of the results does not appear in this counter. The `415` for a charset other than UTF-8 (section 11.5) is another.

`QueueGauges` runs one aggregate query over the pending index on every replica, once when the replica starts and then 15 s after the end of each run. It uses the claim's eligibility predicates: `due` counts rows a claim could take, and `held` counts due rows behind an open circuit or a suspended tenant.

```sql
WITH pending AS (
  SELECT d.next_attempt_at,
         d.next_attempt_at <= now() AS due,
         d.lease_token IS NOT NULL  AS has_token,
         t.status = 'active'                                        -- as the claim decides
           AND (c.state = 'closed' OR c.next_probe_at <= now()) AS claimable
  FROM delivery d
  JOIN endpoint e         ON e.id = d.endpoint_id
  JOIN tenant t           ON t.id = e.tenant_id
  JOIN endpoint_circuit c ON c.endpoint_id = e.id
  WHERE d.state = 'pending' AND e.status = 'active'
)
SELECT count(*) FILTER (WHERE due AND claimable)         AS due,
       count(*) FILTER (WHERE due AND NOT claimable)     AS held,
       count(*) FILTER (WHERE NOT due AND NOT has_token) AS scheduled,
       count(*) FILTER (WHERE NOT due AND has_token)     AS leased,
       coalesce(extract(epoch FROM now() - min(next_attempt_at) FILTER (WHERE due AND claimable)), 0)
         AS oldest_due_age,
       (SELECT count(*) FROM endpoint_circuit c JOIN endpoint e ON e.id = c.endpoint_id
        WHERE c.state = 'open' AND e.status = 'active') AS circuits_open
FROM pending;
```

- The statement runs by itself, outside a transaction, on the `worker` pool, whose statement timeout of 10 s bounds it (limitation 27). It writes nothing and locks no row, so a run that outlives its replica leaves nothing behind (section 3).
- The four states are those of section 4. A delivery whose lease ran out with nothing recorded is `due`, because a claim could take it. A delivery that waits for the time of its next attempt is `scheduled`.
- An open circuit whose probe is due makes its endpoint eligible, so the due deliveries of that endpoint count as `due` until the gate has moved the probe time, and as `held` again after that.
- The bulkheads of a replica are not part of the predicates. They are the state of one replica, and the gauges count the queue of all of them.
- The pending deliveries of a deleted endpoint are in none of the four states. `CancelJob` cancels them, and `webhooks_delivery_terminal_total` counts them then.
- `webhooks_oldest_due_age_seconds` is how long the oldest `due` delivery has been due, and 0 when none is.
- `webhooks_circuits_open` counts the open circuits of active endpoints.
- A value is as old as the last run that the database answered. A run that fails is logged and leaves the gauges as they were (limitation 27). A replica that has read nothing yet shows no number.

Spring Boot adds meters of its own to the scrape, for the JVM, the connection pools and the HTTP server. `http_server_requests_seconds` names a route by its pattern, such as `/v1/deliveries/{id}`, and never by the path of a request.

**Traces** (OpenTelemetry, through the starter of Spring Boot).
- Spans are exported over OTLP once `management.opentelemetry.tracing.export.otlp.endpoint` names a collector. Without it no span leaves the replica.
- A replica samples the share of its traces that `management.tracing.sampling.probability` gives. The decision is made once, for the ingest span, and an attempt span is sampled when the stored context says that its ingest span was.
- Spring Boot would also take such settings from the `OTEL_` variables of the environment. `management.opentelemetry.map-environment-variables` is false, so a replica does not read them: a variable that a platform sets for every process neither switches the export of spans or meters on nor changes what is sampled.
- Ingest starts a new root span. It is the span that the tracing filter of Spring starts for the request, from which `UntrustedTraceContextFilter` has kept the trace context of the caller (section 11.7). `IngestService` attaches a syntactically valid caller `traceparent` to it as a span link.
- The ingest span's own context is stored in `event.trace_context`, in the form of a `traceparent` value.
- Each attempt is one span of kind CLIENT, a child of that stored context, written by hand in `WebhookHttpClient`. An event that was stored without a context gets a trace of its own for each attempt.
- Its attributes are delivery id (`webhooks.delivery_id`), attempt number (`webhooks.attempt`), probe (`webhooks.probe`), outcome (`webhooks.outcome`), status code (`http.response.status_code`) and the destination host (`server.address`). It never carries the URL, path or query.
- The outcome is `success` or `failure`, as in the attempt row. A request that was not sent, because the replica had no room for its lookup (section 7), is no attempt. Its span ends with the outcome `not_sent`.
- The HTTP client is not auto-instrumented, so no span or meter is derived from the request URL.
- Outbound requests carry the attempt span's `traceparent`, and no other trace header.

**Logs.**
- JSON through Spring Boot structured logging.
- Fields: `trace_id`, `span_id`, `tenant_id`, `event_id`, `delivery_id`, `endpoint_id`, `attempt`, `outcome`, `reason`, `state`, `host`, `replica`.
- A line that is logged while a route handles a request carries the trace id and the span id of the request, the replica and, on a tenant route, the tenant.
- A line about a delivery carries the ids of the tenant, the event, the delivery and the endpoint, the number of the attempt and the replica. A line about what came of a request to a subscriber also carries the trace id and the span id of the attempt span. The line that records an attempt adds its outcome, its reason, the state that it left the delivery in (`pending`, `succeeded` or `dead`) and the host that the request went to.
- Never logged: `Authorization` values, API keys, signing secrets, signatures, payload bodies, and any part of a subscriber URL other than its host.

## 11. Security

### 11.1 Authentication

- `ApiKeyFilter` decides by the first path segment, read the way Spring MVC matches it (decoded, without path parameters). `admin` takes the admin token, `actuator` is open, and every other path takes an API key. A route added later is therefore authenticated without being listed, and a path spelled `/%61dmin/tenants` cannot reach an admin route with an API key.
- Under `/actuator` a replica serves its health, its meters and an index that links the two, and nothing else. `ApiKeyAuthIT#actuatorIndexNeedsNoKeyAndLinksHealthAndTheScrapeOnly` reads the index.
- A missing, malformed or unknown credential gets the same `401` with `WWW-Authenticate: Bearer`.
- On a path that takes a credential, no body is read before the credential is checked. `ApiKeyFilter` goes by the path and the `Authorization` header, and the filter of Spring Boot that would read a form ahead of it is turned off (section 11.5). The actuator endpoints take no credential, and a JSON body that is sent to one is read, up to the limit on a request body (section 11.5).
- API keys are `rwh_` plus 32 random bytes in base64url, shown once. Anything of another form is rejected without a lookup.
- Only the SHA-256 is stored. A slow hash would protect nothing here, because the keys are high-entropy.
- Lookups are cached per replica for 30 s (positive results only), together with the tenant's status, rate and burst. The 30 s count from the start of the lookup.
- An eviction also discards the result of every lookup that was running when it happened. A request with the old key that read the tenant row just before a rotation therefore cannot put that key back into the cache.
- A suspended tenant is rejected with 403.
- `/admin/**` requires a static token from the environment, compared in constant time. `StartupInvariants` refuses to start without one of at least 32 characters.

### 11.2 Tenant scoping

- Every statement behind a tenant route carries `tenant_id = :t`. A mutating route whose first statement matches no row answers 404 and runs nothing else. The charge of a single replay comes before that statement and writes the bucket of the caller only. A bulk replay resolves its endpoint first and charges nothing for an id that it does not find.
- `SqlTenantScopeTest` scans the repository SQL constants and fails on a tenant-table statement without it. The exemption list holds worker and job statements only, by class.
  - A tenant table is every table but `tenant`, whose rows are addressed by their own id.
  - An `INSERT` that only supplies values is not checked. It creates a row and can neither read nor change a row of another tenant.
- `TenantIsolationIT#mutatingRoutesLeaveForeignTenantUntouched` calls every mutating route (patch, delete, rotate, single and bulk replay) with a foreign id. It snapshots the foreign tenant's endpoint, delivery, circuit and bucket rows before the call and asserts they are identical after it, in addition to the 404. The single replay with a foreign id is charged all the same, and its unit comes out of the bucket of the caller. Read routes are checked for the 404.

### 11.3 Signatures

The format follows Standard Webhooks.

| Header | Value |
|---|---|
| `webhook-id` | The delivery id, identical on every retry |
| `webhook-timestamp` | Unix seconds from the database clock, read in the load step, fresh per attempt |
| `webhook-signature` | `v1,<base64 HMAC-SHA256(secret, "{id}.{timestamp}.{body}")>` |

- The timestamp is inside the MAC input, and the body is the stored bytes.
- [api.md](api.md) quotes a reference verifier that rejects a timestamp more than 5 minutes away from the time of the check, whether it is older or lies ahead. Because the timestamp comes from Postgres, a replica with a wrong wall clock cannot get deliveries rejected.
- After rotation both signatures are sent, space separated, for 24 h, the one of the new secret first. Then the old secret is ignored and later purged. The database clock decides when the overlap ends: the load of an attempt (section 7) returns the old secret only while `prev_secret_expires_at > now()`.
- A rotation is one autocommit statement:

```sql
UPDATE endpoint
SET prev_secret = secret, prev_secret_expires_at = now() + interval '24 hours', secret = :secret
WHERE id = :id AND tenant_id = :t AND status = 'active';
-- 0 rows: 404
```

- One old secret is kept. A second rotation inside the 24 h replaces it: the secret of before the first rotation is no longer signed with, and a subscriber that still checks with it rejects every request from then on (limitation 33).
- A signing secret is `whsec_` plus 32 random bytes in base64, shown once. The 32 bytes are the MAC key.
- Secrets are encrypted at rest with AES-256-GCM under a key from the environment, with a one-byte key-version prefix. The key is 32 bytes, given in base64. A stored value is the version byte, a random 96-bit nonce, then the ciphertext with its tag.

### 11.4 SSRF and the outbound client

- `WebhookHttpClient` is the only class that builds an HTTP client; an `ArchitectureTest` rule enforces it.
- Its constructor requires a `DnsLookups`, and a `DnsLookups` cannot be built without an `AddressPolicy`. There is no default, so an unguarded client cannot be constructed.
- **Stateless client.** One client serves all tenants, so it must carry nothing from one request to the next. It is built with cookie management, automatic retries, auth caching, content compression and redirect handling all disabled, without proxy support and without system properties.
  - No cookie store: a `Set-Cookie` from one tenant's endpoint can never reach another tenant's request.
  - No automatic retries: the library default would re-send on 429 and 503, which would be a second request for one attempt row.
  - No compression: no `Accept-Encoding` is sent, so a compressed response is never inflated.
- **Bounded response.** The status line and each header line are limited to 8 KiB, counted with the two bytes that end a line, and a response to 100 headers. Exceeding either fails the attempt as `response_too_large`. The body is read through a bounded stream. At 64 KiB the connection is aborted, not drained.
- **Connect-time check.** `SsrfGuardDnsResolver` is the client's `DnsResolver`. It checks every address of the answer and returns the first, which is the address the socket connects to, so there is no gap between check and use, and DNS rebinding is covered. If any resolved address is blocked, the whole lookup is rejected. Reused keep-alive connections were validated when they were opened.
  - The client gets one address because of the attempt deadline. Given several, it tries them one after the other, each with its own connect timeout and its own TLS handshake, and the abort at the deadline ends only the one in progress. With one address the abort ends the attempt (limitation 36).
- **Address policy.** An explicit CIDR list, not JDK predicates.
  - IPv4 deny: 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.0.0.0/24, 192.0.2.0/24, 192.88.99.0/24, 192.168.0.0/16, 198.18.0.0/15, 198.51.100.0/24, 203.0.113.0/24, 224.0.0.0/4, 240.0.0.0/4.
  - IPv6: default deny outside 2000::/3. Inside it, deny 2001::/23 (Teredo and protocol assignments), 2001:db8::/32 and 2002::/16 (6to4).
  - The default deny already covers ::1, fc00::/7 (so fd00:ec2::254), fe80::/10, 64:ff9b::/96 and multicast. IPv4-mapped addresses are unwrapped and checked as IPv4.
- **Redirects** are disabled; a 3xx is a failed attempt.
- **Proxy.** `StartupInvariants` refuses to start if JVM proxy properties are set, because the guard would then validate the proxy and not the target.
- **URL policy at registration.** `https` only, port 443 or 1024 to 65535, no userinfo, at most 2048 characters, and the host must resolve to allowed addresses. The lookup runs before the create transaction opens.
- **No oracle.**
  - "Does not resolve" and "resolves to a blocked address" return the same `422 endpoint_url_not_allowed`. A URL that breaks a rule of form gets that answer too, without a lookup.
  - In the attempt log both appear as `destination_not_allowed`, with no status. The specific reason goes to operator metrics and logs only.
  - No response body is ever stored or returned, so the attempt log cannot read from a destination.
- **Bounded DNS.** All lookups, at registration and at delivery, go through `DnsLookups`.
  - It is a pool of 32 platform threads with a hard 2 s wait.
  - It checks every answer against the address policy and counts the lookup by its result. A caller gets either addresses it may connect to or a refusal: `busy`, `timeout`, `failed` or `blocked`.
  - An address literal takes the same path as a name. The resolver answers it with itself.
  - At most 4 lookups per tenant may be unreturned at once. Beyond that, or when the pool is full, the lookup answers `busy` at once.
  - `busy` is this replica's admission decision, not evidence about the endpoint. At delivery the row is released with a 2 s delay: no attempt is consumed, no attempt row is written and the circuit is not touched (section 7). At registration it is `429 lookup_busy`.
  - A timeout is different: the lookup was admitted and the endpoint's DNS did not answer. At delivery it is a failed attempt that counts toward the breaker, so a tarpit endpoint is down to one lookup per probe interval after five failures. At registration it is the same 422.
  - The tenant id reaches the resolver through the one `ThreadLocal` in the codebase, set and cleared in `WebhookHttpClient.send`.
- **Local allowance.** `webhooks.ssrf.allow-cidrs` and plain `http` (`webhooks.ssrf.allow-http`) exist for tests and docker compose. `StartupInvariants` refuses to start with either set unless the `unsafe-local` profile is active, and the allowlist size is exported as a gauge. An allowed range lets its addresses through at registration and at connect. With `http` allowed, an `http` URL must name its port, which has to be 443 or 1024 to 65535 like any other.

### 11.5 Limits

| Limit | Value |
|---|---|
| Request body | 256 KiB (413 above) |
| Encoding of a request body | UTF-8, without a byte order mark |
| Event type | 128 characters of `[a-z0-9_.-]` |
| Idempotency key | 255 characters |
| Number in an event body | 1,000 digits, those of its exponent included, and an exponent that fits a Java `BigDecimal`, which is about plus or minus 2.1 billion |
| Member name in a request body | 1,024 bytes of UTF-8 |
| Nesting of an event body | 500 levels, the body itself being the first |
| Event-type filter | 1 to 20 entries, each `*` or an event type |
| Endpoints per tenant | `max_endpoints`, default 20, at most 50 |
| Response status line and each header line | 8 KiB |
| Response headers | 100 |
| Response body read | 64 KiB, then the connection is aborted |
| List page | 100 rows |
| Attempt log per read | One run (at most 13 rows) |

`RequestBodyLimit` enforces the limit on a request body where a route reads its body, which is the only place where the service reads one. Left as they ship, Spring Boot and Tomcat read a body in three more places, each ahead of that one and none with its limit. No route takes a form or a multipart body, so all three are turned off:

- **Form content.** Left on, `FormContentFilter` runs ahead of `ApiKeyFilter`. For a `PUT`, `PATCH` or `DELETE` whose `Content-Type` is `application/x-www-form-urlencoded` it reads the whole body into memory, on any path and before the request is authenticated. `spring.mvc.formcontent.filter.enabled` is false, which turns it off. `RequestBodyLimitIT#formBodyIsNotReadBeforeAuthentication` sends such a request without a credential and never ends its body: the `401` arrives while the body is still open. The test sends a `POST` in the same way and gets the same answer. `FormContentFilter` leaves a `POST` alone, and no single setting decides that answer. `HiddenHttpMethodFilter`, which asks for a parameter of a `POST` ahead of authentication, is off by default. Turned on, it would find no form, because the connector reads no body for parameters (next bullet).
- **Form parameters.** Tomcat reads the body of a `POST` that is sent as a form, up to 2 MB of it, when the parameters of the request are first asked for. Spring asks for them when a route is about to read the body of such a request, which is behind authentication and ahead of the limit. No property turns that off, so `RequestBodyLimit` tells the connector of Tomcat to read no body for parameters. `RequestBodyLimitIT#bodyThatNoRouteTakesIsRefusedWithoutBeingRead` sends a registration as a form that never ends, with the key of a tenant: the `415` arrives while the body is still open.
- **Multipart.** Left on, Spring parses a multipart body before it looks for the route of the request, on any path, up to the 10 MB that Spring Boot allows a request. Where a path takes a credential, that is behind authentication. On the actuator endpoints, which take none, it is behind nothing. `spring.servlet.multipart.enabled` is false, which turns it off. `RequestBodyLimitIT#bodyThatNoRouteTakesIsRefusedWithoutBeingRead` sends its registration as a multipart body as well, and `#multipartBodyIsNotReadWhereNoKeyIsTaken` sends such a body to the scrape without a key. Both are answered while the body is still open, the one with a `415` and the other with a `200`.

The actuator endpoints take no credential, and the handler of Spring Boot for them reads a JSON body that is sent to one, also with a `GET`. It reads that body through `RequestBodyLimit`, as a route reads its own, so the limit holds without a key as well. `RequestBodyLimitIT#limitHoldsWhereNoKeyIsTaken` sends the scrape a JSON body of 256 KiB without a key, which is answered with a `200`, and one of a byte more, which is answered with a `413`.

The three limits on an event body are set in the code and not left to the defaults of the JSON library. A body beyond one of them answers `400`, like a body that is not JSON.

The limit on a member name also holds on the other routes that read a body, where it is set for the JSON mapper of the API. It is far below the 50,000 bytes that the library allows, for this reason: the parser keeps the member names it has read in a table that all requests share, so a name stays in memory after its request has ended. The limit bounds what one name can add to that table. The library bounds the number of names in it: once the table holds more than 6,000, it is emptied. That number cannot be set, so `EventRequestTest#memberNameIsKeptAfterItsBodyAndDroppedOnceMoreThanSixThousandAreKept` holds the library to it for an event body. A name of 1,024 bytes is still in the table after its request, and it has left the table after 6,001 further bodies with one new name each.

Every request body is read as UTF-8, the one encoding that RFC 8259 allows. The limits above depend on it. The library has a second parser, for characters. That parser counts a member name by its characters and the digits of a number in another way, and it keeps the names it has read in a second table. A name of 1,024 euro signs, which is 3,072 bytes, passes it, and so does a fraction of 1,001 digits. `EventRequestTest#bodyOutsideTheLimitsDoesNotParseInAnotherEncoding` holds the library to that: a parser that has the limits of an event body and is left to detect the encoding refuses both in UTF-8 and reads both in UTF-16. A body reaches the second parser in two ways, and both are closed:

- **Detection.** Left to itself, the library detects UTF-16 and UTF-32 by the first bytes of a body and reads such a body as characters. The detection is turned off, for the parser of an event body and for the JSON mapper of the API. A body in UTF-16 or UTF-32 is then not JSON and answers `400`. So does a body in UTF-8 that starts with a byte order mark, because the detection was what skipped the mark. RFC 8259 forbids a sender the mark and lets a parser refuse it. `EventRequestTest#bodyInUtf16OrUtf32DoesNotParse`, `#bodyInUtf8ThatStartsWithAByteOrderMarkDoesNotParse` and `#bodyOutsideTheLimitsDoesNotParseInAnotherEncoding` show it for the parser, `EventApiIT#eventBodyInUtf16OrUtf32IsRejected` and `#eventBodyThatStartsWithAByteOrderMarkIsRejected` for ingest, and `RequestBodyCharsetIT#registrationInUtf16OrUtf32IsRejected` and `#registrationThatStartsWithAByteOrderMarkIsRejected` for a route that the JSON mapper of the API reads.
- **Declared charset.** The JSON converter of Spring decodes a body itself when the `Content-Type` names a charset that is not a Unicode one, and hands the library characters. `RequestBodyCharset` therefore refuses a body whose `Content-Type` names any charset other than UTF-8, with a `415`, on every route that reads a body. A `Content-Type` without a charset, or with UTF-8, is read. `RequestBodyCharsetIT#bodyDeclaredInACharsetOtherThanUtf8IsRefused` and `#bodyDeclaredAsUtf8IsRead` show it for ingest and for a registration. The check reads the header only and comes before the limit on the size of a body, so a request that breaks both rules answers `415`: `RequestBodyCharsetIT#bodyAboveTheLimitDeclaredInAnotherCharsetIsRefusedForItsCharset`.

A body in another encoding whose `Content-Type` does not say so is read as UTF-8 like any other. Where that leaves a UTF-8 sequence that breaks off, as at the `é` of `café` in ISO-8859-1, an event body is not JSON and answers `400`. `EventRequestTest#bodyWhoseUtf8SequenceBreaksOffDoesNotParse` shows it for the parser and `EventApiIT#eventBodyWhoseUtf8SequenceBreaksOffIsRejected` for ingest. Beyond that, what the decoder of the library accepts is read. The service makes no check of its own that a body is well-formed UTF-8.

The endpoint cap is exact, and an endpoint is never created without its circuit row:

```sql
-- POST /v1/endpoints, one transaction
SELECT max_endpoints FROM tenant WHERE id = :t FOR NO KEY UPDATE;
SELECT count(*) FROM endpoint WHERE tenant_id = :t AND status = 'active';   -- at the cap: 409
INSERT INTO endpoint (tenant_id, url, event_types, secret) VALUES (...) RETURNING id;
INSERT INTO endpoint_circuit (endpoint_id) VALUES (:id);
```

`FOR NO KEY UPDATE` serialises creates without blocking the tenant's ingest.

Each of the two create transactions, this one and the one of a tenant (section 5), is all or nothing. `EndpointApiIT#failedCircuitInsertLeavesNoEndpoint` and `AdminApiIT#failedBucketInsertLeavesNoTenant` fail the second insert behind a table lock of the test and find no row of the first.

### 11.6 Telemetry hygiene

The redaction list of section 10 applies to logs, spans and metrics. Three tests assert it with a subscriber URL that carries a marker in its path and query:
- `LogFormatIT#subscriberUrlIsReducedToHost`
- `TracingIT#attemptSpanCarriesHostOnly`
- `MetricsIT#noMeterHasHostOrUrlLabel`, which scans the whole scrape for the marker, the receiver host and the ids.

The first of them reads every line written by a replica that runs as a process of its own (section 16). It looks in those lines for the rest of the list as well: the API key, the admin token, the signing secret, the signatures of the requests and the payload of the event.

### 11.7 Untrusted trace context

- `UntrustedTraceContextFilter` runs before the tracing filter on public routes, which are all the routes of a replica. It strips the W3C headers `traceparent`, `tracestate` and `baggage`, and the B3 headers: `b3`, and `x-b3-traceid`, `x-b3-spanid`, `x-b3-parentspanid`, `x-b3-sampled` and `x-b3-flags`.
- It keeps the `traceparent` only if it is version 00, 55 characters, lowercase hex with non-zero ids, and hands that to `IngestService` as a span link. A request with more than one `traceparent` has none that is kept.
- Spring Boot reads the W3C headers and both forms of B3 unless `management.tracing.propagation.type` tells it to read fewer. The filter strips them all and does not go by that setting, so no value of it lets the context of a caller reach the tracing filter.
- The caller therefore cannot set the trace id or the sampling decision.

## 12. Rate limiting, tenant isolation and database time bounds

**Unit.** The bucket counts what an event costs the system: deliveries and stored bytes.
- An event costs `max(1, matched endpoints) + floor(body_bytes / 4096)`. A small event to one endpoint costs 1. A 256 KiB event costs 64 more than a small one.
- A replayed delivery costs 1. It stores nothing new.
- Retries are not charged. They are bounded at 12 per delivery by the schedule and reduced to probes by the breaker.
- The stored body is therefore bounded by `rate * 4 KiB` per second per tenant. At the default rate that is about 16 GiB per day and about 500 GiB over the 30-day retention, before row, index and WAL overhead. The README states this figure, and the operator chooses the rate with it in mind.

**Bucket.** One autocommit statement on the `api` pool, never inside the ingest transaction:

```sql
UPDATE rate_bucket b
SET tokens = least(t.burst, b.tokens + extract(epoch FROM now() - b.refilled_at) * t.rate_per_sec) - :cost,
    refilled_at = now()
FROM tenant t
WHERE t.id = b.tenant_id AND b.tenant_id = :t
  AND least(t.burst, b.tokens + extract(epoch FROM now() - b.refilled_at) * t.rate_per_sec) >= :cost
RETURNING b.tokens;
```

- One row means admitted. Zero rows means `429`.
- The 429 is `rate_limited` with `Retry-After: ceil(cost / rate)` when waiting can help.
- It is `rate_limit_unsatisfiable` without `Retry-After` when the tenant's rate is 0 or the cost exceeds its burst.
- The admission decision is the statement. The type and header are computed from the tenant's cached rate and burst.
- The row lock makes the limit exact across replicas: admitted units never exceed `burst + rate * elapsed`.
- The limiter errs on the strict side. A charge is not refunded if the following transaction fails, and the loser of a concurrent duplicate pays once.

**Isolation layers**, outermost first:

| Layer | Value | Effect |
|---|---|---|
| Request permits (`TenantConcurrencyFilter`) | 8 per tenant per replica, all tenant routes | Beyond 8 concurrent requests a tenant gets `429` without acquiring a database connection, unless its key had to be looked up first (section 6). The routes of a tenant can hold at most 8 of the 16 `api` connections. |
| Token bucket | Per tenant, in Postgres | The exact limit above. It runs after the idempotency lookup, so a retry of an accepted event is neither charged nor rejected. |
| Two pools | `api` 16, `worker` 16 | Ingest pressure cannot delay claim or finalize, so it cannot cause lease overruns. |
| Bounded reads | Keyset pages of 100, required filters, `statement_timeout` | A tenant route cannot run an unbounded statement on the shared primary. |
| Claim fairness | One row per endpoint, tenants round-robin | Section 7. |
| Delivery bulkheads | 4 in flight per endpoint, 16 per tenant, per replica | Section 7. |
| DNS caps | 4 unreturned lookups per tenant, 2 s wait | Section 11.4. |

**Database time bounds.** `lock_timeout` alone does not bound a call whose connection has gone silent. Each pool sets all of these, and `StartupInvariants` checks them:

| Bound | `api` | `worker` | Set by |
|---|---|---|---|
| Borrow wait (`connectionTimeout`) | 3 s | 3 s | HikariCP |
| TCP connect (`connectTimeout`) | 2 s | 2 s | pgjdbc |
| `lock_timeout` | 2 s | 5 s | session |
| `statement_timeout` | 5 s | 10 s | session |
| Socket read (`socketTimeout`) | 10 s | 15 s | pgjdbc |
| `idle_in_transaction_session_timeout` | 10 s | 10 s | session |
| `tcpKeepAlive` | on | on | pgjdbc |

- The order `lock_timeout < statement_timeout < socketTimeout` means the server answers with an error before the client gives up on the socket.
- The socket timeout is the backstop for a connection that carries no answer at all. After a failover or partition, connections can stay half-open with no FIN or RST. Without it the single poller thread would block in a read forever while the pod stays Ready.
- `idle_in_transaction_session_timeout` is the server-side half of the same failure. A transaction whose client vanished without closing is ended by the server, so its row locks cannot pin a delivery that `SKIP LOCKED` claimers would otherwise skip forever.
- HikariCP tests a connection that was idle for more than 500 ms before it hands it out, and looks at the borrow wait again only after that test. A borrow that meets a dead connection can therefore end one test later than its wait. Both pools limit this aliveness test to 1 s (`validationTimeout`). The value is fixed in the code and replaces HikariCP's default of 5 s.
- Borrow wait plus aliveness test plus socket timeout (19 s) fits inside the 35 s start cutoff, so one stuck call cannot use up a lease.
- The three session limits are sent as startup options of the connection. They are the session defaults, so `RESET ALL` leaves them in place.
- A pool applies its bounds after it has read the JDBC URL, so a parameter in the URL cannot remove one.
- `StartupInvariants` reads the session limits from a live session of each pool, not from the configuration.
- On tenant routes these errors map to `503 database_busy`, like every transient error of section 7.

## 13. Failure table

`docs/failure-table.md` has the same rows and is the copy that the build checks. `FailureTableCopiesTest` holds the two to each other: this table in all but its last column, and the kill table of section 3 in full. `docs/failure-table.md` says at more length how each test forces its failure, and what each proof does and does not cover. The README shows ten of the rows, cut to their main claims. `FailureTableCheck` (section 16) fails the build unless every test linked from a table in either of those two files ran and passed in the same build. A row keeps its number, and number 29 is not in use.

| # | Failure | Proven behaviour | Test | Forced by |
|---|---|---|---|---|
| 1 | Worker killed mid-delivery | Redelivered after the lease; attempt 1 `unknown`, attempt 2 `success` | `WorkerKillIT#killedMidRequest` | Receiver holds the request; SIGKILL; lease aged |
| 2 | Crash after subscriber 200, before commit | Exactly two requests with the same `webhook-id`; one `succeeded` | `WorkerKillIT#killedAfterResponseBeforeCommit` | Receiver holds the request while the test locks the delivery row, then answers 200; F1 blocks; SIGKILL once the block is seen |
| 3 | Worker killed after claim, before send | Zero requests from the dead replica; delivered by a replica started after the kill; attempt 1 `unknown` | `WorkerKillIT#killedAfterClaimBeforeSend` | Test `ACCESS EXCLUSIVE` lock on `event` blocks the load of an ordinary closed-circuit delivery |
| 4 | Replica killed during ingest | Nothing or everything; retry with the same key yields one event | `IngestKillIT#killedBeforeCommit`, `#killedAfterCommitBeforeResponse` | Test lock on the endpoint row blocks fan-out |
| 5 | Duplicate submit | One event; same id returned; different body gets 422 | `IdempotencyIT#sameKeyReturnsSameEvent`, `#concurrentSameKeyCreatesOneEvent`, `#sameKeyDifferentBodyIsRejected` | Request 1 blocked in its transaction; request 2 seen waiting on the unique index |
| 6 | Two claimers race for one delivery | One claimer gets it; the other skips it without waiting; four pollers on one database deliver each of 2,000 deliveries once | `ClaimRaceIT#secondClaimerSkipsLockedRow`, `#fourReplicasDeliverEachEventOnce` | Two open transactions; then 4 pollers over 2,000 deliveries |
| 7 | Lease expires while its holder is still sending | The delivery is claimed again and both requests are sent; only the result of the lease holder is recorded | `LeaseFenceIT#lateFinalizeAfterReclaimChangesNothing`, `#repeatedFinalizeIsNoOp` | Lease aged between claim and finalize |
| 8 | Database restart | No delivery lost; the delivery in flight across the restart is recorded once from one request; ingest answers 503 and the same key succeeds afterwards; replicas stay up | `DatabaseRestartIT#inFlightAndQueuedSurviveRestart`, `DatabaseFailuresTest`, `FinalizeRetryTest`, `ProblemsTest#failedConnectionIsDatabaseBusy` | Receiver holds the four requests of each of two replicas, with two more deliveries due and unclaimed; the container is stopped, the receiver released and an event refused; the container is started again on the host port that it had |
| 9 | Endpoint down for hours | Circuit opens after 5 failures; held deliveries use no attempts; one probe per interval across concurrent claimers; probe backoff stays capped; recovery drains | `EndpointOutageIT#circuitOpensAndHolds`, `#oneProbeAdmittedAcrossConcurrentClaimers`, `#probeBackoffIsCappedAfterManyFailures`, `#recoveryDrainsHeldDeliveries`, `#probeSlotReopensAfterProberKilled` | Two connections on the gate; probe time aged; `probe_failures` set to 1000; a canary on a second endpoint witnesses that claims ran |
| 10 | Endpoint down for good | A single delivery ends `dead (exhausted)` after 5 attempts and 7 probes; a backlog ends `dead (expired)` at 72 h; a delivery whose holder was killed expires with that attempt logged `unknown`; replay delivers and its run starts at attempt 1 | `EndpointOutageIT#hardDownEndpointWithDefaultBreakerSettings`, `#heldDeliveriesExpireAfterBudget`, `#expiredAfterHolderKilledReplaysCleanly`, `ExpiryIT#pendingPastBudgetBecomesDead` | Production breaker and retry settings; rows aged |
| 11 | One event always rejected by a healthy endpoint | Twelve attempts, each wait inside its schedule window, then `dead (exhausted)`; replay delivers once | `DeadLetterReplayIT#exhaustedAfterTwelveAttempts`, `#replayedDeliveryIsDeliveredOnce`, `RetryScheduleTest` | Breaker at defaults; other deliveries succeed in between |
| 12 | A delivery whose every claim ends without a record | Dead-lettered on the 13th claim without a send | `PoisonDeliveryIT#thirteenthClaimDeadLetters` | Twelve aged leases |
| 13 | Slow endpoint starving others | At most 4 concurrent requests per replica to it; other endpoints complete meanwhile; a drip-fed response is cut at the deadline | `SlowEndpointIsolationIT#slowEndpointDoesNotDelayOthers`, `#drippingResponseIsCutAtDeadline` | Receiver holds every request to the slow endpoint until the test ends |
| 14 | Subscriber DNS that never answers | Attempt fails inside the DNS budget; at most 4 stuck lookups per tenant; other tenants complete; the tenant's healthy endpoints lose no attempt and keep a closed circuit | `SlowEndpointIsolationIT#slowDnsDoesNotStarveOthers`, `#stuckDnsOfOneEndpointDoesNotFailSiblingEndpoints`, `SsrfIT#registrationDnsLookupIsBounded` | `StubDns` delegate that blocks until the test releases it |
| 15 | One tenant's backlog starving another tenant | Held case: tenant A never exceeds 16 in flight per replica and tenant B completes meanwhile. Fast case: with a deep backlog on A's instantly answering endpoint, B's request arrives before A's 200th of 2,000 | `TenantIsolationIT#backlogOfOneTenantDoesNotStarveAnother`, `#deepBacklogOnFastEndpointDoesNotStarveAnother` | Held case: A has 5 endpoints with 4 deliveries each. The receiver holds the 4 requests of each of four endpoints, and the deliveries for the fifth stay due and unclaimed, so the tenant cap binds where no endpoint cap does. Fast case: 2,000 aged due rows for A, one newer row for B; ordering read from the receiver's request log |
| 16 | Tenant floods ingest | Admitted units equal the bucket exactly across replicas; large bodies cost more; the 9th concurrent request gets 429 without acquiring a database connection; an unsatisfiable request gets 429 without `Retry-After`; tenant B is served | `RateLimitIT#limitIsExactAcrossReplicas`, `#refillIsExact`, `#fanOutIsChargedPerDelivery`, `#largeBodiesAreChargedBySize`, `#costAboveBurstIsUnsatisfiable`, `#ninthConcurrentRequestTakesNoConnection`, `#floodingTenantDoesNotBlockOtherTenant` | Rate 0 and fixed burst; charges of four replicas seen waiting for a bucket row that the test holds; eight requests with unfinished bodies hold A's permits; A's bucket row locked by the test |
| 17 | Tenant replays a huge dead-letter backlog | At most `limit` rows per call, charged per row, spread over 60 s | `ReplayIT#bulkReplayIsBoundedAndCharged` | Dead rows inserted directly |
| 18 | Subscriber URL points at, or rebinds to, a private address | Rejected at registration; blocked at connect with zero accepted connections; redirects not followed | `SsrfIT#registrationRejectsPrivateTargets`, `#rebindingIsBlockedAtConnect`, `#redirectIsNotFollowed`, `AddressPolicyTest`, `DnsLookupsTest#systemResolverDoesNotLetADeniedAddressLiteralPast` | `StubDns` answers public first, loopback later; listener counts accepts; denied address literals given to the system resolver |
| 19 | Absurd `Retry-After` | Clamped to between the own delay and one hour | `RetryAfterIT#clampedAtBothEnds` | Receiver sends 0, 3601 and 10^9 |
| 20 | Rolling deploy (SIGTERM) | Nothing is claimed after the poller stopped; in-flight attempts finish and are recorded; the process exits by itself with code 143 before lease expiry | `GracefulShutdownIT#drainsInFlightAndExits` | Receiver holds 4 requests; a 5th delivery is due and unclaimed only because the endpoint is full; SIGTERM; `poller_stopped` seen; receiver released |
| 21 | Endpoint deleted while deliveries are pending or being created | Delivery to it stops when the delete commits; pending rows become `cancelled`; nothing is created or re-armed for it afterwards; a delete by another tenant changes nothing | `EndpointDeleteIT#deleteStopsDeliveryAndCancelsPending`, `#deliveryClaimedBeforeTheDeleteIsNotSentAfterIt`, `#ingestRacingDeleteLeavesNoOrphan`, `#replayRacingDeleteLeavesNoOrphan`, `TenantIsolationIT#mutatingRoutesLeaveForeignTenantUntouched` | Receiver holds 4 requests with 2 rows still due; canary as witness; test lock on `event` holds a claimed delivery in its load while the endpoint is deleted; both race orders forced with two test-managed transactions |
| 22 | Replay racing retention | A replayed delivery is never purged | `RetentionIT#replayedDeliveryIsNotPurged`, `#purgesOnlyOldTerminalRows` | Replay holds the row uncommitted; a retention run returns without waiting and leaves it; replay commits; a second run leaves it as well; each run deletes another delivery of the same age, which is the witness that it ran |
| 23 | Secret rotation with deliveries in flight | Both signatures during the overlap; old secret rejected after it | `SignatureIT#bothSignaturesDuringOverlap` | Expiry aged |
| 24 | Tampered body or replayed old request | Reference verifier rejects both | `SignatureIT#verifierRejectsTamperedBodyAndStaleTimestamp` | Direct |
| 25 | Caller forces trace sampling or picks a trace id | With sampling off, a caller's sampled flag exports nothing; with sampling on, the root span has its own trace id and carries the caller's context only as a link | `TracingIT#callerCannotForceSampling`, `#callerTraceIdBecomesLinkOnly` | Sampler probability 0, then 1; in-memory span exporter |
| 26 | Connections to Postgres go half-open (failover, partition) | The cut-off replica's blocked call fails at the socket timeout and it resumes on new connections; another replica delivers meanwhile; a transaction left open on the server is ended by the server, so its row lock cannot pin a delivery | `DatabaseBlackholeIT#pollerResumesAfterSocketTimeout`, `#openTransactionIsEndedByServer` | `TcpProxy` stops relaying without closing, after `PgWait` shows the replica inside a statement |
| 27 | Subscriber answers with an endless header line or body | An oversized head fails the attempt as `response_too_large`, not `deadline`; a 2xx with an endless body is a success and the connection is dropped at the limit; another tenant's delivery completes meanwhile | `OutboundRequestIT#endlessHeaderIsCutAtLimit`, `#endlessBodyIsNotDrained`, `WebhookHttpClientIT#bodyOfSixtyFourKibibytesIsReadAndOneByteMoreCostsTheConnection` | `FaultReceiver` scripts `endlessHeader` and `endlessChunkedBody`; a body in chunks is one that a client could read to its end to use the connection again; the receiver records bytes written and who closed; bodies of 64 KiB and of one byte more place the limit |
| 28 | Subscriber answers 503 with `Set-Cookie` | Exactly one request per attempt row; no later request, from any tenant, carries a `Cookie` header | `OutboundRequestIT#oneRequestPerAttemptAndNoCookieReuse` | Two tenants' endpoints on one receiver host |
| 30 | Abusive tenant must be stopped | After suspension nothing more is claimed for the tenant; its key is rejected; held rows are untouched and deliver after resume | `AdminApiIT#suspendStopsOutboundDeliveries`, `#resumeDeliversHeldDeliveries` | Receiver holds the tenant's 4 in-flight requests; a canary of another tenant witnesses that claims ran |
| 31 | Tenant issues expensive reads, or a statement blocks | Lists are keyset pages of at most 100 rows behind a required endpoint and state; a blocked statement answers 503 when the lock timeout ends its wait, before the statement timeout | `DeliveryApiIT#listIsKeysetPagedAndBounded`, `ApiTimeoutIT#blockedStatementAnswers503`, `ProblemsTest` | Test row lock held past `lock_timeout` |
| 32 | Subscriber URL carries a secret in its path or query | The marker appears in no log line, no span and no meter | `LogFormatIT#subscriberUrlIsReducedToHost`, `TracingIT#attemptSpanCarriesHostOnly`, `MetricsIT#noMeterHasHostOrUrlLabel` | URL with a marker in path and query; full scrape and all exported spans scanned |

## 14. Limitations

1. **No ordering.** Retries, holds and replays reorder deliveries.
2. **Duplicates happen** (K4, K5, lease overrun). Subscribers must dedupe on `webhook-id`.
3. **Postgres is the single point of failure.** A failover to an asynchronous replica can lose acknowledged events. High availability is a deployment concern.
4. **Claim cost grows with the number of active endpoints, and with the backlog when PostgreSQL does not read it through `delivery_head`.**
   - With the plan that the claim is written for, every claim does one index probe per eligible endpoint and locks one head row per endpoint with due work.
   - Nothing makes PostgreSQL choose that plan. The pending deliveries have two partial indexes, `delivery_head` and `delivery_expiry`. Both look empty to the planner while the statistics of the table are from a time when next to nothing was pending, which is the usual state of a queue that keeps up. It has then been seen to choose `delivery_expiry`, which is ordered by another column. Each probe reads every pending delivery of every endpoint and sorts those of its own endpoint, so a claim costs as much as the queue is deep, once for each eligible endpoint, and a backlog is drained slowly until the table is analysed again. [performance.md](performance.md) has such a drain, with the plan of the claim before and after.
   - With that plan the backlog of one tenant is paid for by every tenant. The index holds the pending deliveries of all of them, held ones among them, and a replica makes one claim for all tenants, so each of their deliveries waits for a claim that reads the whole queue. The `worker` pool ends a statement after 10 s (section 12). A claim that would take longer fails, and a replica then claims nothing, for any tenant, for as long as the claim is that slow. The drain in performance.md has one tenant with one endpoint and 50,000 pending deliveries, so no run shows what another tenant waits, and none came near those 10 s.
   - The service has no table of the endpoints with due work, which would let a claim read those alone, and the queue table has no time partitions.
   - The queue table needs aggressive autovacuum.
   - Retention is batched deletes.
5. **Bulkheads are per replica.** Fleet-wide concurrency to one endpoint is at most 4 times the replica count; it is not an exact number.
6. **Throughput per endpoint and per tenant is capped by design.**
   - One endpoint drains at one delivery per claim round trip per replica, at most 4 at a time.
   - A tenant can use 16 of a replica's 64 workers unless the cap is raised.
   - When more tenants have due work than a replica has free permits, the tenants with the oldest head rows go first. Fairness is per claim, not a quota over time.
7. **The rate limiter is strict-side.** Failed ingests are not refunded. Retries are not charged. A charge that has waited for the bucket row computes its refill from the `refilled_at` that the charge before it wrote. Where that charge began later than the one that waited, or the clock of the database was set back in between, the time since the last refill is negative and takes tokens away, and a charge that began more than `tokens / rate` seconds before the charge that overtook it is refused although the bucket holds enough. `RateLimitIT#chargeBehindTheLastRefillIsRefusedAlthoughTheBucketHoldsEnough` moves the last refill of a bucket an hour ahead, and an event is refused that the bucket could pay for. A request that the bucket refuses has passed the request permits and has run its statements up to the charge: three for an event, one for a single replay and three for a bulk replay. The type and `Retry-After` of a 429 come from a tenant record cached up to 30 s.
8. **Isolation covers a bounded number of misbehaving tenants.** Two tenants each holding 8 request permits can fill the `api` pool. Eight tenants with tarpit DNS can fill the DNS pool. The 8 permits are fixed and the size of the `api` pool is a setting: in a pool of 8 connections or fewer a single tenant can hold every connection, and no startup invariant refuses such a pool.
9. **Unauthenticated floods** (invalid keys, slow request bodies) reach the service and, for the key lookup, the database. That belongs to the ingress.
10. **Endpoint ownership is not verified.** A tenant can aim signed POSTs at any public host, up to its own rate. There is no per-destination limit across endpoints. The operator's lever is suspension.
11. **The SSRF policy works at the IP layer.** It cannot see an internal service on public address space or a public proxy that forwards inward. HTTP proxies are unsupported.
12. **A stuck native DNS lookup holds its thread** until the OS resolver gives up. The image asks the resolver for a shorter wait (section 15): it sets `RES_OPTIONS` to `timeout:1 attempts:2`. By `resolv.conf(5)` the resolver of glibc reads the variable and then waits 1 s for a name server and asks it twice, where its defaults are 5 s and twice. That is for each name server and each name that the resolver tries, and a search list makes it try several names. No test measures the wait: the options are set, and what they do is what the manual of the resolver says. An image on another C library would not read the variable, and would have to get the options through `/etc/resolv.conf`, which the container runtime writes.
13. **The breaker counts consecutive failures only.** An endpoint that fails most of the time, or is slow but successful, never opens. Backoff and the bulkhead are the only protection there.
14. **Probes consume an attempt** of whichever held delivery wins the gate.
15. **A delete or a suspension does not recall a request already claimed.** If the cancel wins the race, the result of such a request is not recorded.
16. **An `unknown` attempt cannot say whether the subscriber processed the request.** It is also what a worker leaves when it passed its start cutoff and never sent.
17. **No response bodies are stored.** This is safer and harder to debug. A 2xx whose body exceeds the limit still counts as success.
18. **Credentials.**
    - One static admin token; no roles, no audit log.
    - One API key per tenant; rotation is immediate.
    - Revocation and suspension reach the key cache of other replicas within 30 s. Outbound delivery stops at the next claim on every replica.
    - No online rotation of the secret-encryption key.
19. **The idempotency window equals retention** (30 days).
20. **JSON payloads only**, at most 256 KiB. The cap is global, not per tenant.
21. **Producer trace continuity is a link**, not a parent-child relation.
22. **`FailureTableCheck` proves the linked tests ran and passed.** It cannot prove their assertions are strong.
23. **A jump of the database clock shifts every schedule.** A forward jump can expire leases early, which produces fenced duplicates and no loss. A backward jump between the creation of a delivery and the load of its attempt gives the delivery a negative age. The timer behind `webhooks_delivery_end_to_end_seconds` records no negative time, so a delivery whose age and request time together come out negative is missing from it and is counted on `webhooks_delivery_terminal_total` alone (`DeliveryWorkerTest#deliveryWhoseTimeSinceItsCreationComesOutNegativeIsCountedAndIsNotInTheTimer`).
24. **Rows removed or changed by hand are not detected.** An endpoint whose circuit row or a tenant whose bucket row was deleted in SQL stops working silently. A rate written in SQL that the admin route would refuse can fail the bucket statement, and every event of that tenant then answers `500`. The service only guarantees it never creates that state.
25. **A replica-local fault is recorded as endpoint failure.**
    - A replica that still reaches Postgres but has lost its egress fails every attempt quickly.
    - It consumes attempts and can open circuits for healthy endpoints fleet-wide. Nothing is lost (`dead` is replayable), but deliveries are delayed.
    - The signal is a per-replica skew in `webhooks_delivery_attempts_total{reason="connect"}`. There is no self-quarantine.
26. **Cancelling after a delete is asynchronous.** Delivery stops at commit, but rows show `pending` until `CancelJob` reaches them (30 s interval, 1,000 rows per batch).
27. **Time bounds cut both ways.**
    - A legitimate statement slower than `statement_timeout` fails. On a very large pending set the gauge query times out and the gauges go stale.
    - A transaction whose client was cut off pins its row locks for up to 10 s.
    - A call on a dead connection is noticed only after the socket timeout.
    - The other connections of a pool that went half-open are still in it then. Each costs a borrower one aliveness test of 1 s, and a borrow wait of 3 s has room for three of them. A pool of 16 therefore needs about 20 s more until a claim gets a connection that works.
28. **An orphaned autocommit statement can commit after its client died** (section 3). The design tolerates this; it does not prevent it.
29. **DNS admission is per tenant per replica.**
    - While a tenant's lookup slots are stuck, each of its other deliveries on that replica is claimed, refused its lookup and given back for 2 s. The delivery loses no attempt, but every time it comes due it costs the replica and the database one claim, one load and one release.
    - The poller claims again at once after a claim that returned a row. With a deep backlog for that tenant it therefore claims and gives back without a pause for as long as the slots are taken. Other tenants still get their rows in every claim.
    - Its registrations answer 429 during that time.
    - A probe that wins the gate and is then refused a lookup wastes that probe slot.
    - A fifth lookup is refused also when the four before it are not stuck and have only not returned yet. A tenant with more than four attempts that need a lookup at the same moment has the others re-queued for 2 s. If that leaves it with 13 to 15 attempts in flight, it counts as full and stays below its 16 until one of them ends.
30. **No storage quota.** The byte-weighted charge bounds what a tenant can store per second (section 12). There is no cap on stored bytes or on pending rows beyond rate times retention.
31. **The delivery list is narrow.** It needs an endpoint and a state. There is no tenant-wide listing, and the attempt log is read one run at a time.
32. **A request on a connection that the subscriber has just closed fails.** The client keeps connections for reuse and repeats no request by itself, so such a request is a failed attempt and waits for its retry. A connection that was idle for more than 2 s is tested before it is used again, which leaves the short window after a request.
33. **One old signing secret is kept.** A second rotation inside the 24 h of the first ends that overlap early, and so does the repeat of a rotation whose response was lost. A subscriber that still checks with the secret from before the first rotation rejects every request from then on. The deliveries that it rejects are retried on the schedule until it has a secret that is signed with.
34. **The scrape is open to whoever reaches the port.** `/actuator/prometheus` takes no key and is served on the port of the API. It holds no tenant, endpoint or delivery, but it does show the state of the queue and the rate of failures. Keeping it from the public belongs to the ingress.
35. **A delivery can wait up to a second for its claim while the replicas have little to do.** A poller that finds nothing due waits before it claims again, and the wait grows to 1 s (section 7). Attempts that end one after the other, each followed by a claim that finds nothing new, take the wait there within milliseconds. A delivery that becomes due then waits until the poller of one replica wakes. [performance.md](performance.md) has the times that this gave.
36. **An attempt connects to one address of its host.** The client is given the first address of the lookup, in the order in which the resolver returned them, and does not go on to another when that one fails (section 11.4). A host whose first address is down while another one answers fails the attempt, and the delivery is retried on the schedule.
37. **Creating an endpoint or a tenant is not idempotent.** `POST /v1/endpoints` and `POST /admin/tenants` take no idempotency key, and a tenant can have several active endpoints with one URL, each with its own secret (`EndpointApiIT#twoEndpointsGetDifferentSecrets`). A registration whose response was lost after its commit, the state of K2, leaves an active endpoint whose secret nobody holds, and the repeat creates a second one. Every matching event then goes to both and is charged for both, under two `webhook-id` values, and a subscriber that verifies rejects the request that is signed with the secret it does not have. The tenant finds the endpoint in `GET /v1/endpoints` and deletes it or rotates its secret. A tenant that was created that way has a key that nobody holds. No route lists it, and it does nothing. No test loses the response of either call.
38. **A replica has no TLS of its own, and it does not authenticate the database.** It serves plain HTTP, so the admin token, the API keys and the responses that show a key or a signing secret travel as whatever publishes the Service carries them. Ending TLS belongs to the ingress. The connection to the database is encrypted when the JDBC URL says `sslmode=require` or the server offers TLS, and the certificate of the server is not checked: `verify-full` needs a root certificate that the image does not carry and the chart cannot mount ([deploy.md](deploy.md#what-the-chart-needs)). No startup invariant looks at either, and no test connects to a database over TLS.

## 15. Operations

**Startup invariants** (`StartupInvariants`; the process exits non-zero on any failure):
- Admin token present and long enough.
- Secret-encryption key present and usable: 32 bytes in base64.
- `default_transaction_isolation` is `read committed`.
- `synchronous_commit` is not `off`.
- Lease >= attempt deadline + 30 s.
- Start cutoff + attempt deadline <= finalize cutoff, and finalize cutoff < lease. A request that starts at the start cutoff then ends inside its lease, with time left to record it.
- Start cutoff + attempt deadline < 60 s, the probe slot of the gate (section 9). The request of a probe is then over before its slot is free again, so the next claim cannot send a second probe beside it.
- `max-attempts` and `max-in-flight` are at least 1. With 0 attempts every delivery would be dead-lettered by its first claim, and with no room the replica would never claim.
- For each pool:
  - the borrow wait, `connectTimeout` and `socketTimeout` are set, and `tcpKeepAlive` is on;
  - `lock_timeout < statement_timeout < socketTimeout`;
  - `idle_in_transaction_session_timeout` is non-zero;
  - borrow wait plus aliveness test plus socket timeout is below the start cutoff.
- The JVM runs with `-XX:+ExitOnOutOfMemoryError`, so an out-of-memory error is a kill-table case and never a replica that is Ready but no longer claims.
- No JVM proxy properties.
- SSRF allowlist empty and `http` disabled unless the `unsafe-local` profile is active.

**Liveness.** The `poller` health indicator (`PollerHealth`) belongs to the liveness group, next to the liveness state of Spring Boot. It reports down when the poller loop has not finished an iteration for 60 s, which it reads from `PollerHeartbeat` on the monotonic clock of the replica. A failed claim also finishes an iteration, so a database outage does not restart pods. Only a poller that stopped looping does. A heartbeat that was never beaten counts from the start of the replica. `PollerHealth` also registers the gauge of section 10, on the heartbeat that it reads. `PollerHeartbeatTest` covers the rule and the gauge with an injected time source, and `DeliveryPollerTest` that the loop beats after a claim that failed. `PollerHealthIT` asks the probes of a replica whose poller is seen waiting in its claim, behind a table lock of the test, after the test has moved the clock of its heartbeat by 60 s: the liveness probe answers down, and so does `/actuator/health`, which asks every indicator of the replica. The readiness probe answers up, and the liveness probe answers up again once the claim has ended. A scrape of that replica shows the gauge at those 60 s while the probe answers down, and at 0 once it answers up again.

The database health indicator is off. The health endpoint takes no key, so it must not borrow pooled connections.

**Graceful shutdown** (`WorkerLifecycle`, a `SmartLifecycle` that stops before the web server and the pools):
1. Readiness goes out of service.
2. The poller stops claiming and its thread is joined. Only then is the log event `poller_stopped` written.
3. In-flight attempts finish and finalize. The 15 s deadline bounds this, and the drain waits at most 20 s.
4. The web server finishes open requests, then the pools close.

The JVM then exits with code 143 (128 + SIGTERM), which is the documented exit code of a clean drain. The deployment must allow at least 30 s of termination grace. Nothing needs releasing, because nothing is claimed that has not started.

`ExpiryJob`, `CancelJob` and `RetentionJob` are lifecycles of the same phase as the worker, so they stop before the pools close as well. So is `QueueGauges` (section 10), which runs on the same kind of timer. Spring stops the lifecycles of a phase one after the other and then waits until each has reported its end, for at most the 30 s that Spring Boot gives a phase. The stop of a job starts no further run and returns at once, and a run in progress starts no further statement. The job reports its end when its loop is over, which is after the statement that is in progress. A slow run therefore neither delays the stop of the poller nor adds its time to the drain. It still ends before the pools close, because the time bounds of the `worker` pool (section 12) end a statement inside those 30 s.

**Background jobs.** They run on every replica on a timer, with no leader election. Each is an idempotent batch statement that picks its rows in a CTE with `FOR UPDATE SKIP LOCKED`, so a job never waits on a row and concurrent runs are harmless. The CTE runs once, so its `LIMIT` bounds the statement whatever plan it gets (section 8).

- The first run of a job comes when the replica starts, and every later run its interval after the end of the run before it.
- Every statement of a job is a transaction of its own on the `worker` pool. A statement that outlives its replica therefore cannot commit by itself (section 3).
- A run that fails is logged, and the next run follows after the same interval.

- `ExpiryJob` (every 60 s) dead-letters pending rows past `expires_at` that are not leased, 1,000 per statement, those that expired first, repeating while a batch is full. An abandoned row gets its missing attempt row in the same statement:

```sql
WITH picked AS (
  SELECT id, lease_token IS NOT NULL AS abandoned
  FROM delivery
  WHERE state = 'pending' AND expires_at <= now()
    AND NOT (lease_token IS NOT NULL AND next_attempt_at > now())     -- not leased
  ORDER BY expires_at LIMIT 1000
  FOR UPDATE SKIP LOCKED
), expired AS (
  UPDATE delivery d
  SET state = 'dead', dead_reason = 'expired', lease_token = NULL, completed_at = now()
  FROM picked
  WHERE d.id = picked.id
  RETURNING d.id, d.replays, d.attempts, picked.abandoned
), logged AS (
  INSERT INTO delivery_attempt (delivery_id, run, attempt_no, outcome, error)
  SELECT id, replays, attempts, 'unknown', 'lease_expired' FROM expired WHERE abandoned
)
SELECT count(*) AS deliveries, count(*) FILTER (WHERE abandoned) AS abandoned FROM expired;
```

  Each statement answers with the number of deliveries that it dead-lettered and the number of attempt rows that it wrote, which the job counts on the meters of section 10. Postgres runs the insert although nothing reads from it.

- `CancelJob` (every 30 s) cancels the pending rows of deleted endpoints, 1,000 per statement, repeating while a batch is full:

```sql
WITH picked AS (
  SELECT d.id FROM delivery d JOIN endpoint e ON e.id = d.endpoint_id
  WHERE e.status = 'deleted' AND d.state = 'pending'
  LIMIT 1000 FOR UPDATE OF d SKIP LOCKED
)
UPDATE delivery d
SET state = 'cancelled', dead_reason = 'endpoint_deleted', lease_token = NULL, completed_at = now()
FROM picked
WHERE d.id = picked.id;
```

- `RetentionJob` (hourly, batches of 1,000) runs in this order. Each step repeats its statement while a batch is full, and the next step begins after that:
  1. Delete terminal deliveries with `completed_at` older than 30 days:

     ```sql
     WITH picked AS (
       SELECT id FROM delivery
       WHERE state <> 'pending' AND completed_at < now() - interval '720 hours'
       ORDER BY completed_at LIMIT 1000
       FOR UPDATE SKIP LOCKED
     )
     DELETE FROM delivery USING picked WHERE delivery.id = picked.id;
     ```

     The predicate sits on the locked row inside the CTE. A row that a replay holds is skipped without waiting. A row that a replay has already committed fails the predicate. There is no lock wait in this statement.

     The 30 days are written as 720 hours, here and in the next step. Postgres counts the days of an interval in the time zone of the session, which a replica sets from the zone of its JVM. Across a change of the clocks `interval '30 days'` is 719 or 721 hours, and two replicas in different zones would not agree on it.
  2. Delete events older than 30 days that have no deliveries. Deliveries are only created at ingest, so such an event can never gain one. Its idempotency key is free again from then on.

     ```sql
     WITH picked AS (
       SELECT ev.id FROM event ev
       WHERE ev.created_at < now() - interval '720 hours'
         AND NOT EXISTS (SELECT 1 FROM delivery d WHERE d.event_id = ev.id)
       ORDER BY ev.created_at LIMIT 1000
       FOR UPDATE OF ev SKIP LOCKED
     )
     DELETE FROM event USING picked WHERE event.id = picked.id;
     ```
  3. Delete `deleted` endpoints that have no deliveries. Nothing is created for an endpoint once its delete has committed (section 6), so such an endpoint can never gain one either.

     ```sql
     WITH picked AS (
       SELECT e.id FROM endpoint e
       WHERE e.status = 'deleted'
         AND NOT EXISTS (SELECT 1 FROM delivery d WHERE d.endpoint_id = e.id)
       LIMIT 1000
       FOR UPDATE OF e SKIP LOCKED
     )
     DELETE FROM endpoint USING picked WHERE endpoint.id = picked.id;
     ```
  4. Clear expired previous secrets:

     ```sql
     WITH picked AS (
       SELECT id FROM endpoint
       WHERE prev_secret_expires_at <= now()
       LIMIT 1000
       FOR UPDATE SKIP LOCKED
     )
     UPDATE endpoint SET prev_secret = NULL, prev_secret_expires_at = NULL
     FROM picked
     WHERE endpoint.id = picked.id;
     ```

  The foreign keys of a delivery to its event and to its endpoint are `RESTRICT`, so a mistake here fails loudly and does not cascade. The attempt rows of a delivery and the circuit row of an endpoint go with it.

**Endpoint delete** is one statement:

```sql
UPDATE endpoint SET status = 'deleted', deleted_at = now()
WHERE id = :id AND tenant_id = :t AND status = 'active';
-- 0 rows: 404, and nothing else runs
```

- That statement alone stops delivery. The claim and the load both require `status = 'active'`, and ingest and replay re-check it under their share lock.
- From the commit on, the endpoint's set of pending rows can only shrink, and `CancelJob` cancels it in batches.
- The request holds the endpoint row for one statement however deep the backlog is.
- An attempt already in flight is recorded as what it was, unless the cancel reaches its row first (limitation 15).

**Tenant suspension** is the patch statement of section 5 with `status` alone. It writes the tenant row and no delivery rows.

**Container image.** The `Dockerfile` builds the jar with the Maven wrapper on a JDK 21 image and unpacks it into the layers of Spring Boot, so the libraries are a layer that a change to the service does not rebuild. The image that runs is `gcr.io/distroless/java21-debian13`: a JRE on glibc, without a shell or a package manager. Both base images are pinned by digest.

- The process of the container is the JVM, started with `-XX:+ExitOnOutOfMemoryError`, which the startup invariants demand. The SIGTERM of a stop reaches it and starts the drain above. The CI job of the local stack stops a replica and requires the exit code 143.
- It runs as the user 65532 and not as root.
- It writes to `/tmp` and nowhere else, because Tomcat creates its working directory there when it starts. With a writable `/tmp` mounted, the root filesystem can be read-only.
- `RES_OPTIONS` is set to `timeout:1 attempts:2`, which asks the resolver of glibc for a shorter wait for a name server (limitation 12).
- The image holds no credential, and the build takes none: only the Maven wrapper, the POM and the main sources reach it. The database URL with its user and password, the admin token and the secret-encryption key come from the environment of the container.

**Local stack.** `compose.yaml` runs that image on one machine: two replicas on one PostgreSQL, the example subscriber, Prometheus, which scrapes both replicas, Grafana with one dashboard on the meters of section 10, and Jaeger, which the replicas export their spans to. It is for development and for nothing else:

- The replicas run with the `unsafe-local` profile and the allowance of section 11.4: plain `http`, and the address range of the network of the stack, where the subscriber is. A tenant of the stack can therefore aim a delivery at any container of it.
- Its credentials are development values and are written in the file.
- The replicas sample every trace, so that each event of a run has its trace. A replica samples a tenth of them unless `management.tracing.sampling.probability` says otherwise.
- The replicas have a read-only root filesystem with a `/tmp` in memory, and 30 s to stop.
- On a new stack, `docker compose up --wait` returns once the database accepts connections, the subscriber listens and Prometheus has scraped both replicas. A counter that was scraped before its first event shows that event as an increase, which is what the dashboard draws.
- That check of Prometheus is no check of the replicas, whose image has no shell for one of their own. A scrape of the last hour counts for it, so Prometheus stays healthy while a replica is stopped or replaced, and the command then returns before that replica takes requests. `scripts/demo.sh` therefore waits for the readiness probe of both replicas before its first request.
- Every port that the stack publishes is bound to the loopback address of the machine. Grafana, Prometheus and Jaeger have the ports that the README links. The replicas and the subscriber get ports that Docker chooses, which `docker compose port` prints, so the stack needs those three ports free and no others.

`examples/subscriber/Subscriber.java` is the subscriber of the stack: one file that the `java` command of a JDK compiles and starts, and a JRE cannot. The stack runs it on the JDK image of the build, at the same digest, as a user that is not root. It checks a request with a copy of the reference verifier (section 11.3), which `ReferenceVerifierTest#exampleSubscriberCarriesTheVerifier` holds to the original, prints the request and answers `200`, or `401` when the request is not genuine. It is given its signing secret with `PUT /secret`, and `PUT /status` sets the status that it answers a genuine request with, which makes it a subscriber that fails.

`scripts/demo.sh` is the walk through the API on that stack. The walk itself is `scripts/walkthrough.sh`, which `scripts/demo.sh` sources once it has asked Docker where the two replicas and the subscriber are published, and which the smoke test of the chart sources as well (below). It creates a tenant, registers the subscriber as an endpoint, sends an event to one replica, reads the delivery from the other until it has succeeded and finds the line in which the subscriber reports the signature of that delivery as verified. It then sends the subscriber a request with a signature that the secret does not give, which is answered with `401`. Last it tells the subscriber to answer `503` and sends a second event. The first attempt of that delivery fails, the subscriber is told to answer `200` again, and the retry succeeds, 5 to 10 s after the failure (section 8). The subscriber has then verified two requests with the `webhook-id` of that delivery. The script ends with an error when a step fails.

`scripts/check-observability.sh` asks the stack for what the demo left in it. Grafana has to serve the dashboard and to return a number for every query of it. Grafana runs each query through the data source of its panel, over the time range of the dashboard, as it does for the page. Jaeger has to return a trace of the service `reliable-webhooks` with the span of an attempt that succeeded.

The job `local-stack` of the CI workflow builds the image, starts the stack, runs both scripts and then stops a replica, which has to end with the exit code 143.

**Load tests.** `loadtest/run.sh` runs one of four scenarios with k6 on that stack, as `loadtest/compose.yaml` changes it, and writes what it measured to `loadtest/results`. [performance.md](performance.md) describes the scenarios and has the numbers of the runs that are committed, which are the numbers of one laptop.

- A run starts PostgreSQL, the two replicas and its subscriber, and neither Prometheus nor Grafana nor Jaeger. Its network has the address range of the local stack, so that stack has to be down. The replicas keep the `unsafe-local` allowance and otherwise run as they ship: they export their spans nowhere and sample a tenth of their traces.
- PostgreSQL runs with the settings of its image. It writes one more line to its log for every vacuum and every analyze of autovacuum, and a result shows the vacuums of the `delivery` table next to the checkpoints.
- The subscriber is `loadtest/sink/Sink.java`. It verifies no signature, answers at once unless a scenario tells it to wait or to fail, and counts the requests that it received for each `webhook-id`.
- The load generator runs in a container inside the network of the stack.
- The tenants of a run have a rate limit that no scenario comes near. The bucket statement of section 12 runs for every event all the same, and it has refused events in a run. Limitation 7 names the two ways in which a bucket that holds enough refuses a charge, and the runs do not show which of them it was.
- CI does not run the load tests.

**Helm chart.** `deploy/helm/reliable-webhooks` installs that image on Kubernetes: a Deployment, a Service, a ServiceAccount and a PodDisruptionBudget, and a HorizontalPodAutoscaler and a ServiceMonitor that are off by default. It installs no database. [deploy.md](deploy.md) describes its values. What it takes from this design:

- **Credentials.** The database password, the admin token and the secret-encryption key reach a replica from a Secret, which the chart is given the name of. The schema of the values has no place for any of them. It refuses a database URL with a `password` parameter, and a further variable with the name of one that the chart sets.
- **Grace.** A pod has 35 s to end. In the first 5 s a preStop hook sleeps and the replica works as before, while the cluster stops sending it requests. Then the replica gets its SIGTERM and has the 30 s that the drain above asks for. The chart does not render with values that leave the drain less. The hook is the sleep action of Kubernetes, which is on by default from version 1.30, and the chart does not render for an earlier one.
- **Probes.** The liveness and the readiness probe ask the two health groups above. A startup probe on the liveness group gives a replica 5 minutes to start. The liveness probe does not run before it has passed, so a replica is not restarted while it migrates the database.
- **Security context.** A replica runs as the user of the image, with a read-only root filesystem and a volume at `/tmp`, without privilege escalation, without capabilities, with the default seccomp profile of the runtime and without a token for the API of Kubernetes.
- **Rollout.** One new replica is started at a time, and no old one is stopped before it is Ready. A replica that fails a startup invariant never is, so such a release stops at its first replica and the old replicas keep running.
- **Connections.** A replica that is being removed keeps its 32 connections until it has drained, and new replicas start meanwhile. In a rollout the database can therefore see twice the replicas.

**Migrations in a deployment.** A replica runs Flyway when it starts, before its pools open and its web server listens, and nothing else migrates the database. The service leaves the settings of Flyway that matter here at their defaults.

- **Several replicas start at once.** Flyway takes an advisory lock of a transaction in PostgreSQL around the creation of its history table and around each migration. The replica that gets the lock migrates. A replica that does not get it asks again once a second, and when it has the lock it reads the history again and finds nothing to apply. After 50 further tries it gives up, which ends its start, and Kubernetes starts it again. The lock ends with its transaction, so a replica that dies with it leaves no lock behind.
- **A rolling update** migrates the database under the replicas of the old release, which run on it until they are replaced. A migration therefore has to leave the old release working: it may add to the schema and must not remove or rename what the old release uses. Flyway has connections of its own and is not cut off by the time bounds of section 12. The statements of the running replicas are: while a migration holds a lock that one of them needs, a request answers `503 database_busy` and a claim fails and is made again. A migration has the 5 minutes of the startup probe, unless the release that brings it is given more.
- **A rollback of the image** leaves the schema where it is. Flyway does not undo a migration. A replica of the earlier image finds a history with a migration that it does not have, logs that the schema is newer than the latest migration it knows, applies nothing and starts. Whether it then works is the rule for a rolling update again. Where a migration broke that rule, the way back is a release that repairs the schema, or the backup of the database.

There is one migration, so no test applies a second one to a database in use. The smoke test below shows the first and the last of the three as far as it can: two replicas that start together on an empty database, and a replica that starts on a history with the row of a later migration.

**Smoke test on kind.** `scripts/check-chart.sh` needs no cluster. It runs `helm lint` and renders the chart three times: with the values of the smoke test, with those and the two templates that are off by default, and with the five values that [deploy.md](deploy.md) installs it with and nothing else. Each time `kubeconform` holds every manifest against the schema of its kind, and the kinds have to be the four of every release, with the two others only where they were turned on. Then it gives the chart what it has to refuse: its defaults alone, which name none of those five and are refused for each of them, an image tag that is a number, no replica at all, a password among the values and one in the database URL, each of the five variables that the chart sets as a further variable, a Service of the type `LoadBalancer`, a grace period that leaves the drain 29 s, and a cluster of Kubernetes 1.29.

`scripts/kind-smoke.sh` creates a kind cluster of one node, builds the image and loads it into the node, and installs what `deploy/kind` holds for the test and for nothing else: a PostgreSQL in one pod, the example subscriber, and a Secret with the development values of the local stack. Then it installs the chart with two replicas and waits for the rollout. The replicas run with the `unsafe-local` profile, as in the local stack, with the allowance for `http` and for the address range of the Services of the cluster. The namespace enforces the `restricted` Pod Security Standard: the API server refuses a pod there that may run as root, may escalate its privileges, keeps a capability or has no seccomp profile. The standard asks neither for a read-only root filesystem nor for a pod without a token. The script reads both from the replicas, and that the replicas work that way is what the rest of the run shows. The script requires, in this order:

- Of the two replicas, one logged the migration as applied and the other that none was necessary.
- Each of the two has a read-only root filesystem, and nothing is mounted in it but `/tmp`, so it has no token for the API of Kubernetes.
- The PodDisruptionBudget counts two healthy replicas, so its selector finds them.
- The walk of `scripts/walkthrough.sh` passes. Every request of it to the API goes to a node port of the Service of the chart, which hands it to either replica.
- With the subscriber answering `503`, five events are sent. Their five failures open the circuit of the endpoint (section 9), so the five deliveries are pending and nothing is sent for them until the probe that is due 30 s later.
- No container of the two replicas has been restarted. A replica takes its restart count with it when it goes, so the counts are read here, before one of the two is deleted.
- The script adds the row of a later migration to the history of Flyway, deletes one of the two replicas and reads the five deliveries as still pending. It then tells the subscriber to answer `200`, and all five succeed.
- The replica that the Deployment started in place of the deleted one is Ready, and it logged that the schema is newer than its latest migration.
- No container of that replica or of the one that stayed has been restarted.

The deletion does not depend on timing: the deliveries are pending for as long as the subscriber answers `503`, which it does until the replica is gone. No request is in flight when the replica is deleted, so the deletion proves less than rows 1 to 3 and 20 of the failure table, which kill and stop a replica in the middle of a request. It shows, on a cluster and through the chart, that a pending delivery belongs to no replica.

The job `helm-chart` of the CI workflow runs both scripts. The HorizontalPodAutoscaler and the ServiceMonitor are held against their schemas and are installed on no cluster, because the cluster of the test has neither a metrics server nor the Prometheus Operator. No release is upgraded either: the script installs one, and the pod that takes the place of the deleted one is of the same release. The settings of the rollout are rendered and held against the schema of a Deployment, and no rollout has been run on a cluster.

**Infrastructure on AWS.** `infra/terraform` is a Terraform module for what the chart needs on AWS: a VPC over three availability zones, an EKS cluster with one managed node group in its private subnets, and a PostgreSQL 18 instance on RDS. [deploy.md](deploy.md) describes it. What it takes from this design:

- **Startup invariants.** The parameter group of the instance sets `default_transaction_isolation` to `read committed` and `synchronous_commit` to `on`, the two settings of the database that the startup invariants above check.
- **Credentials.** RDS creates the password of the database user and keeps it in Secrets Manager. No variable, no output and no state of the module holds it, and the Secret that the chart reads is filled from there. RDS also rotates that password, and nothing in this repository carries a new one into the Secret. A release whose Secret still holds the old password stops working about half an hour after a rotation, when its pools have replaced their connections, and no probe shows it.
- **Reach.** The database admits TCP port 5432 from the security group of the nodes of the cluster and from nothing else. The nodes have no public address, and a delivery reaches its subscriber through a NAT gateway. The range of the VPC has to be private address space, which the address policy of section 11.4 denies, so a subscriber URL that resolves to a node, a pod or the database is refused.
- **Connections.** The module does not set `max_connections`. RDS derives it from the instance class, which is chosen with the 32 connections of a replica in mind (above).

The module has never been applied. The job `terraform` of the CI workflow checks its format, validates it, plans it against a mocked provider, with the values of `infra/terraform/example.tfvars` and with the defaults that those replace, and lints it with tflint and its AWS rules.

- The plans read the two settings of the parameter group, the source of the rule of the database and the subnets of the VPC. They also give every rule of a variable a value that it has to refuse.
- The lint has to refuse an instance class that RDS does not have, which shows that its rules reach the database instance inside the modules that are called.
- The encryption, the deletion protection, the backups and the private placement of the instance are arguments of the RDS module in `main.tf`, and no step asserts them.
- No step calls AWS, so nothing shows that AWS accepts what the module describes.

## 16. Test strategy and harness

Rules:

- No test sleeps to "let things happen". Waits are condition polls with a timeout.
- **Time is aged in the data, not in the clock.** `Rows` ages a row: it subtracts an interval from its time columns, which are `next_attempt_at`, `expires_at`, `completed_at`, `next_probe_at`, `refilled_at`, `prev_secret_expires_at` and the `created_at` of an event. Production code and `now()` run unchanged. This drives lease expiry, the twelve-step schedule, the probe interval, the 72 h budget, the 24 h of an old secret and retention. `Rows` also postpones a delivery: it moves the due time ahead, so that no claim takes the delivery until the test ages it by as much. In the same way it moves the last refill of a bucket ahead of the clock of the next charge.
- **A negative claim needs a witness.** "Nothing was claimed" or "no attempt was used" is asserted only after something observable proves the poller ran in the state under test. The witness is either a canary (a delivery to another endpoint or tenant, ingested after that state was reached, has succeeded) or a positive signal such as `poller_stopped`.
- **Ordering assertions, not clocks.** Where a test is about who goes first, it reads the receiver's request log.
- **A time limit is checked on the clock that counts it.** The clock of the test JVM and the clock of the database do not run at exactly the same rate, so a duration that the one measured is not compared with a limit that the other counts. `ApiTimeoutIT#blockedStatementAnswers503` reads the database clock before its request and after the answer: the lock timeout and the statement timeout are counted by the database. The lookup wait and the attempt deadline are counted by the replica, and the tests that measure them do so in the JVM of that replica, or read the duration that the replica recorded.
- Integration tests run with production retry, breaker, lease and timeout settings. The test-profile differences are the `unsafe-local` allowance for the loopback receiver with its `http` scheme, in the classes that deliver to the receiver, and in `TracingIT` only, the sampler probability. The replicas that `TracingIT` starts to read spans also hand them to an exporter of the test, which keeps them in memory. One replica of that class names a collector instead, which is a receiver of the test.
- Three tests of `WebhookHttpClientIT` wait for a time bound to end a request, and their clients have a short one: an attempt deadline of 500 ms in the two that are cut at the deadline, and a lookup wait of 20 ms in the one whose lookup does not answer. The client with the short deadline is built from settings, as the application builds its client, so the bound that cuts those requests is `attempt-deadline`. A replica under test always runs with the shipped values.
- **A replica inside the test JVM does not start its worker by itself.** The replicas of a build share one database, so a worker that ran all the time would take the deliveries of tests that are about something else, and those of a test that has to claim them itself. A test that wants deliveries sent starts the worker of its replica, or builds the pollers it needs. The worker is stopped when the test ends. One test is the exception: `WorkerLifecycleIT#replicaStartsItsWorkerByItselfAndStopsItWhenItCloses` starts a second replica the way the application starts, with nothing taken from Spring, and closes it before it ends. The attempt is recorded under the id of that replica, which shows that Spring started its worker. Its pools hold 4 connections each, because the replicas that the build keeps have theirs open.
- **Nor does it start its jobs.** A run that came by itself could take a delivery that a test has aged past its budget before the test is ready for it, cancel one that a test shows as pending, or delete a row that a test has aged past retention. A test that wants a run calls the run of the job. Three tests are the exception here, one for each job: `ExpiryIT#replicaRunsItsExpiryJobByItself`, `EndpointDeleteIT#replicaRunsItsCancelJobByItself` and `RetentionIT#replicaRunsItsRetentionJobByItself`. Each starts a second replica the way the application starts and waits for the first run of that replica's job. In the first of them the delivery belongs to a suspended tenant, so the worker of that replica does not claim it.
- **Nor does it read the queue for its gauges.** `QueueGauges` only reads, so a run that came by itself would harm no test. It is left to the tests all the same, so that the gauges show what the run that a test called has read. `MetricsIT#replicaReadsTheQueueForItsGaugesByItself` is the exception: a second replica starts the way the application starts, and its gauges show a held delivery without a run of the test.
- **Every test of such a replica begins with an empty queue.** The pending deliveries that earlier tests left are deleted first. A worker that a test starts therefore sends the deliveries of that test and no others. The gauges of the queue count every tenant, so the numbers that `MetricsIT` reads from them are the deliveries of its own test for the same reason.
- **A replica that runs as a process of its own is not held back.** `ReplicaProcess` starts the packaged jar, and its worker and its jobs run as they do in production. Such a replica claims whatever is due. A test that starts one runs in a class with a replica inside the test JVM as well, which takes the requests that set the test up and whose worker does not run while the process does, so the test begins with an empty queue and the process finds the deliveries of that test only. Three tests send the process every request, because they are about what the process itself writes or stores: the two tests of `LogFormatIT` that read its lines, and `TracingIT#replicaDoesNotReadTheOpenTelemetryVariablesOfItsEnvironment`. The process is killed when the test ends. In the two kills of `EndpointOutageIT` that worker runs before the process and after it: it fails the delivery five times and is stopped before the process starts, and it is started again once the process is gone. In `probeSlotReopensAfterProberKilled` it is then the second replica, which sends the next probe, and in `expiredAfterHolderKilledReplaysCleanly` it delivers the replay.
- **Spans are read from a replica that the test started.** The share of its traces that a replica samples is read when it starts. Each test of `TracingIT` about spans therefore starts a `SelfStartedReplica` with the share that it is about: 1 where it reads spans, and 0 where it shows that none is exported. That replica takes the requests of the test and sends its deliveries.
  - A replica hands its spans to an exporter in batches. The test asks the replica for the batch that is due once the request or the delivery whose spans it wants is over, and reads the exporter then. The span of a request has ended when the caller has read the answer to its end, and the span of an attempt before its row is written.
  - In `callerCannotForceSampling` the witness is a span that the test starts in the replica as a child of the context that the caller sent. It is exported, by the sampler and through the exporter of that replica, and it is the only span there.
  - `replicaExportsItsSpansNowhereUnlessACollectorIsNamed` reads no span. The replica of the class runs with the configuration that ships and has no exporter. A replica that the test starts with `management.opentelemetry.tracing.export.otlp.endpoint` naming a receiver of the test has one, and the span of a request to it arrives at the receiver as a `POST` of a protobuf message.
  - `replicaDoesNotReadTheOpenTelemetryVariablesOfItsEnvironment` needs a replica with an environment of its own, which only a process has. The environment says two things about sampling: as the setting of Spring Boot (`MANAGEMENT_TRACING_SAMPLING_PROBABILITY`), that every trace is sampled, and as a variable of OpenTelemetry (`OTEL_TRACES_SAMPLER`), that none is. The event that the replica accepts is stored with the context of a sampled span. A replica that read the variable of OpenTelemetry would go by it and sample nothing.
- **Log lines are read from a replica that runs as a process.** Its output is every line that a replica wrote and no line of anything else. The trace id and the span id reach a line through the logging context, which the tracing of Spring Boot fills from a hook in the context storage of OpenTelemetry. The hook has to be installed before that storage is first used. A process installs it when it starts. In the test JVM a test that uses the storage without an application, as `WebhookHttpClientIT` does, can come first, and the hook is then missing for every replica in that JVM. `LogFormatIT#subscriberUrlIsReducedToHost` and `#lineOfARequestNamesItsTraceItsTenantAndTheReplica` therefore read a process, and no test asserts those two ids on a line of a replica inside the test JVM. That process takes every request of its test, the creation of the tenant and the registration of the endpoint among them, so its output holds every line that the test looks through.
- **A replica without a running worker is not live.** Its poller does not loop, so its `poller` indicator reports down once 60 s have passed since the replica started or since a test last stopped its worker. The two tests that ask a replica inside the test JVM for its health, `ApiKeyAuthIT#healthProbesNeedNoKey` and `DatabasePoolsIT#healthEndpointBorrowsNoPooledConnection`, start its worker first.

Harness:

| Piece | What it does |
|---|---|
| `FaultReceiver` | In-process HTTP server. Per-path scripts: status with optional response headers, a body of a given length, delay, hold until released, drip, close without response, redirect, `endlessHeader`, `endlessBody`, `endlessChunkedBody`. Records every request with its headers, counts accepted connections and peak concurrency per path, and for the endless scripts the bytes written and whether the peer closed first. |
| `ReplicaProcess` | Starts the packaged jar as a child JVM against the Testcontainers database, with the replica id as `application_name`, the JVM flag that `StartupInvariants` demands and the `unsafe-local` allowance, on a port that the operating system chose. It can start the replica behind a `TcpProxy`, or with further variables in its environment. Returns once the readiness probe of the replica answers. Captures stdout and passes it on to the output of the build. `kill()` is SIGKILL and returns once the process is gone, `terminate()` is SIGTERM, `exitCode()` waits for the process to end by itself. |
| `PgWait` | `untilBlocked(sqlFragment, replica)` polls `pg_stat_activity` for a backend of that replica waiting on a lock in a statement containing the fragment, or for a given number of such backends. The lock timeout of the replica's pool ends that wait, so the test has that long to act. `endBlocked(sqlFragment, replica)` ends those backends, as a database that shuts down ends a session, and returns how many they were. `untilGone(replica)` waits until its backends have left, which is where a replica that was killed ends up. `untilNoTransaction(replica)` waits until none of its backends is in a transaction. It is for a replica whose connections went silent: nothing closes them, so its backends do not leave. |
| `StubDns` | The resolver behind `DnsLookups` in a replica inside the test JVM, in place of the system resolver: scripted answers per call, or block until released. A name that no test scripted does not resolve. |
| `TcpProxy` | In-process TCP forwarder between one replica and Postgres. `blackhole()` keeps every existing connection open and relays nothing more on it, ever; new connections are accepted and ignored. `heal()` lets new connections relay again. |
| `TestDatabase` | One Testcontainers PostgreSQL per test JVM, started on first use, with room for 300 connections: a replica holds 32, and besides the replicas inside the test JVM a test runs up to two as processes. The server runs with `fsync` off, as Testcontainers starts it. A stopped container leaves its data with the operating system, which keeps running, so the restart of row 8 is no test of a database whose machine went down. Hands its JDBC URL and credentials to replicas. Gives a test its own `JdbcClient` outside the pools of the service, and a connection with autocommit off for a transaction the test holds open. `stop()` and `start()` stop the container and start it again. The container is created with a host port that was free at that moment, because Docker gives a container that starts again another port unless its port was named. |
| `ReplicaSettings` | The settings a replica under test needs beyond the shipped configuration: the `TestDatabase` connection with the replica id as `application_name`, an admin token and a secret-encryption key. |
| `InProcessReplica` | Annotation for a test class that runs against a replica inside the test JVM on a random port, with those settings and the shipped configuration. All such classes share one application context, and so one pair of pools. The worker, the jobs and the queue gauges of the replica are left to the tests, and the queue is emptied before each test. |
| `UnsafeLocalReplica` | An `InProcessReplica` with the `unsafe-local` profile and its allowance: plain `http` and the loopback address, where `FaultReceiver` listens. The classes with this annotation share a second application context. |
| `ReplicaWorker` | Starts and stops the delivery worker of a replica inside the test JVM. Each start gets a `WorkerLifecycle` and a poller of its own from the application context, wired as the application wires them, and runs the start and stop that Spring runs. |
| `SelfStartedReplica` | Starts a second replica inside the test JVM the way the application starts, with pools of 4 connections. Nothing of it is left to the tests: Spring starts its worker, its jobs and its queue gauges, and closing it stops them. A test can give it the heartbeat of its poller, built on a clock that the test moves. A test can also start it with the `unsafe-local` allowance, the share of traces that it samples and an exporter for its spans, or with a collector named that it exports its spans to. Hands out a client for the API of a replica that it started. |
| `TestTenant` | A tenant created through the admin API of a replica, with the API key it was shown. Registers an endpoint, deletes one and rotates its secret, sends an event, and replays one delivery or the dead deliveries of an endpoint as that tenant. Has the tenant's limits and status set through the admin API, and counts the tenant's event, endpoint and delivery rows. |
| `OpenRequests` | Counts, for each tenant, the requests that a route of a replica inside the test JVM has been given and has not finished. A request is counted once every filter has let it through and before the route reads its body, so a test that keeps requests unfinished can wait until they are behind the filters. |

How each proof is forced.

- **(a) Kills inside narrow windows: the lock-block technique.**
  1. The test takes a lock that the replica's next statement needs.
  2. It waits with `PgWait` until the replica is provably blocked in that statement.
  3. It sends SIGKILL, then releases the lock.

  What is blocked, per proof:

  | Proof | Lock taken by the test | Statement blocked |
  |---|---|---|
  | K1, row 4 | Endpoint row | Fan-out insert, inside the ingest transaction |
  | K3, row 3 | `LOCK TABLE event IN ACCESS EXCLUSIVE MODE` | The load `SELECT` of a closed-circuit delivery |
  | K5, row 2 | Delivery row, taken while the receiver holds the request | F1, inside the finalize transaction |

  - **Only transactions and reads.** A lock-block kill is landed only on a statement inside an explicit transaction or on a read. The orphaned backend then finds end-of-file before any `COMMIT` and rolls back, or has nothing to commit. No kill is landed on a blocked autocommit write, because that write would commit after the kill and change the state under test.
  - **K3 in detail.** The delivery is accepted while no worker runs and is postponed by an hour, so the replica finds nothing due when it starts. The test locks `event`, ages the delivery by that hour from a second connection, waits until the load is blocked, kills, and releases. The surviving replica is started after the kill.
  - **After the kill.** `kill()` returns when the process is gone, and only then does the test release its lock. `PgWait.untilGone` then waits for the backend that outlived its replica, so the state that the test asserts next is the state that the kill left.
  - **K2** has no window that a lock could hold open. The test sends the request, reads from the database that the event is committed, kills the replica and sends the request again to another one.
  - **K4 and K6** use the receiver's hold, not a lock.
  - No hook exists in production code and no timing is assumed. The pool `lock_timeout` values (2 s and 5 s) leave room for a 10 ms poll.
- **(b) Claim race.** `secondClaimerSkipsLockedRow` runs the real claim on connection A without committing. A second delivery for the same endpoint is accepted then, and the claim runs on connection B, whose transaction begins with it. B returns without waiting, and with the second delivery only: it saw the due rows and passed over the locked one. B has a lock timeout, so a claim that waited would fail. After both commit, a claim gets zero rows. The four-poller test is a volume check on top of this, not the proof.
- **(c) One probe.** `oneProbeAdmittedAcrossConcurrentClaimers` runs the gate on connection A uncommitted and reads on A the probe time that it wrote, which is 60 s after the `now()` of that transaction, to the microsecond. It sees connection B block on the row, commits A, and asserts B updated zero rows. A second phase runs 4 pollers against a down endpoint with eight held deliveries and counts exactly one request per aged probe interval. In each interval the test holds the circuit row, which the claim does not need, until all eight deliveries are claimed and seen waiting at the gate. Then it lets go: one request arrives, and the seven other deliveries are released without an attempt. No kill is involved, so the gate may be blocked although it is an autocommit write.
- **(d) SSRF.** `SsrfIT` runs in the replica without the loopback allowance.
  - `rebindingIsBlockedAtConnect`: `StubDns` answers with a public address at registration and 127.0.0.1 at delivery, where the receiver is listening. It asserts the attempt error, `webhooks_dns_lookups_total{result="blocked"}` (the operator reason), and zero accepted connections. The control is a connection that the test itself opens to that address afterwards, which the same listener counts. Every integration test that delivers to the receiver is a control too: the receiver is reachable when allowed.
  - `redirectIsNotFollowed` cannot go through that replica, which reaches no receiver. It sends one request with a `WebhookHttpClient` whose address policy allows the loopback address. The receiver answers with a redirect to another of its paths. The client reports the 302, and the receiver has seen one request. That a 3xx is a failed attempt is the classification of step 5, which `DeliveryWorkerTest` covers.
  - `WebhookHttpClientIT#hostIsConnectedAtTheAddressThatTheGuardedLookupReturned` sends to a name that only `StubDns` resolves, to the address of the receiver. The request arrives, so the socket connects to the address that the guarded lookup returned. A client that had the name checked and then resolved it again by itself would find no address.
- **(e) Shutdown.** `GracefulShutdownIT` uses the endpoint bulkhead as the witness.
  1. Five due deliveries for one endpoint; the receiver holds.
  2. Wait until the receiver holds 4 requests. The fifth is due and unclaimed only because the endpoint is full.
  3. SIGTERM. Wait for `poller_stopped` on the child's stdout.
  4. Ask the replica for its readiness. It answers `503` with `OUT_OF_SERVICE`: readiness went out first, and the web server still answers while the worker drains.
  5. Release the receiver.

  It asserts four `success` rows and four requests, the fifth delivery still at `attempts = 0` with no lease, and that the process ended by itself with exit code 143 inside the drain time. A poller that kept claiming would take the fifth as soon as a slot freed. A succeeded delivery keeps the end of its last lease as its due time. For each of the four that time still lies ahead on the database clock when the process has ended, so the exit came before a lease ran out.
- **(f) Rate limit.**
  - A test that counts units gives its tenant a rate of 0, so the bucket holds what the test put into it, and reads the bucket row after each charge.
  - `limitIsExactAcrossReplicas` sets rate 0 and burst 20 and sends 200 requests to 4 replicas, 8 at a time per replica so the request permits never reject. It asserts exactly 20 accepted and that every other response is a well-formed `429 rate_limit_unsatisfiable` problem document without `Retry-After`. Three of the replicas are started inside the test JVM the way the application starts (`SelfStartedReplica`), each with its own pools, key cache and permits. The fourth is the replica of the class.
    - The permits never reject because a caller sends its next request once it has read the answer to the one before to its end, and the end of an answer is sent after the permit of its request is back.
    - The 200 requests leave it to chance whether two charges meet at the bucket row, so a second part forces it. A second tenant has rate 0 and burst 3, and the test holds its bucket row until two charges of each of the 4 replicas are seen waiting for it. Once the row is released, exactly 3 events are accepted and 5 are refused, because a charge that has waited decides on the row as the charge before it left it. The lock timeout of the `api` pool gives the test 2 s for this.
  - `refillIsExact` empties the bucket, then uses rate 0.01 per second and ages `refilled_at` by 500 s. Exactly 5 are accepted, and the next token is 100 s away: the sixth is refused with `Retry-After: 100`, is still refused once the refill is 90 s older, and is accepted once it is 100 s older.
  - `ninthConcurrentRequestTakesNoConnection` opens eight requests and sends half of each body, so each holds a permit and no connection while the route waits for the rest. Their authentication has put the key of the tenant into the cache of the replica. `OpenRequests` shows when all eight are with the route. The ninth gets `429 concurrency_limit`, and so does a request of the tenant to another route. The `api` pool's acquire count is unchanged across the two. Tenant B is served meanwhile, which moves that count. The eight are sent the rest of their bodies at the end and are accepted.
  - `floodingTenantDoesNotBlockOtherTenant` holds the bucket row of tenant A. Eight requests of A are seen waiting in their charge, each with a connection. Further requests of A are refused by the permits without one, and tenant B is served. The lock timeout of the `api` pool gives the test 2 s for this.
- **(g) Long durations.** The 72 h budget and the schedule are driven by `Rows`.
  - `RetryScheduleTest` asserts the eleven production windows as a pure function.
  - `exhaustedAfterTwelveAttempts` asserts that each `next_attempt_at` written by finalize lies inside the window for its attempt number, before ageing the row.
- **(h) Half-open connections.** Both `DatabaseBlackholeIT` tests first make the replica provably busy in a statement, then cut it off.
  - `pollerResumesAfterSocketTimeout`:
    1. The test locks the `delivery` table until the claim of the replica behind `TcpProxy` is seen blocked, calls `blackhole()` and releases the lock.
    2. A second replica delivers a new event meanwhile.
    3. After `heal()` the second replica is stopped and a delivery that was postponed until then is made due, which only the first can now deliver.

    Without a socket timeout the poller never returns from its read. The output of the first replica shows the claim that failed and the socket read that timed out.
  - `openTransactionIsEndedByServer`: F1 is blocked as in K5, then the proxy is blackholed and the lock released. The backend now idles in a transaction holding the row. Its replica is alive and nothing closes the connection, so only the server can end that transaction. The test waits with `PgWait.untilNoTransaction`, starts a second replica, ages the lease and asserts that this replica reclaims and delivers. The test waits for the end of the transaction and does not measure the 10 s. That the limit is 10 s is what `DatabasePoolsIT` reads from a live session.
- **(i) Database restart.** `DatabaseRestartIT` runs two replicas as processes against the database of the build.
  1. Ten deliveries for one endpoint; the receiver holds. Wait until it holds eight requests, four of each replica. Two deliveries are due and unclaimed.
  2. Stop the container. Release the receiver, so eight attempts have their result and no database to record it in. Send an event to one replica: it answers `503 database_busy`.
  3. Start the container. Send the same request until it is no longer refused. Every answer before the `202` is a `503`.

  It asserts that all eleven deliveries succeeded with one attempt, one `success` row and one request each, that each replica recorded at least the four results it held, and that both processes are still running.

  Two time bounds apply. The receiver holds the eight requests until the container has stopped, and a held request is cut at the attempt deadline of 15 s, so the requests have to arrive and the container has to stop inside that time. The eight results then have to be recorded before their replicas give the finalize up, 55 s after the claims began, so stopping and starting the container together have to fit into that time. A run that took longer than either would fail and not pass: an attempt that was cut is recorded as a failure, and a delivery whose finalize was given up is claimed again and sent a second time.

  The test does not choose which failure a call meets while the database is away: a session that the server ends, a borrow that finds no connection inside its wait, or a statement whose connection has failed, which the driver reports with a state of class 08. A run does not have to meet the last of them, so the test is not the proof that such a failure is treated as transient. Three unit tests are, and row 8 links them: `DatabaseFailuresTest` shows that a state of class 08 counts as transient, `FinalizeRetryTest` that a finalize is tried again after a failure with such a state, and `ProblemsTest#failedConnectionIsDatabaseBusy` that the API answers one with `503 database_busy`.

  The container keeps its host port because it was created with one. The replicas inside the test JVM lose their connections as well, and their pools replace them when a later test borrows one.
- **(j) Delete races.** `EndpointDeleteIT` forces both orders of a delete against an ingest, and against the two replays, and a delete between the claim of a delivery and its load. In the races with an ingest or a replay, the racer that goes first runs inside a transaction that the test opens on the `api` pool of the replica: it is the code of the service, and the test decides when it commits. The other racer is a request to the replica, and the test commits once `PgWait` shows that request waiting for the endpoint row. The lock timeout of the `api` pool gives the test 2 s for this.
  - Delete first: the transaction of the test holds the delete. The ingest has matched the endpoint and waits in its fan-out, and each replay has paid and waits at its share lock. After the commit the event is accepted without a delivery for the endpoint, and each replay answers `404` and re-arms nothing.
  - Delete second: the transaction of the test holds the ingest, or the two replays, and the delete waits for the row. After the commit the delete answers `204`. The delivery that the ingest created or a replay re-armed is pending then, and a run of `CancelJob` cancels it.
  - Delete between claim and load: `deliveryClaimedBeforeTheDeleteIsNotSentAfterIt` locks `event` as K3 does, without a kill, so a delivery that is claimed waits in its load. A delivery for a second endpoint is claimed first. Once it is seen waiting, the test makes the delivery for the first endpoint due, sees both loads wait, deletes that endpoint and releases the lock. The second delivery succeeds with its first attempt, so the loads were answered and not ended by the lock timeout of the `worker` pool, which gives the test 5 s from the first wait. The first delivery is not sent. It stays leased, and a run of `CancelJob` cancels it. The worker is stopped before the rows are read, which waits for the attempts in flight.
- **(k) Retention.** `RetentionIT#replayedDeliveryIsNotPurged` holds the replay of a delivery that died more than 30 days ago in such a transaction and calls a retention run while it is open. The run is on the `worker` pool, whose lock timeout would fail a run that waited for the row. `#purgesOnlyOldTerminalRows` has a succeeded, a dead and a cancelled delivery that ended 30 days ago, one of each that ended a minute later, and a pending delivery whose due time and expiry lie more than 30 days in the past. The run deletes the first three and leaves the other four.

**The failure-table check.** `FailureTableCheck` is a small main class run by `exec-maven-plugin` in `verify`, after Failsafe.

- The build deletes `target/surefire-reports` and `target/failsafe-reports` in `initialize`. Reports from an earlier build can therefore never satisfy the check, with or without `clean`.
- It parses the kill table and the failure table in `docs/failure-table.md`, the headline table in `README.md` and the four tables in `docs/behaviour.md` that link tests. A table is checked when its header has a `Test` column, and the links of a row are read from that column alone. Each file must contain a fixed number of checked tables, one in `README.md`, two in `docs/failure-table.md` and four in `docs/behaviour.md`, so renaming the column of any table cannot switch its check off. Matching rules:
  - `Class#method` must match at least one `testcase` of that class named `method`, `method[...]` or `method(...)`. Every match must have passed.
  - `#method` inherits the class named before it in the same cell.
  - A bare `Class` means every `testcase` of that class: at least one, all passed.
  - Passed means no `skipped`, `failure`, `error`, `flakyFailure`, `flakyError`, `rerunFailure` or `rerunError` child, so a test that passed only when it was run again does not count. The POM does not enable reruns.
- It fails on a row with no link, on a missing method, and on a linked test that did not run. That covers `@Disabled`, an excluded tag, `-DskipITs` and a renamed method with one rule.
- `FailureTableCheckTest` feeds it one fixture report per rule above.
- It does not parse this document. `FailureTableCopiesTest` compares the kill table of section 3 and the failure table of section 13 with the two tables of `docs/failure-table.md`, the kill table in full and the failure table in all but its last column. A test that one of them links is therefore a test that the check has looked for.
- The default `mvn verify` runs every tag. `process` and `dbrestart` exist only so a developer can exclude them locally, and then the check fails by design.
- `-DfailureTable.skip=true` skips the check, so that a single test can be run locally. CI never sets it.

## 17. Rejected alternatives

| Decision | Rejected | Why |
|---|---|---|
| Postgres as queue, `SKIP LOCKED` claim | Kafka, SQS, Redis | One store means no dual write between "event saved" and "message queued". |
| Deliveries written in the ingest transaction | Async fan-out relay over an outbox table | A relay adds a second claimer and a window where an event has no deliveries. Fan-out is at most 50 rows. |
| Lease is `next_attempt_at` | `in_flight` state plus a reaper job | The reaper is one more component that can die. A timestamp cannot. |
| Lease token as fence | Trusting lease time alone | A stalled replica cannot know its lease expired. The token makes a late write harmless. |
| Polling with idle backoff | `LISTEN/NOTIFY` | Needs a dedicated connection per replica and still needs polling for missed notifications. |
| Attempts counted at claim | Counted at finalize | A delivery that kills its worker would retry forever. |
| Any non-2xx is retried | 4xx as permanent failure | A subscriber deploy bug returning 404 for ten minutes would destroy events. |
| Endpoint-first claim: one head row per eligible endpoint | A window of the oldest due rows, reduced to one per endpoint | The window is filled by whichever endpoint has the oldest backlog, so one fast endpoint with a deep backlog starves every other tenant. |
| The same | A larger window | Any single endpoint can still fill it. |
| The same | Repeating the windowed claim with already-picked endpoints excluded | Fair, but every repeat scans past the excluded backlog, and it needs a per-tenant pending cap to bound that scan. |
| The same | A per-endpoint queue-head table | A second row to keep consistent in ingest, finalize and replay. That is worth its price only where the claim is the bottleneck (limitation 4). |
| One row per endpoint per claim | Claim a batch, then release the overshoot | Releasing causes a claim-and-release loop on deep backlogs and adds a release race. |
| Breaker state in Postgres | Resilience4j, or a per-replica breaker | In-memory state cannot hold deliveries fleet-wide or admit one probe across replicas, and is lost on restart. The price is that one replica's faulty view becomes everyone's (limitation 25). |
| No replica self-quarantine | Stop claiming when the last N attempts across several endpoints all failed to connect | It cannot tell "my egress is broken" from "these endpoints are down" without a known-good probe target, and a false positive takes a healthy replica out. |
| Probe slot as a pushed timestamp | `half_open` state | A state needs someone to leave it; a killed prober would wedge the circuit. |
| DNS `busy` releases the row | Record it as a failed attempt | It is this replica's admission decision. Recording it would burn attempts and open circuits on the tenant's healthy endpoints. The price is the claim-and-release loop of limitation 29. |
| Token bucket row in Postgres | In-memory limiter per replica | The limit becomes N times the configured value and resets on restart. |
| Bucket charged outside the transaction | Charged inside the ingest transaction | Holds the bucket lock across the transaction and serialises the tenant fleet-wide. |
| Charge is deliveries plus 4 KiB blocks | A second byte bucket per tenant | Two rows and two statements per ingest for one decision. |
| The same | Deliveries multiplied by blocks | The body is stored once, not per delivery. Multiplying makes an ordinary fan-out of a medium event cost more than the default burst. |
| Delete is a status flip; cancelling is a batched job | Cancel every pending row inside the delete transaction | After an outage that is millions of rows in one API transaction, with the endpoint row locked against ingest for its whole duration. |
| Suspension is a claim predicate | Suspend by deleting the tenant's endpoints | `cancelled` is not replayable, so a mistaken suspension would destroy deliveries. A predicate writes nothing and is reversible. |
| Client socket timeout plus server idle-in-transaction timeout | TCP keepalive alone | Keepalive defaults are measured in hours, and it does not help while unacknowledged data is in flight. |
| Apache HttpClient 5 | JDK `HttpClient` | No per-client resolver hook, so connect-time address checks would need a global resolver. |
| Stateless client, every default feature switched off | Library defaults | The defaults share a cookie jar across tenants and re-send on 429 and 503. |
| Hand-written attempt span | Auto-instrumented HTTP client | Stock instrumentation records the full URL on the span and the host on a timer label. |
| Redirects off | Follow and re-validate each hop | More code on the most attacked path, for a feature webhooks do not need. |
| No stored response bodies | Store a snippet for debugging | It is the read primitive of an SSRF. |
| Member names limited to 1,024 bytes | Turning off the table of names in the parser of an event body | The library then reads the body with its parser for characters, which counts a name by its characters. A name of 513 characters in 1,025 bytes is accepted, and so is a fraction of 1,001 digits. |
| Every request body is read as UTF-8 | Leaving the JSON library to detect UTF-16 and UTF-32 | The same parser for characters reads such a body, so the limits on names and numbers would hold for UTF-8 only (section 11.5). |
| The same | Skipping a byte order mark in front of a body in UTF-8 | RFC 8259 forbids a sender the mark. Skipping it is code in front of two parsers, and the same event with and without the mark would be two different bodies under one idempotency key. |
| A declared charset other than UTF-8 answers `415` | Reading every body as UTF-8 whatever its `Content-Type` says | It takes a JSON converter written for this service in place of the one of Spring. A client that names another charset and sends it would be told that its JSON is wrong, when its encoding is. |
| The same | A `consumes` condition with the charset on every route that reads a body | A route added later would have to repeat it. The check sits where the body limit sits, in front of every body. |
| `JdbcClient` and explicit SQL | JPA | The locking clauses are the design; they must be visible. |
| READ COMMITTED plus row locks | SERIALIZABLE | Retry loops everywhere, and `SKIP LOCKED` queues do not need it. |
| SHA-256 for API keys | bcrypt | Slow hashes defend low-entropy secrets; these are 256-bit random. |
| Jobs on every replica | Leader election | The statements are idempotent, so a leader adds only a failure mode. |
| New root span at ingest | Caller's `traceparent` as parent | The caller would control the sampling decision and the trace id. |
| `Idempotency-Key` required | Optional key | An optional key makes non-idempotent producers the silent default. |
| Report directories deleted at build start | Comparing report timestamps with the build start | Deleting needs no clock and no extra argument to the check. |
