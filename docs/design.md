# reliable-webhooks: design

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
| (root) | `ReliableWebhooksApplication`, `WebhooksProperties` (every `webhooks.*` setting), `DatabasePools` |
| `api/` | Controllers, `Problems`, `ApiKeyFilter`, `TenantConcurrencyFilter`, `UntrustedTraceContextFilter`, `IngestMeter` |
| `tenant/` | `TenantRepository` |
| `endpoint/` | `EndpointService`, `EndpointRepository`, `UrlPolicy`, `SecretCipher` |
| `ingest/` | `IngestService`, `EventRepository`, `TokenBucket` |
| `delivery/` | `DeliveryPoller`, `DeliveryWorker`, `DeliveryRepository`, `CircuitBreaker`, `RetrySchedule`, `Bulkhead` |
| `http/` | `WebhookHttpClient`, `SsrfGuardDnsResolver`, `AddressPolicy`, `DnsLookups`, `Signer` |
| `ops/` | `WorkerLifecycle`, `StartupInvariants`, `ExpiryJob`, `CancelJob`, `RetentionJob`, `QueueGauges`, `PollerHealth` |
| test `harness/` | `ReplicaProcess`, `FaultReceiver`, `Rows`, `PgWait`, `StubDns`, `TcpProxy`, `TestDatabase`, `ReplicaSettings`, `InProcessReplica`, `UnsafeLocalReplica`, `ReplicaWorker`, `TestTenant` |
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

Each row is a SIGKILL of a real replica process, landed deterministically with the lock-block or receiver-hold technique of section 16.

| # | SIGKILL lands | State left | Recovery | Visible effect | Test |
|---|---|---|---|---|---|
| K1 | In the ingest transaction, before commit | Nothing; Postgres rolls back | Producer retries with the same key | One event, one delivery per endpoint | `IngestKillIT#killedBeforeCommit` |
| K2 | After ingest commit, before the response | Event and deliveries committed | Retry returns the original event id with `Idempotent-Replay: true` | One event | `IngestKillIT#killedAfterCommitBeforeResponse` |
| K3 | After claim commit, inside the load of an ordinary closed-circuit delivery, before any byte is sent | Row leased, attempt consumed, no request | Lease expires, another replica reclaims | Attempt 1 logged `unknown`; subscriber sees one request | `WorkerKillIT#killedAfterClaimBeforeSend` |
| K4 | Mid-request | Row leased; subscriber may have processed | Same as K3 | Up to two requests, same `webhook-id` | `WorkerKillIT#killedMidRequest` |
| K5 | After the subscriber's 2xx, before finalize commits | Row leased; subscriber has processed | Same as K3 | Exactly two requests, same `webhook-id`; one `succeeded` | `WorkerKillIT#killedAfterResponseBeforeCommit` |
| K6 | Mid-request of a probe, holding the probe slot of an open circuit | `next_probe_at` pushed one lease ahead; probing delivery leased | Slot reopens when that time passes; no stuck state | Next probe one lease later | `EndpointOutageIT#probeSlotReopensAfterProberKilled` |
| K7 | In a bulk replay, after the charge and before the re-arm statement | Tokens spent; nothing re-armed | Caller repeats the call | The repeated call re-arms the rows | `ReplayIT#killedMidReplayRearmsNothing` |
| K8 | In endpoint create, between the endpoint insert and the circuit insert | Nothing; Postgres rolls back | Caller repeats the call | No endpoint without a circuit row | `EndpointApiIT#killedDuringCreateLeavesNoHalfEndpoint` |
| K9 | In tenant create, between the tenant insert and the bucket insert | Nothing; Postgres rolls back | Operator repeats the call | No tenant without a bucket row | `AdminApiIT#killedDuringCreateLeavesNoHalfTenant` |

- K2 is state-equivalent to a lost response. The test discards the first response, kills the replica and retries on another.
- A SIGKILL during graceful drain is K4 or K5.
- K7 shows what its setup can show. That the re-arm itself is all-or-nothing is single-statement atomicity, not something the kill proves.
- **Orphaned statements.** Killing a replica does not cancel the statement its backend is running. That statement may still execute, and an autocommit statement may still commit. Safety relies on the lease token and on every autocommit write being harmless when orphaned, not on the statement having stopped. The autocommit writes are the gate, the release, the bucket charge, the delete flip, the admin patches, the endpoint patch and the key rotation. Each of the first five leaves a state another row of this table or the strict-side limiter already covers. An orphaned endpoint patch sets the event types the tenant asked for, and repeating the call changes nothing. A rotation whose response was lost leaves a key that nobody holds, and the operator repeats the call.

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
- **Abandoned** means a `pending` row with a non-null `lease_token` whose `next_attempt_at` has passed. Its holder recorded nothing. Whoever touches it next (a claim or `ExpiryJob`) logs that attempt as `unknown`.
- **Held** means `pending` and due, but not claimable because the endpoint's circuit is open or the tenant is suspended. It is derived by the claim predicate and costs zero writes.

Every writer other than the claim sets `lease_token = NULL`: finalize, release, expiry, cancel and replay. A `dead` or `cancelled` row therefore never carries a stale token into its next run.

## 5. API

Tenant routes take `Authorization: Bearer <api key>`. Errors are RFC 9457 problem documents. An id that belongs to another tenant returns 404.

A named problem type such as `rate_limited` is sent as the `type` member, as a relative reference. Any other error (401, 404, a malformed request, 500) carries no `type`, which RFC 9457 reads as `about:blank`.

| Route | Behaviour |
|---|---|
| `POST /v1/events` | Body `{"type": "...", "data": {...}}`, header `Idempotency-Key` (required, 1 to 255 printable ASCII). Returns `202 {"id"}`. |
| `GET /v1/events/{id}` | Event with its deliveries (at most 50). Its `created_at` is the `now()` of the ingest transaction, the database time that its deliveries are due from. The `created_at` in the body that subscribers receive is the clock of a replica (section 6), so the two can differ. |
| `POST /v1/endpoints` | `{url, event_types}`. Returns `201 {"id", "secret"}`. The signing secret is shown this once. At the endpoint cap it answers `409` (section 11.5). |
| `GET /v1/endpoints`, `GET /v1/endpoints/{id}` | Includes derived circuit state. At most 50 rows by the endpoint cap. |
| `PATCH /v1/endpoints/{id}` | `event_types` only. Returns `204`. A new URL is a new endpoint. |
| `DELETE /v1/endpoints/{id}` | Soft delete in one statement. Delivery to the endpoint stops at commit; its pending deliveries become `cancelled` in batches (section 15). |
| `POST /v1/endpoints/{id}/secret/rotate` | New secret; the old one stays valid for 24 h. |
| `POST /v1/endpoints/{id}/replay` | `{limit}`; re-arms up to `limit` dead deliveries. Returns `{replayed, remaining}`. |
| `GET /v1/deliveries?endpoint_id=&state=` | Both filters are required. Keyset pages on `(created_at, id)`, `limit` at most 100, with a `next` cursor. The endpoint is resolved under the tenant first (404 otherwise). |
| `GET /v1/deliveries/{id}` | The delivery and the attempt log of its current run (at most 13 rows). `?run=n` selects an earlier run. |
| `POST /v1/deliveries/{id}/replay` | Re-arms one dead delivery. |

**Derived circuit state.** An endpoint reads as `closed`, `open` or `half_open`. Only `closed` and `open` are stored (section 9). `half_open` is an open circuit whose `next_probe_at` has passed, which means the next claim may take one delivery to the endpoint as a probe. The read statement decides it with the database clock.

Problem types used for rejection:

| Status | Type | When |
|---|---|---|
| 429 | `concurrency_limit` | More than 8 concurrent requests for the tenant on this replica. `Retry-After: 1`. |
| 429 | `rate_limited` | Bucket empty. `Retry-After: ceil(cost / rate)`. |
| 429 | `rate_limit_unsatisfiable` | The tenant's rate is 0 or the cost exceeds its burst. No `Retry-After`, because waiting cannot help. |
| 429 | `lookup_busy` | Registration could not get a DNS slot. `Retry-After: 2`. |
| 403 | `tenant_suspended` | The tenant is suspended. |
| 503 | `database_busy` | Lock timeout, statement timeout or no connection inside the borrow wait. `Retry-After: 1`. |

Admin routes take `Authorization: Bearer <admin token>`:

- `POST /admin/tenants` takes `{"name"}` (1 to 200 characters) and returns `201 {"id", "api_key"}`. The API key is shown this once. The tenant and its bucket are created together:

```sql
-- one transaction
INSERT INTO tenant (name, api_key_hash) VALUES (:name, :hash) RETURNING id, burst;
INSERT INTO rate_bucket (tenant_id, tokens) VALUES (:id, :burst);
```

- `PATCH /admin/tenants/{id}` sets rate, burst, `max_endpoints` and `status` (`active` or `suspended`). It evicts the tenant from the local key cache; other replicas follow within 30 s.
- `POST /admin/tenants/{id}/api-key/rotate` replaces the key immediately and returns the new one as `{"api_key"}`. Like the patch, it evicts the tenant from the local key cache; other replicas follow within 30 s.

**Suspension** is the operator's kill switch. `status = 'suspended'` does two things: `ApiKeyFilter` rejects the tenant's key, and the claim stops taking the tenant's deliveries (section 7). The rows are not written to. They are held, expire as `dead (expired)` after 72 h like any held row, and deliver after `status = 'active'` is set again.

## 6. Ingest: idempotency and transactional-outbox fan-out

The delivery rows are the outbox. They are written in the same transaction as the event, so there is no relay process and no window between "event stored" and "deliveries exist".

Order of work for `POST /v1/events`:

1. **Authenticate**, then take a per-tenant request permit (section 12). A request rejected here has not acquired a database connection.
2. **Validate** size, JSON, type and key against the limits of section 11.5. Render the body `{"id","type","created_at","data"}` once.
   - `data` must be a JSON object. Its numbers keep their exact value. None is read into a double.
   - Of members with the same name in one object, the last one counts.
   - `created_at` is the replica's wall clock at this step, in milliseconds and always with three fraction digits. It is information for the subscriber. Nothing is scheduled from it, and it is never compared with a database timestamp.
3. **Idempotency lookup**: `SELECT id, request_hash FROM event WHERE tenant_id = :t AND idempotency_key = :k`. The request hash is the SHA-256 of the request body as received.
   - Same hash: return `202` with the original id and `Idempotent-Replay: true`, not charged.
   - Different hash: `422 idempotency_key_reused`.
4. **Match**: `SELECT id FROM endpoint WHERE tenant_id = :t AND status = 'active' AND (event_types @> ARRAY['*'] OR event_types @> ARRAY[:type::text])`. Call the result `matched`. The cast is there because the driver sends a string parameter as `varchar`, and Postgres has no `text[] @> varchar[]`.
5. **Charge** `max(1, |matched|) + floor(body_bytes / 4096)` units to the token bucket in one autocommit statement (section 12). No tokens means `429`.
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
- An empty claim starts an idle wait of 50 ms that doubles to 1 s (`ClaimerBackoff`). A finished attempt ends the wait.
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
- **Backlog depth is invisible to the claim.** Each eligible endpoint contributes one probe of `delivery_head` and at most one row, whether it has one due delivery or a million. An endpoint with a deep backlog and a fast receiver gets one row per claim, like every other endpoint. Held rows are never scanned, because their endpoints are not in `eligible`.
- **Cost.** One claim does one index probe per eligible endpoint and locks one head row per endpoint with due work. Rows locked but not picked unlock at commit. The cost grows with the number of active endpoints, not with the queue (limitation 4).
- **Bulkheads without a release path.** A claim takes at most one delivery per endpoint and four per tenant. `Bulkhead` lists an endpoint as full at 4 in flight and a tenant as full above 12, so the per-replica caps (4 per endpoint, 16 per tenant) are never exceeded.
- **Abandoned attempts are logged.** For each returned row with `abandoned = true`, the same transaction inserts `delivery_attempt (delivery_id, run, attempt_no = attempts - 1, outcome 'unknown', error 'lease_expired')`. It means the holder recorded no result before its lease ran out, and the request may or may not have arrived.
- **Poison deliveries.** `attempts` is counted at claim, so a delivery that kills its worker every time still runs out. On the 13th claim the worker dead-letters it (`exhausted`) before the gate and without sending: the finalize transaction with F1 only, and no attempt row.

**Release.** Two steps below give a row back without cost. They use one statement:

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
       extract(epoch FROM now())::bigint AS signed_at        -- the signature timestamp
FROM delivery d
JOIN endpoint e ON e.id = d.endpoint_id
JOIN event ev   ON ev.id = d.event_id
WHERE d.id = :id AND d.lease_token = :token AND e.status = 'active';
```

No row means the delivery was cancelled or reclaimed, or its endpoint was deleted. The worker stops. A row of a deleted endpoint stays leased until `CancelJob` cancels it.

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
VALUES (:id, :run, :attempt_no, false, :outcome, :http_status, :error, :remote_addr::inet,
        now() - :duration_ms * interval '1 millisecond', :duration_ms, :replica);

-- F3: one of three circuit statements (section 9)
```

- `:duration_ms` is how long the request took on the monotonic clock of the replica. `started_at` is that long before the `now()` of the finalize transaction, so `started_at` plus `duration_ms` is the database time at which the attempt was recorded.
- `:replica` is `webhooks.replica-id`. It defaults to the `HOSTNAME` of the container, and to a random id where there is none.
- `probe` is `false` until the gate of section 9 exists.

Timing rules:

| Rule | Value | Why | Proven by |
|---|---|---|---|
| Attempt deadline | 15 s, hard | Covers DNS (2 s wait), connect (3 s), TLS, request and the whole response. A watchdog aborts the request at the deadline, so a drip-fed response cannot outlive it. | `SlowEndpointIsolationIT#drippingResponseIsCutAtDeadline` |
| Lease | 60 s | `StartupInvariants` requires lease >= deadline + 30 s. | `StartupInvariantsTest` |
| Start cutoff | 35 s after the claim call began | A worker that could not finish inside its lease does not send. The row stays leased and is reclaimed. `StartupInvariants` requires start cutoff + deadline <= finalize cutoff. | `DeliveryWorkerTest#doesNotSendAfterStartCutoff` (injected monotonic time source), `StartupInvariantsTest` |
| Finalize retry | Until 55 s after the claim call began, transient errors only | Covers a database restart; after that the row is left for reclaim. `StartupInvariants` requires finalize cutoff < lease. | `DatabaseRestartIT` (one request, one `success` row across the restart), `FinalizeRetryTest`, `StartupInvariantsTest` |

- **The lease is liveness, the token is safety.** If a replica stalls past its lease, another replica reclaims and both may send. That is a duplicate with the same `webhook-id`, which at-least-once allows. Only the current token holder can pass F1, so the stale replica's result, attempt row and circuit update all roll back.
- **Clocks.** Lease expiry is decided by Postgres. The replica's cutoffs use `System.nanoTime()` measured from before the claim was sent, so its view of the lease is always the shorter one. Wall-clock skew between replica and database has no effect, including on the signature timestamp.
- **Commit-unknown.** If the connection dies during finalize commit, the retry's F1 matches 0 rows when the first commit landed, and applies the result when it did not. Either way there is one outcome.
- **What finalize retries.** Only transient errors:
  - connection failures and socket timeouts (SQLSTATE class 08, `57P01` to `57P03`);
  - no connection inside the borrow wait;
  - deadlock (`40P01`), lock timeout (`55P03`) and statement timeout (`57014`).

  Anything else, such as a data or constraint error (classes 22, 23), is a bug. It is not retried, it is counted on `webhooks_finalize_errors_total` and logged at error, and the row is left for reclaim.

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

Bulk replay (`POST /v1/endpoints/{id}/replay`) is bounded and resumable:

- `limit` is at most `min(500, burst)`.
- The service resolves the endpoint under the tenant (404 otherwise), counts dead rows up to `limit` and charges that many units.
- It then runs one transaction:

```sql
SELECT id FROM endpoint WHERE id = :e AND tenant_id = :t AND status = 'active' FOR SHARE;   -- no row: 404
UPDATE delivery
SET (the same columns as the single replay)
WHERE id IN (SELECT id FROM delivery
             WHERE endpoint_id = :e AND tenant_id = :t AND state = 'dead'
             ORDER BY created_at, id LIMIT :k
             FOR UPDATE SKIP LOCKED);
```

- Re-armed rows are spread over 60 s.
- The caller repeats until `remaining` is false. No cursor is needed, because replayed rows leave the dead set.

## 9. Circuit breaker

State lives in `endpoint_circuit`, so it is fleet-wide and survives restarts.

- **Opens** after 5 consecutive failed attempts on the endpoint, across deliveries. Any recorded success resets the count and closes the circuit.
- **While open**, the endpoint is not in the claim's `eligible` set. Held deliveries are not written to and consume no attempts.
- **Probe.** When `next_probe_at` passes, the endpoint is eligible again. Each replica can claim at most one of its rows (one per endpoint per claim) and runs the gate:

```sql
-- gate, one autocommit statement
UPDATE endpoint_circuit SET next_probe_at = now() + interval '60 seconds'
WHERE endpoint_id = :e AND state = 'open' AND next_probe_at <= now();
```

One row updated means this delivery is the probe. Zero rows means another replica took the slot, and the delivery is released with delay 0 (section 7).

- **Exactly one probe fleet-wide.** The gate is a single-row `UPDATE` whose predicate Postgres re-evaluates after the row-lock wait. The second claimer sees the pushed `next_probe_at` and updates nothing.
- **No stuck probe.** Taking the slot is pushing a timestamp. A killed prober leaves nothing to clean up (K6).

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

**Metrics** (Micrometer, Prometheus). No metric carries a tenant, endpoint, delivery, host or URL label. Each meter ships with the code it counts.

| Metric | Labels |
|---|---|
| `webhooks_ingest_requests_total` | `result` = accepted, replayed, rate_limited, unsatisfiable, busy, invalid |
| `webhooks_delivery_attempts_total` | `outcome`, `reason` = http_2xx, http_3xx, http_4xx, http_5xx, timeout, connect, tls, dns, blocked, deadline, response_too_large, lease_expired |
| `webhooks_delivery_released_total` | `reason` = gate_lost, dns_busy |
| `webhooks_delivery_attempt_seconds` (histogram) | `outcome` |
| `webhooks_delivery_terminal_total` | `state`, `reason` |
| `webhooks_delivery_end_to_end_seconds` (histogram) | none |
| `webhooks_claim_seconds`, `webhooks_claim_rows`, `webhooks_claim_errors_total` | none |
| `webhooks_finalize_lost_lease_total`, `webhooks_finalize_errors_total` | none |
| `webhooks_poller_last_iteration_age_seconds` (gauge) | none |
| `webhooks_deliveries` (gauge) | `state` = due, scheduled, leased, held |
| `webhooks_oldest_due_age_seconds`, `webhooks_circuits_open` (gauges) | none |
| `webhooks_dns_lookups_total` | `result` = ok, timeout, busy, failed, blocked |
| `webhooks_ssrf_allowlist_entries` (gauge) | none; must be 0 in production |

`webhooks_delivery_attempts_total` counts attempt rows: an attempt is counted when its finalize has committed, and an abandoned attempt (`unknown`, `lease_expired`) when the claim that found it has. An attempt whose finalize lost its lease is counted on `webhooks_finalize_lost_lease_total` only.

`webhooks_ingest_requests_total` counts a request that the ingest route answered with one of these results, once, when the request is over. `invalid` is a `400`, a `413` or a `422`, and `busy` is a `503`. Nothing else is counted:
- a request that is refused before it reaches the route, by authentication or for its media type;
- a request to any other route;
- a request that the ingest route answered with any other status. A `500` is such an answer, so a failure that the service maps to none of the results does not appear in this counter.

`QueueGauges` runs one aggregate query over the pending index every 15 s per replica. It uses the claim's eligibility predicates: `due` counts rows a claim could take, and `held` counts due rows behind an open circuit or a suspended tenant.

**Traces.**
- Ingest starts a new root span. A syntactically valid caller `traceparent` is attached as a span link (section 11.7).
- The ingest span's own context is stored in `event.trace_context`.
- Each attempt is one span of kind CLIENT, a child of that stored context, written by hand in `WebhookHttpClient`.
- Its attributes are delivery id, attempt number, probe, outcome, status code and the destination host. It never carries the URL, path or query.
- The HTTP client is not auto-instrumented, so no span or meter is derived from the request URL.
- Outbound requests carry the attempt span's `traceparent`.

**Logs.**
- JSON through Spring Boot structured logging.
- Fields: `trace_id`, `span_id`, `tenant_id`, `event_id`, `delivery_id`, `endpoint_id`, `attempt`, `outcome`, `reason`, `replica`.
- Never logged: `Authorization` values, API keys, signing secrets, signatures, payload bodies, and any part of a subscriber URL other than its host.

## 11. Security

**11.1 Authentication.**
- `ApiKeyFilter` decides by the first path segment, read the way Spring MVC matches it (decoded, without path parameters). `admin` takes the admin token, `actuator` is open, and every other path takes an API key. A route added later is therefore authenticated without being listed, and a path spelled `/%61dmin/tenants` cannot reach an admin route with an API key.
- A missing, malformed or unknown credential gets the same `401` with `WWW-Authenticate: Bearer`.
- API keys are `rwh_` plus 32 random bytes in base64url, shown once. Anything of another form is rejected without a lookup.
- Only the SHA-256 is stored. A slow hash would protect nothing here, because the keys are high-entropy.
- Lookups are cached per replica for 30 s (positive results only), together with the tenant's status, rate and burst. The 30 s count from the start of the lookup.
- An eviction also discards the result of every lookup that was running when it happened. A request with the old key that read the tenant row just before a rotation therefore cannot put that key back into the cache.
- A suspended tenant is rejected with 403.
- `/admin/**` requires a static token from the environment, compared in constant time. `StartupInvariants` refuses to start without one of at least 32 characters.

**11.2 Tenant scoping.**
- Every statement behind a tenant route carries `tenant_id = :t`. A mutating route whose first statement matches no row answers 404 and runs nothing else.
- `SqlTenantScopeTest` scans the repository SQL constants and fails on a tenant-table statement without it. The exemption list holds worker and job statements only, by class.
  - A tenant table is every table but `tenant`, whose rows are addressed by their own id.
  - An `INSERT` that only supplies values is not checked. It creates a row and can neither read nor change a row of another tenant.
- `TenantIsolationIT#mutatingRoutesLeaveForeignTenantUntouched` calls every mutating route (patch, delete, rotate, single and bulk replay) with a foreign id. It snapshots the foreign tenant's endpoint, delivery and circuit rows before the call and asserts they are identical after it, in addition to the 404. Read routes are checked for the 404.

**11.3 Signatures.** The format follows Standard Webhooks.

| Header | Value |
|---|---|
| `webhook-id` | The delivery id, identical on every retry |
| `webhook-timestamp` | Unix seconds from the database clock, read in the load step, fresh per attempt |
| `webhook-signature` | `v1,<base64 HMAC-SHA256(secret, "{id}.{timestamp}.{body}")>` |

- The timestamp is inside the MAC input, and the body is the stored bytes.
- The README ships a reference verifier that rejects a timestamp more than 5 minutes away from the time of the check, whether it is older or lies ahead. Because the timestamp comes from Postgres, a replica with a wrong wall clock cannot get deliveries rejected.
- After rotation both signatures are sent, space separated, for 24 h. Then the old secret is ignored and later purged. The database clock decides when the overlap ends.
- A signing secret is `whsec_` plus 32 random bytes in base64, shown once. The 32 bytes are the MAC key.
- Secrets are encrypted at rest with AES-256-GCM under a key from the environment, with a one-byte key-version prefix. The key is 32 bytes, given in base64. A stored value is the version byte, a random 96-bit nonce, then the ciphertext with its tag.

**11.4 SSRF and the outbound client.**
- `WebhookHttpClient` is the only class that builds an HTTP client; an `ArchitectureTest` rule enforces it.
- Its constructor requires a `DnsLookups`, and a `DnsLookups` cannot be built without an `AddressPolicy`. There is no default, so an unguarded client cannot be constructed.
- **Stateless client.** One client serves all tenants, so it must carry nothing from one request to the next. It is built with cookie management, automatic retries, auth caching, content compression and redirect handling all disabled, without proxy support and without system properties.
  - No cookie store: a `Set-Cookie` from one tenant's endpoint can never reach another tenant's request.
  - No automatic retries: the library default would re-send on 429 and 503, which would be a second request for one attempt row.
  - No compression: no `Accept-Encoding` is sent, so a compressed response is never inflated.
- **Bounded response.** The status line and each header line are limited to 8 KiB, counted with the two bytes that end a line, and a response to 100 headers. Exceeding either fails the attempt as `response_too_large`. The body is read through a bounded stream. At 64 KiB the connection is aborted, not drained.
- **Connect-time check.** `SsrfGuardDnsResolver` is the client's `DnsResolver`. The addresses it returns are the addresses the socket connects to, so there is no gap between check and use, and DNS rebinding is covered. If any resolved address is blocked, the whole lookup is rejected. Reused keep-alive connections were validated when they were opened.
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

**11.5 Limits.**

| Limit | Value |
|---|---|
| Request body | 256 KiB (413 above) |
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

The three limits on an event body are set in the code and not left to the defaults of the JSON library. A body beyond one of them answers `400`, like a body that is not JSON.

The limit on a member name also holds on the other routes that read a body, where it is set for the JSON mapper of the API. It is far below the 50,000 bytes that the library allows, for this reason: the parser keeps the member names it has read in a table that all requests share, so a name stays in memory after its request has ended. The limit bounds what one name can add to that table.

The endpoint cap is exact, and an endpoint is never created without its circuit row:

```sql
-- POST /v1/endpoints, one transaction
SELECT max_endpoints FROM tenant WHERE id = :t FOR NO KEY UPDATE;
SELECT count(*) FROM endpoint WHERE tenant_id = :t AND status = 'active';   -- at the cap: 409
INSERT INTO endpoint (tenant_id, url, event_types, secret) VALUES (...) RETURNING id;
INSERT INTO endpoint_circuit (endpoint_id) VALUES (:id);
```

`FOR NO KEY UPDATE` serialises creates without blocking the tenant's ingest.

**11.6 Telemetry hygiene.** The redaction list of section 10 applies to logs, spans and metrics. Three tests assert it with a subscriber URL that carries a marker in its path and query:
- `LogFormatIT#subscriberUrlIsReducedToHost`
- `TracingIT#attemptSpanCarriesHostOnly`
- `MetricsIT#noMeterHasHostOrUrlLabel`, which scans the whole scrape for the marker, the receiver host and the ids.

**11.7 Untrusted trace context.**
- `UntrustedTraceContextFilter` runs before the tracing filter on public routes. It strips `traceparent`, `tracestate` and `baggage`.
- It keeps the `traceparent` only if it is version 00, 55 characters, lowercase hex with non-zero ids, and hands that to `IngestService` as a span link.
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
| Request permits (`TenantConcurrencyFilter`) | 8 per tenant per replica, all tenant routes | Beyond 8 concurrent requests a tenant gets `429` without acquiring a database connection. It can hold at most 8 of the 16 `api` connections. |
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
- On tenant routes these errors map to `503 database_busy`.

## 13. Failure table

The checked copy of this table lives in `docs/failure-table.md`, where a row appears only once the test that proves it exists. The README shows the headline rows. `FailureTableCheck` (section 16) fails the build unless every test linked from a table in either file ran and passed in the same build.

| # | Failure | Proven behaviour | Test | Forced by |
|---|---|---|---|---|
| 1 | Worker killed mid-delivery | Redelivered after the lease; attempt 1 `unknown`, attempt 2 `success` | `WorkerKillIT#killedMidRequest` | Receiver holds the request; SIGKILL; lease aged |
| 2 | Crash after subscriber 200, before commit | Exactly two requests with the same `webhook-id`; one `succeeded` | `WorkerKillIT#killedAfterResponseBeforeCommit` | Receiver holds the request while the test locks the delivery row, then answers 200; F1 blocks; SIGKILL once the block is seen |
| 3 | Worker killed after claim, before send | Zero requests from the dead replica; delivered by a replica started after the kill; attempt 1 `unknown` | `WorkerKillIT#killedAfterClaimBeforeSend` | Test `ACCESS EXCLUSIVE` lock on `event` blocks the load of an ordinary closed-circuit delivery |
| 4 | Replica killed during ingest | Nothing or everything; retry with the same key yields one event | `IngestKillIT#killedBeforeCommit`, `#killedAfterCommitBeforeResponse` | Test lock on the endpoint row blocks fan-out |
| 5 | Duplicate submit | One event; same id returned; different body gets 422 | `IdempotencyIT#sameKeyReturnsSameEvent`, `#concurrentSameKeyCreatesOneEvent`, `#sameKeyDifferentBodyIsRejected` | Request 1 blocked in its transaction; request 2 seen waiting on the unique index |
| 6 | Two replicas race for one delivery | One claimer gets it; the other skips without waiting | `ClaimRaceIT#secondClaimerSkipsLockedRow`, `#fourReplicasDeliverEachEventOnce` | Two open transactions; then 4 pollers over 2,000 deliveries |
| 7 | Lease expires while the first replica is still sending | Both may send; only the lease holder's result is recorded | `LeaseFenceIT#lateFinalizeAfterReclaimChangesNothing`, `#repeatedFinalizeIsNoOp` | Lease aged between claim and finalize |
| 8 | Database restart | No delivery lost; the delivery in flight across the restart is recorded once from one request; ingest answers 503 and the same key succeeds afterwards; replicas stay up | `DatabaseRestartIT#inFlightAndQueuedSurviveRestart` | Receiver holds a request; container restart on a fixed port; receiver released |
| 9 | Endpoint down for hours | Circuit opens after 5 failures; held deliveries use no attempts; one probe fleet-wide per interval; probe backoff stays capped; recovery drains | `EndpointOutageIT#circuitOpensAndHolds`, `#oneProbeAdmittedAcrossConcurrentClaimers`, `#probeBackoffIsCappedAfterManyFailures`, `#recoveryDrainsHeldDeliveries`, `#probeSlotReopensAfterProberKilled` | Two connections on the gate; probe time aged; `probe_failures` set to 1000; a canary on a second endpoint witnesses that claims ran |
| 10 | Endpoint down for good | A single delivery ends `dead (exhausted)` after 5 attempts and 7 probes; a backlog ends `dead (expired)` at 72 h; a delivery whose holder was killed expires with that attempt logged `unknown`; replay delivers and its run starts at attempt 1 | `EndpointOutageIT#hardDownEndpointWithDefaultBreakerSettings`, `#heldDeliveriesExpireAfterBudget`, `#expiredAfterHolderKilledReplaysCleanly`, `ExpiryIT#pendingPastBudgetBecomesDead` | Production breaker and retry settings; rows aged |
| 11 | One event always rejected by a healthy endpoint | Twelve attempts, each wait inside its schedule window, then `dead (exhausted)`; replay delivers once | `DeadLetterReplayIT#exhaustedAfterTwelveAttempts`, `#replayedDeliveryIsDeliveredOnce`, `RetryScheduleTest` | Breaker at defaults; other deliveries succeed in between |
| 12 | A delivery that kills its worker every time | Dead-lettered on the 13th claim without a send | `PoisonDeliveryIT#thirteenthClaimDeadLetters` | Twelve aged leases |
| 13 | Slow endpoint starving others | At most 4 concurrent requests per replica to it; other endpoints complete meanwhile; a drip-fed response is cut at the deadline | `SlowEndpointIsolationIT#slowEndpointDoesNotDelayOthers`, `#drippingResponseIsCutAtDeadline` | Receiver holds every request to the slow endpoint until the test ends |
| 14 | Subscriber DNS that never answers | Attempt fails inside the DNS budget; at most 4 stuck lookups per tenant; other tenants complete; the tenant's healthy endpoints lose no attempt and keep a closed circuit | `SlowEndpointIsolationIT#slowDnsDoesNotStarveOthers`, `#stuckDnsOfOneEndpointDoesNotFailSiblingEndpoints`, `SsrfIT#registrationDnsLookupIsBounded` | `StubDns` delegate that blocks until the test releases it |
| 15 | One tenant's backlog starving another tenant | Held case: tenant A never exceeds 16 in flight per replica and tenant B completes meanwhile. Fast case: with a deep backlog on A's instantly answering endpoint, B's request arrives before A's 200th of 2,000 | `TenantIsolationIT#backlogOfOneTenantDoesNotStarveAnother`, `#deepBacklogOnFastEndpointDoesNotStarveAnother` | Held case: A has 5 endpoints and the receiver holds 4 requests on each, so the tenant cap binds before the endpoint caps. Fast case: 2,000 aged due rows for A, one newer row for B; ordering read from the receiver's request log |
| 16 | Tenant floods ingest | Admitted units equal the bucket exactly across replicas; large bodies cost more; the 9th concurrent request gets 429 without acquiring a database connection; an unsatisfiable request gets 429 without `Retry-After`; tenant B is served | `RateLimitIT#limitIsExactAcrossReplicas`, `#refillIsExact`, `#fanOutIsChargedPerDelivery`, `#largeBodiesAreChargedBySize`, `#costAboveBurstIsUnsatisfiable`, `#ninthConcurrentRequestTakesNoConnection`, `#floodingTenantDoesNotBlockOtherTenant` | Rate 0 and fixed burst; eight requests with unfinished bodies hold A's permits; A's bucket row locked by the test |
| 17 | Tenant replays a huge dead-letter backlog | At most `limit` rows per call, charged per row, spread over 60 s; a kill after the charge re-arms nothing | `ReplayIT#bulkReplayIsBoundedAndCharged`, `#killedMidReplayRearmsNothing` | Dead rows inserted directly; test lock on the endpoint row blocks the replay's share lock |
| 18 | Subscriber URL points at, or rebinds to, a private address | Rejected at registration; blocked at connect with zero accepted connections; redirects not followed | `SsrfIT#registrationRejectsPrivateTargets`, `#rebindingIsBlockedAtConnect`, `#redirectIsNotFollowed`, `AddressPolicyTest`, `DnsLookupsTest#systemResolverDoesNotLetADeniedAddressLiteralPast` | `StubDns` answers public first, loopback later; listener counts accepts; denied address literals given to the system resolver |
| 19 | Absurd `Retry-After` | Clamped to between the own delay and one hour | `RetryAfterIT#clampedAtBothEnds` | Receiver sends 0 and 10^9 |
| 20 | Rolling deploy (SIGTERM) | Nothing is claimed after the poller stopped; in-flight attempts finish and are recorded; the process exits by itself with code 143 before lease expiry | `GracefulShutdownIT#drainsInFlightAndExits` | Receiver holds 4 requests; a 5th delivery is due and unclaimed only because the endpoint is full; SIGTERM; `poller_stopped` seen; receiver released |
| 21 | Endpoint deleted while deliveries are pending or being created | Delivery to it stops when the delete commits; pending rows become `cancelled`; nothing is created or re-armed for it afterwards; a delete by another tenant changes nothing | `EndpointDeleteIT#deleteStopsDeliveryAndCancelsPending`, `#ingestRacingDeleteLeavesNoOrphan`, `#replayRacingDeleteLeavesNoOrphan`, `TenantIsolationIT#mutatingRoutesLeaveForeignTenantUntouched` | Receiver holds 4 requests with 2 rows still due; canary as witness; both race orders forced with two test-managed transactions |
| 22 | Replay racing retention | A replayed delivery is never purged | `RetentionIT#replayedDeliveryIsNotPurged`, `#purgesOnlyOldTerminalRows` | Replay holds the row uncommitted; a retention run returns without waiting and leaves it; replay commits; a second run deletes nothing |
| 23 | Secret rotation with deliveries in flight | Both signatures during the overlap; old secret rejected after it | `SignatureIT#bothSignaturesDuringOverlap` | Expiry aged |
| 24 | Tampered body or replayed old request | Reference verifier rejects both | `SignatureIT#verifierRejectsTamperedBodyAndStaleTimestamp` | Direct |
| 25 | Caller forces trace sampling or picks a trace id | With sampling off, a caller's sampled flag exports nothing; with sampling on, the root span has its own trace id and carries the caller's context only as a link | `TracingIT#callerCannotForceSampling`, `#callerTraceIdBecomesLinkOnly` | Sampler probability 0, then 1; in-memory span exporter |
| 26 | Connections to Postgres go half-open (failover, partition) | The cut-off replica's blocked call fails at the socket timeout and it resumes on new connections; another replica delivers meanwhile; a transaction left open on the server is ended within 10 s, so its row lock cannot pin a delivery | `DatabaseBlackholeIT#pollerResumesAfterSocketTimeout`, `#openTransactionIsEndedByServer` | `TcpProxy` stops relaying without closing, after `PgWait` shows the replica inside a statement |
| 27 | Subscriber answers with an endless header line or body | An oversized head fails the attempt as `response_too_large`, not `deadline`; a 2xx with an endless body is a success and the connection is dropped at the limit; another tenant's delivery completes meanwhile | `OutboundRequestIT#endlessHeaderIsCutAtLimit`, `#endlessBodyIsNotDrained` | `FaultReceiver` scripts `endlessHeader` and `endlessBody`; the receiver records bytes written and who closed |
| 28 | Subscriber answers 503 with `Set-Cookie` | Exactly one request per attempt row; no later request, from any tenant, carries a `Cookie` header | `OutboundRequestIT#oneRequestPerAttemptAndNoCookieReuse` | Two tenants' endpoints on one receiver host |
| 29 | Replica killed while creating an endpoint or a tenant | No endpoint without its circuit row and no tenant without its bucket row; the repeated call succeeds | `EndpointApiIT#killedDuringCreateLeavesNoHalfEndpoint`, `AdminApiIT#killedDuringCreateLeavesNoHalfTenant` | Test table lock blocks the second insert of the transaction |
| 30 | Abusive tenant must be stopped | After suspension nothing more is claimed for the tenant; its key is rejected; held rows are untouched and deliver after resume | `AdminApiIT#suspendStopsOutboundDeliveries`, `#resumeDeliversHeldDeliveries` | Receiver holds the tenant's 4 in-flight requests; a canary of another tenant witnesses that claims ran |
| 31 | Tenant issues expensive reads, or a statement blocks | Lists are keyset pages of at most 100 rows behind a required endpoint and state; a blocked statement answers 503 inside the lock timeout | `DeliveryApiIT#listIsKeysetPagedAndBounded`, `ApiTimeoutIT#blockedStatementAnswers503`, `ProblemsTest` | Test row lock held past `lock_timeout` |
| 32 | Subscriber URL carries a secret in its path or query | The marker appears in no log line, no span and no meter | `LogFormatIT#subscriberUrlIsReducedToHost`, `TracingIT#attemptSpanCarriesHostOnly`, `MetricsIT#noMeterHasHostOrUrlLabel` | URL with a marker in path and query; full scrape and all exported spans scanned |

## 14. Limitations

1. **No ordering.** Retries, holds and replays reorder deliveries.
2. **Duplicates happen** (K4, K5, lease overrun). Subscribers must dedupe on `webhook-id`.
3. **Postgres is the single point of failure.** A failover to an asynchronous replica can lose acknowledged events. High availability is a deployment concern.
4. **Claim cost grows with the number of active endpoints, not with the backlog.**
   - Every claim does one index probe per eligible endpoint and locks one head row per endpoint with due work.
   - At tens of thousands of endpoints the claim becomes the bottleneck. The scaling path is a ready-endpoint table and time partitions.
   - The queue table needs aggressive autovacuum.
   - Retention is batched deletes.
5. **Bulkheads are per replica.** Fleet-wide concurrency to one endpoint is at most 4 times the replica count; it is not an exact number.
6. **Throughput per endpoint and per tenant is capped by design.**
   - One endpoint drains at one delivery per claim round trip per replica, at most 4 at a time.
   - A tenant can use 16 of a replica's 64 workers unless the cap is raised.
   - When more tenants have due work than a replica has free permits, the tenants with the oldest head rows go first. Fairness is per claim, not a quota over time.
7. **The rate limiter is strict-side.** Failed ingests are not refunded. Retries are not charged. Each rejection past the request permits still costs one statement. The type and `Retry-After` of a 429 come from a tenant record cached up to 30 s.
8. **Isolation covers a bounded number of misbehaving tenants.** Two tenants each holding 8 request permits can fill the `api` pool. Eight tenants with tarpit DNS can fill the DNS pool.
9. **Unauthenticated floods** (invalid keys, slow request bodies) reach the service and, for the key lookup, the database. That belongs to the ingress.
10. **Endpoint ownership is not verified.** A tenant can aim signed POSTs at any public host, up to its own rate. There is no per-destination limit across endpoints. The operator's lever is suspension.
11. **The SSRF policy works at the IP layer.** It cannot see an internal service on public address space or a public proxy that forwards inward. HTTP proxies are unsupported.
12. **A stuck native DNS lookup holds its thread** until the OS resolver gives up. The image should set short resolver timeouts.
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
23. **A jump of the database clock shifts every schedule.** A forward jump can expire leases early, which produces fenced duplicates and no loss.
24. **Rows removed by hand are not detected.** An endpoint whose circuit row or a tenant whose bucket row was deleted in SQL stops working silently. The service only guarantees it never creates that state.
25. **A replica-local fault is recorded as endpoint failure.**
    - A replica that still reaches Postgres but has lost its egress fails every attempt quickly.
    - It consumes attempts and can open circuits for healthy endpoints fleet-wide. Nothing is lost (`dead` is replayable), but deliveries are delayed.
    - The signal is a per-replica skew in `webhooks_delivery_attempts_total{reason="connect"}`. There is no self-quarantine.
26. **Cancelling after a delete is asynchronous.** Delivery stops at commit, but rows show `pending` until `CancelJob` reaches them (30 s interval, 1,000 rows per batch).
27. **Time bounds cut both ways.**
    - A legitimate statement slower than `statement_timeout` fails. On a very large pending set the gauge query times out and the gauges go stale.
    - A transaction whose client was cut off pins its row locks for up to 10 s.
    - A call on a dead connection is noticed only after the socket timeout.
28. **An orphaned autocommit statement can commit after its client died** (section 3). The design tolerates this; it does not prevent it.
29. **DNS admission is per tenant per replica.**
    - While a tenant's lookup slots are stuck, its other deliveries on that replica are re-queued every 2 s without cost.
    - Its registrations answer 429 during that time.
    - A probe that wins the gate and is then refused a lookup wastes that probe slot.
30. **No storage quota.** The byte-weighted charge bounds what a tenant can store per second (section 12). There is no cap on stored bytes or on pending rows beyond rate times retention.
31. **The delivery list is narrow.** It needs an endpoint and a state. There is no tenant-wide listing, and the attempt log is read one run at a time.
32. **A request on a connection that the subscriber has just closed fails.** The client keeps connections for reuse and repeats no request by itself, so such a request is a failed attempt and waits for its retry. A connection that was idle for more than 2 s is tested before it is used again, which leaves the short window after a request.

## 15. Operations

**Startup invariants** (`StartupInvariants`; the process exits non-zero on any failure):
- Admin token present and long enough.
- Secret-encryption key present and usable: 32 bytes in base64.
- `default_transaction_isolation` is `read committed`.
- `synchronous_commit` is not `off`.
- Lease >= attempt deadline + 30 s.
- Start cutoff + attempt deadline <= finalize cutoff, and finalize cutoff < lease. A request that starts at the start cutoff then ends inside its lease, with time left to record it.
- `max-attempts` and `max-in-flight` are at least 1. With 0 attempts every delivery would be dead-lettered by its first claim, and with no room the replica would never claim.
- For each pool:
  - the borrow wait, `connectTimeout` and `socketTimeout` are set, and `tcpKeepAlive` is on;
  - `lock_timeout < statement_timeout < socketTimeout`;
  - `idle_in_transaction_session_timeout` is non-zero;
  - borrow wait plus aliveness test plus socket timeout is below the start cutoff.
- The JVM runs with `-XX:+ExitOnOutOfMemoryError`, so an out-of-memory error is a kill-table case and never a replica that is Ready but no longer claims.
- No JVM proxy properties.
- SSRF allowlist empty and `http` disabled unless the `unsafe-local` profile is active.

**Liveness.** The `poller` health indicator belongs to the liveness group. It reports down when the poller loop has not finished an iteration for 60 s. A failed claim also finishes an iteration, so a database outage does not restart pods. Only a poller that stopped looping does. `PollerHeartbeatTest` covers it with an injected time source.

The database health indicator is off. The health endpoint takes no key, so it must not borrow pooled connections.

**Graceful shutdown** (`WorkerLifecycle`, a `SmartLifecycle` that stops before the web server and the pools):
1. Readiness goes out of service.
2. The poller stops claiming and its thread is joined. Only then is the log event `poller_stopped` written.
3. In-flight attempts finish and finalize. The 15 s deadline bounds this, and the drain waits at most 20 s.
4. The web server finishes open requests, then the pools close.

The JVM then exits with code 143 (128 + SIGTERM), which is the documented exit code of a clean drain. The deployment must allow at least 30 s of termination grace. Nothing needs releasing, because nothing is claimed that has not started.

**Background jobs.** They run on every replica on a timer, with no leader election. Each is an idempotent batch statement whose subquery uses `FOR UPDATE SKIP LOCKED`, so a job never waits on a row and concurrent runs are harmless.

- `ExpiryJob` (every 60 s) dead-letters pending rows past `expires_at` that are not leased. An abandoned row gets its missing attempt row in the same statement:

```sql
WITH expired AS (
  UPDATE delivery d
  SET state = 'dead', dead_reason = 'expired', lease_token = NULL, completed_at = now()
  FROM (SELECT id, lease_token IS NOT NULL AS abandoned
        FROM delivery
        WHERE state = 'pending' AND expires_at <= now()
          AND NOT (lease_token IS NOT NULL AND next_attempt_at > now())     -- not leased
        ORDER BY expires_at LIMIT 1000
        FOR UPDATE SKIP LOCKED) x
  WHERE d.id = x.id
  RETURNING d.id, d.replays, d.attempts, x.abandoned
)
INSERT INTO delivery_attempt (delivery_id, run, attempt_no, outcome, error)
SELECT id, replays, attempts, 'unknown', 'lease_expired' FROM expired WHERE abandoned;
```

- `CancelJob` (every 30 s) cancels the pending rows of deleted endpoints, 1,000 per statement, repeating while a batch is full:

```sql
UPDATE delivery
SET state = 'cancelled', dead_reason = 'endpoint_deleted', lease_token = NULL, completed_at = now()
WHERE id IN (SELECT d.id FROM delivery d JOIN endpoint e ON e.id = d.endpoint_id
             WHERE e.status = 'deleted' AND d.state = 'pending'
             LIMIT 1000 FOR UPDATE OF d SKIP LOCKED);
```

- `RetentionJob` (hourly, batches of 1,000) runs in this order:
  1. Delete terminal deliveries with `completed_at` older than 30 days:

     ```sql
     DELETE FROM delivery
     WHERE id IN (SELECT id FROM delivery
                  WHERE state <> 'pending' AND completed_at < now() - interval '30 days'
                  ORDER BY completed_at LIMIT 1000
                  FOR UPDATE SKIP LOCKED);
     ```

     The predicate sits on the locked row inside the subquery. A row that a replay holds is skipped without waiting. A row that a replay has already committed fails the predicate. There is no lock wait in this statement.
  2. Delete events older than 30 days that have no deliveries. Deliveries are only created at ingest, so such an event can never gain one.
  3. Delete `deleted` endpoints that have no deliveries.
  4. Clear expired previous secrets.

  The foreign keys are `RESTRICT`, so a mistake here fails loudly and does not cascade.

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

**Tenant suspension** is `UPDATE tenant SET status = :status WHERE id = :id` (section 5). It writes no delivery rows.

## 16. Test strategy and harness

Rules:

- No test sleeps to "let things happen". Waits are condition polls with a timeout.
- **Time is aged in the data, not in the clock.** `Rows` ages a row: it subtracts an interval from its time columns, which are `next_attempt_at`, `expires_at`, `completed_at`, `next_probe_at`, `refilled_at` and `prev_secret_expires_at`. Production code and `now()` run unchanged. This drives lease expiry, the twelve-step schedule, the probe interval, the 72 h budget and retention.
- **A negative claim needs a witness.** "Nothing was claimed" or "no attempt was used" is asserted only after something observable proves the poller ran in the state under test. The witness is either a canary (a delivery to another endpoint or tenant, ingested after that state was reached, has succeeded) or a positive signal such as `poller_stopped`.
- **Ordering assertions, not clocks.** Where a test is about who goes first, it reads the receiver's request log.
- Integration tests run with production retry, breaker, lease and timeout settings. The test-profile differences are the `unsafe-local` allowance for the loopback receiver with its `http` scheme, in the classes that deliver to the receiver, and in `TracingIT` only, the sampler probability.
- Three tests of `WebhookHttpClientIT` wait for a time bound to end a request, and their clients have a short one: an attempt deadline of 500 ms in the two that are cut at the deadline, and a lookup wait of 20 ms in the one whose lookup does not answer. The client with the short deadline is built from settings, as the application builds its client, so the bound that cuts those requests is `attempt-deadline`. A replica under test always runs with the shipped values.
- **A replica inside the test JVM does not start its worker by itself.** The replicas of a build share one database, so a worker that ran all the time would take the deliveries of tests that are about something else, and those of a test that has to claim them itself. A test that wants deliveries sent starts the worker of its replica, or builds the pollers it needs. The worker is stopped when the test ends. One test is the exception: `WorkerLifecycleIT#replicaStartsItsWorkerByItselfAndStopsItWhenItCloses` starts a second replica the way the application starts, with nothing taken from Spring, and closes it before it ends. The attempt is recorded under the id of that replica, which shows that Spring started its worker. Its pools hold 4 connections each, because the replicas that the build keeps have theirs open.
- **Every test of such a replica begins with an empty queue.** The pending deliveries that earlier tests left are deleted first. A worker that a test starts therefore sends the deliveries of that test and no others.

Harness:

| Piece | What it does |
|---|---|
| `FaultReceiver` | In-process HTTP server. Per-path scripts: status with optional response headers, a body of a given length, delay, hold until released, drip, close without response, redirect, `endlessHeader`, `endlessBody`. Records every request with its headers, counts accepted connections and peak concurrency per path, and for the endless scripts the bytes written and whether the peer closed first. |
| `ReplicaProcess` | Starts the packaged jar as a child JVM against the Testcontainers database, with the replica id as `application_name`. Captures stdout. `kill()` is SIGKILL, `terminate()` is SIGTERM, `exitCode()` waits for the process to end by itself. |
| `PgWait` | `untilBlocked(sqlFragment, replica)` polls `pg_stat_activity` for a backend of that replica waiting on a lock in a statement containing the fragment. The lock timeout of the replica's pool ends that wait, so the test has that long to act. `untilGone(replica)` waits until its backends have left. |
| `StubDns` | The resolver behind `DnsLookups` in a replica inside the test JVM, in place of the system resolver: scripted answers per call, or block until released. A name that no test scripted does not resolve. |
| `TcpProxy` | In-process TCP forwarder between one replica and Postgres. `blackhole()` keeps every existing connection open and relays nothing more on it, ever; new connections are accepted and ignored. `heal()` lets new connections relay again. |
| `TestDatabase` | One Testcontainers PostgreSQL per test JVM, started on first use. Hands its JDBC URL and credentials to replicas. Gives a test its own `JdbcClient` outside the pools of the service, and a connection with autocommit off for a transaction the test holds open. |
| `ReplicaSettings` | The settings a replica under test needs beyond the shipped configuration: the `TestDatabase` connection with the replica id as `application_name`, an admin token and a secret-encryption key. |
| `InProcessReplica` | Annotation for a test class that runs against a replica inside the test JVM on a random port, with those settings and the shipped configuration. All such classes share one application context, and so one pair of pools. The worker of the replica is left to the tests, and the queue is emptied before each test. |
| `UnsafeLocalReplica` | An `InProcessReplica` with the `unsafe-local` profile and its allowance: plain `http` and the loopback address, where `FaultReceiver` listens. The classes with this annotation share a second application context. |
| `ReplicaWorker` | Starts and stops the delivery worker of a replica inside the test JVM. Each start gets a `WorkerLifecycle` and a poller of its own from the application context, wired as the application wires them, and runs the start and stop that Spring runs. |
| `TestTenant` | A tenant created through the admin API of a replica, with the API key it was shown. Registers an endpoint and sends an event as that tenant, and counts the tenant's event and delivery rows. |

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
  | K7, row 17 | Endpoint row | The replay's share lock, after the charge committed |
  | K8, K9, row 29 | `LOCK TABLE` on `endpoint_circuit` or `rate_bucket` | The second insert of the create transaction |

  - **Only transactions and reads.** A lock-block kill is landed only on a statement inside an explicit transaction or on a read. The orphaned backend then finds end-of-file before any `COMMIT` and rolls back, or has nothing to commit. No proof blocks an autocommit write, because that write would commit after the kill and change the state under test.
  - **K3 in detail.** The delivery is inserted not yet due. The test locks `event`, makes the row due from a second connection, waits until the load is blocked, kills, and releases. The surviving replica is started after the kill.
  - **K4 and K6** use the receiver's hold, not a lock.
  - No hook exists in production code and no timing is assumed. The pool `lock_timeout` values (2 s and 5 s) leave room for a 10 ms poll.
- **(b) Claim race.** `secondClaimerSkipsLockedRow` runs the real claim on connection A without committing. A second delivery for the same endpoint is accepted then, and the claim runs on connection B, whose transaction begins with it. B returns without waiting, and with the second delivery only: it saw the due rows and passed over the locked one. B has a lock timeout, so a claim that waited would fail. After both commit, a claim gets zero rows. The four-poller test is a volume check on top of this, not the proof.
- **(c) One probe.** `oneProbeAdmittedAcrossConcurrentClaimers` runs the gate on connection A uncommitted, sees connection B block on the row, commits A, and asserts B updated zero rows. A second phase runs 4 replicas against a down endpoint and counts exactly one request per aged probe interval.
- **(d) SSRF.** `SsrfIT` runs in the replica without the loopback allowance.
  - `rebindingIsBlockedAtConnect`: `StubDns` answers with a public address at registration and 127.0.0.1 at delivery, where the receiver is listening. It asserts the attempt error, `webhooks_dns_lookups_total{result="blocked"}` (the operator reason), and zero accepted connections. The control is a connection that the test itself opens to that address afterwards, which the same listener counts. Every integration test that delivers to the receiver is a control too: the receiver is reachable when allowed.
  - `redirectIsNotFollowed` cannot go through that replica, which reaches no receiver. It sends one request with a `WebhookHttpClient` whose address policy allows the loopback address. The receiver answers with a redirect to another of its paths. The client reports the 302, and the receiver has seen one request. That a 3xx is a failed attempt is the classification of step 5, which `DeliveryWorkerTest` covers.
  - `WebhookHttpClientIT#hostIsConnectedAtTheAddressThatTheGuardedLookupReturned` sends to a name that only `StubDns` resolves, to the address of the receiver. The request arrives, so the socket connects to the address that the guarded lookup returned. A client that had the name checked and then resolved it again by itself would find no address.
- **(e) Shutdown.** `GracefulShutdownIT` uses the endpoint bulkhead as the witness.
  1. Five due deliveries for one endpoint; the receiver holds.
  2. Wait until the receiver holds 4 requests. The fifth is due and unclaimed only because the endpoint is full.
  3. SIGTERM. Wait for `poller_stopped` on the child's stdout.
  4. Release the receiver.

  It asserts four `success` rows and four requests, the fifth delivery still at `attempts = 0` with no lease, and that the process ended by itself with exit code 143 inside the drain time. A poller that kept claiming would take the fifth as soon as a slot freed.
- **(f) Rate limit.**
  - `limitIsExactAcrossReplicas` sets rate 0 and burst 20 and sends 200 requests to 4 replicas, 8 at a time per replica so the request permits never reject. It asserts exactly 20 accepted and that every other response is a well-formed `429 rate_limit_unsatisfiable` problem document without `Retry-After`.
  - `refillIsExact` uses rate 0.01 per second and ages `refilled_at` by 500 s. Exactly 5 are accepted, and the next token is 100 s away.
  - `ninthConcurrentRequestTakesNoConnection` opens eight requests whose bodies are never finished, so each holds a permit before any database work. The ninth gets `429 concurrency_limit`, and the `api` pool's acquire count is unchanged across it. Tenant B is served meanwhile.
- **(g) Long durations.** The 72 h budget and the schedule are driven by `Rows`.
  - `RetryScheduleTest` asserts the eleven production windows as a pure function.
  - `exhaustedAfterTwelveAttempts` asserts that each `next_attempt_at` written by finalize lies inside the window for its attempt number, before ageing the row.
- **(h) Half-open connections.** Both `DatabaseBlackholeIT` tests first make the replica provably busy in a statement, then cut it off.
  - `pollerResumesAfterSocketTimeout`:
    1. The test locks the `delivery` table until the claim of the replica behind `TcpProxy` is seen blocked, calls `blackhole()` and releases the lock.
    2. A second replica delivers a new event meanwhile.
    3. After `heal()` the second replica is stopped and a delivery is made due, which only the first can now deliver.

    Without a socket timeout the poller never returns from its read.
  - `openTransactionIsEndedByServer`: F1 is blocked as in K5, then the proxy is blackholed and the lock released. The backend now idles in a transaction holding the row. The test waits with `PgWait.untilGone`, ages the lease and asserts a second replica reclaims and delivers.

**The failure-table check.** `FailureTableCheck` is a small main class run by `exec-maven-plugin` in `verify`, after Failsafe.

- The build deletes `target/surefire-reports` and `target/failsafe-reports` in `initialize`. Reports from an earlier build can therefore never satisfy the check, with or without `clean`.
- It parses the kill table and the failure table in `docs/failure-table.md` and the headline table in `README.md`. A table is checked when its header has a `Test` column. Each file must contain a fixed number of checked tables, one in `README.md` and two in `docs/failure-table.md`, so renaming the column of any table cannot switch its check off. Matching rules:
  - `Class#method` must match at least one `testcase` of that class named `method`, `method[...]` or `method(...)`. Every match must have passed.
  - `#method` inherits the class named before it in the same cell.
  - A bare `Class` means every `testcase` of that class: at least one, all passed.
  - Passed means no `skipped`, `failure`, `error`, `flakyFailure` or `rerunFailure` child. The POM does not enable reruns.
- It fails on a row with no link, on a missing method, and on a linked test that did not run. That covers `@Disabled`, an excluded tag, `-DskipITs` and a renamed method with one rule.
- `FailureTableCheckTest` feeds it one fixture report per rule above.
- The default `mvn verify` runs every tag. `process` and `dbrestart` exist only so a developer can exclude them locally, and then the check fails by design.
- `-DfailureTable.skip=true` skips the check, so that a single test can be run locally. CI never sets it.

## 17. Rejected alternatives

| Decision | Rejected | Why |
|---|---|---|
| Postgres as queue, `SKIP LOCKED` claim | Kafka, SQS, Redis | Fixed constraint, and one store means no dual write between "event saved" and "message queued". |
| Deliveries written in the ingest transaction | Async fan-out relay over an outbox table | A relay adds a second claimer and a window where an event has no deliveries. Fan-out is at most 50 rows. |
| Lease is `next_attempt_at` | `in_flight` state plus a reaper job | The reaper is one more component that can die. A timestamp cannot. |
| Lease token as fence | Trusting lease time alone | A stalled replica cannot know its lease expired. The token makes a late write harmless. |
| Polling with idle backoff | `LISTEN/NOTIFY` | Needs a dedicated connection per replica and still needs polling for missed notifications. |
| Attempts counted at claim | Counted at finalize | A delivery that kills its worker would retry forever. |
| Any non-2xx is retried | 4xx as permanent failure | A subscriber deploy bug returning 404 for ten minutes would destroy events. |
| Endpoint-first claim: one head row per eligible endpoint | A window of the oldest due rows, reduced to one per endpoint | The window is filled by whichever endpoint has the oldest backlog, so one fast endpoint with a deep backlog starved every other tenant. |
| The same | A larger window | Any single endpoint can still fill it. |
| The same | Repeating the windowed claim with already-picked endpoints excluded | Fair, but every repeat scans past the excluded backlog, and it needs a per-tenant pending cap to bound that scan. |
| The same | A per-endpoint queue-head table | A second row to keep consistent in ingest, finalize and replay. It is the scaling path, not the starting point. |
| One row per endpoint per claim | Claim a batch, then release the overshoot | Releasing causes a claim-and-release loop on deep backlogs and adds a release race. |
| Breaker state in Postgres | Resilience4j, or a per-replica breaker | In-memory state cannot hold deliveries fleet-wide or admit one probe across replicas, and is lost on restart. The price is that one replica's faulty view becomes everyone's (limitation 25). |
| No replica self-quarantine | Stop claiming when the last N attempts across several endpoints all failed to connect | It cannot tell "my egress is broken" from "these endpoints are down" without a known-good probe target, and a false positive takes a healthy replica out. |
| Probe slot as a pushed timestamp | `half_open` state | A state needs someone to leave it; a killed prober would wedge the circuit. |
| DNS `busy` releases the row | Record it as a failed attempt | It is this replica's admission decision. Recording it burned attempts and opened circuits on the tenant's healthy endpoints. |
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
| `JdbcClient` and explicit SQL | JPA | The locking clauses are the design; they must be visible. |
| READ COMMITTED plus row locks | SERIALIZABLE | Retry loops everywhere, and `SKIP LOCKED` queues do not need it. |
| SHA-256 for API keys | bcrypt | Slow hashes defend low-entropy secrets; these are 256-bit random. |
| Jobs on every replica | Leader election | The statements are idempotent, so a leader adds only a failure mode. |
| New root span at ingest | Caller's `traceparent` as parent | The caller would control the sampling decision and the trace id. |
| `Idempotency-Key` required | Optional key | An optional key makes non-idempotent producers the silent default. |
| Report directories deleted at build start | Comparing report timestamps with the build start | Deleting needs no clock and no extra argument to the check. |
