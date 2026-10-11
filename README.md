# reliable-webhooks

A webhook delivery service on PostgreSQL, under construction. The finished service accepts events from producers and delivers each event to the subscriber endpoints registered for it, with retries, signatures and isolation between tenants. Java 21, Spring Boot 4.1, PostgreSQL 18.

The design is in [docs/design.md](docs/design.md). The next section lists the parts of it that exist today. Everything else in this README describes the design.

## Current state

Built and tested:

- **Schema.** The complete data model of the design, as one Flyway migration.
- **Tenant administration.** `POST /admin/tenants` creates a tenant together with its rate bucket and returns the API key once. `POST /admin/tenants/{id}/api-key/rotate` replaces the key, and the replica that handled the call rejects the old key from then on.
- **Authentication.** The health endpoints are open, admin routes take a static admin token, and every other path takes a tenant API key, whether a route exists there or not. Only the SHA-256 of a key is stored. A key that is missing, malformed, unknown, rotated away or owned by a suspended tenant is rejected. A replica caches a key it has looked up for 30 seconds.
- **Endpoints.** `POST /v1/endpoints` registers an endpoint for a URL and 1 to 20 event types, and returns its signing secret once. `GET /v1/endpoints` and `GET /v1/endpoints/{id}` read endpoints with their circuit state, and `PATCH /v1/endpoints/{id}` replaces the event types. The id of another tenant's endpoint answers 404. An endpoint and its circuit row are created in one transaction. A tenant has room for 20 endpoints unless its cap is changed, and concurrent creates cannot exceed the cap.
- **URL policy.** An endpoint is registered only for an `https` URL on port 443 or 1024 to 65535, without userinfo, of at most 2048 characters, whose host resolves to public addresses only. The address ranges that count as not public are an explicit list. A refused URL gets `422 endpoint_url_not_allowed`, and the answer is the same for a private address, for a host that does not resolve and for a lookup that timed out.
- **Bounded DNS.** A registration waits 2 seconds for a lookup and is refused if the resolver has not answered by then. A replica runs at most 32 lookups at a time and at most 4 for one tenant, counting every lookup that has not returned. Beyond that a registration gets `429 lookup_busy` at once. A lookup that is stuck holds no database connection, and other tenants register meanwhile.
- **Encrypted secrets.** A signing secret is stored encrypted with AES-256-GCM under `webhooks.secret-encryption-key`, with a random nonce per secret.
- **Ingest.** `POST /v1/events` takes `{"type", "data"}` with an `Idempotency-Key` header and answers `202` with the id of the event. One transaction stores the event and one `pending` delivery for each active endpoint of the tenant whose event types hold the type of the event or `*`. A delivery is due at once and expires 72 hours later. An event that matches no endpoint is stored without deliveries. A request whose transaction cannot finish stores nothing, and the same key is accepted afterwards.
- **Event body.** The body that the deliveries of an event will send, `{"id", "type", "created_at", "data"}`, is rendered once and stored with the event. A number in `data` keeps its exact value, also when it is too large or too precise for a double. None is read into a double. `created_at` is the clock of the replica in milliseconds, always with three fraction digits. Of members with the same name in one object, the last one counts.
- **Idempotency.** A tenant and an `Idempotency-Key` give one event. A repeat with the same body answers `202` with the id of that event and `Idempotent-Replay: true`, and stores nothing. A different body under the same key answers `422 idempotency_key_reused`. Bodies are compared by the SHA-256 of their bytes. Of two concurrent requests with one key, one creates the event and the other waits for it and answers as a repeat. The same key under another tenant is another event.
- **Ingest limits.** The type of an event has 1 to 128 characters of `[a-z0-9_.-]`, `data` is a JSON object, and the key has 1 to 255 printable ASCII characters. In the body, a number has at most 1,000 digits, those of its exponent included, and an exponent that fits a Java `BigDecimal`, which is about plus or minus 2.1 billion. A member name has at most 1,024 bytes of UTF-8, and the body nests at most 500 levels deep, itself being the first. Anything else answers `400`.
- **Request body limit.** A request body above 256 KiB answers `413`, with or without a declared length. The limit is not special to ingest: an endpoint registration above it is refused in the same way. The limit on a member name is not special to ingest either: a registration with a member name above 1,024 bytes answers `400`.
- **Event read.** `GET /v1/events/{id}` returns an event with the endpoint, state and attempt count of each of its deliveries. Its `created_at` is the time of the database when the event was stored. The `created_at` in the body is the clock of the replica, so the two can differ. The id of another tenant's event answers 404.
- **Ingest meter.** The counter `webhooks.ingest.requests`, which the design lists under its Prometheus name `webhooks_ingest_requests_total`, counts the requests that `POST /v1/events` answered with one of four results: `accepted`, `replayed`, `invalid` for a `400`, `413` or `422`, and `busy` for a `503`. Nothing else is counted: not a request that fails authentication or is not sent as JSON, which does not reach the route, not a request to another route, and not a request that the route answered with a `500`. The results `rate_limited` and `unsatisfiable` of the design arrive with rate limiting.
- **Tenant scope.** A test reads the SQL of every repository and fails on a statement that names a table of the schema other than `tenant` without `tenant_id = :t`. An `INSERT` that only supplies values is not checked.
- **Errors.** Errors are RFC 9457 problem documents. A lock timeout, a statement timeout and a borrow wait that ran out are mapped to `503 database_busy` with `Retry-After: 1`. An ingest that waits for a locked endpoint row gets that answer when the lock timeout of 2 seconds ends the wait.
- **Bounded database calls.** Two connection pools, `api` and `worker`. Each sets a bound on the borrow wait, the aliveness test of a borrowed connection, the TCP connect, the lock wait, the statement, the socket read and the idle transaction. A parameter in the JDBC URL cannot remove a bound.
- **Startup invariants.** The service refuses to start if the admin token is shorter than 32 characters, the secret-encryption key is not 32 bytes in base64, a pool lacks a time limit or TCP keepalive or has its limits in the wrong order, the database is not at `read committed` or has `synchronous_commit` off, the lease is shorter than the attempt deadline plus 30 seconds, the JVM runs without `-XX:+ExitOnOutOfMemoryError`, or JVM proxy properties are set.
- **Logging.** Console logs are JSON. The `local` profile turns the JSON format off, so Spring Boot logs its usual plain text.
- **The failure-table check.** The build deletes the old test reports, runs the tests and then fails unless every test linked from a failure table ran and passed.

Not built yet: delivery, retries, the circuit breaker, rate limiting, the per-tenant request permits, signatures, the address check at connect time, endpoint delete, secret rotation, replay, the background jobs, the delivery meters, the metrics endpoint and tracing. An event is accepted and its deliveries are stored, and nothing sends them yet: every delivery stays `pending`. Ingest is not charged against the rate bucket, and an event is stored without a trace context. No circuit opens yet, so every endpoint reads `closed`.

## Guarantees

The design commits the finished service to the guarantees below. Section numbers refer to [docs/design.md](docs/design.md).

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

A guarantee counts as delivered only once the tests that prove it are listed in the failure table. Today:

- **G3 is delivered.** Row 5 of the failure table proves it, including two concurrent requests with one key. Nothing deletes an event yet, so a key holds for as long as its event exists.
- **G1 is built and tested, and not delivered yet.** `FanOutIT` shows that an event and its deliveries are written by one transaction, and `ApiTimeoutIT` shows that a transaction that cannot finish leaves no event behind. The row of the failure table for G1 (row 4) kills a replica in the middle of that transaction, and the test harness cannot start a replica as a process of its own yet.
- **Of G7 the part about registration exists.** An endpoint cannot be registered for a host with a non-public address.
- **The others do not hold yet.**

## How it works

This is the design. Of the diagram, the first line exists today: a replica accepts an event and writes it with its delivery rows in one transaction. The poller and the workers do not exist yet.

```
producer --POST /v1/events--> [replica: api] --one tx--> event + delivery rows (Postgres)
                                                              |
[replica: poller] --claim (SKIP LOCKED)--> [worker virtual threads] --signed POST--> subscriber
                                                              |
                                        finalize tx: delivery + attempt row + circuit
```

- Accepting an event writes the event and one delivery row per matching endpoint in a single transaction. The delivery rows are the queue, so there is no second system to keep consistent with the first.
- Every replica runs a poller that claims due deliveries with `FOR UPDATE SKIP LOCKED`. A claim is a lease with a random token. If the replica dies, the lease runs out and another replica takes the delivery.
- A worker sends the signed request and records the outcome in one transaction. Only the holder of the current lease token can record anything.
- Failed deliveries are retried on a backoff schedule and end as `dead`, which can be replayed. A circuit breaker per endpoint, kept in Postgres, holds deliveries to an endpoint that is down.

## Requirements

- JDK 21 or later.
- Docker, for the integration tests. Testcontainers starts PostgreSQL 18 in a container.

Maven comes with the wrapper (`./mvnw`).

## Build and test

```
./mvnw verify
```

This runs, in order:

1. the unit tests (`*Test`, Surefire);
2. the packaging of the executable jar;
3. the integration tests (`*IT`, Failsafe), against a replica on a random port and a PostgreSQL container;
4. the format check (google-java-format through Spotless);
5. the failure-table check.

`./mvnw spotless:apply` formats the sources. `.mvn/jvm.config` exports the JDK compiler packages that google-java-format needs when it runs inside Maven.

## Running one test

One unit test class, or one method of it:

```
./mvnw test -Dtest=ProblemsTest
./mvnw test -Dtest='ProblemsTest#lockTimeoutIsDatabaseBusy'
```

One integration test class, or one method of it:

```
./mvnw verify -Dit.test=AdminApiIT -DfailureTable.skip=true
./mvnw verify -Dit.test='AdminApiIT#rotatedKeyReplacesTheOldOne' -DfailureTable.skip=true
```

The unit tests run first in both integration commands. `-DfailureTable.skip=true` turns off the failure-table check. Without it the build fails as soon as a table links a test that was left out of the run, which is what the check is for.

## Failure table

The headline rows of [docs/failure-table.md](docs/failure-table.md). Each row names a failure, the behaviour that a test proves and that test. A row is added only together with its test.

| # | Failure | Proven behaviour | Test |
|---|---|---|---|
| 5 | Duplicate submit | One event; same id returned; different body gets 422 | `IdempotencyIT#sameKeyReturnsSameEvent`, `#concurrentSameKeyCreatesOneEvent`, `#sameKeyDifferentBodyIsRejected` |
| 18 | Subscriber URL points at a private address | Rejected at registration | `SsrfIT#registrationRejectsPrivateTargets`, `AddressPolicyTest`, `DnsLookupsTest#systemResolverDoesNotLetADeniedAddressLiteralPast` |

Row 5 is the whole row of the design. Row 18 covers registration only. The design row also covers the address check at connect time and redirects, which arrive with delivery.

Tenant creation, endpoint creation and ingest are built, and the design has rows for a replica that is killed in the middle of each of them (rows 29 and 4). The tests of those rows kill a replica that runs as a process of its own, and the test harness cannot start one yet.

## Documents

- [docs/design.md](docs/design.md): the design, with the SQL, the failure analysis and the limitations.
- [docs/failure-table.md](docs/failure-table.md): the kill table and the full failure table, as far as tests prove them.

## License

Apache License 2.0. See [LICENSE](LICENSE).
