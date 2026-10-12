# reliable-webhooks

A webhook delivery service on PostgreSQL. Java 21, Spring Boot 4.1, PostgreSQL 18.

What it does today:

- **It accepts an event once.** A producer sends an event with `POST /v1/events` and an `Idempotency-Key`. One transaction stores the event together with one delivery for each endpoint of the tenant that takes the type of the event, and the `202` is sent after the commit. A request that is sent again with the same key gets the same event, from whichever replica answers it.
- **It does not lose a delivery when a replica dies.** Each replica claims due deliveries from the one database, sends each as a signed `POST` and records the attempt and the next state of the delivery in one transaction. A claim is a lease of 60 seconds under a random token. A replica that is killed leaves a lease that runs out, and another replica takes the delivery over. A subscriber can get a delivery twice and knows it by the `webhook-id`, which is the same on every request for it.
- **It retries, and keeps what it could not deliver.** A failed attempt is followed by the next one on a backoff schedule. After twelve failed attempts, or after 72 hours, a delivery is `dead`. A `dead` delivery is kept for 30 days and can be replayed.
- **It keeps running when the database does not.** While the database restarts, a replica answers an event with `503` and goes on. A result that it could not record is recorded if the database is back within 55 seconds of the claim. After that the result is given up, and the delivery is sent again once its lease has run out.
- **It keeps endpoints and tenants apart, and connects to public addresses only.** A slow endpoint, a host whose lookup never answers, a tenant with a deep backlog or a tenant that floods the API does not keep another tenant waiting.

Every feature of the service that the design describes is built, and [Current state](#current-state) lists them. What remains lies outside the service: a deployment and load testing. The design is in [docs/design.md](docs/design.md).

## Failure table

The headline rows of [docs/failure-table.md](docs/failure-table.md). Each row names a failure, the behaviour that a test proves and that test. A row is added only together with its test, and the build fails unless every test that a row links ran and passed.

| # | Failure | Proven behaviour | Test |
|---|---|---|---|
| 1 | Worker killed mid-delivery | Redelivered after the lease; attempt 1 `unknown`, attempt 2 `success` | `WorkerKillIT#killedMidRequest` |
| 2 | Crash after subscriber 200, before commit | Exactly two requests with the same `webhook-id`; one `succeeded` | `WorkerKillIT#killedAfterResponseBeforeCommit` |
| 4 | Replica killed during ingest | Nothing or everything; retry with the same key yields one event | `IngestKillIT#killedBeforeCommit`, `#killedAfterCommitBeforeResponse` |
| 5 | Duplicate submit | One event; same id returned; different body gets 422 | `IdempotencyIT#sameKeyReturnsSameEvent`, `#concurrentSameKeyCreatesOneEvent`, `#sameKeyDifferentBodyIsRejected` |
| 6 | Two claimers race for one delivery | One claimer gets it; the other skips it without waiting; four pollers on one database deliver each of 2,000 deliveries once | `ClaimRaceIT#secondClaimerSkipsLockedRow`, `#fourReplicasDeliverEachEventOnce` |
| 8 | Database restart | No delivery lost; the delivery in flight across the restart is recorded once from one request; ingest answers 503 and the same key succeeds afterwards; replicas stay up | `DatabaseRestartIT#inFlightAndQueuedSurviveRestart`, `DatabaseFailuresTest`, `FinalizeRetryTest`, `ProblemsTest#failedConnectionIsDatabaseBusy` |
| 9 | Endpoint down for hours | Circuit opens after 5 failures; held deliveries use no attempts; one probe per interval across concurrent claimers; probe backoff stays capped; recovery drains | `EndpointOutageIT#circuitOpensAndHolds`, `#oneProbeAdmittedAcrossConcurrentClaimers`, `#probeBackoffIsCappedAfterManyFailures`, `#recoveryDrainsHeldDeliveries`, `#probeSlotReopensAfterProberKilled` |

How these rows are proven:

- **Rows 1, 2, 4 and 8 run replicas as processes of their own**, started from the packaged jar. In rows 1, 2 and 4 a replica is killed with a real SIGKILL. A kill is never landed by timing: either the receiver holds the request of the replica, or the test holds a lock that the next statement of the replica needs and kills once the database shows the replica waiting for it. In row 8 no replica gets a signal: the PostgreSQL container is stopped and started again under two replicas that keep running.
- **Row 4 has two kills.** The one inside the ingest transaction is landed with a lock. No signal can be landed between a commit and the response that follows it, and what such a kill leaves is what a lost response leaves. The test therefore throws the first answer away, kills the replica and sends the request again to another one.
- **Row 8 links three unit tests as well.** The restart does not decide which failure a call meets while the database is away, and a run does not have to meet a statement whose connection has failed. `DatabaseFailuresTest` shows that such a failure can pass, `FinalizeRetryTest` that a result is tried again after one, and `ProblemsTest#failedConnectionIsDatabaseBusy` that the API answers one with a `503`.
- **Row 6 is proven inside one test JVM.** Its claimers are pollers with replica ids of their own on one database.
- **Row 9 is proven inside one test JVM, apart from its kill.** The claimers that meet at the gate are two connections of the test and then four pollers on one database. The test keeps the eight deliveries that they claim waiting at the gate until all eight are there. In `probeSlotReopensAfterProberKilled` the replica that probes is a process, killed while the receiver holds the request of the probe. The replica inside the test JVM sends the next probe.

[docs/failure-table.md](docs/failure-table.md) has the kill table and every row that is proven today, which besides these are rows 3, 7, 10 to 28 and 30 to 32. It also says how each row is forced.

## Current state

Built and tested:

- **Schema.** The complete data model of the design, as one Flyway migration.
- **Tenant administration.** `POST /admin/tenants` creates a tenant together with its rate bucket and returns the API key once. `PATCH /admin/tenants/{id}` sets the rate, the burst, the endpoint cap and the status of a tenant. `POST /admin/tenants/{id}/api-key/rotate` replaces the key, and the replica that handled the call rejects the old key from then on.
- **Authentication.** The actuator endpoints, which are the health probes and the Prometheus scrape, are open, admin routes take a static admin token, and every other path takes a tenant API key, whether a route exists there or not. Only the SHA-256 of a key is stored. A key that is missing, malformed, unknown, rotated away or owned by a suspended tenant is rejected. A replica caches a key it has looked up for 30 seconds.
- **Endpoints.** `POST /v1/endpoints` registers an endpoint for a URL and 1 to 20 event types, and returns its signing secret once. `GET /v1/endpoints` and `GET /v1/endpoints/{id}` read endpoints with their circuit state, and `PATCH /v1/endpoints/{id}` replaces the event types. The id of another tenant's endpoint answers 404. An endpoint and its circuit row are created in one transaction. A tenant has room for 20 endpoints unless its cap is changed, and concurrent creates cannot exceed the cap.
- **URL policy.** An endpoint is registered only for an `https` URL on port 443 or 1024 to 65535, without userinfo, of at most 2048 characters, whose host resolves to public addresses only. The address ranges that count as not public are an explicit list. A refused URL gets `422 endpoint_url_not_allowed`, and the answer is the same for a private address, for a host that does not resolve and for a lookup that timed out.
- **Bounded DNS.** A registration waits 2 seconds for a lookup and is refused if the resolver has not answered by then. A replica runs at most 32 lookups at a time and at most 4 for one tenant, counting every lookup that has not returned. Beyond that a registration gets `429 lookup_busy` at once. A lookup that is stuck holds no database connection, and other tenants register meanwhile.
- **Encrypted secrets.** A signing secret is stored encrypted with AES-256-GCM under `webhooks.secret-encryption-key`, with a random nonce per secret.
- **Ingest.** `POST /v1/events` takes `{"type", "data"}` with an `Idempotency-Key` header and answers `202` with the id of the event. One transaction stores the event and one `pending` delivery for each active endpoint of the tenant whose event types hold the type of the event or `*`. A delivery is due at once and expires 72 hours later. An event that matches no endpoint is stored without deliveries. A request whose transaction cannot finish stores nothing, and the same key is accepted afterwards.
- **Event body.** The body that the deliveries of an event will send, `{"id", "type", "created_at", "data"}`, is rendered once and stored with the event. A number in `data` keeps its exact value, also when it is too large or too precise for a double. None is read into a double. `created_at` is the clock of the replica in milliseconds, always with three fraction digits. Of members with the same name in one object, the last one counts.
- **Idempotency.** A tenant and an `Idempotency-Key` give one event. A repeat with the same body answers `202` with the id of that event and `Idempotent-Replay: true`, and stores nothing. A different body under the same key answers `422 idempotency_key_reused`. Bodies are compared by the SHA-256 of their bytes. Of two concurrent requests with one key, one creates the event and the other waits for it and answers as a repeat. The same key under another tenant is another event.
- **Ingest limits.** The type of an event has 1 to 128 characters of `[a-z0-9_.-]`, `data` is a JSON object, and the key has 1 to 255 printable ASCII characters. In the body, a number has at most 1,000 digits, those of its exponent included, and an exponent that fits a Java `BigDecimal`, which is about plus or minus 2.1 billion. A member name has at most 1,024 bytes of UTF-8, and the body nests at most 500 levels deep, itself being the first. Anything else answers `400`. So does a body in UTF-16 or UTF-32, a body that starts with a byte order mark, and a body in which a UTF-8 sequence breaks off, as it does at the `é` of `café` sent in ISO-8859-1.
- **Request body limit.** A request body above 256 KiB answers `413`, with or without a declared length. The limit is not special to ingest: an endpoint registration above it is refused in the same way. The limit on a member name is not special to ingest either: a registration with a member name above 1,024 bytes answers `400`. Nothing reads a body ahead of the limit: the filter of Spring Boot that reads the form of a `PUT`, `PATCH` or `DELETE` into memory before authentication is turned off, and so are its multipart parsing and the reading of a form for request parameters by Tomcat. A request that is sent as a form without a key gets its `401` while its body is still open, and a registration that is sent as a form or as a multipart body gets its `415` the same way. The actuator endpoints take no key, and a JSON body that is sent to one is read, up to the same limit: sent to the scrape, a body above it answers `413`.
- **Request body charset.** Every request body is read as UTF-8. A request whose `Content-Type` names another charset answers `415`, on ingest and on an endpoint registration alike, and one that names UTF-8 or no charset is read. The charset is checked before the size: an event above 256 KiB that names another charset answers `415`. A registration in UTF-16 or UTF-32, or one that starts with a byte order mark, answers `400` like an event in that form. The limits on a body are counted in UTF-8, which is why no body is read in another encoding.
- **Delivery.** A replica claims the deliveries that are due and sends each as one JSON `POST` of the stored body to the URL of its endpoint, on a virtual thread of its own. A response with a 2xx status makes the delivery `succeeded`. Any other status is a failed attempt, a redirect included, and so is a request that got no response: the host does not resolve or resolves to an address that is not allowed, no connection could be made or it ended early, the TLS handshake failed, or the response did not arrive in time. The attempt and the next state of the delivery are recorded in one transaction.
- **Fair claim.** One statement leases the deliveries of a claim. It takes at most one delivery for an endpoint and four for a tenant, the oldest first, and gives every tenant its first delivery before any tenant gets a second. A row that another claimer holds is skipped, not waited for. A replica runs at most 64 attempts at a time, 4 for one endpoint and 16 for one tenant: it claims no more rows than it has room for, and names the endpoints and tenants that are full to the claim, which leaves them out. The claim also leaves out a suspended tenant and an endpoint behind an open circuit.
- **Lease and fence.** A claim leases a delivery for 60 seconds under a random token. Recording a result, giving a delivery back and dead-lettering it as `exhausted` all require that token, so a holder whose lease another claim has taken over changes nothing. A delivery whose holder recorded nothing becomes due again when the lease ends. The claim that takes it then writes the missing attempt row with the outcome `unknown`.
- **Timing of an attempt.** A request with all of its response has 15 seconds. A watchdog closes the connection at that deadline, also under a response that arrives a byte at a time, and the attempt is recorded as `deadline`. A worker that has not sent by 35 seconds after its claim began does not send, and the delivery waits for the end of its lease. A result that cannot be recorded because the database failed in a way that can pass, such as a lost connection, a lock timeout or a statement timeout, is tried again every 200 milliseconds until 55 seconds after the claim began. A failure of any other kind is not tried again. Row 8 of the failure table shows a result that is recorded this way after the database has been stopped and started again.
- **Retries and dead letter.** After failed attempt `n` a delivery is due again after between half of `min(4 h, 10 s * 2^(n-1))` and all of it. A `Retry-After` in seconds on a failure response can lengthen that wait to at most one hour and cannot shorten it. Behind an open circuit a delivery that is due is held and waits longer. A delivery whose twelfth attempt fails becomes `dead` with the reason `exhausted`. A delivery that is claimed a thirteenth time becomes `dead` the same way, and no request is sent for that claim.
- **Circuit breaker.** Five failed attempts in a row open the circuit of an endpoint, and any success closes it. While it is open, the deliveries of the endpoint are held: no claim takes them, nothing is written to them and they use no attempt. One delivery at a time is sent as a probe, however many claimers come for one: the first 30 seconds after the circuit opened, and each failed probe moves the next one further away, up to 5 minutes. A probe uses an attempt of its delivery, and the others are given back without one. These are rows 9 and 10 of the failure table.
- **Expiry.** A delivery that is still `pending` when its 72 hours are over becomes `dead` with the reason `expired`, and keeps the attempts it has used. One statement does this for up to 1,000 deliveries, those that expired first. It leaves a delivery whose lease is still running, and it passes over a row that another transaction holds without waiting for it. A delivery whose lease ran out with nothing recorded gets the missing attempt row in the same statement, with the outcome `unknown`, and a result that its former holder brings afterwards is not recorded. A replica runs the statement when it starts and then 60 seconds after the end of each run. A run that fails is followed by the next one, and stopping ends the wait without another run. Every replica does this, and none leads.
- **Replay.** `POST /v1/deliveries/{id}/replay` makes a `dead` delivery `pending` again and answers `204`. The delivery starts a new run: no attempt is counted, its number of replays is one higher, it is due within the next 60 seconds and it has 72 hours of its own. The attempt rows of the earlier run stay. The first attempt of the new run is attempt 1, and its request carries the same `webhook-id` as every request before it. A delivery that is not `dead` answers `409`, so a delivery that has succeeded cannot be sent again this way. A delivery whose endpoint is not active answers `404`. A replay costs one unit of the rate limit.
- **Bulk replay.** `POST /v1/endpoints/{id}/replay` with `{"limit"}` replays the `dead` deliveries of one endpoint, the oldest first. A call replays at most `limit` of them, at most 500 and at most as many as the burst of the tenant, costs one unit for each, and answers `{"replayed", "remaining"}`. A caller repeats it until `remaining` is false. This is row 17 of the failure table.
- **Endpoint deletion.** `DELETE /v1/endpoints/{id}` marks an endpoint as deleted and answers `204`. From that commit on nothing is claimed for the endpoint, and no delivery is created or replayed for it, also not by an ingest or a replay that was under way. Every 30 seconds each replica cancels the `pending` deliveries of deleted endpoints, 1,000 in a statement. A request that was already sent is recorded as what it was, unless its delivery was cancelled first. This is row 21.
- **Retention.** Once an hour each replica deletes the deliveries that reached a final state more than 30 days ago, then the events of that age without a delivery, then the deleted endpoints without one, and clears the secrets from before a rotation that no longer count. A `pending` delivery is never deleted, and neither is one that a replay is making `pending` at that moment. This is row 22.
- **Delivery reads.** `GET /v1/deliveries?endpoint_id=&state=` lists the deliveries of one endpoint in one state, in the order of their creation time and id. Both parameters are required. A page holds at most 100 deliveries, or at most as many as `limit` says, and a `limit` above 100 answers `400`. A page that another page follows carries `next`, which the next request sends back as `cursor`. The deliveries of a deleted endpoint are still listed. `GET /v1/deliveries/{id}` returns a delivery with the attempt log of its current run: for each attempt its number, its outcome, its status or its error, its start and its duration. An attempt of unknown outcome has no status and no duration. `?run=n` returns the log of that run, and a run that the delivery does not have answers `404`.
- **Signatures.** Every request carries `webhook-id`, `webhook-timestamp` and `webhook-signature`, in the format of Standard Webhooks. The id is the id of the delivery, the same on every attempt. The timestamp is the clock of the database when the attempt was loaded. `POST /v1/endpoints/{id}/secret/rotate` gives an endpoint a new secret and returns it once. For 24 hours of the database clock a request then carries two signatures, the one of the new secret first, and after that the one of the new secret alone. This is row 23. Only one old secret is kept: a second rotation inside those 24 hours keeps the secret of the first, and the one from before the first is gone. [Verifying a request](#verifying-a-request) shows how a subscriber checks a request.
- **Outbound client.** One client sends every request. It keeps no cookies, follows no redirect, asks for no compressed response and sends no request a second time, also not after a 429 or a 503. It reads a status line and header lines of up to 8 KiB each, with their line ends, and up to 100 headers. A response beyond that is a failed attempt, recorded as `response_too_large`. It reads 64 KiB of a body and then closes the connection without reading the rest, which does not change the outcome of the attempt. Cutting a response costs its own connection and nothing else: a request of another tenant that is open to the same host at that moment is answered, and its delivery succeeds.
- **Address check at connect time.** The client looks a host up through the same bounded lookups as registration and connects only to addresses that the lookup returned and the address policy allows. An address literal in a URL takes the same path. A host that resolved to a public address when its endpoint was registered and resolves to a private one at delivery gets no connection. When a replica has no room for a lookup, the delivery is given back for 2 seconds without an attempt being counted or recorded.
- **Isolation.** An endpoint that does not answer has four requests open and no more, and deliveries for another endpoint of its tenant and for another tenant succeed meanwhile. A host whose lookup never answers fails an attempt as `timeout` after the 2 seconds that a lookup may take. No more than four lookups of its tenant are stuck at a time, and another tenant is delivered meanwhile. The tenant itself waits while they are stuck: a delivery for another of its endpoints, whose host does resolve, is given back without an attempt, and its first attempt succeeds once the resolver answers again. A tenant has at most 16 attempts in flight. While 16 are held, its other deliveries stay unclaimed, also those for an endpoint with nothing in flight, and another tenant is delivered meanwhile. With 2,000 deliveries due for one endpoint that answers at once, a newer delivery of another tenant arrives before a tenth of them have. These are rows 13 to 15 of the failure table.
- **Local allowance.** `webhooks.ssrf.allow-cidrs` lets address ranges through that the policy would deny, and `webhooks.ssrf.allow-http` lets an endpoint use `http`. Both are for tests and for the stack of [Run it locally](#run-it-locally). The gauge `webhooks.ssrf.allowlist.entries` holds the number of allowed ranges.
- **Stopping.** A replica that gets a SIGTERM takes itself out of readiness and stops its poller, which claims nothing from then on. The attempts that are in flight finish and record their results, and the web server still answers while they do. Then the process ends by itself with exit code 143. A delivery that is due and was not claimed stays as it is. This is row 20 of the failure table.
- **Kills.** The tests kill a replica with SIGKILL at the six points of the [kill table](docs/failure-table.md#kill-table). Killed inside its ingest transaction, it leaves no event, and the same key is accepted afterwards. Killed after the commit, it leaves the event with its deliveries, and the same key is answered as a repeat. Killed after a claim, in the middle of a request or after the response and before its commit, it leaves a lease that runs out. The replica that claims the delivery then logs the attempt of the dead one as `unknown` and delivers. These are rows 1 to 4. Killed in the middle of a probe, it leaves the probe slot taken for 60 seconds and nothing to clean up, and the next probe comes after that. No kill lands while an endpoint or a tenant is created. That neither is left half created is tested with those two transactions failing without a kill.
- **Liveness.** `/actuator/health/liveness` reports a replica as down when the loop of its poller has finished no iteration for 60 seconds, and as up again once it has. The readiness probe does not ask the poller. A claim that fails ends its iteration like any other, so a database that is down does not make a replica look dead.
- **Database outage.** While the database is down, an event that is sent is answered with `503 database_busy`, and the replicas keep running. When a connection goes silent, as after a failover, the claim that waits on it ends with the socket timeout of the worker pool, which is 15 seconds, and the poller goes on with connections that it opens afterwards. A transaction that such a replica left open is ended by the database, which gives a session 10 seconds to idle in one, so the delivery that it locked can be claimed again. These are rows 8 and 26.
- **Delivery meters.** `webhooks.delivery.attempts` counts attempt rows by `outcome` and `reason`. `webhooks.delivery.released` counts deliveries given back, by `reason`: a lost gate or a busy lookup. `webhooks.finalize.lost.lease` counts results that were turned away because the lease was lost, `webhooks.finalize.errors` results that could not be recorded for a reason that does not pass, and `webhooks.claim.errors` claims that failed.
- **One HTTP client.** A test reads the compiled classes and fails if any class uses an HTTP client of the JDK or of Spring, or a `java.net.URL`, which opens connections by itself. It also fails if a class outside the `http` package uses Apache HttpClient, or if a class other than `WebhookHttpClient` uses anything of that library but its resolver interface, so no other class can build a client.
- **Event read.** `GET /v1/events/{id}` returns an event with the endpoint, state and attempt count of each of its deliveries. Its `created_at` is the time of the database when the event was stored. The `created_at` in the body is the clock of the replica, so the two can differ. The id of another tenant's event answers 404.
- **Ingest meter.** The counter `webhooks.ingest.requests`, which the design lists under its Prometheus name `webhooks_ingest_requests_total`, counts the requests that `POST /v1/events` answered with one of six results: `accepted`, `replayed`, `rate_limited` and `unsatisfiable` for the two refusals of the rate limit, `invalid` for a `400`, `413` or `422`, and `busy` for a `503`. Nothing else is counted: not a request that fails authentication, is refused by the request permits or is not sent as JSON, which does not reach the route, not a request to another route, and not a request that the route answered with a `500` or with the `415` for a charset other than UTF-8.
- **Rate limit.** A tenant has a token bucket in the database, which fills at 50 units a second up to 100 unless an operator sets other values. An event costs one unit for each endpoint that it goes to, and one where it goes to none, plus one for each full 4 KiB of its stored body. A unit therefore pays for less than 4 KiB of stored body, so a tenant at the default rate stores about 200 KiB of event bodies a second at most, which is about 16 GiB a day. A refused event answers `429 rate_limited` with `Retry-After`, or `429 rate_limit_unsatisfiable` without it when the rate is 0 or the cost is above the burst. The limit is one limit for all replicas. The repeat of an accepted event is not charged, and a charge is not given back when its request fails afterwards. This is row 16 of the failure table.
- **Request permits and suspension.** A tenant may have 8 requests open on a replica. A further one is answered with `429 concurrency_limit` before its route runs, so the routes of one tenant hold at most 8 of the 16 connections of the `api` pool. While a tenant is suspended, the replica that handled the suspension rejects its key at once, and no claim takes its deliveries or writes to them. They are delivered once it is active again. The permits are part of row 16, and suspension is row 30.
- **Tenant scope.** A test reads the SQL of every repository and fails on a statement that names a table of the schema other than `tenant` without `tenant_id = :t`. An `INSERT` that only supplies values is not checked. The repositories of the delivery worker, of the expiry, cancel and retention jobs and of the queue gauges are left out by class: no tenant calls them. A second test sends every request that names an endpoint, an event or a delivery by its id with the key of another tenant. Each is answered with 404, and the endpoint, circuit, delivery and bucket rows of the owner are afterwards what they were.
- **Errors.** Errors are RFC 9457 problem documents. A database failure that can pass is mapped to `503 database_busy` with `Retry-After: 1`: a lock timeout, a statement timeout, a deadlock, a borrow wait that ran out, and a connection that failed or that the database ended. These are the failures after which a replica tries to record a result again. An ingest that waits for a locked endpoint row gets that answer when the lock timeout of 2 seconds ends the wait.
- **Bounded database calls.** Two connection pools, `api` and `worker`. Each sets a bound on the borrow wait, the aliveness test of a borrowed connection, the TCP connect, the lock wait, the statement, the socket read and the idle transaction. A parameter in the JDBC URL cannot remove a bound.
- **Startup invariants.** The service refuses to start if the admin token is shorter than 32 characters, the secret-encryption key is not 32 bytes in base64, a pool lacks a time limit or TCP keepalive or has its limits in the wrong order, the database is not at `read committed` or has `synchronous_commit` off, the lease is shorter than the attempt deadline plus 30 seconds, the start cutoff plus the attempt deadline is above the finalize cutoff or not below the 60 seconds of a probe slot, the finalize cutoff is not below the lease, `max-attempts` or `max-in-flight` is below 1, the JVM runs without `-XX:+ExitOnOutOfMemoryError`, JVM proxy properties are set, or an address range or `http` is allowed while the `unsafe-local` profile is not active.
- **Logging.** Console logs are JSON. The `local` profile turns the JSON format off, so Spring Boot logs its usual plain text.
- **Metrics endpoint.** `/actuator/prometheus` answers a scrape and takes no key. Besides the counters above it holds how long an attempt took, how long a delivery took from its creation to its success, the deliveries that reached a final state, the time and the rows of a claim, and the time since the poller last looped. One statement, run when a replica starts and then 15 seconds after the end of each run, reads the queue for the gauges: the pending deliveries that are due, scheduled, leased or held, the age of the oldest due one and the number of open circuits. No meter of the service has a label for a tenant, an endpoint, a delivery, a host or a URL. The meters are scraped and pushed nowhere.
- **Traces.** The span of an ingest request starts a trace of its own. The trace of a caller is not continued, so a caller can neither choose the trace id nor have a trace sampled, and a valid `traceparent` that it sent is kept as a link on that span. This is row 25. The context of the span is stored with the event. Each attempt writes one client span as its child, with the delivery, the attempt, the outcome, the status and the host, and its request carries the `traceparent` of that span. As it ships, a replica exports its spans nowhere: it has no exporter until `management.opentelemetry.tracing.export.otlp.endpoint`, a setting of Spring Boot, names a collector. It does not read the `OTEL_` variables of its environment.
- **Log fields.** The line that records an attempt carries the ids of its tenant, event, delivery and endpoint, the number of the attempt, its outcome and reason, the state that it left the delivery in, the replica, and the trace id and span id of the span of that attempt. A line that is logged while a route handles a request carries the trace id and span id of the request, the replica and, on a tenant route, the tenant. Of a subscriber URL, a line and a span hold the host and nothing else. A marker in the path and the query of such a URL is in no line that a replica wrote, in no span that it exported and in no scrape, and no line holds the API key, the admin token, the signing secret, a signature or the payload. This is row 32.
- **The failure-table check.** The build deletes the old test reports, runs the tests and then fails unless every test linked from a failure table ran and passed.

Not built, and outside the service: a deployment and load tests. There is a container image and a stack for one machine ([Run it locally](#run-it-locally)), but no deployment to a cluster, and the service has not been run under load.

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

- **G1 is delivered.** Row 4 of the failure table proves it. A replica that is killed inside the ingest transaction leaves no event and no delivery, and one that is killed after the commit leaves the event with a delivery for each endpoint. In both cases the same key then yields that one event. `FanOutIT` shows that the event and its deliveries are written by one transaction.
- **G2 is delivered.** Rows 1 to 3, 8 and 26 prove that a delivery whose replica is killed, cut off from the database or left without one is delivered afterwards. Rows 10 and 11 prove the two ways in which a delivery that is not delivered ends, and that neither is final. A delivery whose twelve attempts fail becomes `dead`, and so does a delivery that is still pending after 72 hours. A replay makes either of them `pending` again in a new run, and the one whose attempts ran out is then delivered with one request. Row 9 proves that a delivery held behind an open circuit is not written to, and is delivered once its endpoint answers again. Row 21 proves the third end: the `pending` deliveries of a deleted endpoint become `cancelled`, also those that an ingest or a replay was creating or re-arming while the endpoint was deleted. Row 22 proves that retention deletes only deliveries that reached a final state more than 30 days ago, and none that is `pending`.
- **G3 is delivered.** Row 5 proves it, including two concurrent requests with one key. Retention deletes an event only once it is 30 days old and has no delivery left, and its key is free from then on. `RetentionIT` shows both: an event a minute younger keeps its key.
- **G4 is delivered.** Every request for a delivery carries the same `webhook-id`, also after a kill (rows 1 and 2), after a lease that ran out (row 7) and after a replay (row 11). One attempt is one request, also when the answer is a 503 (row 28). A second request is sent only for an attempt whose outcome was never recorded: the replica was killed in the middle of the request or before it had committed the result (rows 1 and 2), or its lease ran out while it was still sending (row 7). Such an attempt is in the log as `unknown`.
- **G5 is delivered.** Row 7 proves it: a result that arrives after another claim has taken the delivery over changes no column of the delivery and adds no attempt row, and neither does a result that is recorded a second time.
- **G6 is delivered.** Rows 13 to 15 prove it for delivery. One slow endpoint keeps neither the other endpoints of its tenant nor another tenant waiting. One host whose lookup never answers and one tenant with a backlog, whether its requests are held or answered at once, keep no other tenant waiting. Inside one tenant, deliveries do wait: while its four lookups are stuck, a delivery for an endpoint whose host resolves is given back without an attempt, and while it has 16 attempts held, its other deliveries stay unclaimed. The caps for an endpoint, for a tenant and for the lookups of a tenant hold. `ClaimIT` shows that a claim takes one row for an endpoint and gives every tenant its first row before any tenant gets a second. Row 16 proves it for the API: a tenant may have 8 requests open on a replica, so its routes hold at most 8 of the 16 connections of the `api` pool, and another tenant is served meanwhile.
- **G7 is delivered.** Row 18 proves it for registration and for delivery.

## How it works

This is the design, and everything in the diagram exists today: a replica accepts an event and writes it with its delivery rows in one transaction, and its poller claims those rows and sends them.

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
- Failed deliveries are retried on a backoff schedule and end as `dead`, and so does a delivery that is still pending after 72 hours. A `dead` delivery can be replayed. A circuit breaker per endpoint, kept in Postgres, holds the deliveries to an endpoint that is down.

## Run it locally

```
docker compose up --build --detach --wait
scripts/demo.sh
```

The first command needs Docker with Compose. It builds the image and starts PostgreSQL, two replicas, the subscriber of [examples/subscriber](examples/subscriber/Subscriber.java), Prometheus, Grafana and Jaeger. The second needs `curl` 7.82 or later and `jq` and is the API in one run: it creates a tenant, registers the subscriber as an endpoint, sends an event, reads the delivery until it has succeeded and prints the line in which the subscriber reports its signature as verified. Then the subscriber answers a forged request with `401`, and fails the first attempt of a second delivery, which succeeds with its retry. CI runs both, and then `scripts/check-observability.sh`, which asks Grafana for the data of every query of the dashboard and Jaeger for the trace of a delivery.

- Grafana, <http://localhost:3000/d/reliable-webhooks>: a dashboard of the queue, the attempts, the time to success, the open circuits and the ingest results.
- Prometheus, <http://localhost:9090/targets>: the two replicas as scrape targets.
- Jaeger, <http://localhost:16686>: the spans that the replicas export, under the service `reliable-webhooks`.

The stack is for a development machine. Its credentials are written in `compose.yaml`, and its replicas run with the `unsafe-local` profile: they deliver over plain `http` and to the private addresses of the compose network, where the subscriber is. A replica in production runs without that profile and then refuses to start with either allowance. `docker compose down --volumes` removes the stack with its data.

## Requirements

- JDK 21 or later.
- Docker, for the integration tests. Testcontainers starts PostgreSQL 18 in a container.
- Linux or macOS, for the integration tests that start a replica as a process and send it SIGTERM or SIGKILL.

Maven comes with the wrapper (`./mvnw`).

## Build and test

```
./mvnw verify
```

This runs, in order:

1. the unit tests (`*Test`, Surefire);
2. the packaging of the executable jar;
3. the integration tests (`*IT`, Failsafe), against one PostgreSQL container, replicas on random ports inside the test JVM, and replicas that the tests start from the packaged jar as processes of their own;
4. the format check (google-java-format through Spotless);
5. the failure-table check.

`./mvnw spotless:apply` formats the sources. `.mvn/jvm.config` exports the JDK compiler packages that google-java-format needs when it runs inside Maven.

The integration tests that start a replica as a process carry the JUnit tag `process`. The one that stops the PostgreSQL container and starts it again carries the tag `dbrestart` as well. `./mvnw verify` runs them all. To leave them out of a local run:

```
./mvnw verify -DexcludedGroups=process -DfailureTable.skip=true
```

The failure-table check has to be turned off for such a run, because rows of the failure table link those tests.

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

## Verifying a request

A subscriber checks a request with the signing secret that the registration of its endpoint returned. This is the reference verifier, which the tests run against requests that the service sent:

```java
public final class ReferenceVerifier {

  private static final Duration TOLERANCE = Duration.ofMinutes(5);

  private ReferenceVerifier() {}

  /**
   * Whether a request is genuine and recent.
   *
   * @param secret the signing secret of the endpoint, as its registration returned it
   * @param id the value of the webhook-id header
   * @param timestamp the value of the webhook-timestamp header
   * @param signatures the value of the webhook-signature header
   * @param body the body of the request, byte for byte as it arrived
   * @param now the current time
   */
  public static boolean isGenuine(
      String secret, String id, String timestamp, String signatures, byte[] body, Instant now)
      throws GeneralSecurityException {
    Instant signedAt;
    try {
      signedAt = Instant.ofEpochSecond(Long.parseLong(timestamp));
    } catch (NumberFormatException | DateTimeException notATime) {
      return false;
    }
    // A request that was recorded and is sent again later carries a valid signature. Its age is
    // what gives it away.
    if (Duration.between(signedAt, now).abs().compareTo(TOLERANCE) > 0) {
      return false;
    }

    byte[] key = Base64.getDecoder().decode(secret.substring("whsec_".length()));
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(key, "HmacSHA256"));
    mac.update((id + "." + timestamp + ".").getBytes(StandardCharsets.UTF_8));
    byte[] expected = Base64.getEncoder().encode(mac.doFinal(body));

    // The header can carry several signatures, separated by spaces. One match is enough.
    for (String signature : signatures.split(" ")) {
      if (signature.startsWith("v1,")
          && MessageDigest.isEqual(
              expected, signature.substring(3).getBytes(StandardCharsets.UTF_8))) {
        return true;
      }
    }
    return false;
  }
}
```

A subscriber also has to expect the same delivery more than once. Every attempt of a delivery carries the same `webhook-id`.

## Documents

- [docs/design.md](docs/design.md): the design, with the SQL, the failure analysis and the limitations.
- [docs/failure-table.md](docs/failure-table.md): the kill table and the full failure table, as far as tests prove them.

## License

Apache License 2.0. See [LICENSE](LICENSE).
