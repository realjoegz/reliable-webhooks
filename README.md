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
- **Delivery.** A replica claims the deliveries that are due and sends each as one JSON `POST` of the stored body to the URL of its endpoint, on a virtual thread of its own. A response with a 2xx status makes the delivery `succeeded`. Any other status is a failed attempt, a redirect included, and so is a request that got no response: the host does not resolve or resolves to an address that is not allowed, no connection could be made or it ended early, the TLS handshake failed, or the response did not arrive in time. The attempt and the next state of the delivery are recorded in one transaction.
- **Fair claim.** One statement leases the deliveries of a claim. It takes at most one delivery for an endpoint and four for a tenant, the oldest first, and gives every tenant its first delivery before any tenant gets a second. A row that another claimer holds is skipped, not waited for. A replica runs at most 64 attempts at a time, 4 for one endpoint and 16 for one tenant: it claims no more rows than it has room for, and names the endpoints and tenants that are full to the claim, which leaves them out. The claim also leaves out a suspended tenant and an endpoint behind an open circuit. Nothing in the service suspends a tenant or opens a circuit yet, so the tests write those two states themselves.
- **Lease and fence.** A claim leases a delivery for 60 seconds under a random token. Recording a result, giving a delivery back and dead-lettering it all require that token, so a holder whose lease another claim has taken over changes nothing. A delivery whose holder recorded nothing becomes due again when the lease ends. The claim that takes it then writes the missing attempt row with the outcome `unknown`.
- **Timing of an attempt.** A request with all of its response has 15 seconds. A watchdog closes the connection at that deadline, also under a response that arrives a byte at a time. A worker that has not sent by 35 seconds after its claim began does not send, and the delivery waits for the end of its lease. A result that cannot be recorded because the database failed in a way that can pass, such as a lost connection, a lock timeout or a statement timeout, is tried again every 200 milliseconds until 55 seconds after the claim began. A failure of any other kind is not tried again.
- **Retries and dead letter.** After failed attempt `n` a delivery waits between half of `min(4 h, 10 s * 2^(n-1))` and all of it. A `Retry-After` in seconds on a failure response can lengthen that wait to at most one hour and cannot shorten it. A delivery whose twelfth attempt fails becomes `dead` with the reason `exhausted`. A delivery that is claimed a thirteenth time becomes `dead` the same way, and no request is sent for that claim.
- **Signatures.** Every request carries `webhook-id`, `webhook-timestamp` and `webhook-signature`, in the format of Standard Webhooks. The id is the id of the delivery, the same on every attempt. The timestamp is the clock of the database when the attempt was loaded. [Verifying a request](#verifying-a-request) shows how a subscriber checks them.
- **Outbound client.** One client sends every request. It keeps no cookies, follows no redirect, asks for no compressed response and sends no request a second time, also not after a 429 or a 503. It reads a status line and header lines of up to 8 KiB each, with their line ends, and up to 100 headers. A response beyond that is a failed attempt. It reads 64 KiB of a body and then closes the connection without reading the rest, which does not change the outcome of the attempt.
- **Address check at connect time.** The client looks a host up through the same bounded lookups as registration and connects only to addresses that the lookup returned and the address policy allows. An address literal in a URL takes the same path. A host that resolved to a public address when its endpoint was registered and resolves to a private one at delivery gets no connection. When a replica has no room for a lookup, the delivery is given back for 2 seconds without an attempt being counted or recorded.
- **Local allowance.** `webhooks.ssrf.allow-cidrs` lets address ranges through that the policy would deny, and `webhooks.ssrf.allow-http` lets an endpoint use `http`. Both are for tests. The gauge `webhooks.ssrf.allowlist.entries` holds the number of allowed ranges.
- **Stopping.** When the worker of a replica stops, its poller stops claiming first. The attempts that are in flight then finish and record their results. A delivery that is due and was not claimed stays as it is.
- **Delivery meters.** `webhooks.delivery.attempts` counts attempt rows by `outcome` and `reason`. `webhooks.delivery.released` counts deliveries given back for a busy lookup, `webhooks.finalize.lost.lease` results that were turned away because the lease was lost, `webhooks.finalize.errors` results that could not be recorded for a reason that does not pass, and `webhooks.claim.errors` claims that failed.
- **One HTTP client.** A test reads the compiled classes and fails if any class uses an HTTP client of the JDK or of Spring, or a `java.net.URL`, which opens connections by itself. It also fails if a class outside the `http` package uses Apache HttpClient, or if a class other than `WebhookHttpClient` uses anything of that library but its resolver interface, so no other class can build a client.
- **Event read.** `GET /v1/events/{id}` returns an event with the endpoint, state and attempt count of each of its deliveries. Its `created_at` is the time of the database when the event was stored. The `created_at` in the body is the clock of the replica, so the two can differ. The id of another tenant's event answers 404.
- **Ingest meter.** The counter `webhooks.ingest.requests`, which the design lists under its Prometheus name `webhooks_ingest_requests_total`, counts the requests that `POST /v1/events` answered with one of four results: `accepted`, `replayed`, `invalid` for a `400`, `413` or `422`, and `busy` for a `503`. Nothing else is counted: not a request that fails authentication or is not sent as JSON, which does not reach the route, not a request to another route, and not a request that the route answered with a `500`. The results `rate_limited` and `unsatisfiable` of the design arrive with rate limiting.
- **Tenant scope.** A test reads the SQL of every repository and fails on a statement that names a table of the schema other than `tenant` without `tenant_id = :t`. An `INSERT` that only supplies values is not checked. The repository of the delivery worker is left out by class: no tenant calls it.
- **Errors.** Errors are RFC 9457 problem documents. A lock timeout, a statement timeout and a borrow wait that ran out are mapped to `503 database_busy` with `Retry-After: 1`. An ingest that waits for a locked endpoint row gets that answer when the lock timeout of 2 seconds ends the wait.
- **Bounded database calls.** Two connection pools, `api` and `worker`. Each sets a bound on the borrow wait, the aliveness test of a borrowed connection, the TCP connect, the lock wait, the statement, the socket read and the idle transaction. A parameter in the JDBC URL cannot remove a bound.
- **Startup invariants.** The service refuses to start if the admin token is shorter than 32 characters, the secret-encryption key is not 32 bytes in base64, a pool lacks a time limit or TCP keepalive or has its limits in the wrong order, the database is not at `read committed` or has `synchronous_commit` off, the lease is shorter than the attempt deadline plus 30 seconds, the start cutoff plus the attempt deadline is above the finalize cutoff, the finalize cutoff is not below the lease, `max-attempts` or `max-in-flight` is below 1, the JVM runs without `-XX:+ExitOnOutOfMemoryError`, JVM proxy properties are set, or an address range or `http` is allowed while the `unsafe-local` profile is not active.
- **Logging.** Console logs are JSON. The `local` profile turns the JSON format off, so Spring Boot logs its usual plain text.
- **The failure-table check.** The build deletes the old test reports, runs the tests and then fails unless every test linked from a failure table ran and passed.

Not built yet: the circuit breaker, rate limiting, the per-tenant request permits, endpoint delete, secret rotation, replay, the background jobs, the routes that read deliveries and their attempts, the liveness check of the poller, the metrics endpoint and tracing. Of the meters of the design, these do not exist yet either: the queue gauges, the gauge of the poller, the timers, the counter of terminal deliveries (`webhooks_delivery_terminal_total`) and the meter of the rows of a claim (`webhooks_claim_rows`). What that means today:

- No circuit opens, so every endpoint reads `closed` and an endpoint that is down gets every attempt of every delivery.
- A `dead` delivery stays dead, because nothing replays it.
- Nothing expires a delivery after its 72 hours, cancels the deliveries of a deleted endpoint or deletes old rows.
- A request carries one signature. The second one, for a secret that was rotated, comes with secret rotation.
- Ingest is not charged against the rate bucket, and an event is stored without a trace context.
- The attempt log of a delivery can be read in the database only.

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
- **G5 is delivered.** Row 7 proves it: a result that arrives after another claim has taken the delivery over changes no column of the delivery and adds no attempt row, and neither does a result that is recorded a second time.
- **G7 is delivered.** Row 18 proves it for registration and for delivery.
- **G1 is built and tested, and not delivered yet.** `FanOutIT` shows that an event and its deliveries are written by one transaction, and `ApiTimeoutIT` shows that a transaction that cannot finish leaves no event behind. The row of the failure table for G1 (row 4) kills a replica in the middle of that transaction, and the test harness cannot start a replica as a process of its own yet.
- **G4 is built and tested, and not delivered yet.** Every request for a delivery carries the same `webhook-id` (row 7 and `DeadLetterReplayIT`), and the client sends one request for one attempt (`WebhookHttpClientIT`). The rows that kill a replica in the middle of a request (rows 1 and 2) and the row that counts requests against attempt rows across tenants (row 28) are not proven yet.
- **G6 is built in part.** The claim is fair as the guarantee says, and a replica keeps to its caps for an endpoint and a tenant (`ClaimIT`, `DeliveryPollerTest`, `WorkerLifecycleIT`). The rows that show one slow endpoint, one slow DNS server or one deep backlog not delaying others (rows 13 to 15) are not proven yet, and the per-tenant request permits do not exist.
- **G2 does not hold yet.** A delivery ends as `succeeded` or as `dead`, but a `dead` one cannot be replayed, and nothing expires or cancels a delivery.

## How it works

This is the design. Of the diagram, everything exists today except the circuit part of the finalize transaction: a replica accepts an event and writes it with its delivery rows in one transaction, and its poller claims those rows and sends them.

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
- Failed deliveries are retried on a backoff schedule and end as `dead`. The design lets a `dead` delivery be replayed and has a circuit breaker per endpoint, kept in Postgres, that holds deliveries to an endpoint that is down. Neither exists yet.

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
| 6 | Two claimers race for one delivery | One claimer gets it; the other skips it without waiting; four pollers on one database deliver each of 2,000 deliveries once | `ClaimRaceIT#secondClaimerSkipsLockedRow`, `#fourReplicasDeliverEachEventOnce` |
| 7 | Lease expires while its holder is still sending | The delivery is claimed again and both requests are sent; only the result of the lease holder is recorded | `LeaseFenceIT#lateFinalizeAfterReclaimChangesNothing`, `#repeatedFinalizeIsNoOp` |
| 12 | A delivery whose every claim ends without a record | Dead-lettered on the 13th claim without a send | `PoisonDeliveryIT#thirteenthClaimDeadLetters` |
| 18 | Subscriber URL points at, or rebinds to, a private address | Rejected at registration; blocked at connect with zero accepted connections; redirects not followed | `SsrfIT#registrationRejectsPrivateTargets`, `#rebindingIsBlockedAtConnect`, `#redirectIsNotFollowed`, `AddressPolicyTest`, `DnsLookupsTest#systemResolverDoesNotLetADeniedAddressLiteralPast` |
| 19 | Absurd `Retry-After` | Clamped to between the own delay and one hour | `RetryAfterIT#clampedAtBothEnds` |
| 24 | Tampered body or replayed old request | Reference verifier rejects both | `SignatureIT#verifierRejectsTamperedBodyAndStaleTimestamp` |

Rows 5, 18, 19 and 24 are the whole rows of the design. Rows 6, 7 and 12 are proven inside one test JVM: the claimers of row 6 are pollers with replica ids of their own, the second claim of row 7 is made by the replica that made the first, and the delivery of row 12 kills nobody, it is claimed twelve times by the test. [docs/failure-table.md](docs/failure-table.md) says how each row is forced.

Tenant creation, endpoint creation and ingest are built, and the design has rows for a replica that is killed in the middle of each of them (rows 29 and 4). The tests of those rows kill a replica that runs as a process of its own, and the test harness cannot start one yet.

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
