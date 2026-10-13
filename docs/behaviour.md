# Behaviour

What the service does, by area, as far as a test proves it. [design.md](design.md) says how and why, and this document does not repeat it. For each area it names the section to read and the tests to run. Then it lists what the tests show and the design does not spell out.

Row numbers are those of [failure-table.md](failure-table.md). A test is named as `Class#method`, and its class is under `src/test/java`. Every test that this document names stands in a `Test` column, and the build fails unless each of them ran and passed in that build (design section 16). Requests, responses and status codes are in [api.md](api.md).

- [The guarantees and their proof](#the-guarantees-and-their-proof)
- [Where each area is specified and proven](#where-each-area-is-specified-and-proven)
- [What the tests show beyond the design](#what-the-tests-show-beyond-the-design)
- [How the documents are held to the code](#how-the-documents-are-held-to-the-code)

## The guarantees and their proof

Section 1 of the design states seven guarantees and what enforces each. This is what proves each.

| Guarantee | Proven by |
|---|---|
| G1 Durable accept | Row 4. A replica that is killed inside the ingest transaction leaves no event and no delivery. Killed after the commit, it leaves the event with a delivery for each endpoint. Either way the same key then yields that one event. |
| G2 At-least-once, never silently dropped | Rows 1 to 3, 8 and 26: a delivery is delivered after its replica was killed, lost its database or was cut off from it. Rows 10 and 11: a delivery that cannot be delivered ends `dead`, by its attempts or by its age, and a replay delivers it. Row 9: a delivery held behind an open circuit is not written to and is delivered once its endpoint answers again. Row 21: the pending deliveries of a deleted endpoint end `cancelled`. Row 22: retention deletes no pending delivery. |
| G3 Idempotent ingest | Row 5, with two concurrent requests under one key. |
| G4 Duplicates are possible and identifiable | Every request for a delivery carries the same `webhook-id`: after a kill (rows 1 and 2), after a lease that ran out (row 7) and after a replay (row 11). One attempt is one request, also when the answer is a 503 (row 28). An attempt that nobody recorded is in the log as `unknown`, and only such an attempt leads to a duplicate (rows 1 to 3). |
| G5 One recorded outcome per attempt | Row 7. A result that arrives after another claim took the delivery changes no column and adds no attempt row. Neither does a result that is recorded a second time. |
| G6 Isolation | Rows 13 to 15 for delivery and row 16 for the API. What a claim costs is not part of the proof (limitation 4). |
| G7 No connection to non-public addresses | Row 18, at registration and at delivery. |

Three of the guarantees also rest on a test that no row links:

| Guarantee | What the test shows | Test |
|---|---|---|
| G1 | Which endpoints get a delivery | `FanOutIT#eachMatchingActiveEndpointGetsOneDelivery` |
| G3 | A key is free again once its event is 30 days old and has no delivery left, and the key of an event a minute younger still holds | `RetentionIT#oldEventIsDeletedOnceItHasNoDeliveryAndItsKeyIsFreeThen` |
| G6 | The order of a claim | `ClaimIT#claimTakesOneRowForAnEndpointAndFourForATenant`, `#everyTenantGetsItsFirstRowBeforeAnyTenantGetsASecond` |

## Where each area is specified and proven

| Area | Design section | Rows | Test |
|---|---|---|---|
| Tenants, API keys, suspension | 5, 11.1 | 30 | `AdminApiIT`, `ApiKeyAuthIT`, `ApiKeyCacheTest`, `ApiKeyTest` |
| Endpoints, URL policy, stored secrets | 5, 11.3, 11.4, 11.5 | 18 | `EndpointApiIT`, `EndpointLimitIT`, `UrlPolicyTest`, `SecretCipherTest` |
| Ingest and idempotency | 6 | 4, 5 | `EventApiIT`, `FanOutIT`, `IdempotencyIT`, `IngestKillIT`, `EventRequestTest` |
| Request bodies | 11.5 | | `RequestBodyLimitIT`, `RequestBodyCharsetIT`, `EventRequestTest` |
| Claim, lease and fence | 7 | 1 to 3, 6, 7, 12 | `ClaimIT`, `ClaimRaceIT`, `LeaseFenceIT`, `WorkerKillIT`, `PoisonDeliveryIT`, `DeliveryWorkerTest`, `DeliveryPollerTest` |
| Retry, dead letter and replay | 8 | 10, 11, 17, 19 | `DeadLetterReplayIT`, `ReplayIT`, `RetryAfterIT`, `ExpiryIT`, `RetryScheduleTest` |
| Circuit breaker | 9 | 9, 10 | `EndpointOutageIT` |
| Outbound client and SSRF | 11.4 | 18, 27, 28 | `SsrfIT`, `OutboundRequestIT`, `WebhookHttpClientIT`, `AddressPolicyTest`, `DnsLookupsTest`, `ArchitectureTest` |
| Signatures | 11.3 | 23, 24 | `SignatureIT`, `ReferenceVerifierTest` |
| Isolation and rate limit | 12 | 13 to 16 | `SlowEndpointIsolationIT`, `TenantIsolationIT`, `RateLimitIT`, `TenantConcurrencyFilterTest`, `BulkheadTest` |
| Tenant scoping | 11.2 | 21 | `SqlTenantScopeTest`, `TenantIsolationIT` |
| Endpoint deletion and retention | 15 | 21, 22 | `EndpointDeleteIT`, `RetentionIT`, `CancelJobTest`, `RetentionJobTest` |
| Reads | 5 | 31 | `DeliveryApiIT`, `EventApiIT`, `PageCursorTest` |
| Database failures and time bounds | 7, 12 | 8, 26, 31 | `DatabaseRestartIT`, `DatabaseBlackholeIT`, `DatabasePoolsIT`, `ApiTimeoutIT`, `FinalizeRetryIT`, `FinalizeRetryTest`, `DatabaseFailuresTest`, `ProblemsTest` |
| Startup, liveness and shutdown | 15 | 20 | `StartupInvariantsTest`, `StartupInvariantsIT`, `PollerHealthIT`, `PollerHeartbeatTest`, `GracefulShutdownIT`, `WorkerLifecycleIT` |
| Metrics, traces and logs | 10, 11.6, 11.7 | 25, 32 | `MetricsIT`, `IngestMeterIT`, `TracingIT`, `LogFormatIT`, `QueueGaugesTest`, `UntrustedTraceContextFilterTest` |

## What the tests show beyond the design

| Area | What the tests show | Test |
|---|---|---|
| Authentication | A path without a route still takes a key. Without one it answers `401`, and `404` only with one. | `ApiKeyAuthIT#pathWithoutARouteStillTakesAKey` |
| Authentication | The scheme of the `Authorization` header is read without regard to case, so `bearer` is taken as well. | `ApiKeyAuthIT#authenticationSchemeIsCaseInsensitive` |
| Event bodies | The stored body has no whitespace between its members, and it holds no member of the request other than `type` and `data`. | `EventRequestTest#whitespaceAndMembersOtherThanTypeAndDataAreNotRendered` |
| Event bodies | `data` keeps the order of its members and of its nested values. | `EventRequestTest#dataKeepsTheOrderOfItsMembersAndItsNestedValues` |
| Event bodies | A number keeps its value and can change its notation: `1e2` is stored as `1E+2`, and `1.50e1` as `15.0`. | `EventRequestTest#numberMayBeRenderedInAnotherNotationOfTheSameValue` |
| Idempotency | A key belongs to its tenant. The same key under another tenant is another event, and so is the same body under another key. | `IdempotencyIT#sameKeyOfAnotherTenantIsAnotherEvent`, `#sameBodyUnderAnotherKeyIsAnotherEvent` |
| Request bodies | The limit of 256 KiB is exact, and it does not depend on a declared length. A body of exactly that size is accepted, one of a byte more answers `413`, and so does a body above the limit that is sent in chunks. | `RequestBodyLimitIT#eventBodyOfExactlyTheLimitIsAccepted`, `#eventBodyOneByteAboveTheLimitIsRejected`, `#bodyAboveTheLimitWithoutADeclaredLengthIsRejected` |
| Endpoints | A verdict on a URL is not kept. Each registration looks the host up again, so a host that has moved to a private address since the last one is refused. | `UrlPolicyTest#verdictIsNotRememberedForTheNextCheck` |
| Replay | A replay starts a new run of the same delivery. Its first attempt is attempt 1, the attempt rows of the earlier run stay, and its request carries the `webhook-id` of every request before it. | `DeadLetterReplayIT#replayedDeliveryIsDeliveredOnce` |
| Secrets | A value that holds a credential or a subscriber URL does not show it when it is printed: an API key, a signing secret, the admin token, the encryption key and the database password in the configuration, and the URL of an endpoint. | `ApiKeyTest#toStringDoesNotRevealTheKey`, `SigningSecretTest#toStringDoesNotRevealTheSecret`, `WebhooksPropertiesTest#toStringDoesNotRevealTheSecrets`, `EndpointTest#toStringDoesNotRevealTheUrl`, `AdminControllerTest`, `EndpointControllerTest` |
| Secrets | A stored signing secret that was changed in any byte, or encrypted under another key or another key version, is refused. | `SecretCipherTest#valueChangedInAnyByteAfterTheVersionIsRefused`, `#valueEncryptedWithAnotherKeyIsRefused`, `#valueOfAnotherKeyVersionIsRefused` |
| HTTP clients | The compiled classes are held to three rules. No class uses an HTTP client of the JDK or of Spring, or a `java.net.URL`, which opens connections by itself. No class outside the `http` package uses Apache HttpClient. No class but `WebhookHttpClient` uses more of that library than its resolver interface. | `ArchitectureTest` |
| Logging | With the `local` profile no structured format is configured for the console. Without it a line is a JSON object. | `LoggingConfigurationTest#localProfileConfiguresNoStructuredConsoleFormat`, `LogFormatIT#consoleLineIsAJsonObject` |

## How the documents are held to the code

A document can go stale without anyone noticing. Each of these tests fails when the part that it compares does.

| What is compared | Test |
|---|---|
| The migration and the schema in section 4, character for character | `SchemaMigrationTest#migrationIsTheSchemaOfTheDesign` |
| The claim, the release, the load, the finalize statements, the gate and the three circuit statements, and the statements of both replays, with the SQL in sections 7 to 9 | `DeliveryStatementsTest` |
| The delete of an endpoint and the rotation of its secret, with sections 15 and 11.3 | `EndpointStatementsTest` |
| The two inserts that create a tenant and the patch, with section 5 | `TenantStatementsTest` |
| The bucket charge, with section 12 | `TokenBucketTest` |
| The statements of the expiry, cancel and retention jobs and of the queue gauges, with sections 15 and 10 | `JobStatementsTest` |
| The claim that the load tests ask PostgreSQL to explain, with the claim of the code | `DeliveryStatementsTest#claimThatTheLoadTestsExplainIsTheClaimOfTheCode` |
| The delivery settings that ship, with the values of the timing rules of section 7, which the test writes out | `WebhooksPropertiesTest#shippedDeliverySettingsAreTheTimingRulesOfTheDesign` |
| The meters and label names of a scrape, with the table of section 10, which the test writes out | `MetricsIT#scrapeHasEveryMeterOfTheDesignWithItsLabelNames` |
| Every repository statement on a tenant table, for its tenant scope (section 11.2) | `SqlTenantScopeTest` |
| The reference verifier in [api.md](api.md) and the copy in the example subscriber, with the verifier that the tests run | `ReferenceVerifierTest#apiDocumentQuotesTheVerifier`, `#exampleSubscriberCarriesTheVerifier` |
| The kill table and the failure table of sections 3 and 13, with the two tables of [failure-table.md](failure-table.md): the kill table in full, and the failure table in all but its last column | `FailureTableCopiesTest` |
| The tests that a `Test` column links in the README, in failure-table.md and in this document, with the tests that ran and passed in the build. `FailureTableCheck` (section 16) compares them once the build has run its tests, and this is the test of its rules | `FailureTableCheckTest` |

A statement is compared as text, apart from how its lines are laid out. No test compares the four statements of ingest in section 6 or the four of an endpoint registration in section 11.5 with the code. The design writes two of them with `VALUES (...)`.

No test compares the routes in [api.md](api.md) with the controllers. The build looks for a test only where a table that it checks links one, so a test that the design or the notes of failure-table.md name in a sentence can be renamed without a failure.
