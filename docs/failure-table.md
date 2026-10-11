# Failure table

Each row names one failure, what the service does about it and the test that proves it. A row is added only together with its test. The build runs `FailureTableCheck` after the integration tests, and it fails unless every test linked from a table in this file ran and passed in the same build.

[design.md](design.md) lists the rows the finished service has to prove, in sections 3 and 13. A row here claims only the part of its design row that tests prove today.

- Rows 5, 18, 19 and 24 are the whole design rows.
- Rows 6, 7 and 12 are proven inside one test JVM, because the test harness cannot start a replica as a process of its own yet.
  - The four claimers of row 6 are pollers in the test JVM, each with a worker and a replica id of its own, on one database.
  - In row 7 the claim that takes the delivery over is made by the replica that made the first claim. The fence does not know which replica holds a token, only whether the token is still the lease.
  - The delivery of row 12 kills no worker. The test claims it twelve times and lets each lease run out, which leaves the delivery as twelve killed workers would.
- Row 11 of the design (a delivery that a healthy endpoint always rejects) is not here, because half of it is replay. Its first half is tested in `DeadLetterReplayIT#exhaustedAfterTwelveAttempts`.

The kill table has no rows yet. Its failures can already happen to the parts that exist: a replica killed while it creates a tenant (K9) or an endpoint (K8), which together are row 29, a replica killed during ingest, before the commit (K1) or after it (K2), which together are row 4, and a replica killed after a claim or in the middle of a request (K3 to K5). Their tests kill a replica that runs as a process of its own. What is tested today is the three transactions failing without a kill, in `AdminApiIT#failedBucketInsertLeavesNoTenant`, `EndpointApiIT#failedCircuitInsertLeavesNoEndpoint` and `ApiTimeoutIT#blockedStatementAnswers503`, and for the worker the state that a kill leaves behind, a lease that ran out with nothing recorded, in rows 7 and 12.

## Kill table

Each row is a SIGKILL of a real replica process, landed at a chosen point.

| # | SIGKILL lands | State left | Recovery | Visible effect | Test |
|---|---|---|---|---|---|

## Failure table

| # | Failure | Proven behaviour | Test | Forced by |
|---|---|---|---|---|
| 5 | Duplicate submit | One event; same id returned; different body gets 422 | `IdempotencyIT#sameKeyReturnsSameEvent`, `#concurrentSameKeyCreatesOneEvent`, `#sameKeyDifferentBodyIsRejected` | Request 1 blocked in its transaction; request 2 seen waiting on the unique index |
| 6 | Two claimers race for one delivery | One claimer gets it; the other skips it without waiting; four pollers on one database deliver each of 2,000 deliveries once | `ClaimRaceIT#secondClaimerSkipsLockedRow`, `#fourReplicasDeliverEachEventOnce` | Two transactions that the test holds open, the second with a lock timeout that would fail a claim that waits; a second delivery for the same endpoint, accepted after the first claim, is what the second claim returns; then four pollers over 2,000 deliveries that are all due before the first poller starts |
| 7 | Lease expires while its holder is still sending | The delivery is claimed again and both requests are sent; only the result of the lease holder is recorded | `LeaseFenceIT#lateFinalizeAfterReclaimChangesNothing`, `#repeatedFinalizeIsNoOp` | Receiver holds the first request; lease aged; the second request succeeds; the first is answered afterwards, and the lost-lease counter shows that its finalize ran |
| 12 | A delivery whose every claim ends without a record | Dead-lettered on the 13th claim without a send | `PoisonDeliveryIT#thirteenthClaimDeadLetters` | Twelve claims by the test, each lease aged; a canary to the same receiver, accepted after the delivery is dead, is delivered |
| 18 | Subscriber URL points at, or rebinds to, a private address | Rejected at registration; blocked at connect with zero accepted connections; redirects not followed | `SsrfIT#registrationRejectsPrivateTargets`, `#rebindingIsBlockedAtConnect`, `#redirectIsNotFollowed`, `AddressPolicyTest`, `DnsLookupsTest#systemResolverDoesNotLetADeniedAddressLiteralPast` | `StubDns` answers public first, loopback later; listener counts accepts, and counts a connection that the test opens afterwards; receiver answers a request of the client with a redirect to another of its paths; denied address literals given to the system resolver |
| 19 | Absurd `Retry-After` | Clamped to between the own delay and one hour | `RetryAfterIT#clampedAtBothEnds` | Receiver sends 0 and 10^9, and 120 as the control that the header is read |
| 24 | Tampered body or replayed old request | Reference verifier rejects both | `SignatureIT#verifierRejectsTamperedBodyAndStaleTimestamp` | A request that the service sent, with one byte of its body changed, and unchanged 5 minutes and 1 second after it was signed |
