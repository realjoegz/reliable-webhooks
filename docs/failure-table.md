# Failure table

Each row names one failure, what the service does about it and the test that proves it. A row is added only together with its test. The build runs `FailureTableCheck` after the integration tests, and it fails unless every test linked from a table in this file ran and passed in the same build.

[design.md](design.md) lists the rows the finished service has to prove, in sections 3 and 13. A row here claims only the part of its design row that tests prove today. Row 5 is the whole design row. Row 18 covers registration. The design row also covers the address check at connect time and redirects, which arrive with delivery.

The kill table has no rows yet. Four of its failures can already happen to the parts that exist: a replica killed while it creates a tenant (K9) or an endpoint (K8), which together are row 29, and a replica killed during ingest, before the commit (K1) or after it (K2), which together are row 4. Their tests kill a replica that runs as a process of its own, and the test harness cannot start one yet. What is tested today is the same three transactions failing without a kill, in `AdminApiIT#failedBucketInsertLeavesNoTenant`, `EndpointApiIT#failedCircuitInsertLeavesNoEndpoint` and `ApiTimeoutIT#blockedStatementAnswers503`.

## Kill table

Each row is a SIGKILL of a real replica process, landed at a chosen point.

| # | SIGKILL lands | State left | Recovery | Visible effect | Test |
|---|---|---|---|---|---|

## Failure table

| # | Failure | Proven behaviour | Test | Forced by |
|---|---|---|---|---|
| 5 | Duplicate submit | One event; same id returned; different body gets 422 | `IdempotencyIT#sameKeyReturnsSameEvent`, `#concurrentSameKeyCreatesOneEvent`, `#sameKeyDifferentBodyIsRejected` | Request 1 blocked in its transaction; request 2 seen waiting on the unique index |
| 18 | Subscriber URL points at a private address | Rejected at registration | `SsrfIT#registrationRejectsPrivateTargets`, `AddressPolicyTest`, `DnsLookupsTest#systemResolverDoesNotLetADeniedAddressLiteralPast` | Address literals in the URL, answered by `StubDns` in the replica and by the system resolver in `DnsLookupsTest`; `StubDns` answers a host name with a private address |
