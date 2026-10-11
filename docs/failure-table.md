# Failure table

Each row names one failure, what the service does about it and the test that proves it. A row is added only together with its test. The build runs `FailureTableCheck` after the integration tests, and it fails unless every test linked from this file ran and passed in the same build.

The tables have no rows yet, because no row has all of its tests. [design.md](design.md) lists the rows the finished service has to prove, in sections 3 and 13.

Some of those failures can already happen to the parts that exist. The nearest is a replica killed while it creates a tenant (K9 and row 29). Its test kills a replica that runs as a process of its own, and the test harness cannot start one yet. What is tested today is the same transaction failing without a kill, in `AdminApiIT#failedBucketInsertLeavesNoTenant`.

## Kill table

Each row is a SIGKILL of a real replica process, landed at a chosen point.

| # | SIGKILL lands | State left | Recovery | Visible effect | Test |
|---|---|---|---|---|---|

## Failure table

| # | Failure | Proven behaviour | Test | Forced by |
|---|---|---|---|---|
