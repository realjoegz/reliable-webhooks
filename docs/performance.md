# Performance

Four load tests, each run twice on one laptop on 13 October 2026. This document says what a run does, what the runs measured and what limits each number. Every number that a run measured is in a file under [loadtest/results](../loadtest/results), which holds one file for each run, and a number below is quoted as it stands there. A number that is worked out from those is given with the numbers that it comes from. A time of day is in UTC, and a run is named by the time at which its stack was up, which is in the name of its file.

## What the numbers are, and what they are not

- **One laptop.** An Apple M4 with 10 cores and 16 GiB of memory. Docker runs in the virtual machine of Colima, which has 6 CPUs and 7.7 GiB. The database, the two replicas, the subscriber and the load generator are five containers in that machine and share its CPUs. None of them has a limit of its own. What the load generator and the subscriber use is not there for the service, so a number here is a floor for this hardware. The numbers are good for comparing the scenarios with each other. None of them is the capacity of a deployment.
- **A disk that does not make a commit wait.** Each result has what `pg_test_fsync` measured on the volume of the database before the run: between 11245.855 and 17255.590 flushes a second with `fdatasync`. The volume is a file of the virtual machine, and nothing here shows that such a flush reaches the disk of the laptop. On storage that makes a commit wait, every number below is lower. In this one respect the laptop flatters the service.
- **PostgreSQL as its image sets it up.** PostgreSQL 18.6 with 16384 shared buffers of 8 kB, which is 128 MB, a `max_wal_size` of 1024 MB, and autovacuum with its defaults. Nothing is tuned, and each result lists the settings. [What limits the numbers](#what-limits-the-numbers) says what these settings cost.
- **A subscriber that answers at once.** `loadtest/sink/Sink.java` verifies no signature and answers `200` as soon as it has read a request, in the same network as the replicas. Only the kill scenario makes it wait 50 ms.
- **Small events.** An event body of about 1 KiB. An event has one delivery, and three in the kill scenario.
- **A rate limit that no scenario comes near.** Every tenant of a run may send 100,000 units a second. The statement that charges the bucket runs for every event, and [scenario 1](#1-ingest) has the events that it refused all the same.
- **Two replicas as they ship**, apart from the `unsafe-local` allowance for the subscriber: two pools of 16 connections each, at most 64 attempts in flight, a tenth of the traces sampled and no span exported.

## How a run works

`loadtest/run.sh` takes the name of a scenario and does this:

1. It removes the stack of an earlier run with its volume, builds the image of the checkout and starts PostgreSQL, two replicas and the sink.
2. It warms the stack up for five minutes and records nothing of that: events for 20 tenants with one endpoint each, at a rate that rises to 1,000 a second in 30 seconds and stays there.
   - A replica that has just started runs its code interpreted.
   - A database that has just been created has one WAL segment. It creates every further one, 16 MB at a time, and commits wait while it does, until it has the segments of two checkpoints and uses them again. A result says how many segments the database had when the scenario began, which was 63 in every run.
   - The warm-up waits until its deliveries are made and deletes its endpoints, because a claim looks at every active endpoint.
3. It runs `VACUUM (ANALYZE)` and `CHECKPOINT`, and the sink forgets what it counted.
4. It runs the scenario with k6 2.3.0, in a container inside the network of the stack.
5. It waits until no delivery is pending.
6. It writes the result and removes the stack.

A result has these parts:

| Part | What it holds |
|---|---|
| Head | The scenario, the date, the commit, the command, the machine and the machine that Docker runs on |
| PostgreSQL | The version, the settings and `pg_test_fsync` |
| k6 | The summary that k6 printed, unchanged |
| Database | The queries of `loadtest/queries` for the scenario, each followed by its answer. They read only what was created after the scenario began |
| Claim | Only in the per-endpoint ceiling: the plan that PostgreSQL made for a copy of the claim statement in a session of `psql`, and the time that the copy took, read once when the drain of the backlog had begun and once when nothing was pending any more |
| PostgreSQL log | The checkpoints of the database and its vacuums of the `delivery` table during the scenario. An analyze that autovacuum ran without a vacuum is not among them |
| Meters | What the meters of each replica counted during the scenario: the scrape after it less the scrape before it. A run that kills a replica has no such part |
| Sink | The requests that the sink received during the scenario, the `webhook-id`s among them and how many of those arrived more than once |

The commit in a result is the one that the image of the replicas was built from, and `loadtest/run.sh` marks it when the service has changes that are not committed. The load tests themselves were added by the commit after it, which changes nothing of the service. Every result in `loadtest/results` was measured at commit `b735e65`, and the load tests were not run again after the changes that the service has had since. Those changes are: the expiry job repeats its statement while a run takes a full batch, an attempt connects to one address of its host only, a transaction whose commit fails on its connection counts as a transient failure, and a class was renamed. A result does not say which state of the load tests wrote it, and it does not record the settings of the replicas: that they ran as they ship is what `loadtest/compose.yaml` says.

To run them again:

```
loadtest/run.sh ingest
loadtest/run.sh end-to-end
loadtest/run.sh endpoint-ceiling
loadtest/run.sh kill
```

A run needs Docker with Compose 2.24 or later, `curl` and `jq`, and nothing else on the machine: k6 runs as a container. The stack of `compose.yaml` has to be down, because a run gives its network the same address range. CI does not run the load tests.

## 1. Ingest

`loadtest/ingest.js` raises the rate of `POST /v1/events` in six steps of two minutes. The events go to 20 tenants with one endpoint each, so the replicas make one delivery for every event while the next events arrive. A step passes when the 99th percentile of the time that a request took is under 100 ms and the step had no error: every request was answered with a `202`, and the load generator started every request when it was due.

[Run of 05:25](../loadtest/results/ingest-20261013T052509Z.txt):

| Events a second | Requests | Median | 99th percentile | Longest | Not a `202` | Not started | Passes |
|---|---|---|---|---|---|---|---|
| 250 | 30001 | 1.11 ms | 10.99 ms | 215.89 ms | 0 | 0 | yes |
| 500 | 60001 | 980.28 µs | 46.77 ms | 373.29 ms | 1 | 0 | no |
| 750 | 90001 | 2.4 ms | 158.27 ms | 1.57 s | 308 | 0 | no |
| 1,000 | 120001 | 3.86 ms | 471.61 ms | 1.59 s | 1952 | 0 | no |
| 1,250 | 150001 | 7.58 ms | 791.06 ms | 4.31 s | 10356 | 0 | no |
| 1,500 | 180001 | 12.95 ms | 601.55 ms | 2.21 s | 13332 | 0 | no |

[Run of 06:42](../loadtest/results/ingest-20261013T064225Z.txt):

| Events a second | Requests | Median | 99th percentile | Longest | Not a `202` | Not started | Passes |
|---|---|---|---|---|---|---|---|
| 250 | 30001 | 1.12 ms | 16.41 ms | 98.76 ms | 0 | 0 | yes |
| 500 | 59597 | 898.5 µs | 517.26 ms | 1.08 s | 103 | 404 | no |
| 750 | 90000 | 1.12 ms | 609.46 ms | 1.55 s | 1618 | 0 | no |
| 1,000 | 119762 | 2.59 ms | 652.91 ms | 1.76 s | 3549 | 239 | no |
| 1,250 | 150000 | 11.52 ms | 477.1 ms | 3.47 s | 8103 | 0 | no |
| 1,500 | 179882 | 12.95 ms | 603.68 ms | 2.06 s | 13250 | 118 | no |

**The highest rate that passed is 250 events a second, in both runs.** The runs disagree by a lot at 500. In the first, the 99th percentile was 46.77 ms and one request of 60001 was refused. In the second it was 517.26 ms, 103 requests were refused and 404 were not started, which means that for half a second no replica answered anything.

What the tables show besides:

- **The median stays low and the tail does not.** Half of the requests were answered within 12.95 ms at every rate. The 99th percentile and the longest request are those of stalls: stretches of some tenths of a second, and up to 4.31 s, in which requests wait.
- **A stall becomes a `429`.** A tenant may have 8 requests open on a replica. With 20 tenants a stall of a few tenths of a second fills those permits, and the replica answers `429 concurrency_limit` before a route runs. That was 25935 of the 25949 requests that failed in the first run and 23704 of the 26623 in the second.
- **The rate limit refused events that it should not have.** 1 request in the first run and 2919 in the second were answered with `429 rate_limited`, although no tenant came near 100,000 units a second. The statement that charges the bucket takes the time since the last refill from the clock of its own transaction, and a negative time takes tokens away. With a bucket that is full again after one second, a charge whose clock lies a second behind the last refill is refused. Limitation 7 of the design names two ways to that: a charge that waited for the bucket row behind a charge that began after it, and a database clock that was set back. The results hold the count and neither a bucket row nor a wait, so they do not show which of the two it was. They show this much: the run with 2919 is one in which the clock of the database was set back while deliveries were under way: the timer of its replicas holds 602556 of the 602619 deliveries that succeeded, and a timer leaves out a time below zero ([scenario 2](#2-end-to-end)). The run with 1 has every delivery in its timer, and requests that waited up to 4.31 s.
- **The other 13 failures of the first run were `503 database_busy`**: 13 requests found no connection of the `api` pool inside the borrow wait of 3 s.

The deliveries of those events, from the database. A row is two minutes of the clock of the database, counted from the first delivery:

| Events a second | Deliveries, 05:25 | Median, 05:25 | 99th percentile, 05:25 | Deliveries, 06:42 | Median, 06:42 | 99th percentile, 06:42 |
|---|---|---|---|---|---|---|
| 250 | 30001 | 0.066 s | 0.672 s | 30113 | 0.064 s | 0.550 s |
| 500 | 60008 | 0.048 s | 0.350 s | 59777 | 0.045 s | 0.364 s |
| 750 | 89795 | 0.033 s | 0.258 s | 88818 | 0.041 s | 0.397 s |
| 1,000 | 118077 | 0.009 s | 0.594 s | 116463 | 0.020 s | 0.350 s |
| 1,250 | 139668 | 0.028 s | 3.338 s | 143150 | 0.074 s | 8.127 s |
| 1,500 | 166508 | 50.292 s | 64.014 s | 164298 | 55.428 s | 70.459 s |

- **A row is not exactly a step.** The load generator counts the two minutes of a step on its own clock, and the clock of the database fell behind it. Added up, the first five rows hold 437549 deliveries in the first run and 438321 in the second. The first five steps had 437388 and 435987 events accepted, which is the requests of those steps in the tables above less the ones that were not a `202`. So 161 deliveries of the last step are in the row of 1,250 in the first run, and 2334 in the second. Those 2334 are more than one in a hundred of the 143150 deliveries of that row, so its 99th percentile of 8.127 s may be that of deliveries of the last step.
- **Up to 1,000 events a second a delivery is made sooner the more there is to do.** The median falls from 0.066 s at 250 a second to 0.009 s at 1,000. A delivery waits for the next claim, and a poller that found nothing due waits before it claims again ([the idle wait](#what-limits-the-numbers)).
- **At 1,500 a second the replicas no longer kept up.** The median delivery of that step took 50.292 s and 55.428 s. When the last event was accepted, 85223 and 96430 deliveries were still to be made, and the last of them was made 25.6 s and 30.8 s later. 85223 deliveries in 25.6 s and 96430 in 30.8 s are more than 3,000 a second over 20 endpoints: the replicas fell behind while events arrived and caught up as soon as none did. The runs did not record why they fell behind. They have neither the plan of a claim of that step nor a statement of the database that was slow.
- In the second run 6 attempts failed with `connect` and succeeded with their retry. The longest deliveries of its third and its fifth step, 9.056 s and 9.063 s, are such retries.

## 2. End to end

`loadtest/end-to-end.js` sends a steady 500 events a second for five minutes, spread at random over 100 endpoints of 20 tenants. An event goes to one endpoint, so the replicas have 500 deliveries a second to make. It is the highest rate at which a step of scenario 1 had a 99th percentile under its bound, in one of its two runs.

| | [Run of 07:42](../loadtest/results/end-to-end-20261013T074224Z.txt) | [Run of 07:59](../loadtest/results/end-to-end-20261013T075955Z.txt) |
|---|---|---|
| Events sent | 150001 | 150001 |
| Refused with `429 concurrency_limit` | 13 | 601 |
| Accepted, and deliveries that succeeded | 149988 | 149400 |
| Deliveries a second, from the first creation to the last recorded 2xx | 503.1 | 500.9 |
| 99th percentile of a request of the run | 63.43 ms | 354.29 ms |

The time of a delivery from its creation, which is the start of the transaction that accepted its event, to its 2xx:

| | Run of 07:42 | Run of 07:59 |
|---|---|---|
| **From the database** | | |
| Median | 0.016 s | 0.016 s |
| 90th percentile | 0.095 s | 0.099 s |
| 99th percentile | 0.194 s | 0.272 s |
| 99.9th percentile | 0.572 s | 0.788 s |
| Longest | 0.878 s | 0.970 s |
| Mean | 0.038 s | 0.041 s |
| Within 1 s | 149988 of 149988 | 149400 of 149400 |
| **From `webhooks_delivery_end_to_end_seconds`** | | |
| Deliveries in the timer | 149966 | 149382 |
| In its bucket of 1 s | 149966 | 149382 |
| Mean | 0.036406 s | 0.039225 s |

- **The meter does not contradict the database, and it cannot say more.** Its smallest bucket is one second. Every delivery that the timer holds is in that bucket, and every delivery took a second or less by the database, so the percentiles here are those of the database alone. The mean of the meter is a little below that of the database in each run: the meter measures to the end of the request, and the database to the start of the transaction that recorded the 2xx.
- **The timer holds fewer deliveries than succeeded**: 149966 of 149988 and 149382 of 149400, which leaves 22 and 18 out. A timer records no negative time, so a delivery whose time came out negative is missing from it (limitation 23 of the design). The time of the timer starts from the age that the database gave the delivery when its attempt was loaded, and the clock of the database, which is the clock of the virtual machine, was set back during both runs: the database has 24 and 37 deliveries whose 2xx was recorded with an earlier time than their creation, the shortest at -0.123 s and -0.124 s. The two counts are not of the same deliveries, and the results do not say which deliveries the timer lacks. The timer ends with the request and the database with the transaction that recorded it, so a delivery whose clock went back after its load is below zero in the database and still in the timer. Every time that this document takes from the database is off by as much for a delivery that was under way at such a moment.
- **The deliveries agree between the runs and the requests do not.** The median delivery took 0.016 s in both. One run refused 13 events and the other 601, all of them with a `429` of the request permits during a stall, and the 99th percentile of a request was 63.43 ms in the one and 354.29 ms in the other.
- **A delivery mostly waits.** The two pollers made 55333 claims in the first run, which took 180.842 s in all, 0.003268 s each, and brought 2.710643 rows each. The run lasted 298.1 s, so each poller spent more than two thirds of it between claims. A request to the sink took 0.000858 s on average. What a delivery waits for is the next claim ([the idle wait](#what-limits-the-numbers)).
- In the half minutes of a run the median lies between 0.007 s and 0.045 s, and the 99th percentile between 0.052 s and 0.779 s.

## 3. Per-endpoint ceiling

`loadtest/endpoint-ceiling.js` gives one endpoint a backlog of 50,000 deliveries and lets two replicas, which have nothing else to do, drain it. The backlog is built behind an open circuit: the sink answers `503` while twelve senders send the events, the first attempts fail, which opens the circuit, and the deliveries that follow are held. When the last event is in, the sink answers `200` again, the next probe closes the circuit, and the drain begins.

Limitation 6 of the design says that one endpoint is drained at one delivery per claim round trip per replica, at most 4 at a time. The sink answers a request of this scenario in 0.000295 s and 0.000292 s on average, so four at a time are never open, and what a replica can do for one endpoint is one delivery for each claim.

The drain, in the deliveries that the two replicas together made in each five seconds, and how many a second that is. The five seconds are counted from the first recorded 2xx, so the first row is full and the last row of a run is not: the drain of the first run lasted 62.0 s and that of the second 59.8 s. The rate of such a row is its deliveries over five seconds all the same. In the two results the comment above this query says that the first five seconds are not full either. They are: the statement there is the one of `loadtest/queries/endpoint-ceiling.sql`, and it counts from the first recorded 2xx.

| Five seconds of the drain | [Run of 07:35](../loadtest/results/endpoint-ceiling-20261013T073522Z.txt) | Deliveries a second | [Run of 07:52](../loadtest/results/endpoint-ceiling-20261013T075253Z.txt) | Deliveries a second |
|---|---|---|---|---|
| 1st | 656 | 131.2 | 629 | 125.8 |
| 2nd | 563 | 112.6 | 556 | 111.2 |
| 3rd | 574 | 114.8 | 560 | 112.0 |
| 4th | 556 | 111.2 | 544 | 108.8 |
| 5th | 550 | 110.0 | 574 | 114.8 |
| 6th | 576 | 115.2 | 598 | 119.6 |
| 7th | 632 | 126.4 | 587 | 117.4 |
| 8th | 1002 | 200.4 | 6280 | 1256.0 |
| 9th | 8055 | 1611.0 | 8697 | 1739.4 |
| 10th | 12146 | 2429.2 | 10049 | 2009.8 |
| 11th | 9921 | 1984.2 | 10078 | 2015.6 |
| 12th | 10156 | 2031.2 | 10848, in 4.8 s | 2169.6 |
| 13th | 4613, in 2.0 s | 922.6 | | |

Each run has two rates, and they differ twentyfold.

- **The ceiling that limitation 6 describes is the second rate.** One endpoint got 1984.2 to 2429.2 deliveries a second from two replicas in the last three full five seconds of the first run, and 2009.8 and 2015.6 in the last two of the second. The two replicas share them evenly: 6057 and 6089 deliveries in the best five seconds of the first run. A replica makes one delivery of this endpoint for each claim, so 6057 deliveries in five seconds leave less than a millisecond for a claim and for what the poller does between two of them.
- **In the first seven of those five seconds the replicas made 108.8 to 131.2 deliveries a second**, with the same backlog, the same subscriber and nothing else to do.

The part of a result that is headed Claim points at the reason. `loadtest/run.sh` has a copy of the claim statement explained from `psql`, with `EXPLAIN (ANALYZE)`, once when the drain had begun and once when it was over, and the part has the plan and the time of each. Both are those of the session of `psql`. A replica runs the claim in sessions of its own, and the plan that it had there was not read. The second reading found nothing pending, so it shows the plan that PostgreSQL chose by then and not what a claim that takes a row costs with it.

| | Run of 07:35 | Run of 07:52 |
|---|---|---|
| Deliveries that were pending at the first reading | 49911 | 49923 |
| While the drain was slow, the copy of the claim found the head of the backlog with | an index scan of `delivery_expiry` and a sort | the same |
| Rows that it read to find one | 49907 | 49921 |
| Time that it took | 17.216 ms | 16.337 ms |
| The statistics of the table were then from | 07:40:38, before the backlog was built | 07:58:10, before the backlog was built |
| Autovacuum analysed the table again at | 07:41:51 | 07:59:20 |
| After the drain, with nothing pending, the copy looked for the head with | an index scan of `delivery_head` | the same |
| Time that it took, with no row found | 1.114 ms | 1.409 ms |

- **The claim is written for the index `delivery_head`**, which holds the pending deliveries of an endpoint in the order of their due time, so that the oldest is the first entry. `delivery_expiry` holds every pending delivery in the order of its expiry. Read through it, the head of an endpoint is found by reading every pending delivery and sorting them.
- **PostgreSQL chose `delivery_expiry` with statistics that said that nothing was pending.** They were gathered before the backlog existed, by the `ANALYZE` of step 3 of a run. The plan expects one row from the index where it read 49907 and 49921. That is the state of the statistics of any queue that keeps up. The rate of the replicas fits that plan: each of the two made 328 deliveries in the first five seconds of the first run, one for each claim, which is a claim in 15 ms, and the copy took 17.216 ms. The rate rose when autovacuum analysed the table again, at 07:41:51 and at 07:59:20.
- **A backlog that meets this plan is drained slowly until autovacuum has analysed the table**, and the deeper the backlog, the slower each claim. The plan filters the entries of `delivery_expiry` by their endpoint and does not look them up by it, so it reads the pending deliveries of every endpoint, held ones among them, once for each eligible endpoint. With several endpoints a claim therefore costs more than here, and it costs every tenant the same, because a replica makes one claim for all of them. No run measured that: this scenario has one tenant with one endpoint. Limitation 4 of the design describes the plan. The service is as it was: these runs measure it and change nothing of it.
- **Two runs do not say that a drain always begins with the slow rate.** In both, the drain began before autovacuum had analysed the backlog, and that depends on timing. The circuit opens with the first events, and the drain begins with the first probe that finds the sink answering `200`. The first probe is due 30 s after the circuit has opened. In both runs the last event was in 4.4 s before the first 2xx: the last delivery was made 66.4 s and 64.2 s after the last event, and the drain lasted 62.0 s and 59.8 s. The first 2xx was recorded at 07:41:11 and at 07:58:43, and autovacuum analysed the table at 07:41:51 and at 07:59:20. On a machine that takes more than those 30 s for the events, the first probe meets a sink that still answers `503`, and the next one is due 60 s later. The table may be analysed by then. Nor do the runs show that a replica gets the plan of the copy every time that the statistics are stale: its own plan was not read.
- **Whether the plan had a part in the last step of [scenario 1](#1-ingest) is not shown.** Those runs did not read the plan. What was left of that step was drained at more than 3,000 deliveries a second, against the 108.8 to 131.2 that this plan gave here.

Over the whole drain, slow part and fast part together, the two replicas made 805.9 and 836.2 deliveries a second, which describes neither part.

## 4. Kill under load

`loadtest/kill.js` sends 300 events a second for two minutes to 10 tenants with three endpoints each. An event goes to all three endpoints of its tenant, so the replicas have 900 deliveries a second to make. In this scenario the sink waits 50 ms before it answers, so that requests are open when the kill comes. Once a third of the events are in, `loadtest/run.sh` kills the second replica with SIGKILL, and the first one goes on alone.

A sender of this scenario is a producer that knows both replicas. It gives a replica a second to answer and then sends the same request, under the same idempotency key, to the other one. A `429` or a `503` makes it wait a second and send again. It sends an event five times at most.

| | [Run of 06:34](../loadtest/results/kill-20261013T063450Z.txt) | [Run of 07:18](../loadtest/results/kill-20261013T071811Z.txt) |
|---|---|---|
| Events that a `202` accepted | 36001 | 36001 |
| Events that the producer gave up | 0 | 0 |
| Requests that it sent again | 212 | 303 |
| Events in the database | 36001 | 36001 |
| Events with all three deliveries succeeded | 36001 | 36001 |
| Deliveries that succeeded | 108003 | 108003 |
| Deliveries that did not | 0 | 0 |
| Deliveries that the killed replica held | 39 | 12 |
| Attempts logged as `unknown`, `lease_expired` | 39 | 12 |
| Those deliveries were recorded as succeeded, after the kill | 59.6 s to 59.8 s | 59.7 s |
| Requests that the sink received | 108042 | 108015 |
| `webhook-id`s that it received more than once | 39 | 12 |
| Exit code of the killed replica | 137 | 137 |

- **Every accepted event was delivered to all of its endpoints**, in both runs, and the database holds as many events as the producer had accepted.
- **The subscriber received 39 requests twice in one run and 12 in the other.** Those are the deliveries that the killed replica had sent and not recorded. Each kept its lease until it ran out, 60 s after its claim, and the surviving replica then logged the attempt as `unknown` and sent the request again with the same `webhook-id`.
- **What a kill costs a delivery is the rest of its lease**, and how many deliveries pay it depends on the moment: with 900 deliveries a second that take 50 ms each, the kill found 39 on its replica in one run and 12 in the other.
- In the first run one request was answered as the repeat of an event: the killed replica had committed that event and not answered, and the surviving one answered the second request for it with the same event.
- The surviving replica recorded 71640 and 71034 deliveries after the kill, alone.

## What limits the numbers

- **Stalls.** Most requests are answered within a few milliseconds at every rate that was tried, and every 99th percentile above is that of stretches in which requests wait: some tenths of a second, a few times more than a second. They come at 500 events a second in one run and not in the next. The log of the database in each result shows when it began a checkpoint and when autovacuum had processed the `delivery` table, and at the higher rates it began one checkpoint after the other: seven between 05:34:55 and 05:42:33 in the first ingest run. No run recorded the slow statements of the database, so the runs show the stalls and not what each of them was. The settings of the database are those of its image, with 128 MB of shared buffers and 1 GB of WAL between checkpoints.
- **The request permits.** A tenant has 8 requests open on a replica at most. They are the first thing that a producer sees of a stall: nearly every request that failed in any run was a `429 concurrency_limit`.
- **The connection pools.** A borrower of the `api` pool, which has 16 connections on each replica, waited 0.000227 s and 0.001862 s on average at 500 events a second, and 0.005969 s and 0.006478 s over the steps of the ingest runs, where 13 borrowers of one run waited 3 s and got none. The `worker` pool was waited for 0.000764 s and 0.000974 s at 500 events a second, and 0.001451 s and 0.001593 s in the ingest runs.
- **The idle wait of the poller.** A poller that finds nothing due waits before it claims again, 50 ms at first and twice as long after every further claim without a row, up to 1 s. An attempt that ends wakes it, and the claim that follows counts like any other. Attempts that end one after the other, with nothing new due in between, therefore take the wait to its longest within milliseconds. That is why the median delivery took 0.066 s at 250 events a second and 0.009 s at 1,000, and why the 99th percentile of a delivery was 0.194 s and more at a rate at which a poller spends most of its time waiting. Limitation 35 of the design describes it.
- **The claim round trip, and the cap for one endpoint.** A replica has one poller, and a claim takes one delivery for an endpoint. In the fast part of scenario 3 a replica made up to 6089 deliveries of the one endpoint in five seconds, one for each claim, and the endpoint got 1984.2 to 2429.2 deliveries a second from two replicas. A subscriber that takes longer to answer than this sink meets the other half of the cap first, which is 4 requests at a time from a replica. No run measured that.
- **The plan of the claim.** With statistics from a time when nothing was pending, a copy of the claim read the whole queue to find the head of one endpoint and took 17.216 ms and 16.337 ms, and the replicas made 108.8 to 131.2 deliveries a second for that endpoint meanwhile. A backlog that meets this plan is drained at that rate until the table is analysed. Of everything that these runs found, this costs the most.
- **The laptop.** Five containers share 6 CPUs. `process_cpu_time_ns_total` of the two replicas rose by 97870000000 and 90320000000 in the 298.1 s of an end-to-end run, which is 97.87 s and 90.32 s of CPU time and a third of one CPU each. In the twelve minutes of an ingest run it rose by 384340000000 and 381790000000, which is 384.34 s and 381.79 s and about half of one CPU each. The replicas were not what ran out. What the database, the sink and the load generator used was not measured. A flush of the WAL costs next to nothing here. The clock of the virtual machine is set back every now and then: the shortest delivery of each end-to-end run took -0.123 s and -0.124 s by it.

## What the runs do not show

- **What one tenant can send.** Every scenario but the third spreads its events over 10 or 20 tenants. One tenant has 8 request permits on a replica and one bucket row, which every one of its events updates.
- **A subscriber as slow as a real one.** The sink answers in less than a millisecond, and in 50 ms in the kill scenario. No request crosses a network or uses TLS.
- **More replicas, or replicas with limits.** Two replicas ran, without a limit on CPU or memory. The chart of this repository gives a replica 2 CPUs and 1 Gi at most.
- **A database that is set up for the load**, with more memory than the 128 MB of shared buffers of the image, with more room between checkpoints, or with autovacuum told to analyse the queue more often, and storage that makes a commit wait.
- **A second tenant beside a backlog.** Scenario 3 has one tenant with one endpoint. No run measured what the deliveries of another tenant wait for while a claim reads a backlog through `delivery_expiry`, and none had a backlog so deep that a claim came near the 10 s that the `worker` pool gives a statement (limitation 4 of the design).
- **How often a drain is slow.** Both runs of scenario 3 began their drain with the slow rate, and two runs are few. The plan was read from a session of `psql` and not from the sessions of the replicas, so nothing here shows that every replica gets that plan whenever the statistics are stale.
- **How fast many endpoints are drained with the right plan.** Scenario 3 has it for one endpoint, and without the plan or the time of a claim that took a row with it. The step of scenario 1 that fell behind did so with statistics that these runs did not record.
- **When the queue was analysed in scenarios 1, 2 and 4.** The log part of a result keeps the vacuums of autovacuum and not its analyzes, and only scenario 3 reads the time of the last one from the database.
- **Why the bucket refused.** The results count the events that it refused. They hold no bucket row and no wait for one.
- **What a stall is.** The results have the times of checkpoints and vacuums next to the slow stretches. They do not have the statements that were slow.
- **Larger events and wider fan-out.** An event body is about 1 KiB, and an event goes to one endpoint, or to three.
- **Hours.** The longest scenario lasts twelve minutes after a warm-up of five. Nothing here shows a database after days of retention.
