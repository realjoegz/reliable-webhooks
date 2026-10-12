package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.delivery.CircuitBreaker.Verdict;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Attempt;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.NextState;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.SelfStartedReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.ops.ExpiryJob;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The expiry job against deliveries in the states that a claim and a finalize leave them in. The
 * job of the replica does not run by itself in these tests, so a run is the one that the test
 * calls, and what a delivery is afterwards is what that run made of it. The seventy-two hours pass
 * in the rows: the test moves their times into the past.
 *
 * <p>The class sits beside the worker and not beside the job. It makes those states with the claim
 * and the finalize of the worker, which only this package can call.
 */
@InProcessReplica
class ExpiryIT {

  private static final String HOST = "hooks.expiry.example";
  private static final String HOOK = "https://" + HOST + "/hook";
  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final Duration BUDGET = Duration.ofHours(72);
  private static final Duration HALF_A_LEASE = Duration.ofSeconds(30);
  private static final int BATCH = 1_000;
  private static final UUID[] NONE = {};
  private static final String STARTED_BY_SPRING = "replica-with-its-own-expiry-job";

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired DeliveryRepository deliveries;
  @Autowired WebhooksProperties properties;
  @Autowired ExpiryJob expiry;
  @Autowired MeterRegistry meters;

  @Autowired
  @Qualifier("workerJdbc")
  JdbcClient workerJdbc;

  @Autowired
  @Qualifier("workerTransactions")
  TransactionTemplate workerTransactions;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  // Six deliveries for one endpoint, each in another state when the run comes.
  //
  // Three are pending past their seventy-two hours and not leased: one that was never claimed,
  // one that waits for its retry after a failed attempt, and one whose holder recorded nothing
  // before its lease ran out. The run dead-letters all three, and writes the attempt row that the
  // third is missing.
  //
  // Three are left as they are: one that is past its budget but leased, one that has a minute of
  // its budget left, and one that succeeded. The run that leaves them is the run that took the
  // other three, so it did run while they were in that state.
  //
  // The leased one was claimed half a lease before its budget ended. The test has the other half
  // of the lease to make the run. A test that took longer would fail: the lease would be over and
  // the run would take the delivery. Once the lease is aged past its end, the next run does take
  // it, and the result of its former holder is turned away.
  //
  // Dead as expired is no more final than dead as exhausted. The delivery that waited for its
  // retry is replayed at the end: it is pending again, in a new run with no attempt counted, and
  // the attempt row of its first run stays.
  @Test
  void pendingPastBudgetBecomesDead() {
    TestTenant tenant = TestTenant.create(http, "expiry");
    tenant.register(http, HOOK, "*");
    Attempt failure = new Attempt("failure", 500, null, null, Duration.ofMillis(80));
    Attempt success = new Attempt("success", 200, null, null, Duration.ofMillis(80));

    UUID waitingForRetry = Rows.deliveryOf(tenant.accept(http, EVENT));
    deliveries.recordAttempt(
        claimOnly(waitingForRetry),
        NextState.retryIn(Duration.ofHours(1)),
        failure,
        Verdict.FAILURE,
        "replica-1");
    UUID succeeded = Rows.deliveryOf(tenant.accept(http, EVENT));
    deliveries.recordAttempt(
        claimOnly(succeeded), NextState.succeeded(), success, Verdict.SUCCESS, "replica-1");
    UUID abandoned = Rows.deliveryOf(tenant.accept(http, EVENT));
    claimOnly(abandoned);
    UUID leased = Rows.deliveryOf(tenant.accept(http, EVENT));
    Rows.ageDelivery(leased, BUDGET.minus(HALF_A_LEASE));
    Claimed holderOfTheLease = claimOnly(leased);
    Rows.ageDelivery(leased, HALF_A_LEASE.plusSeconds(1));
    Rows.ageDelivery(waitingForRetry, BUDGET);
    Rows.ageDelivery(succeeded, BUDGET);
    Rows.ageDelivery(abandoned, BUDGET);
    UUID neverClaimed = Rows.deliveryOf(tenant.accept(http, EVENT));
    Rows.ageDelivery(neverClaimed, BUDGET);
    UUID withBudgetLeft = Rows.deliveryOf(tenant.accept(http, EVENT));
    Rows.ageDelivery(withBudgetLeft, BUDGET.minusMinutes(1));
    List<String> leftAlone = Rows.everyColumnOf(List.of(leased, withBudgetLeft, succeeded));
    double countedBefore = attemptsOfUnknownOutcome();

    expiry.run();

    assertThat(Rows.delivery(neverClaimed))
        .isEqualTo(new Rows.Delivery("dead", 0, 0, false, "expired", true));
    assertThat(Rows.attempts(neverClaimed)).isEmpty();
    assertThat(Rows.delivery(waitingForRetry))
        .isEqualTo(new Rows.Delivery("dead", 1, 0, false, "expired", true));
    assertThat(Rows.attempts(waitingForRetry))
        .containsExactly(new Rows.Attempt(0, 1, false, "failure", 500, null, null, "replica-1"));
    assertThat(Rows.delivery(abandoned))
        .isEqualTo(new Rows.Delivery("dead", 1, 0, false, "expired", true));
    assertThat(Rows.attempts(abandoned))
        .containsExactly(
            new Rows.Attempt(0, 1, false, "unknown", null, "lease_expired", null, null));
    assertThat(attemptsOfUnknownOutcome() - countedBefore).isEqualTo(1);

    assertThat(Rows.everyColumnOf(List.of(leased, withBudgetLeft, succeeded))).isEqualTo(leftAlone);
    assertThat(Rows.delivery(leased))
        .isEqualTo(new Rows.Delivery("pending", 1, 0, true, null, false));
    assertThat(Rows.delivery(withBudgetLeft))
        .isEqualTo(new Rows.Delivery("pending", 0, 0, false, null, false));
    assertThat(Rows.delivery(succeeded).state()).isEqualTo("succeeded");

    Rows.ageDelivery(leased, HALF_A_LEASE);
    expiry.run();

    assertThat(Rows.delivery(leased))
        .isEqualTo(new Rows.Delivery("dead", 1, 0, false, "expired", true));
    assertThat(
            deliveries.recordAttempt(
                holderOfTheLease, NextState.succeeded(), success, Verdict.SUCCESS, "late"))
        .isFalse();
    assertThat(Rows.delivery(leased).state()).isEqualTo("dead");
    assertThat(Rows.attempts(leased))
        .containsExactly(
            new Rows.Attempt(0, 1, false, "unknown", null, "lease_expired", null, null));
    assertThat(attemptsOfUnknownOutcome() - countedBefore).isEqualTo(2);

    // The tenant reads the attempt that nobody recorded as one without a status and a duration.
    // Its start is the time at which the run logged it, which is when the delivery was completed.
    http.get()
        .uri("/v1/deliveries/{id}", abandoned)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.state")
        .isEqualTo("dead")
        .jsonPath("$.dead_reason")
        .isEqualTo("expired")
        .jsonPath("$.attempt_log.length()")
        .isEqualTo(1)
        .jsonPath("$.attempt_log[0].attempt_no")
        .isEqualTo(1)
        .jsonPath("$.attempt_log[0].outcome")
        .isEqualTo("unknown")
        .jsonPath("$.attempt_log[0].error")
        .isEqualTo("lease_expired")
        .jsonPath("$.attempt_log[0].http_status")
        .isEmpty()
        .jsonPath("$.attempt_log[0].duration_ms")
        .isEmpty()
        .jsonPath("$.attempt_log[0].started_at")
        .value(
            String.class, at -> assertThat(Instant.parse(at)).isEqualTo(completedAtOf(abandoned)));

    tenant.replay(http, waitingForRetry).expectStatus().isNoContent();

    assertThat(Rows.delivery(waitingForRetry))
        .isEqualTo(new Rows.Delivery("pending", 0, 1, false, null, false));
    assertThat(Rows.attempts(waitingForRetry))
        .containsExactly(new Rows.Attempt(0, 1, false, "failure", 500, null, null, "replica-1"));
  }

  // Twice as many deliveries as a run takes are past their budget. In the order in which they
  // were written, every other one expired an hour before the ones beside it. The run takes the
  // thousand that expired first, and the next run takes the rest.
  //
  // That order has to come from the statement. The index on the time of expiry hands the rows
  // over in that order whatever the statement asks for, and the planner reads them through it
  // here. The first run is therefore made inside a transaction of the test, on the pool of the
  // worker, that has the index scans turned off. Its rows come as they lie in the table, where
  // the two halves alternate, so a run that took them as they came would take some of each.
  //
  // A session keeps the plan of a statement that it has run often, and goes on using it under
  // other settings. The plans are therefore discarded before the run, and after it, so that the
  // plan of this run is not kept either.
  //
  // If the run were outside the transaction, or still read through the index, the test would
  // pass whatever the statement asked for, so it shows that neither is the case. What the run
  // did is hidden from every other session until the transaction ends: the run was a part of
  // it. And the statement reads the table twice, once to pick its deliveries and once to change
  // them. The session counts two sequential scans of the table over the run, so neither read
  // went through an index.
  @Test
  void runTakesTheThousandDeliveriesThatExpiredFirst() {
    TestTenant tenant = TestTenant.create(http, "expiry-batch");
    UUID endpoint = tenant.register(http, HOOK, "*");
    insertDeliveriesPastTheirBudget(tenant, endpoint, 2 * BATCH);

    workerTransactions.executeWithoutResult(
        transaction -> {
          workerJdbc.sql("SET LOCAL enable_indexscan = off").update();
          workerJdbc.sql("SET LOCAL enable_bitmapscan = off").update();
          workerJdbc.sql("DISCARD PLANS").update();
          long scansBeforeTheRun = sequentialScansOfTheDeliveryTable();
          expiry.run();
          long scansOfTheRun = sequentialScansOfTheDeliveryTable() - scansBeforeTheRun;
          workerJdbc.sql("DISCARD PLANS").update();
          assertThat(scansOfTheRun).as("sequential scans of the table by the run").isEqualTo(2);
          assertThat(deliveriesIn("dead", tenant)).isZero();
        });

    assertThat(deliveriesIn("dead", tenant)).isEqualTo(BATCH);
    assertThat(latestExpiryIn("dead", tenant)).isBefore(earliestExpiryIn("pending", tenant));

    expiry.run();

    assertThat(deliveriesIn("dead", tenant)).isEqualTo(2 * BATCH);
  }

  // The test holds the row of one expired delivery in a transaction of its own. A run that waited
  // for the row would wait until the lock timeout of its pool and fail. This one returns, has
  // taken the other delivery, and has left the held one for the run that follows the rollback.
  @Test
  void runPassesOverADeliveryThatAnotherTransactionHolds() throws Exception {
    TestTenant tenant = TestTenant.create(http, "expiry-skip-locked");
    tenant.register(http, HOOK, "*");
    UUID held = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID free = Rows.deliveryOf(tenant.accept(http, EVENT));
    Rows.ageDelivery(held, BUDGET);
    Rows.ageDelivery(free, BUDGET);

    try (Connection holder = TestDatabase.connection();
        PreparedStatement lock =
            holder.prepareStatement("SELECT 1 FROM delivery WHERE id = ? FOR UPDATE")) {
      lock.setObject(1, held);
      lock.execute();

      expiry.run();

      assertThat(Rows.delivery(free).state()).isEqualTo("dead");
      assertThat(Rows.delivery(held).state()).isEqualTo("pending");
      holder.rollback();
    }

    expiry.run();

    assertThat(Rows.delivery(held))
        .isEqualTo(new Rows.Delivery("dead", 0, 0, false, "expired", true));
  }

  // Every other test calls the run itself. Here a second replica starts as the application does,
  // and Spring starts its expiry job, whose first run comes with the start. That replica runs
  // its worker too, so the delivery belongs to a suspended tenant: no claim takes it, and it is
  // dead without an attempt.
  @Test
  void replicaRunsItsExpiryJobByItself() {
    TestTenant tenant = TestTenant.create(http, "expired-by-a-replica");
    tenant.register(http, HOOK, "*");
    UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
    tenant.patch(http, Map.of("status", "suspended"));
    Rows.ageDelivery(delivery, BUDGET);

    try (ConfigurableApplicationContext replica = SelfStartedReplica.start(STARTED_BY_SPRING)) {
      await().untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("dead"));
    }

    assertThat(Rows.delivery(delivery))
        .isEqualTo(new Rows.Delivery("dead", 0, 0, false, "expired", true));
    assertThat(Rows.attempts(delivery)).isEmpty();
  }

  // The delivery is the only one that is due and not leased when it is claimed.
  private Claimed claimOnly(UUID delivery) {
    List<Claimed> claimed = deliveries.claim(NONE, NONE, 1, properties.delivery().lease());
    assertThat(claimed).extracting(Claimed::id).containsExactly(delivery);
    return claimed.getFirst();
  }

  private double attemptsOfUnknownOutcome() {
    return meters
        .get("webhooks.delivery.attempts")
        .tag("outcome", "unknown")
        .tag("reason", "lease_expired")
        .counter()
        .count();
  }

  // Events with one delivery each, as ingest leaves them for one matching endpoint, but created
  // more than seventy-two hours ago: a second apart, and every other one an hour before that.
  // They are written here and not accepted through the API: no request can be that old.
  private static void insertDeliveriesPastTheirBudget(
      TestTenant tenant, UUID endpointId, int count) {
    TestDatabase.jdbc()
        .sql(
            """
            WITH old AS (
              INSERT INTO event (id, tenant_id, type, body, idempotency_key, request_hash,
                                 created_at)
              SELECT gen_random_uuid(), :t, 'order.created', :body, 'expired-' || n, sha256(:body),
                     now() - interval '72 hours' - n * interval '1 second'
                       - (n % 2) * interval '1 hour'
              FROM generate_series(1, :count) AS n
              RETURNING id, created_at
            )
            INSERT INTO delivery (tenant_id, event_id, endpoint_id, next_attempt_at, expires_at,
                                  created_at)
            SELECT :t, id, :e, created_at, created_at + interval '72 hours', created_at
            FROM old
            """)
        .param("t", tenant.id())
        .param("e", endpointId)
        .param("body", EVENT.getBytes(StandardCharsets.UTF_8))
        .param("count", count)
        .update();
  }

  // What the session of the calling transaction has counted and not yet reported. That can
  // include scans from just before the transaction began, so the caller compares two counts.
  private long sequentialScansOfTheDeliveryTable() {
    return workerJdbc
        .sql("SELECT seq_scan FROM pg_stat_xact_user_tables WHERE relname = 'delivery'")
        .query(Long.class)
        .single();
  }

  private static Instant completedAtOf(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql("SELECT completed_at FROM delivery WHERE id = :id")
        .param("id", deliveryId)
        .query(OffsetDateTime.class)
        .single()
        .toInstant();
  }

  private static int deliveriesIn(String state, TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM delivery WHERE tenant_id = :t AND state = :state")
        .param("t", tenant.id())
        .param("state", state)
        .query(Integer.class)
        .single();
  }

  private static Instant latestExpiryIn(String state, TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT max(expires_at) FROM delivery WHERE tenant_id = :t AND state = :state")
        .param("t", tenant.id())
        .param("state", state)
        .query(OffsetDateTime.class)
        .single()
        .toInstant();
  }

  private static Instant earliestExpiryIn(String state, TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT min(expires_at) FROM delivery WHERE tenant_id = :t AND state = :state")
        .param("t", tenant.id())
        .param("state", state)
        .query(OffsetDateTime.class)
        .single()
        .toInstant();
  }
}
