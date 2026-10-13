package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The expiry job against more deliveries than one of its statements takes. The job of the replica
 * does not run by itself in these tests, so a statement or a run is the one that the test calls.
 * The test writes the deliveries as rows that are already past their seventy-two hours.
 *
 * <p>The class sits beside the job, whose statement only this package can call. {@code ExpiryIT}
 * sits beside the worker and has the states in which a run finds a delivery.
 */
@InProcessReplica
class ExpiryBatchIT {

  private static final String HOST = "hooks.expiry-batch.example";
  private static final String HOOK = "https://" + HOST + "/hook";
  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int BATCH = 1_000;

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired ExpiryJob expiry;
  @Autowired ExpiryRepository statement;
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

  // Twice as many deliveries as a statement takes are past their budget. In the order in which
  // they were written, every other one expired an hour before the ones beside it. The statement
  // takes the thousand that expired first, and the run after it takes the rest.
  //
  // That order has to come from the statement. The index on the time of expiry hands the rows
  // over in that order whatever the statement asks for, and the planner reads them through it
  // here. The statement is therefore run inside a transaction of the test, on the pool of the
  // worker, that has the index scans turned off. Its rows come as they lie in the table, where
  // the two halves alternate, so a statement that took them as they came would take some of
  // each.
  //
  // A session keeps the plan of a statement that it has run often, and goes on using it under
  // other settings. The plans are therefore discarded before the statement, and after it, so
  // that the plan of this one is not kept either.
  //
  // If the statement were outside the transaction, or still read through the index, the test
  // would pass whatever the statement asked for, so it shows that neither is the case. What the
  // statement did is hidden from every other session until the transaction ends: it was a part
  // of it. And the statement reads the table twice, once to pick its deliveries and once to
  // change them. The session counts two sequential scans of the table over it, so neither read
  // went through an index.
  @Test
  void statementTakesTheThousandDeliveriesThatExpiredFirst() {
    TestTenant tenant = TestTenant.create(http, "expiry-batch");
    UUID endpoint = tenant.register(http, HOOK, "*");
    insertDeliveriesPastTheirBudget(tenant, endpoint, 2 * BATCH);

    workerTransactions.executeWithoutResult(
        transaction -> {
          workerJdbc.sql("SET LOCAL enable_indexscan = off").update();
          workerJdbc.sql("SET LOCAL enable_bitmapscan = off").update();
          workerJdbc.sql("DISCARD PLANS").update();
          long scansBefore = sequentialScansOfTheDeliveryTable();
          statement.expire();
          long scans = sequentialScansOfTheDeliveryTable() - scansBefore;
          workerJdbc.sql("DISCARD PLANS").update();
          assertThat(scans).as("sequential scans of the table by the statement").isEqualTo(2);
          assertThat(deliveriesIn("dead", tenant)).isZero();
        });

    assertThat(deliveriesIn("dead", tenant)).isEqualTo(BATCH);
    assertThat(latestExpiryIn("dead", tenant)).isBefore(earliestExpiryIn("pending", tenant));

    expiry.run();

    assertThat(deliveriesIn("dead", tenant)).isEqualTo(2 * BATCH);
  }

  // One delivery more than two statements take is past its budget. A run goes on while a
  // statement takes a full batch, so one run dead-letters them all, in three statements, and
  // counts the deliveries of each.
  @Test
  void runDeadLettersMoreDeliveriesThanAStatementTakes() {
    TestTenant tenant = TestTenant.create(http, "expiry-run");
    UUID endpoint = tenant.register(http, HOOK, "*");
    insertDeliveriesPastTheirBudget(tenant, endpoint, 2 * BATCH + 1);
    double expiredBefore = deliveriesExpired();

    expiry.run();

    assertThat(deliveriesIn("dead", tenant)).isEqualTo(2 * BATCH + 1);
    assertThat(deliveriesIn("pending", tenant)).isZero();
    assertThat(deliveriesExpired() - expiredBefore).isEqualTo(2 * BATCH + 1);
  }

  private double deliveriesExpired() {
    return meters
        .get("webhooks.delivery.terminal")
        .tag("state", "dead")
        .tag("reason", "expired")
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
