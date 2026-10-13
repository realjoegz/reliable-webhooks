package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class FinalizeRetryIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";

  // A piece of the statement that fences and records, as the database shows it: with the
  // numbered parameters of the driver.
  private static final String FENCE_AND_CHANGE_STATE = "SET state = $1";

  // A transaction that is committing, as the database shows it.
  private static final String COMMIT = "COMMIT";

  // The database runs a deferred trigger as part of the commit. This one waits for a lock that
  // the test holds, so the commit of a transaction that has written an attempt row waits in the
  // database, where the test sees it.
  private static final long LOCK_OF_THE_TEST = 5508;
  private static final String CREATE_FUNCTION_THAT_WAITS =
      """
      CREATE FUNCTION wait_for_the_test() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM pg_advisory_xact_lock(%d);
        RETURN NULL;
      END $$
      """
          .formatted(LOCK_OF_THE_TEST);
  private static final String CREATE_TRIGGER_AT_COMMIT =
      """
      CREATE CONSTRAINT TRIGGER commit_waits_for_the_test AFTER INSERT ON delivery_attempt
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wait_for_the_test()
      """;
  private static final String DROP_TRIGGER =
      "DROP TRIGGER IF EXISTS commit_waits_for_the_test ON delivery_attempt";
  private static final String DROP_FUNCTION = "DROP FUNCTION IF EXISTS wait_for_the_test()";

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired MeterRegistry meters;

  // The test holds the delivery row, so the first statement of the finalize transaction waits.
  // Then it ends the backend of that statement, which is what a database that shuts down does to
  // every session. The statement fails, and so does the rollback that follows it on the same
  // connection. What the transaction throws is the failure of the rollback, which names no
  // SQLSTATE. The worker has to see the failure of the statement behind it and try again. A worker
  // that took it for a fault would leave the delivery to its lease, and the delivery would be
  // sent a second time.
  @Test
  void finalizeWhoseConnectionTheDatabaseEndedIsTriedAgainAndRecordedOnce() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold request = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer("/hook", request);
      TestTenant tenant = TestTenant.create(http, "finalize-on-ended-connection");
      tenant.register(http, receiver.url("/hook"), "*");
      double errorsBefore = finalizeErrors();
      worker.start();
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await().until(() -> request.waiting() == 1);

      try (Connection blocker = TestDatabase.connection();
          PreparedStatement lock =
              blocker.prepareStatement("SELECT 1 FROM delivery WHERE id = ? FOR UPDATE")) {
        lock.setObject(1, delivery);
        lock.execute();
        request.release();
        PgWait.untilBlocked(FENCE_AND_CHANGE_STATE, ReplicaSettings.IN_PROCESS_REPLICA);
        assertThat(PgWait.endBlocked(FENCE_AND_CHANGE_STATE, ReplicaSettings.IN_PROCESS_REPLICA))
            .isEqualTo(1);
        blocker.rollback();
      }

      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
      assertThat(Rows.attempts(delivery)).containsExactly(Rows.Attempt.answered(1, 200));
      assertThat(receiver.requests()).hasSize(1);
      assertThat(finalizeErrors() - errorsBefore).isZero();
    }
  }

  // The statements of the finalize succeed, and its commit waits in the database for the lock of
  // the test. The test ends the backend then, as a database that shuts down does. The commit
  // fails and nothing of the transaction is kept. The pool closes the connection, so the rollback
  // that follows fails as well, and what the transaction throws is the failure of that rollback:
  // it names no SQLSTATE, and no failure of a statement is beside it. The worker has to take that
  // for a connection that failed and try again. The next try finds the lease as the claim left it
  // and records the result. A worker that took the failure for a fault would leave the delivery
  // to its lease, and the delivery would be sent a second time.
  @Test
  void finalizeWhoseCommitTheDatabaseEndedIsTriedAgainAndRecordedOnce() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold request = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer("/hook", request);
      TestTenant tenant = TestTenant.create(http, "finalize-commit-on-ended-connection");
      tenant.register(http, receiver.url("/hook"), "*");
      double errorsBefore = finalizeErrors();
      double lostLeasesBefore = finalizeLostLeases();
      worker.start();
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await().until(() -> request.waiting() == 1);

      try {
        TestDatabase.jdbc().sql(CREATE_FUNCTION_THAT_WAITS).update();
        TestDatabase.jdbc().sql(CREATE_TRIGGER_AT_COMMIT).update();
        try (Connection blocker = TestDatabase.connection();
            Statement lock = blocker.createStatement()) {
          lock.execute("SELECT pg_advisory_xact_lock(%d)".formatted(LOCK_OF_THE_TEST));
          request.release();
          PgWait.untilBlocked(COMMIT, ReplicaSettings.IN_PROCESS_REPLICA);
          assertThat(PgWait.endBlocked(COMMIT, ReplicaSettings.IN_PROCESS_REPLICA)).isEqualTo(1);
          blocker.rollback();
        }
        await()
            .untilAsserted(
                () -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
      } finally {
        // The lock of the test is free by now, so no commit waits while the trigger is removed.
        TestDatabase.jdbc().sql(DROP_TRIGGER).update();
        TestDatabase.jdbc().sql(DROP_FUNCTION).update();
      }

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
      assertThat(Rows.attempts(delivery)).containsExactly(Rows.Attempt.answered(1, 200));
      assertThat(receiver.requests()).hasSize(1);
      assertThat(finalizeErrors() - errorsBefore).isZero();
      // The commit that failed recorded nothing: the try after it still held the lease.
      assertThat(finalizeLostLeases() - lostLeasesBefore).isZero();
    }
  }

  private double finalizeErrors() {
    return meters.get("webhooks.finalize.errors").counter().count();
  }

  private double finalizeLostLeases() {
    return meters.get("webhooks.finalize.lost.lease").counter().count();
  }
}
