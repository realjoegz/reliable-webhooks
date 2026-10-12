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

  private double finalizeErrors() {
    return meters.get("webhooks.finalize.errors").counter().count();
  }
}
