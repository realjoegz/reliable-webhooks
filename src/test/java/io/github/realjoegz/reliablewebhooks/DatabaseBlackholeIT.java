package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaProcess;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TcpProxy;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * The connections of one replica to the database go half-open: nothing arrives on them any more,
 * and nothing closes them. That replica runs as a process of its own and reaches the database
 * through a proxy that the test turns into a blackhole. In both tests the replica is first seen
 * inside a statement, so the cut finds it waiting for an answer that will not come.
 *
 * <p>The replica inside the test JVM only takes the requests that set a test up.
 */
@UnsafeLocalReplica
@Tag("process")
class DatabaseBlackholeIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String CUT_OFF = "replica-that-is-cut-off";
  private static final String OTHER = "replica-that-is-not-cut-off";
  private static final Duration AN_HOUR = Duration.ofHours(1);
  private static final Duration PAST_THE_LEASE = Duration.ofSeconds(61);
  private static final int SIGTERM = 15;
  private static final Duration DRAIN_TIME = Duration.ofSeconds(20);

  // The socket read of the worker pool gives up after 15 s. After that the poller tests the
  // connections that the cut left in its pool, three in each borrow wait, before it gets to one
  // that was opened after the heal.
  private static final Duration SOCKET_TIMEOUT_AND_RECOVERY = Duration.ofMinutes(2);

  // A piece of the claim and of the statement that fences and records, as the database shows
  // them: with the numbered parameters of the driver.
  private static final String CLAIM = "WITH eligible AS MATERIALIZED";
  private static final String FENCE_AND_CHANGE_STATE = "SET state = $1";
  private static final String CLAIM_FAILED = "\"message\":\"claim_failed\"";
  private static final String READ_TIMED_OUT = "java.net.SocketTimeoutException: Read timed out";

  @Autowired RestTestClient http;

  // The test locks the delivery table, which stops the claim of the poller, and cuts the replica
  // off once that claim is seen waiting. The poller has one thread, and that thread now reads
  // from a socket that stays silent. A second replica delivers meanwhile.
  //
  // Then the proxy relays again, for new connections only, and the second replica is stopped. A
  // delivery that was postponed until now is made due. The replica that was cut off is the only
  // one left to deliver it, and can do so only if the read of its poller has ended: without the
  // socket timeout it would wait for ever.
  @Test
  void pollerResumesAfterSocketTimeout() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        TcpProxy proxy = TcpProxy.toDatabase()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "silent-socket");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID later = Rows.deliveryOf(tenant.accept(http, EVENT));
      Rows.postponeDelivery(later, AN_HOUR);

      try (ReplicaProcess cutOff = ReplicaProcess.startBehind(proxy, CUT_OFF);
          ReplicaProcess other = ReplicaProcess.start(OTHER)) {
        try (Connection blocker = TestDatabase.connection();
            Statement lock = blocker.createStatement()) {
          lock.execute("LOCK TABLE delivery IN ACCESS EXCLUSIVE MODE");
          PgWait.untilBlocked(CLAIM, CUT_OFF);
          proxy.blackhole();
          blocker.rollback();
        }

        UUID meanwhile = Rows.deliveryOf(tenant.accept(http, EVENT));
        await()
            .untilAsserted(
                () -> assertThat(Rows.delivery(meanwhile).state()).isEqualTo("succeeded"));
        assertThat(Rows.attempts(meanwhile)).containsExactly(successBy(OTHER, 1));

        proxy.heal();
        other.terminate();
        assertThat(other.exitCode(DRAIN_TIME)).isEqualTo(128 + SIGTERM);
        Rows.ageDelivery(later, AN_HOUR);

        await()
            .atMost(SOCKET_TIMEOUT_AND_RECOVERY)
            .untilAsserted(() -> assertThat(Rows.delivery(later).state()).isEqualTo("succeeded"));
        assertThat(Rows.attempts(later)).containsExactly(successBy(CUT_OFF, 1));
        assertThat(cutOff.output()).contains(CLAIM_FAILED).contains(READ_TIMED_OUT);
      }
    }
  }

  // The replica is seen waiting for the delivery row in the first statement of its finalize
  // transaction, as in the kill after the response. Here it is not killed but cut off. When the
  // test lets go of the row, the statement gets it, and the backend then waits inside its
  // transaction for a client that it will not hear from again, holding the row.
  //
  // Nothing closes that connection, and the replica is alive. The transaction can therefore only
  // have ended because the server ended it. Once it has, the lease is aged, and a second replica
  // claims the delivery: no lock is left that its claim would pass over.
  @Test
  void openTransactionIsEndedByServer() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold firstRequest = FaultReceiver.hold(FaultReceiver.status(200));
        TcpProxy proxy = TcpProxy.toDatabase();
        ReplicaProcess cutOff = ReplicaProcess.startBehind(proxy, CUT_OFF)) {
      receiver.answerOnce("/hook", firstRequest);
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "open-transaction");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await().until(() -> firstRequest.waiting() == 1);

      try (Connection blocker = TestDatabase.connection();
          PreparedStatement lock =
              blocker.prepareStatement("SELECT 1 FROM delivery WHERE id = ? FOR UPDATE")) {
        lock.setObject(1, delivery);
        lock.execute();
        firstRequest.release();
        PgWait.untilBlocked(FENCE_AND_CHANGE_STATE, CUT_OFF);
        proxy.blackhole();
        blocker.rollback();
      }
      PgWait.untilNoTransaction(CUT_OFF);

      assertThat(cutOff.isAlive()).isTrue();
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 1, 0, true, null, false));
      assertThat(Rows.attempts(delivery)).isEmpty();

      try (ReplicaProcess other = ReplicaProcess.start(OTHER)) {
        Rows.ageDelivery(delivery, PAST_THE_LEASE);
        await()
            .untilAsserted(
                () -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
      }

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 2, 0, false, null, true));
      assertThat(Rows.attempts(delivery))
          .containsExactly(
              new Rows.Attempt(0, 1, false, "unknown", null, "lease_expired", null, null),
              successBy(OTHER, 2));
      assertThat(receiver.requests())
          .extracting(request -> request.header("webhook-id"))
          .containsExactly(delivery.toString(), delivery.toString());
    }
  }

  private static Rows.Attempt successBy(String replica, int attemptNo) {
    return new Rows.Attempt(0, attemptNo, false, "success", 200, null, "127.0.0.1", replica);
  }
}
