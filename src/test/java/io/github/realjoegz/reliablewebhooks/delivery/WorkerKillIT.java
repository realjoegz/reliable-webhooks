package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaProcess;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
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
 * A replica that runs as a process of its own is killed with SIGKILL at a chosen point of an
 * attempt. A second replica, started after the kill, then takes the delivery over. The replica
 * inside the test JVM only takes the requests that set a test up. Its worker does not run, so the
 * delivery of a test is claimed by the replica that the test kills and by no other.
 *
 * <p>No kill is landed by timing. Either the receiver holds the request, or the test holds a lock
 * that the next statement of the replica needs and kills once the database shows the replica
 * waiting for it.
 */
@UnsafeLocalReplica
@Tag("process")
class WorkerKillIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String KILLED = "replica-that-is-killed";
  private static final String SURVIVOR = "replica-that-survives";
  private static final Duration AN_HOUR = Duration.ofHours(1);
  private static final Duration PAST_THE_LEASE = Duration.ofSeconds(61);

  // A piece of the load and of the statement that fences and records, as the database shows
  // them: with the numbered parameters of the driver.
  private static final String LOAD = "JOIN event ev";
  private static final String FENCE_AND_CHANGE_STATE = "SET state = $1";

  private static final Rows.Delivery LEASED_ONCE =
      new Rows.Delivery("pending", 1, 0, true, null, false);
  private static final Rows.Delivery SUCCEEDED_ON_THE_SECOND_ATTEMPT =
      new Rows.Delivery("succeeded", 2, 0, false, null, true);

  @Autowired RestTestClient http;

  // The delivery is accepted while no worker runs and is then moved an hour ahead, so the replica
  // finds nothing due when it starts. The test locks the event table, which the claim does not
  // read and the load does, and only then makes the delivery due. The replica claims it and is
  // seen waiting in the load. That is a read, so the backend that outlives the kill has nothing
  // to commit.
  @Test
  void killedAfterClaimBeforeSend() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "killed-after-claim");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      Rows.postponeDelivery(delivery, AN_HOUR);

      try (ReplicaProcess killed = ReplicaProcess.start(KILLED);
          Connection blocker = TestDatabase.connection();
          Statement lock = blocker.createStatement()) {
        lock.execute("LOCK TABLE event IN ACCESS EXCLUSIVE MODE");
        Rows.ageDelivery(delivery, AN_HOUR);
        PgWait.untilBlocked(LOAD, KILLED);

        killed.kill();
        blocker.rollback();
      }
      PgWait.untilGone(KILLED);

      assertThat(Rows.delivery(delivery)).isEqualTo(LEASED_ONCE);
      assertThat(Rows.attempts(delivery)).isEmpty();
      assertThat(receiver.requests()).isEmpty();

      deliverWithAReplicaStartedNow(delivery);

      assertThat(receiver.requests())
          .extracting(request -> request.header("webhook-id"))
          .containsExactly(delivery.toString());
    }
  }

  // The receiver holds the request of the first attempt, so the replica is killed while that
  // request is open. The request arrived, and nobody will ever know what became of it: the
  // attempt is logged as unknown, and the delivery is sent again with the same id.
  @Test
  void killedMidRequest() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold firstRequest = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answerOnce("/hook", firstRequest);
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "killed-mid-request");
      tenant.register(http, receiver.url("/hook"), "*");

      UUID delivery;
      try (ReplicaProcess killed = ReplicaProcess.start(KILLED)) {
        delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
        await().until(() -> firstRequest.waiting() == 1);

        killed.kill();
      }

      assertThat(Rows.delivery(delivery)).isEqualTo(LEASED_ONCE);
      assertThat(Rows.attempts(delivery)).isEmpty();

      deliverWithAReplicaStartedNow(delivery);

      assertThat(receiver.requests())
          .extracting(request -> request.header("webhook-id"))
          .containsExactly(delivery.toString(), delivery.toString());
    }
  }

  // While the receiver holds the request, the test locks the delivery row. Then the receiver
  // answers 200, and the replica is seen waiting for the row in the first statement of its
  // finalize transaction. It is killed there. The backend that outlives it gets the row when the
  // test lets go, finds nobody to answer and rolls back, so the success that the subscriber gave
  // is recorded nowhere, and the delivery is sent a second time.
  @Test
  void killedAfterResponseBeforeCommit() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold firstRequest = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answerOnce("/hook", firstRequest);
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "killed-before-commit");
      tenant.register(http, receiver.url("/hook"), "*");

      UUID delivery;
      try (ReplicaProcess killed = ReplicaProcess.start(KILLED);
          Connection blocker = TestDatabase.connection();
          PreparedStatement lock =
              blocker.prepareStatement("SELECT 1 FROM delivery WHERE id = ? FOR UPDATE")) {
        delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
        await().until(() -> firstRequest.waiting() == 1);
        lock.setObject(1, delivery);
        lock.execute();
        firstRequest.release();
        PgWait.untilBlocked(FENCE_AND_CHANGE_STATE, KILLED);

        killed.kill();
        blocker.rollback();
      }
      PgWait.untilGone(KILLED);

      assertThat(Rows.delivery(delivery)).isEqualTo(LEASED_ONCE);
      assertThat(Rows.attempts(delivery)).isEmpty();
      assertThat(receiver.requests()).hasSize(1);

      deliverWithAReplicaStartedNow(delivery);

      assertThat(receiver.requests())
          .extracting(request -> request.header("webhook-id"))
          .containsExactly(delivery.toString(), delivery.toString());
    }
  }

  // The lease that the dead replica took is aged past its end. The replica that claims the
  // delivery then writes the attempt row that the dead one did not, and delivers.
  private static void deliverWithAReplicaStartedNow(UUID delivery) {
    try (ReplicaProcess survivor = ReplicaProcess.start(SURVIVOR)) {
      Rows.ageDelivery(delivery, PAST_THE_LEASE);
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
    }
    assertThat(Rows.delivery(delivery)).isEqualTo(SUCCEEDED_ON_THE_SECOND_ATTEMPT);
    assertThat(Rows.attempts(delivery))
        .containsExactly(
            new Rows.Attempt(0, 1, false, "unknown", null, "lease_expired", null, null),
            new Rows.Attempt(0, 2, false, "success", 200, null, "127.0.0.1", SURVIVOR));
  }
}
