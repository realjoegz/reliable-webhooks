package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.DatabaseFailures;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.delivery.CircuitBreaker.Verdict;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Attempt;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.NextState;
import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class LeaseFenceIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final Duration PAST_THE_LEASE = Duration.ofSeconds(61);
  private static final UUID[] NONE = {};

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired MeterRegistry meters;
  @Autowired DeliveryRepository deliveries;
  @Autowired WebhooksProperties properties;

  // The receiver holds the first request, and the lease of its attempt is aged past its end. The
  // delivery is claimed again and delivered by a second request while the first is still open.
  // Then the first request is answered with a 200 as well. Its attempt holds a token that is no
  // longer the lease, and the counter shows that its finalize ran and was turned away.
  @Test
  void lateFinalizeAfterReclaimChangesNothing() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold firstRequest = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answerOnce("/hook", firstRequest);
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "late-finalize");
      tenant.register(http, receiver.url("/hook"), "*");
      double lostLeasesBefore = lostLeases();
      worker.start();

      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await().until(() -> firstRequest.waiting() == 1);
      Rows.ageDelivery(delivery, PAST_THE_LEASE);
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 2, 0, false, null, true));
      assertThat(Rows.attempts(delivery))
          .containsExactly(
              new Rows.Attempt(0, 1, false, "unknown", null, "lease_expired", null, null),
              new Rows.Attempt(
                  0,
                  2,
                  false,
                  "success",
                  200,
                  null,
                  "127.0.0.1",
                  ReplicaSettings.IN_PROCESS_REPLICA));
      List<String> recorded = everyColumnOf(delivery);

      firstRequest.release();
      await().untilAsserted(() -> assertThat(lostLeases() - lostLeasesBefore).isEqualTo(1));

      assertThat(everyColumnOf(delivery)).isEqualTo(recorded);
      assertThat(receiver.requests())
          .extracting(request -> request.header("webhook-id"))
          .containsExactly(delivery.toString(), delivery.toString());
    }
  }

  // The second finalize is what a retry does after a commit whose answer was lost. The others
  // show that the token is spent for every result, not only for the one that was recorded. They
  // bring failures, which would count toward opening the circuit, and the circuit row is among
  // the rows that stay as they are.
  @Test
  void repeatedFinalizeIsNoOp() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "repeated-finalize");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      Claimed claimed = claimOnly(delivery);
      Attempt success =
          new Attempt(
              "success", 200, null, InetAddress.getByName("127.0.0.1"), Duration.ofMillis(120));
      Attempt failure = new Attempt("failure", 500, null, null, Duration.ofMillis(80));

      assertThat(
              deliveries.recordAttempt(
                  claimed, NextState.succeeded(), success, Verdict.SUCCESS, "replica-1"))
          .isTrue();

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
      assertThat(Rows.attempts(delivery))
          .containsExactly(
              new Rows.Attempt(0, 1, false, "success", 200, null, "127.0.0.1", "replica-1"));
      List<String> recorded = everyColumnOf(delivery);

      assertThat(
              deliveries.recordAttempt(
                  claimed, NextState.succeeded(), success, Verdict.SUCCESS, "replica-1"))
          .isFalse();
      assertThat(
              deliveries.recordAttempt(
                  claimed,
                  NextState.retryIn(Duration.ofSeconds(10)),
                  failure,
                  Verdict.FAILURE,
                  "replica-2"))
          .isFalse();
      assertThat(
              deliveries.recordAttempt(
                  claimed, NextState.exhausted(), failure, Verdict.FAILURE, "replica-2"))
          .isFalse();
      assertThat(deliveries.deadLetterExhausted(claimed)).isFalse();

      assertThat(everyColumnOf(delivery)).isEqualTo(recorded);
    }
  }

  // The state change and the attempt row are one transaction. The test has put a row where the
  // attempt row belongs, so the insert fails after the state change has run. The delivery is
  // then as it was: not succeeded, and still under its lease. Two statements that each committed
  // by themselves would leave it succeeded without the row of the attempt.
  @Test
  void finalizeWhoseAttemptRowIsRefusedChangesNothing() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "refused-attempt-row");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      Claimed claimed = claimOnly(delivery);
      TestDatabase.jdbc()
          .sql(
              """
              INSERT INTO delivery_attempt (delivery_id, run, attempt_no, outcome)
              VALUES (:id, 0, 1, 'unknown')
              """)
          .param("id", delivery)
          .update();
      List<String> leased = everyColumnOf(delivery);

      assertThatThrownBy(
              () ->
                  deliveries.recordAttempt(
                      claimed,
                      NextState.succeeded(),
                      new Attempt("success", 200, null, null, Duration.ZERO),
                      Verdict.SUCCESS,
                      "replica-1"))
          .isInstanceOf(DuplicateKeyException.class);

      assertThat(everyColumnOf(delivery)).isEqualTo(leased);
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 1, 0, true, null, false));
    }
  }

  // The circuit statement is the last one of the finalize transaction. The test holds the circuit
  // row, so the statement waits for it until the lock timeout of the worker pool ends the wait.
  // By then the state change and the attempt row have been written, and both are undone: the
  // delivery is still under its lease and has no attempt row. An attempt is recorded and counted
  // together, or not at all.
  //
  // The failure is one that can pass, so a worker would try the finalize again. The test does,
  // once it has let go of the row, and the failure is recorded and counted.
  @Test
  void finalizeWhoseCircuitStatementFailsChangesNothing() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "blocked-circuit-row");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      Claimed claimed = claimOnly(delivery);
      Attempt failure = new Attempt("failure", 500, null, null, Duration.ofMillis(80));
      NextState retry = NextState.retryIn(Duration.ofSeconds(10));
      List<String> leased = everyColumnOf(delivery);

      try (Connection blocker = TestDatabase.connection();
          PreparedStatement lock =
              blocker.prepareStatement(
                  "SELECT 1 FROM endpoint_circuit WHERE endpoint_id = ? FOR UPDATE")) {
        lock.setObject(1, endpoint);
        lock.execute();

        assertThatThrownBy(
                () ->
                    deliveries.recordAttempt(claimed, retry, failure, Verdict.FAILURE, "replica-1"))
            .matches(DatabaseFailures::isTransient, "is a failure that can pass");

        blocker.rollback();
      }
      assertThat(everyColumnOf(delivery)).isEqualTo(leased);
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 1, 0, true, null, false));

      assertThat(deliveries.recordAttempt(claimed, retry, failure, Verdict.FAILURE, "replica-1"))
          .isTrue();
      assertThat(Rows.attempts(delivery))
          .containsExactly(new Rows.Attempt(0, 1, false, "failure", 500, null, null, "replica-1"));
      assertThat(Rows.circuit(endpoint)).isEqualTo(new Rows.Circuit("closed", 1, 0, false, false));
    }
  }

  // A release is fenced like a finalize. The first holder gives its row back after another claim
  // has taken it over, and the row stays with its second holder.
  @Test
  void releaseAfterReclaimChangesNothing() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "late-release");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      Claimed firstHolder = claimOnly(delivery);
      Rows.ageDelivery(delivery, PAST_THE_LEASE);
      Claimed secondHolder = claimOnly(delivery);
      List<String> reclaimed = everyColumnOf(delivery);

      deliveries.release(firstHolder, Duration.ZERO);

      assertThat(everyColumnOf(delivery)).isEqualTo(reclaimed);
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 2, 0, true, null, false));
      assertThat(
              deliveries.recordAttempt(
                  secondHolder,
                  NextState.succeeded(),
                  new Attempt("success", 204, null, null, Duration.ZERO),
                  Verdict.SUCCESS,
                  "replica-2"))
          .isTrue();
    }
  }

  private Claimed claimOnly(UUID delivery) {
    List<Claimed> claimed = deliveries.claim(NONE, NONE, 1, properties.delivery().lease());
    assertThat(claimed).extracting(Claimed::id).containsExactly(delivery);
    return claimed.getFirst();
  }

  private double lostLeases() {
    return meters.get("webhooks.finalize.lost.lease").counter().count();
  }

  /**
   * The delivery row, the circuit row of its endpoint and its attempt rows, each as the text of all
   * of its columns.
   */
  private static List<String> everyColumnOf(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT d::text FROM delivery d WHERE d.id = :id
            UNION ALL
            SELECT c::text FROM endpoint_circuit c JOIN delivery d ON d.endpoint_id = c.endpoint_id
            WHERE d.id = :id
            UNION ALL
            (SELECT a::text FROM delivery_attempt a WHERE a.delivery_id = :id ORDER BY a.id)
            """)
        .param("id", deliveryId)
        .query(String.class)
        .list();
  }
}
