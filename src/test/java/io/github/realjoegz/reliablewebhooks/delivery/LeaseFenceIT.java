package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
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
  // show that the token is spent for every result, not only for the one that was recorded.
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

      assertThat(deliveries.recordAttempt(claimed, NextState.succeeded(), success, "replica-1"))
          .isTrue();

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
      assertThat(Rows.attempts(delivery))
          .containsExactly(
              new Rows.Attempt(0, 1, false, "success", 200, null, "127.0.0.1", "replica-1"));
      List<String> recorded = everyColumnOf(delivery);

      assertThat(deliveries.recordAttempt(claimed, NextState.succeeded(), success, "replica-1"))
          .isFalse();
      assertThat(
              deliveries.recordAttempt(
                  claimed, NextState.retryIn(Duration.ofSeconds(10)), failure, "replica-2"))
          .isFalse();
      assertThat(deliveries.recordAttempt(claimed, NextState.exhausted(), failure, "replica-2"))
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
                      "replica-1"))
          .isInstanceOf(DuplicateKeyException.class);

      assertThat(everyColumnOf(delivery)).isEqualTo(leased);
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 1, 0, true, null, false));
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

  /** The delivery row and its attempt rows, each as the text of all of its columns. */
  private static List<String> everyColumnOf(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT d::text FROM delivery d WHERE d.id = :id
            UNION ALL
            (SELECT a::text FROM delivery_attempt a WHERE a.delivery_id = :id ORDER BY a.id)
            """)
        .param("id", deliveryId)
        .query(String.class)
        .list();
  }
}
