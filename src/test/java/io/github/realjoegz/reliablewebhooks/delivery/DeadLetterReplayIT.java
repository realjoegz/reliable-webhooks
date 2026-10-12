package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class DeadLetterReplayIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int MAX_ATTEMPTS = 12;
  private static final int SUCCESSES_IN_BETWEEN = MAX_ATTEMPTS - 1;
  private static final Duration SPREAD_OF_A_REPLAY = Duration.ofSeconds(60);
  private static final Duration BUDGET = Duration.ofHours(72);
  private static final Duration AN_HOUR = Duration.ofHours(1);

  /** When a delivery is due next and when it expires. */
  private record Times(Instant nextAttemptAt, Instant expiresAt) {}

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;

  // The endpoint is healthy and rejects one event: it answers 500 to every request of that
  // delivery, and 200 to another event that is sent to it after each of those attempts. The
  // circuit breaker runs as it ships. Each success starts its count of failures over, so the
  // circuit is closed with one failure counted after every attempt, and none of the twelve is a
  // probe. Without the successes in between it would open at the fifth.
  //
  // After each of the first eleven attempts the test reads the wait that the finalize wrote,
  // which has to lie in the window of that attempt. Only then does it move the delivery: an hour
  // ahead while the other event is delivered, and then back by that hour and by the longest wait
  // of the window, which makes it due. The schedule itself is never shortened: the worker runs
  // with the retry settings that ship.
  @Test
  void exhaustedAfterTwelveAttempts() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "always-rejected");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      worker.start();

      receiver.answerOnce("/hook", FaultReceiver.status(500));
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      for (int attempt = 1; attempt < MAX_ATTEMPTS; attempt++) {
        int recorded = attempt;
        await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(recorded));

        assertThat(Rows.delivery(delivery))
            .isEqualTo(new Rows.Delivery("pending", attempt, 0, false, null, false));
        assertThat(Rows.waitAfter(delivery, attempt))
            .as("wait after attempt %d", attempt)
            .isBetween(RetrySchedule.delay(attempt, 0), RetrySchedule.delay(attempt, 1));
        assertThat(Rows.circuit(endpoint))
            .isEqualTo(new Rows.Circuit("closed", 1, 0, false, false));
        deliverAnotherEventMeanwhile(tenant, delivery);
        assertThat(Rows.circuit(endpoint)).isEqualTo(Rows.Circuit.CLOSED);
        rejectTheNextAttempt(receiver, delivery, attempt);
      }
      await().untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("dead"));

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("dead", MAX_ATTEMPTS, 0, false, "exhausted", true));
      List<Rows.Attempt> twelveFailures = new ArrayList<>();
      for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        twelveFailures.add(Rows.Attempt.answered(attempt, 500));
      }
      assertThat(Rows.attempts(delivery)).isEqualTo(twelveFailures);
      assertThat(Rows.circuit(endpoint)).isEqualTo(new Rows.Circuit("closed", 1, 0, false, false));
      assertThat(requestsFor(delivery, receiver)).isEqualTo(MAX_ATTEMPTS);
      assertThat(receiver.requests()).hasSize(MAX_ATTEMPTS + SUCCESSES_IN_BETWEEN);
    }
  }

  // The endpoint rejects the delivery until it is dead, as in the test above, and answers 200 to
  // every request from then on. The replay makes the delivery pending in a new run: no attempt
  // counted, due within sixty seconds and with seventy-two hours of its own. The twelve rows of
  // the first run stay.
  //
  // The worker is stopped while the delivery is replayed, so that the row which the test reads
  // is the row that the replay wrote. Once the delivery is aged past the sixty seconds, the
  // worker delivers it with one request, recorded as attempt 1 of run 1 and sent under the same
  // webhook-id as the twelve before it.
  //
  // That no second request follows needs a witness. Another event for the same endpoint is
  // accepted after the delivery has succeeded and is delivered, so the worker went on claiming.
  // A delivery that has succeeded is not replayed.
  @Test
  void replayedDeliveryIsDeliveredOnce() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "replayed");
      tenant.register(http, receiver.url("/hook"), "*");
      worker.start();
      receiver.answerOnce("/hook", FaultReceiver.status(500));
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      for (int attempt = 1; attempt < MAX_ATTEMPTS; attempt++) {
        int recorded = attempt;
        await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(recorded));
        deliverAnotherEventMeanwhile(tenant, delivery);
        rejectTheNextAttempt(receiver, delivery, attempt);
      }
      await().untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("dead"));
      worker.stop();
      List<Rows.Attempt> firstRun = Rows.attempts(delivery);
      assertThat(firstRun).hasSize(MAX_ATTEMPTS);

      // The replay reads the database clock between these two readings of it.
      Instant before = databaseClock();
      tenant.replay(http, delivery).expectStatus().isNoContent().expectBody().isEmpty();
      Instant after = databaseClock();

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 0, 1, false, null, false));
      Times rearmed = timesOf(delivery);
      assertThat(rearmed.nextAttemptAt()).isBetween(before, after.plus(SPREAD_OF_A_REPLAY));
      assertThat(rearmed.expiresAt()).isBetween(before.plus(BUDGET), after.plus(BUDGET));
      assertThat(Rows.attempts(delivery)).isEqualTo(firstRun);
      tenant.replay(http, delivery).expectStatus().isEqualTo(HttpStatus.CONFLICT);
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 0, 1, false, null, false));

      Rows.ageDelivery(delivery, SPREAD_OF_A_REPLAY);
      worker.start();
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));

      Rows.Delivery delivered = new Rows.Delivery("succeeded", 1, 1, false, null, true);
      List<Rows.Attempt> bothRuns = new ArrayList<>(firstRun);
      bothRuns.add(
          new Rows.Attempt(
              1, 1, false, "success", 200, null, "127.0.0.1", ReplicaSettings.IN_PROCESS_REPLICA));
      assertThat(Rows.delivery(delivery)).isEqualTo(delivered);
      assertThat(Rows.attempts(delivery)).isEqualTo(bothRuns);

      deliverAnotherEvent(tenant);
      assertThat(requestsFor(delivery, receiver)).isEqualTo(MAX_ATTEMPTS + 1);
      assertThat(receiver.requests()).hasSize(MAX_ATTEMPTS + SUCCESSES_IN_BETWEEN + 2);

      tenant.replay(http, delivery).expectStatus().isEqualTo(HttpStatus.CONFLICT);
      assertThat(Rows.delivery(delivery)).isEqualTo(delivered);
      assertThat(Rows.attempts(delivery)).isEqualTo(bothRuns);
    }
  }

  // What makes the endpoint a healthy one between two attempts of the delivery that it rejects:
  // another event for it is delivered. The rejected delivery is moved an hour ahead first, so
  // that it cannot come due meanwhile and take the 200, however long the other event takes.
  private void deliverAnotherEventMeanwhile(TestTenant tenant, UUID rejected) {
    Rows.postponeDelivery(rejected, AN_HOUR);
    deliverAnotherEvent(tenant);
  }

  private void deliverAnotherEvent(TestTenant tenant) {
    UUID other = Rows.deliveryOf(tenant.accept(http, EVENT));
    await().untilAsserted(() -> assertThat(Rows.delivery(other).state()).isEqualTo("succeeded"));
  }

  // The next request to the endpoint is answered with a 500. The rejected delivery is the only
  // one of the endpoint that is pending, and it is made due: aged by the hour that it was moved
  // ahead and by the longest wait after the attempt that it has just made.
  private static void rejectTheNextAttempt(FaultReceiver receiver, UUID rejected, int attempt) {
    receiver.answerOnce("/hook", FaultReceiver.status(500));
    Rows.ageDelivery(rejected, AN_HOUR.plus(RetrySchedule.delay(attempt, 1)));
  }

  private static long requestsFor(UUID delivery, FaultReceiver receiver) {
    return receiver.requests().stream()
        .filter(request -> request.header("webhook-id").equals(delivery.toString()))
        .count();
  }

  private static Instant databaseClock() {
    return TestDatabase.jdbc().sql("SELECT now()").query(OffsetDateTime.class).single().toInstant();
  }

  private static Times timesOf(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql("SELECT next_attempt_at, expires_at FROM delivery WHERE id = :id")
        .param("id", deliveryId)
        .query(
            (row, n) ->
                new Times(
                    row.getObject("next_attempt_at", OffsetDateTime.class).toInstant(),
                    row.getObject("expires_at", OffsetDateTime.class).toInstant()))
        .single();
  }
}
