package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaProcess;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.ExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.json.JsonMapper;

/**
 * The database is stopped and started again under two replicas that run as processes of their own
 * and are not restarted. It is the database of the whole build, so the replicas inside the test JVM
 * lose their connections as well. Their pools replace them when a later test borrows one.
 */
@UnsafeLocalReplica
@Tag("process")
@Tag("dbrestart")
class DatabaseRestartIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String FIRST = "replica-across-restart-1";
  private static final String SECOND = "replica-across-restart-2";
  private static final int MAX_IN_FLIGHT_FOR_AN_ENDPOINT = 4;
  private static final int IN_FLIGHT = 2 * MAX_IN_FLIGHT_FOR_AN_ENDPOINT;
  private static final int QUEUED = 2;

  // A replica gives a finalize up 55 s after the claim of its delivery began. The pools need a
  // borrow wait or two to find out that the database is back.
  private static final Duration RECOVERY = Duration.ofMinutes(1);

  @Autowired RestTestClient http;
  @Autowired JsonMapper json;

  // Ten deliveries are due for one endpoint, and the receiver holds its requests. Each replica
  // has four in flight, which is all that an endpoint may have, so two deliveries are queued:
  // due and unclaimed. Then the database is stopped. The receiver answers while it is down, so
  // eight attempts have a result and no database to record it in. An event that is sent now is
  // refused with a 503.
  //
  // After the database is back, the same request is accepted, the eight results are recorded by
  // the replicas that hold them, and the queued deliveries and the new one are delivered. Every
  // delivery has been sent once and recorded once.
  //
  // The receiver holds the requests until the container has stopped, and a replica cuts a request
  // at the attempt deadline of 15 s. The eight requests have to arrive and the container has to
  // stop inside that time. In a run that took longer the attempts would be recorded as failures,
  // and the test would fail.
  @Test
  void inFlightAndQueuedSurviveRestart() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200));
        ReplicaProcess first = ReplicaProcess.start(FIRST);
        ReplicaProcess second = ReplicaProcess.start(SECOND)) {
      receiver.answer("/hook", hold);
      TestTenant tenant = TestTenant.create(http, "database-restart");
      tenant.register(http, receiver.url("/hook"), "*");
      List<UUID> deliveries = new ArrayList<>();
      for (int i = 0; i < IN_FLIGHT + QUEUED; i++) {
        deliveries.add(Rows.deliveryOf(tenant.accept(first.http(), EVENT)));
      }
      await().until(() -> hold.waiting() == IN_FLIGHT);
      String key = TestTenant.newIdempotencyKey();

      // Every later test of the build needs this database. It is started again also when the
      // request fails, so that such a run shows the one failure and not theirs as well.
      TestDatabase.stop();
      ExchangeResult refused;
      try {
        hold.release();
        refused = tenant.send(first.http(), key, EVENT).returnResult();
      } finally {
        TestDatabase.start();
      }

      assertThat(refused.getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
      assertThat(refused.getResponseHeaders().getContentType())
          .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
      assertThat(refused.getResponseHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
      assertThat(json.readTree(refused.getResponseBodyContent()).get("type").asString())
          .isEqualTo("database_busy");

      // Until its pool has a connection to the database that is back, the replica goes on
      // refusing. It never answers anything else, and what it accepts in the end is a new event.
      List<HttpStatusCode> answers = new ArrayList<>();
      ExchangeResult accepted =
          await()
              .atMost(RECOVERY)
              .until(
                  () -> {
                    ExchangeResult answer = tenant.send(first.http(), key, EVENT).returnResult();
                    answers.add(answer.getStatus());
                    return answer;
                  },
                  answer -> answer.getStatus() != HttpStatus.SERVICE_UNAVAILABLE);
      assertThat(answers.subList(0, answers.size() - 1))
          .allMatch(HttpStatus.SERVICE_UNAVAILABLE::equals);
      assertThat(accepted.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      assertThat(accepted.getResponseHeaders().get("Idempotent-Replay")).isNull();
      deliveries.add(Rows.deliveryOf(eventIdIn(accepted)));
      assertThat(tenant.eventRows()).isEqualTo(deliveries.size());

      await()
          .atMost(RECOVERY)
          .untilAsserted(
              () ->
                  assertThat(deliveries)
                      .allSatisfy(
                          delivery ->
                              assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded")));

      assertThat(deliveries)
          .allSatisfy(
              delivery -> {
                assertThat(Rows.delivery(delivery))
                    .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
                assertThat(Rows.attempts(delivery))
                    .singleElement()
                    .isIn(firstAttemptSucceededFor(FIRST), firstAttemptSucceededFor(SECOND));
              });
      assertThat(receiver.requests())
          .extracting(request -> UUID.fromString(request.header("webhook-id")))
          .containsExactlyInAnyOrderElementsOf(deliveries);
      // Each replica held four results when the database went away, and has recorded them since.
      assertThat(attemptsRecordedBy(FIRST, deliveries))
          .isGreaterThanOrEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      assertThat(attemptsRecordedBy(SECOND, deliveries))
          .isGreaterThanOrEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      assertThat(first.isAlive()).isTrue();
      assertThat(second.isAlive()).isTrue();
    }
  }

  private static Rows.Attempt firstAttemptSucceededFor(String replica) {
    return new Rows.Attempt(0, 1, false, "success", 200, null, "127.0.0.1", replica);
  }

  private UUID eventIdIn(ExchangeResult accepted) {
    return UUID.fromString(json.readTree(accepted.getResponseBodyContent()).get("id").asString());
  }

  private static int attemptsRecordedBy(String replica, List<UUID> deliveries) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM delivery_attempt
            WHERE delivery_id = ANY(:ids) AND replica = :replica
            """)
        .param("ids", deliveries.toArray(UUID[]::new))
        .param("replica", replica)
        .query(Integer.class)
        .single();
  }
}
