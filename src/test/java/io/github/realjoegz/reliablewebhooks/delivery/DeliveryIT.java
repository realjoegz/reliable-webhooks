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
import io.github.realjoegz.reliablewebhooks.http.ReferenceVerifier;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class DeliveryIT {

  private static final String ORDER_CREATED =
      """
      {"type":"order.created","data":{"order":17}}""";

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired MeterRegistry meters;

  // The timestamp of the request is compared with the database clock, read before the event is
  // sent and after its delivery has succeeded. The clock of this JVM plays no part.
  @Test
  void acceptedEventIsDeliveredWithOneSignedRequestAndRecordedOnce() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "plain-delivery");
      TestTenant.Endpoint endpoint =
          tenant.registerForSecret(http, receiver.url("/hook"), "order.created");
      double countedBefore = successfulAttempts();
      long before = databaseClock();
      worker.start();

      UUID event = tenant.accept(http, ORDER_CREATED);
      UUID delivery = Rows.deliveryOf(event);

      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
      long after = databaseClock();

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
      assertThat(Rows.attempts(delivery))
          .containsExactly(
              new Rows.Attempt(
                  0,
                  1,
                  false,
                  "success",
                  200,
                  null,
                  "127.0.0.1",
                  ReplicaSettings.IN_PROCESS_REPLICA));
      // The worker counts the attempt once the commit that made it visible has returned to it.
      await().untilAsserted(() -> assertThat(successfulAttempts() - countedBefore).isEqualTo(1));

      List<FaultReceiver.Request> requests = receiver.requests();
      assertThat(requests).hasSize(1);
      FaultReceiver.Request request = requests.getFirst();
      assertThat(request.method()).isEqualTo("POST");
      assertThat(request.path()).isEqualTo("/hook");
      assertThat(request.header("content-type")).isEqualTo("application/json");
      assertThat(request.body()).isEqualTo(storedBodyOf(event));
      assertThat(request.header("webhook-id")).isEqualTo(delivery.toString());
      long signedAt = Long.parseLong(request.header("webhook-timestamp"));
      assertThat(signedAt).isBetween(before, after);
      assertThat(
              ReferenceVerifier.isGenuine(
                  endpoint.secret(),
                  request.header("webhook-id"),
                  request.header("webhook-timestamp"),
                  request.header("webhook-signature"),
                  request.body(),
                  Instant.ofEpochSecond(signedAt)))
          .isTrue();

      http.get()
          .uri("/v1/events/{id}", event)
          .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
          .exchange()
          .expectBody()
          .jsonPath("$.deliveries[0].id")
          .isEqualTo(delivery.toString())
          .jsonPath("$.deliveries[0].state")
          .isEqualTo("succeeded")
          .jsonPath("$.deliveries[0].attempts")
          .isEqualTo(1);
    }
  }

  private double successfulAttempts() {
    return meters
        .get("webhooks.delivery.attempts")
        .tag("outcome", "success")
        .tag("reason", "http_2xx")
        .counter()
        .count();
  }

  private static long databaseClock() {
    return TestDatabase.jdbc()
        .sql("SELECT extract(epoch FROM now())::bigint")
        .query(Long.class)
        .single();
  }

  private static byte[] storedBodyOf(UUID eventId) {
    return TestDatabase.jdbc()
        .sql("SELECT body FROM event WHERE id = :id")
        .param("id", eventId)
        .query(byte[].class)
        .single();
  }
}
