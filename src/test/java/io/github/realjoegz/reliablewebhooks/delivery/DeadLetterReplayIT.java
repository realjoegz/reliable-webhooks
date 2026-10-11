package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class DeadLetterReplayIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int MAX_ATTEMPTS = 12;

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;

  // The endpoint answers 500 to every request. After each of the first eleven attempts the test
  // reads the wait that the finalize wrote, which has to lie in the window of that attempt, and
  // only then ages the delivery by the longest wait of the window to make it due. The schedule
  // itself is never shortened: the worker runs with the retry settings that ship.
  @Test
  void exhaustedAfterTwelveAttempts() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "always-rejected");
      tenant.register(http, receiver.url("/hook"), "*");
      worker.start();

      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      for (int attempt = 1; attempt < MAX_ATTEMPTS; attempt++) {
        int recorded = attempt;
        await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(recorded));

        assertThat(Rows.delivery(delivery))
            .isEqualTo(new Rows.Delivery("pending", attempt, 0, false, null, false));
        assertThat(Rows.waitAfter(delivery, attempt))
            .as("wait after attempt %d", attempt)
            .isBetween(RetrySchedule.delay(attempt, 0), RetrySchedule.delay(attempt, 1));
        Rows.ageDelivery(delivery, RetrySchedule.delay(attempt, 1));
      }
      await().untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("dead"));

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("dead", MAX_ATTEMPTS, 0, false, "exhausted", true));
      List<Rows.Attempt> twelveFailures = new ArrayList<>();
      for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        twelveFailures.add(
            new Rows.Attempt(
                0,
                attempt,
                false,
                "failure",
                500,
                null,
                "127.0.0.1",
                ReplicaSettings.IN_PROCESS_REPLICA));
      }
      assertThat(Rows.attempts(delivery)).isEqualTo(twelveFailures);
      assertThat(receiver.requests())
          .hasSize(MAX_ATTEMPTS)
          .allSatisfy(
              request -> assertThat(request.header("webhook-id")).isEqualTo(delivery.toString()));
    }
  }
}
