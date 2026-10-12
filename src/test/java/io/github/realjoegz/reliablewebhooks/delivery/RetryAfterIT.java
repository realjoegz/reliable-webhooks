package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class RetryAfterIT {

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;

  // Four endpoints answer 503 and ask for a wait of nothing, of two minutes, of an hour and a
  // second, and of a billion seconds. The wait of two minutes is the control: it is taken as it
  // is, so the header is read. The others are clamped: the wait of nothing to the delay that the
  // schedule gives by itself, and the two above an hour to one hour. An hour and a second is the
  // shortest wait that is cut, so it shows where the limit lies.
  @Test
  void clampedAtBothEnds() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/zero", FaultReceiver.status(503, "Retry-After", "0"));
      receiver.answer("/two-minutes", FaultReceiver.status(503, "Retry-After", "120"));
      receiver.answer("/hour-and-a-second", FaultReceiver.status(503, "Retry-After", "3601"));
      receiver.answer("/absurd", FaultReceiver.status(503, "Retry-After", "1000000000"));
      TestTenant tenant = TestTenant.create(http, "retry-after");
      tenant.register(http, receiver.url("/zero"), "wait.zero");
      tenant.register(http, receiver.url("/two-minutes"), "wait.two-minutes");
      tenant.register(http, receiver.url("/hour-and-a-second"), "wait.hour-and-a-second");
      tenant.register(http, receiver.url("/absurd"), "wait.absurd");
      worker.start();

      assertThat(waitAfterFirstFailure(tenant, "wait.zero"))
          .isBetween(RetrySchedule.delay(1, 0), RetrySchedule.delay(1, 1));
      assertThat(waitAfterFirstFailure(tenant, "wait.two-minutes"))
          .isEqualTo(Duration.ofMinutes(2));
      assertThat(waitAfterFirstFailure(tenant, "wait.hour-and-a-second"))
          .isEqualTo(Duration.ofHours(1));
      assertThat(waitAfterFirstFailure(tenant, "wait.absurd")).isEqualTo(Duration.ofHours(1));
    }
  }

  /**
   * Sends an event of the type and returns the wait that its delivery got when the first attempt
   * had failed. The wait is read at once, long before a second attempt could change it.
   */
  private Duration waitAfterFirstFailure(TestTenant tenant, String type) {
    UUID delivery =
        Rows.deliveryOf(tenant.accept(http, "{\"type\":\"%s\",\"data\":{}}".formatted(type)));
    await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(1));
    Duration wait = Rows.waitAfter(delivery, 1);
    assertThat(Rows.attempts(delivery))
        .containsExactly(
            new Rows.Attempt(
                0,
                1,
                false,
                "failure",
                503,
                null,
                "127.0.0.1",
                ReplicaSettings.IN_PROCESS_REPLICA));
    assertThat(Rows.delivery(delivery))
        .isEqualTo(new Rows.Delivery("pending", 1, 0, false, null, false));
    return wait;
  }
}
