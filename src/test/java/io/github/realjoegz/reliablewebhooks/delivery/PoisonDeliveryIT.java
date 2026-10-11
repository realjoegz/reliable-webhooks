package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class PoisonDeliveryIT {

  private static final String POISON =
      """
      {"type":"poison.sent","data":{}}""";
  private static final String CANARY =
      """
      {"type":"canary.sent","data":{}}""";
  private static final Duration PAST_THE_LEASE = Duration.ofSeconds(61);
  private static final UUID[] NONE = {};

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired DeliveryRepository deliveries;
  @Autowired WebhooksProperties properties;

  // The test claims the delivery twelve times and lets every lease run out, which is what the
  // database sees of a delivery that kills its worker on every attempt. Then the worker starts.
  // Its claim is the thirteenth, and it dead-letters the delivery.
  //
  // That nothing was sent needs a witness. The canary is accepted after the delivery is dead and
  // goes to the same receiver, so the worker was claiming and the receiver could be reached.
  @Test
  void thirteenthClaimDeadLetters() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/poison", FaultReceiver.status(200));
      receiver.answer("/canary", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "poison");
      tenant.register(http, receiver.url("/poison"), "poison.sent");
      tenant.register(http, receiver.url("/canary"), "canary.sent");
      UUID poison = Rows.deliveryOf(tenant.accept(http, POISON));

      for (int claim = 1; claim <= 12; claim++) {
        List<Claimed> claimed = deliveries.claim(NONE, NONE, 1, properties.delivery().lease());
        assertThat(claimed).extracting(Claimed::id).containsExactly(poison);
        assertThat(claimed.getFirst().attempts()).isEqualTo(claim);
        Rows.ageDelivery(poison, PAST_THE_LEASE);
      }
      worker.start();
      await().untilAsserted(() -> assertThat(Rows.delivery(poison).state()).isEqualTo("dead"));

      assertThat(Rows.delivery(poison))
          .isEqualTo(new Rows.Delivery("dead", 13, 0, false, "exhausted", true));
      List<Rows.Attempt> twelveUnknown = new ArrayList<>();
      for (int attempt = 1; attempt <= 12; attempt++) {
        twelveUnknown.add(
            new Rows.Attempt(0, attempt, false, "unknown", null, "lease_expired", null, null));
      }
      assertThat(Rows.attempts(poison)).isEqualTo(twelveUnknown);

      UUID canary = Rows.deliveryOf(tenant.accept(http, CANARY));
      await().untilAsserted(() -> assertThat(Rows.delivery(canary).state()).isEqualTo("succeeded"));
      assertThat(receiver.requests("/canary")).hasSize(1);
      assertThat(receiver.requests("/poison")).isEmpty();
      assertThat(Rows.delivery(poison))
          .isEqualTo(new Rows.Delivery("dead", 13, 0, false, "exhausted", true));
    }
  }
}
