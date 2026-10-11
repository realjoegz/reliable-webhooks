package io.github.realjoegz.reliablewebhooks.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class SignatureIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17,"note":"café"}}""";

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;

  // The verifier gets a request that the service sent, with the secret that the registration of
  // the endpoint returned. It accepts the request as it arrived, which is the control. It rejects
  // the same request with one byte of its body changed, and the unchanged request when it is
  // presented again more than five minutes after it was signed.
  @Test
  void verifierRejectsTamperedBodyAndStaleTimestamp() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "signature");
      TestTenant.Endpoint endpoint = tenant.registerForSecret(http, receiver.url("/hook"), "*");
      worker.start();

      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
      FaultReceiver.Request request = receiver.requests("/hook").getFirst();
      Instant signedAt = Instant.ofEpochSecond(Long.parseLong(request.header("webhook-timestamp")));

      assertThat(isGenuine(endpoint, request, request.body(), signedAt)).isTrue();

      byte[] tampered = request.body().clone();
      tampered[tampered.length / 2] ^= 1;
      assertThat(isGenuine(endpoint, request, tampered, signedAt)).isFalse();

      Instant fiveMinutesLater = signedAt.plus(Duration.ofMinutes(5));
      assertThat(isGenuine(endpoint, request, request.body(), fiveMinutesLater)).isTrue();
      assertThat(isGenuine(endpoint, request, request.body(), fiveMinutesLater.plusSeconds(1)))
          .isFalse();
    }
  }

  private static boolean isGenuine(
      TestTenant.Endpoint endpoint, FaultReceiver.Request request, byte[] body, Instant now)
      throws GeneralSecurityException {
    return ReferenceVerifier.isGenuine(
        endpoint.secret(),
        request.header("webhook-id"),
        request.header("webhook-timestamp"),
        request.header("webhook-signature"),
        body,
        now);
  }
}
