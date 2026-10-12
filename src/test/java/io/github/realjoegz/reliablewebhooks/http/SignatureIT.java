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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class SignatureIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17,"note":"café"}}""";
  private static final Duration OVERLAP = Duration.ofHours(24);
  private static final Duration AN_HOUR = Duration.ofHours(1);

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

  // The secret of an endpoint is rotated while a delivery for it waits for its second attempt.
  // The first request went out before the rotation, with one signature, which is the one of the
  // secret that the endpoint had then. It is answered with a 500 and a Retry-After of an hour, so
  // the delivery waits for its second attempt until the test ages it by that hour, however long
  // the rotation takes.
  //
  // The second request of that delivery carries two signatures. The first of them is the one of
  // the new secret and the second the one of the old, so a subscriber that still checks with the
  // old secret accepts the request, and so does one that has the new secret already.
  //
  // The old secret counts for 24 hours of the database clock. With a minute of them left, a
  // request still carries both signatures. Once that minute is aged away, a request carries the
  // signature of the new secret alone, and a check with the old secret rejects it. The test has
  // that minute to deliver the event before it. A run that took longer would fail and not pass.
  @Test
  void bothSignaturesDuringOverlap() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      receiver.answerOnce("/hook", FaultReceiver.status(500, "Retry-After", "3600"));
      TestTenant tenant = TestTenant.create(http, "rotated-signature");
      TestTenant.Endpoint endpoint = tenant.registerForSecret(http, receiver.url("/hook"), "*");
      String old = endpoint.secret();
      worker.start();
      UUID inFlight = Rows.deliveryOf(tenant.accept(http, EVENT));
      await().untilAsserted(() -> assertThat(Rows.attempts(inFlight)).hasSize(1));
      FaultReceiver.Request beforeTheRotation = lastRequestOf(inFlight, receiver);

      String rotated =
          tenant
              .rotateSecret(http, endpoint.id())
              .expectStatus()
              .isOk()
              .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
              .returnResult()
              .getResponseBody()
              .get("secret");
      Rows.ageDelivery(inFlight, AN_HOUR);
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(inFlight).state()).isEqualTo("succeeded"));
      FaultReceiver.Request duringTheOverlap = lastRequestOf(inFlight, receiver);

      assertThat(signaturesOf(beforeTheRotation)).hasSize(1);
      assertThat(isGenuine(old, beforeTheRotation)).isTrue();
      assertThat(isGenuine(rotated, beforeTheRotation)).isFalse();

      List<String> both = signaturesOf(duringTheOverlap);
      assertThat(both).hasSize(2);
      assertThat(isGenuine(rotated, duringTheOverlap)).isTrue();
      assertThat(isGenuine(old, duringTheOverlap)).isTrue();
      assertThat(isGenuine(rotated, duringTheOverlap, both.get(0))).isTrue();
      assertThat(isGenuine(old, duringTheOverlap, both.get(0))).isFalse();
      assertThat(isGenuine(old, duringTheOverlap, both.get(1))).isTrue();

      Rows.agePreviousSecret(endpoint.id(), OVERLAP.minusMinutes(1));
      FaultReceiver.Request withAMinuteLeft = requestOfAnotherEvent(tenant, receiver);

      assertThat(signaturesOf(withAMinuteLeft)).hasSize(2);
      assertThat(isGenuine(old, withAMinuteLeft)).isTrue();

      Rows.agePreviousSecret(endpoint.id(), Duration.ofMinutes(1));
      FaultReceiver.Request afterTheOverlap = requestOfAnotherEvent(tenant, receiver);

      assertThat(signaturesOf(afterTheOverlap)).hasSize(1);
      assertThat(isGenuine(rotated, afterTheOverlap)).isTrue();
      assertThat(isGenuine(old, afterTheOverlap)).isFalse();
    }
  }

  private FaultReceiver.Request requestOfAnotherEvent(TestTenant tenant, FaultReceiver receiver) {
    UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
    await().untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
    return lastRequestOf(delivery, receiver);
  }

  private static FaultReceiver.Request lastRequestOf(UUID delivery, FaultReceiver receiver) {
    return receiver.requests("/hook").stream()
        .filter(request -> request.header("webhook-id").equals(delivery.toString()))
        .reduce((earlier, later) -> later)
        .orElseThrow();
  }

  private static List<String> signaturesOf(FaultReceiver.Request request) {
    return List.of(request.header("webhook-signature").split(" "));
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

  /** Whether the request is genuine under the secret, when it is checked as it arrives. */
  private static boolean isGenuine(String secret, FaultReceiver.Request request)
      throws GeneralSecurityException {
    return isGenuine(secret, request, request.header("webhook-signature"));
  }

  /** The same for a request that carried the given signatures and no others. */
  private static boolean isGenuine(String secret, FaultReceiver.Request request, String signatures)
      throws GeneralSecurityException {
    return ReferenceVerifier.isGenuine(
        secret,
        request.header("webhook-id"),
        request.header("webhook-timestamp"),
        signatures,
        request.body(),
        Instant.ofEpochSecond(Long.parseLong(request.header("webhook-timestamp"))));
  }
}
