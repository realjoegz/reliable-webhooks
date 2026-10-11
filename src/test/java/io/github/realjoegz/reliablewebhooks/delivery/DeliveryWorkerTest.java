package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Delivery;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Ssrf;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Attempt;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Loaded;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.NextState;
import io.github.realjoegz.reliablewebhooks.endpoint.SecretCipher;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.http.AddressPolicy;
import io.github.realjoegz.reliablewebhooks.http.DnsLookups;
import io.github.realjoegz.reliablewebhooks.http.ReferenceVerifier;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Failed;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.LookupBusy;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Outcome;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Reason;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Response;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class DeliveryWorkerTest {

  private static final Delivery SETTINGS =
      new Delivery(
          Duration.ofSeconds(15),
          Duration.ofSeconds(60),
          Duration.ofSeconds(35),
          Duration.ofSeconds(55),
          12,
          64);
  private static final WebhooksProperties PROPERTIES =
      new WebhooksProperties(
          null,
          Base64.getEncoder().encodeToString(new byte[32]),
          "replica-1",
          null,
          SETTINGS,
          new Ssrf(List.of(), false));
  private static final String URL = "https://hooks.example.com/hook";
  private static final byte[] KEY =
      "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
  private static final byte[] BODY = "{\"id\":\"1\"}".getBytes(StandardCharsets.UTF_8);
  private static final long SIGNED_AT = 1_800_000_000;

  /** What the worker asked the client to send. */
  private record Sent(UUID tenantId, URI url, Map<String, String> headers, byte[] body) {}

  /** What the worker asked the repository to record. */
  private record Recorded(Claimed delivery, NextState next, Attempt attempt, String replica) {}

  private record Released(Claimed delivery, Duration delay) {}

  // System.nanoTime may be anywhere in the long range, so this clock starts just before it wraps.
  private final AtomicLong nanoTime = new AtomicLong(Long.MAX_VALUE - 5);
  private final long claimStarted = nanoTime.get();
  private final MeterRegistry registry = new SimpleMeterRegistry();
  private final SecretCipher cipher = new SecretCipher(PROPERTIES);
  private final DnsLookups lookups =
      new DnsLookups(new StubDns(), new AddressPolicy(PROPERTIES, registry), registry);
  private final Deliveries deliveries = new Deliveries();
  private final Client client = new Client();
  private final DeliveryWorker worker =
      new DeliveryWorker(
          deliveries,
          client,
          cipher,
          new DeliveryMeters(registry),
          SETTINGS,
          "replica-1",
          nanoTime::get,
          pause -> nanoTime.addAndGet(pause.toNanos()));

  /** Answers from memory, records what it is asked to write, and can take time on its clock. */
  private final class Deliveries extends DeliveryRepository {

    private Optional<Loaded> loaded =
        Optional.of(new Loaded(URL, cipher.encrypt(KEY), BODY, SIGNED_AT));
    private Duration loadTakes = Duration.ZERO;
    private RuntimeException loadFailure;
    private boolean leaseHeld = true;
    private int loads;
    private final List<Recorded> recorded = new ArrayList<>();
    private final List<Claimed> deadLettered = new ArrayList<>();
    private final List<Released> released = new ArrayList<>();

    private Deliveries() {
      super(null, null);
    }

    @Override
    Optional<Loaded> load(Claimed delivery) {
      loads++;
      nanoTime.addAndGet(loadTakes.toNanos());
      if (loadFailure != null) {
        throw loadFailure;
      }
      return loaded;
    }

    @Override
    void release(Claimed delivery, Duration delay) {
      released.add(new Released(delivery, delay));
    }

    @Override
    boolean recordAttempt(Claimed delivery, NextState next, Attempt attempt, String replica) {
      recorded.add(new Recorded(delivery, next, attempt, replica));
      return leaseHeld;
    }

    @Override
    boolean deadLetterExhausted(Claimed delivery) {
      deadLettered.add(delivery);
      return leaseHeld;
    }
  }

  /** Sends nothing. It answers as scripted, after the time on the clock that a request takes. */
  private final class Client extends WebhookHttpClient {

    private Outcome outcome = new Response(200, null, null);
    private Duration sendTakes = Duration.ZERO;
    private final List<Sent> sent = new ArrayList<>();

    private Client() {
      super(lookups, PROPERTIES);
    }

    @Override
    public Outcome send(UUID tenantId, URI url, Map<String, String> headers, byte[] body) {
      sent.add(new Sent(tenantId, url, headers, body));
      nanoTime.addAndGet(sendTakes.toNanos());
      return outcome;
    }
  }

  @AfterEach
  void closeClientAndLookups() {
    client.close();
    lookups.close();
  }

  // The load returns 35 s after the claim began. A request that started then could still be on
  // its way when the lease of 60 s ends, so nothing is sent and nothing is written: the delivery
  // keeps its lease and is reclaimed.
  @Test
  void doesNotSendAfterStartCutoff() {
    deliveries.loadTakes = Duration.ofSeconds(35);

    worker.attempt(claimed(1), claimStarted);

    assertThat(deliveries.loads).isEqualTo(1);
    assertThat(client.sent).isEmpty();
    assertThat(deliveries.recorded).isEmpty();
    assertThat(deliveries.released).isEmpty();
    assertThat(deliveries.deadLettered).isEmpty();
  }

  @Test
  void sendsUntilTheStartCutoff() {
    deliveries.loadTakes = Duration.ofSeconds(35).minusNanos(1);

    worker.attempt(claimed(1), claimStarted);

    assertThat(client.sent).hasSize(1);
    assertThat(deliveries.recorded).hasSize(1);
  }

  @Test
  void requestGoesToTheUrlOfTheEndpointSignedWithItsSecretAndTheTimeOfTheLoad()
      throws GeneralSecurityException {
    Claimed delivery = claimed(1);

    worker.attempt(delivery, claimStarted);

    assertThat(client.sent).hasSize(1);
    Sent sent = client.sent.getFirst();
    assertThat(sent.tenantId()).isEqualTo(delivery.tenantId());
    assertThat(sent.url()).isEqualTo(URI.create(URL));
    assertThat(sent.body()).isEqualTo(BODY);
    assertThat(sent.headers())
        .containsOnlyKeys("webhook-id", "webhook-timestamp", "webhook-signature")
        .containsEntry("webhook-id", delivery.id().toString())
        .containsEntry("webhook-timestamp", Long.toString(SIGNED_AT));
    assertThat(
            ReferenceVerifier.isGenuine(
                "whsec_" + Base64.getEncoder().encodeToString(KEY),
                sent.headers().get("webhook-id"),
                sent.headers().get("webhook-timestamp"),
                sent.headers().get("webhook-signature"),
                sent.body(),
                Instant.ofEpochSecond(SIGNED_AT)))
        .isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 201, 204, 299})
  void statusOfTheSuccessClassIsRecordedAsSuccess(int status) throws UnknownHostException {
    InetAddress remote = InetAddress.getByName("93.184.216.34");
    client.outcome = new Response(status, null, remote);
    client.sendTakes = Duration.ofMillis(120);
    Claimed delivery = claimed(3);

    worker.attempt(delivery, claimStarted);

    assertThat(deliveries.recorded)
        .containsExactly(
            new Recorded(
                delivery,
                NextState.succeeded(),
                new Attempt("success", status, null, remote, Duration.ofMillis(120)),
                "replica-1"));
    assertThat(attemptsCounted("success", "http_2xx")).isEqualTo(1);
  }

  // No status ends a delivery for good: not a redirect, which is not followed, and not a 4xx.
  @ParameterizedTest
  @CsvSource({
    "199, http_5xx",
    "301, http_3xx",
    "302, http_3xx",
    "400, http_4xx",
    "404, http_4xx",
    "410, http_4xx",
    "429, http_4xx",
    "500, http_5xx",
    "503, http_5xx",
    "600, http_5xx"
  })
  void everyOtherStatusIsAFailureThatIsTriedAgainOnTheSchedule(int status, String reason) {
    client.outcome = new Response(status, null, null);
    Claimed delivery = claimed(4);

    worker.attempt(delivery, claimStarted);

    assertThat(deliveries.recorded).hasSize(1);
    Recorded recorded = deliveries.recorded.getFirst();
    assertThat(recorded.attempt())
        .isEqualTo(new Attempt("failure", status, null, null, Duration.ZERO));
    assertThat(recorded.next().state()).isEqualTo("pending");
    assertThat(recorded.next().deadReason()).isNull();
    assertThat(recorded.next().delay())
        .isBetween(RetrySchedule.delay(4, 0), RetrySchedule.delay(4, 1));
    assertThat(attemptsCounted("failure", reason)).isEqualTo(1);
  }

  // A host that does not resolve and a host with a denied address read the same in the attempt
  // row. The meter tells them apart.
  @ParameterizedTest
  @CsvSource({
    "TIMEOUT,            timeout,                 timeout",
    "CONNECT,            connect,                 connect",
    "TLS,                tls,                     tls",
    "DNS,                destination_not_allowed, dns",
    "BLOCKED,            destination_not_allowed, blocked",
    "DEADLINE,           deadline,                deadline",
    "RESPONSE_TOO_LARGE, response_too_large,      response_too_large"
  })
  void attemptWithoutAResponseIsAFailureRecordedWithItsError(
      Reason reason, String error, String counted) {
    client.outcome = new Failed(reason, null);
    Claimed delivery = claimed(1);

    worker.attempt(delivery, claimStarted);

    assertThat(deliveries.recorded).hasSize(1);
    Recorded recorded = deliveries.recorded.getFirst();
    assertThat(recorded.attempt())
        .isEqualTo(new Attempt("failure", null, error, null, Duration.ZERO));
    assertThat(recorded.next().state()).isEqualTo("pending");
    assertThat(recorded.next().delay())
        .isBetween(RetrySchedule.delay(1, 0), RetrySchedule.delay(1, 1));
    assertThat(attemptsCounted("failure", counted)).isEqualTo(1);
  }

  @Test
  void retryAfterOfAFailureResponseLengthensTheWait() {
    client.outcome = new Response(503, "1800", null);

    worker.attempt(claimed(1), claimStarted);

    assertThat(deliveries.recorded.getFirst().next())
        .isEqualTo(NextState.retryIn(Duration.ofMinutes(30)));
  }

  @Test
  void eleventhFailedAttemptIsTriedAgain() {
    client.outcome = new Response(500, null, null);

    worker.attempt(claimed(11), claimStarted);

    assertThat(deliveries.recorded.getFirst().next().state()).isEqualTo("pending");
  }

  @Test
  void twelfthFailedAttemptIsRecordedAndDeadLettersTheDeliveryAsExhausted() {
    client.outcome = new Response(500, "60", null);
    Claimed delivery = claimed(12);

    worker.attempt(delivery, claimStarted);

    assertThat(deliveries.recorded)
        .containsExactly(
            new Recorded(
                delivery,
                NextState.exhausted(),
                new Attempt("failure", 500, null, null, Duration.ZERO),
                "replica-1"));
    assertThat(deliveries.deadLettered).isEmpty();
  }

  @Test
  void twelfthAttemptThatSucceedsIsASuccess() {
    worker.attempt(claimed(12), claimStarted);

    assertThat(deliveries.recorded.getFirst().next()).isEqualTo(NextState.succeeded());
  }

  // The claim counted a thirteenth attempt. The delivery is dead-lettered before anything is
  // loaded or sent, and no attempt row is written, because there was no attempt.
  @Test
  void thirteenthClaimDeadLettersWithoutLoadingOrSending() {
    Claimed delivery = claimed(13);

    worker.attempt(delivery, claimStarted);

    assertThat(deliveries.deadLettered).containsExactly(delivery);
    assertThat(deliveries.loads).isZero();
    assertThat(client.sent).isEmpty();
    assertThat(deliveries.recorded).isEmpty();
  }

  // The replica had no room for the lookup. That is no verdict on the endpoint, so the row is
  // given back and nothing is recorded.
  @Test
  void busyLookupReleasesTheRowForTwoSecondsAndRecordsNothing() {
    client.outcome = new LookupBusy();
    Claimed delivery = claimed(2);

    worker.attempt(delivery, claimStarted);

    assertThat(deliveries.released).containsExactly(new Released(delivery, Duration.ofSeconds(2)));
    assertThat(deliveries.recorded).isEmpty();
    assertThat(
            registry.get("webhooks.delivery.released").tag("reason", "dns_busy").counter().count())
        .isEqualTo(1);
  }

  @Test
  void deliveryThatLostItsLeaseOrItsEndpointBeforeTheLoadIsNotSent() {
    deliveries.loaded = Optional.empty();

    worker.attempt(claimed(1), claimStarted);

    assertThat(client.sent).isEmpty();
    assertThat(deliveries.recorded).isEmpty();
    assertThat(deliveries.released).isEmpty();
  }

  @Test
  void attemptWhoseLoadFailsSendsNothingAndLeavesTheRowLeased(CapturedOutput output) {
    deliveries.loadFailure = new IllegalStateException("a fault of the service");

    worker.attempt(claimed(1), claimStarted);

    assertThat(client.sent).isEmpty();
    assertThat(deliveries.recorded).isEmpty();
    assertThat(deliveries.released).isEmpty();
    assertThat(lineLogged(output, "attempt_left_for_reclaim")).contains("ERROR");
  }

  // What a restart of the database does to the load of every attempt in flight. It passes and
  // the lease brings the delivery back, so the line is a warning and not an error.
  @Test
  void loadThatFailsInAWayThatCanPassIsLoggedAsAWarning(CapturedOutput output) {
    deliveries.loadFailure =
        new IllegalStateException(
            "A statement failed", new SQLException("reported by the database", "08006"));

    worker.attempt(claimed(1), claimStarted);

    assertThat(client.sent).isEmpty();
    assertThat(deliveries.recorded).isEmpty();
    assertThat(lineLogged(output, "attempt_left_for_reclaim")).contains("WARN");
  }

  // The request was sent, and another replica holds the lease by the time its result is in.
  @Test
  void resultOfAnAttemptThatLostItsLeaseIsNotCountedAsRecorded() {
    deliveries.leaseHeld = false;

    worker.attempt(claimed(1), claimStarted);

    assertThat(client.sent).hasSize(1);
    assertThat(attemptsCounted("success", "http_2xx")).isZero();
    assertThat(registry.get("webhooks.finalize.lost.lease").counter().count()).isEqualTo(1);
  }

  private static Claimed claimed(int attempts) {
    return new Claimed(
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        attempts,
        0,
        UUID.randomUUID(),
        false);
  }

  private static String lineLogged(CapturedOutput output, String message) {
    return output.getOut().lines().filter(line -> line.contains(message)).findFirst().orElseThrow();
  }

  private double attemptsCounted(String outcome, String reason) {
    return registry
        .get("webhooks.delivery.attempts")
        .tag("outcome", outcome)
        .tag("reason", reason)
        .counter()
        .count();
  }
}
