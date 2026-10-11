package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * Every test compares all four counters with what they were before its requests, so it also shows
 * that a request is counted once and under one result. The replica and its counters are shared with
 * the other test classes, which is why the tests compare and do not expect absolute numbers.
 */
@InProcessReplica
class IngestMeterIT {

  private static final String HOST = "hooks.ingest-meter.example";
  private static final int BODY_LIMIT = 256 * 1024;
  private static final String ORDER_17 =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String ORDER_18 =
      """
      {"type":"order.created","data":{"order":18}}""";
  private static final String NO_DATA =
      """
      {"type":"order.created"}""";
  private static final String REGISTRATION_OF_AN_HTTP_URL =
      """
      {"url":"http://hooks.ingest-meter.example/hook","event_types":["*"]}""";

  private record Counts(double accepted, double replayed, double invalid, double busy) {

    Counts plusAccepted(int requests) {
      return new Counts(accepted + requests, replayed, invalid, busy);
    }

    Counts plusReplayed(int requests) {
      return new Counts(accepted, replayed + requests, invalid, busy);
    }

    Counts plusInvalid(int requests) {
      return new Counts(accepted, replayed, invalid + requests, busy);
    }

    Counts plusBusy(int requests) {
      return new Counts(accepted, replayed, invalid, busy + requests);
    }
  }

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired MeterRegistry meters;

  @Test
  void newEventIsCountedAsAcceptedAndItsRepeatAsReplayed() {
    TestTenant tenant = TestTenant.create(http, "meter-accepted");
    String key = TestTenant.newIdempotencyKey();
    Counts before = counts();

    tenant.send(http, key, ORDER_17).expectStatus().isAccepted();
    awaitCounts(before.plusAccepted(1));

    tenant.send(http, key, ORDER_17).expectStatus().isAccepted();
    awaitCounts(before.plusAccepted(1).plusReplayed(1));
  }

  @Test
  void requestRefusedForWhatItSentIsCountedAsInvalid() {
    TestTenant tenant = TestTenant.create(http, "meter-invalid");
    String key = TestTenant.newIdempotencyKey();
    Counts before = counts();
    tenant.send(http, key, ORDER_17).expectStatus().isAccepted();
    Counts withTheEvent = before.plusAccepted(1);
    awaitCounts(withTheEvent);

    tenant.send(http, TestTenant.newIdempotencyKey(), NO_DATA).expectStatus().isBadRequest();
    awaitCounts(withTheEvent.plusInvalid(1));

    tenant.send(http, "", ORDER_17).expectStatus().isBadRequest();
    awaitCounts(withTheEvent.plusInvalid(2));

    tenant
        .send(http, TestTenant.newIdempotencyKey(), eventOf(BODY_LIMIT + 1))
        .expectStatus()
        .isEqualTo(HttpStatus.CONTENT_TOO_LARGE);
    awaitCounts(withTheEvent.plusInvalid(3));

    tenant.send(http, key, ORDER_18).expectStatus().isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
    awaitCounts(withTheEvent.plusInvalid(4));
  }

  // The test holds the endpoint row that the fan-out locks, until the lock timeout has ended the
  // wait of the request.
  @Test
  void requestThatTheDatabaseDidNotAnswerInTimeIsCountedAsBusy() throws Exception {
    TestTenant tenant = TestTenant.create(http, "meter-busy");
    dns.answer(HOST, "93.184.216.34");
    UUID endpoint = tenant.register(http, "https://" + HOST + "/hook", "*");
    Counts before = counts();

    try (ExecutorService caller = Executors.newVirtualThreadPerTaskExecutor();
        Connection blocker = TestDatabase.connection();
        PreparedStatement lock =
            blocker.prepareStatement("SELECT 1 FROM endpoint WHERE id = ? FOR UPDATE")) {
      lock.setObject(1, endpoint);
      lock.execute();

      Future<HttpStatusCode> answer =
          caller.submit(
              () ->
                  tenant
                      .send(http, TestTenant.newIdempotencyKey(), ORDER_17)
                      .returnResult()
                      .getStatus());
      PgWait.untilBlocked("INSERT INTO delivery", ReplicaSettings.IN_PROCESS_REPLICA);

      assertThat(answer.get()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
      blocker.rollback();
    }
    awaitCounts(before.plusBusy(1));
  }

  // The test ends the backend of the request while it waits for the endpoint row, which is what a
  // database that shuts down does to it. The statement fails, and so does the rollback that
  // follows it on the same connection. A failure of that kind can pass, so the answer is a 503
  // and the request is counted as busy, like the one that waited too long.
  @Test
  void requestWhoseConnectionTheDatabaseEndedIsCountedAsBusy() throws Exception {
    TestTenant tenant = TestTenant.create(http, "meter-ended");
    dns.answer(HOST, "93.184.216.34");
    UUID endpoint = tenant.register(http, "https://" + HOST + "/hook", "*");
    Counts before = counts();

    try (ExecutorService caller = Executors.newVirtualThreadPerTaskExecutor();
        Connection blocker = TestDatabase.connection();
        PreparedStatement lock =
            blocker.prepareStatement("SELECT 1 FROM endpoint WHERE id = ? FOR UPDATE")) {
      lock.setObject(1, endpoint);
      lock.execute();

      Future<HttpStatusCode> answer =
          caller.submit(
              () ->
                  tenant
                      .send(http, TestTenant.newIdempotencyKey(), ORDER_17)
                      .returnResult()
                      .getStatus());
      PgWait.untilBlocked("INSERT INTO delivery", ReplicaSettings.IN_PROCESS_REPLICA);
      assertThat(PgWait.endBlocked("INSERT INTO delivery", ReplicaSettings.IN_PROCESS_REPLICA))
          .isEqualTo(1);

      assertThat(answer.get()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
      blocker.rollback();
    }
    awaitCounts(before.plusBusy(1));
    assertThat(tenant.eventRows()).isZero();
  }

  // The replica has the key of the tenant in its cache when the test deletes the row of the
  // tenant. The next request with that key passes the authentication, and its event then names a
  // tenant that does not exist. That is a fault and none of the results, so the answer is a 500.
  // The accepted event of another tenant at the end is the witness: it is counted, so the
  // counters were at work while the request before it was not counted.
  @Test
  void requestAnsweredWithAStatusThatHasNoResultIsNotCounted() {
    TestTenant removed = TestTenant.create(http, "meter-no-result");
    TestTenant witness = TestTenant.create(http, "meter-witness");
    http.get()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(removed.apiKey()))
        .exchange()
        .expectStatus()
        .isOk();
    deleteByHand(removed);
    Counts before = counts();

    removed
        .send(http, TestTenant.newIdempotencyKey(), ORDER_17)
        .expectStatus()
        .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);

    witness.send(http, TestTenant.newIdempotencyKey(), ORDER_17).expectStatus().isAccepted();
    awaitCounts(before.plusAccepted(1));
  }

  // The ingest route answers a charset other than UTF-8 with a 415, which has no result. The
  // accepted event at the end is the witness, as above.
  @Test
  void requestRefusedForItsCharsetIsNotCounted() {
    TestTenant tenant = TestTenant.create(http, "meter-charset");
    Counts before = counts();

    http.post()
        .uri("/v1/events")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .header("Idempotency-Key", TestTenant.newIdempotencyKey())
        .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.ISO_8859_1))
        .body(ORDER_17.getBytes(StandardCharsets.UTF_8))
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);

    tenant.send(http, TestTenant.newIdempotencyKey(), ORDER_17).expectStatus().isAccepted();
    awaitCounts(before.plusAccepted(1));
  }

  // The accepted event at the end is the witness, as above. The first two requests are refused
  // on the way to the ingest route. The other three are answered by another route, each with a
  // status that is a result on the ingest route, so a meter that looked at more than the ingest
  // route would count them as invalid.
  @Test
  void requestThatDoesNotReachTheIngestRouteIsNotCounted() {
    TestTenant tenant = TestTenant.create(http, "meter-not-counted");
    Counts before = counts();

    http.post()
        .uri("/v1/events")
        .header("Idempotency-Key", TestTenant.newIdempotencyKey())
        .contentType(MediaType.APPLICATION_JSON)
        .body(ORDER_17.getBytes(StandardCharsets.UTF_8))
        .exchange()
        .expectStatus()
        .isUnauthorized();
    http.post()
        .uri("/v1/events")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .header("Idempotency-Key", TestTenant.newIdempotencyKey())
        .contentType(MediaType.TEXT_PLAIN)
        .body(ORDER_17.getBytes(StandardCharsets.UTF_8))
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
    http.get()
        .uri("/v1/events/{id}", "not-a-uuid")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange()
        .expectStatus()
        .isBadRequest();
    register(tenant, registrationOf(BODY_LIMIT + 1))
        .expectStatus()
        .isEqualTo(HttpStatus.CONTENT_TOO_LARGE);
    register(tenant, REGISTRATION_OF_AN_HTTP_URL.getBytes(StandardCharsets.UTF_8))
        .expectStatus()
        .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);

    tenant.send(http, TestTenant.newIdempotencyKey(), ORDER_17).expectStatus().isAccepted();
    awaitCounts(before.plusAccepted(1));
  }

  // A request is counted when it is over, which can be a moment after its answer has arrived.
  private void awaitCounts(Counts expected) {
    await().untilAsserted(() -> assertThat(counts()).isEqualTo(expected));
  }

  private Counts counts() {
    return new Counts(count("accepted"), count("replayed"), count("invalid"), count("busy"));
  }

  private double count(String result) {
    return meters.get("webhooks.ingest.requests").tag("result", result).counter().count();
  }

  private RestTestClient.ResponseSpec register(TestTenant tenant, byte[] body) {
    return http.post()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .exchange();
  }

  private static void deleteByHand(TestTenant tenant) {
    TestDatabase.jdbc()
        .sql("DELETE FROM rate_bucket WHERE tenant_id = :t")
        .param("t", tenant.id())
        .update();
    TestDatabase.jdbc().sql("DELETE FROM tenant WHERE id = :t").param("t", tenant.id()).update();
  }

  private static byte[] eventOf(int bytes) {
    return padded("{\"type\":\"order.created\",\"data\":{\"padding\":\"", "\"}}", bytes);
  }

  private static byte[] registrationOf(int bytes) {
    return padded(
        "{\"url\":\"https://" + HOST + "/hook\",\"event_types\":[\"*\"],\"padding\":\"",
        "\"}",
        bytes);
  }

  private static byte[] padded(String head, String tail, int bytes) {
    String body = head + "x".repeat(bytes - head.length() - tail.length()) + tail;
    return body.getBytes(StandardCharsets.US_ASCII);
  }
}
