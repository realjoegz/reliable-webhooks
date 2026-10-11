package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class EventApiIT {

  private static final String HOST = "hooks.event-api.example";
  private static final String HOOK = "https://" + HOST + "/hook";
  private static final int SOCKET_TIMEOUT_MILLIS = 10_000;
  private static final String INSTANT_IN_MILLISECONDS =
      "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z";
  private static final String ORDER_CREATED =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String INVALID_BODY =
      "The body must be a JSON object in UTF-8 without a byte order mark, with a type of 1 to"
          + " 128 characters of [a-z0-9_.-] and an object as data. It must keep to the limits on"
          + " numbers, member names and nesting depth.";
  private static final String INVALID_KEY =
      "Idempotency-Key is required and must have 1 to 255 printable ASCII characters";

  private record StoredEvent(
      String type, String idempotencyKey, byte[] requestHash, String traceContext, String body) {}

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @LocalServerPort int port;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  // The data holds numbers that a double cannot carry and a character outside ASCII. The stored
  // body is compared as text, so a value that came back changed would fail the test.
  @Test
  void acceptedEventIsStoredWithTheBodyItsDeliveriesWillSend() throws NoSuchAlgorithmException {
    TestTenant tenant = TestTenant.create(http, "accepted");
    String key = TestTenant.newIdempotencyKey();
    String data =
        """
        {"price":12.50,"precise":0.1000000000000000000000001,"huge":1E+400,"note":"café"}""";
    byte[] request =
        "{ \"type\": \"order.created\",\n  \"data\": %s }"
            .formatted(data)
            .getBytes(StandardCharsets.UTF_8);
    Instant before = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    Map<String, String> accepted =
        tenant
            .send(http, key, request)
            .expectStatus()
            .isAccepted()
            .expectHeader()
            .doesNotExist("Idempotent-Replay")
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();

    Instant after = Instant.now();
    assertThat(accepted).containsOnlyKeys("id");
    UUID id = UUID.fromString(accepted.get("id"));
    StoredEvent stored = storedEvent(tenant, id);
    assertThat(stored.type()).isEqualTo("order.created");
    assertThat(stored.idempotencyKey()).isEqualTo(key);
    assertThat(stored.requestHash())
        .isEqualTo(MessageDigest.getInstance("SHA-256").digest(request));
    assertThat(stored.traceContext()).isNull();
    Matcher body =
        Pattern.compile(
                "\\{\"id\":\"%s\",\"type\":\"order.created\",\"created_at\":\"(%s)\",\"data\":%s}"
                    .formatted(id, INSTANT_IN_MILLISECONDS, Pattern.quote(data)))
            .matcher(stored.body());
    assertThat(body.matches()).as(stored.body()).isTrue();
    assertThat(Instant.parse(body.group(1))).isBetween(before, after);
  }

  // The time that the read returns is the one the database gave the row of the event.
  @Test
  void eventIsReadWithItsDeliveries() {
    TestTenant tenant = TestTenant.create(http, "read-event");
    List<String> endpoints =
        Stream.of(tenant.register(http, HOOK, "*"), tenant.register(http, HOOK, "order.created"))
            .map(UUID::toString)
            .sorted()
            .toList();
    tenant.register(http, HOOK, "order.paid");
    UUID id = accept(tenant, ORDER_CREATED);

    read(tenant, id)
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.length()")
        .isEqualTo(4)
        .jsonPath("$.id")
        .isEqualTo(id.toString())
        .jsonPath("$.type")
        .isEqualTo("order.created")
        .jsonPath("$.created_at")
        .value(
            String.class,
            createdAt -> assertThat(Instant.parse(createdAt)).isEqualTo(createdAtOf(id)))
        .jsonPath("$.deliveries.length()")
        .isEqualTo(2)
        .jsonPath("$.deliveries[*].endpoint_id")
        .isEqualTo(endpoints)
        .jsonPath("$.deliveries[*].id")
        .isEqualTo(deliveryIdsByEndpoint(id))
        .jsonPath("$.deliveries[0].length()")
        .isEqualTo(4)
        .jsonPath("$.deliveries[0].state")
        .isEqualTo("pending")
        .jsonPath("$.deliveries[0].attempts")
        .isEqualTo(0);
  }

  @Test
  void eventWithoutDeliveriesIsReadWithAnEmptyList() {
    TestTenant tenant = TestTenant.create(http, "read-event-without-deliveries");
    UUID id = accept(tenant, ORDER_CREATED);

    read(tenant, id).expectStatus().isOk().expectBody().jsonPath("$.deliveries").isEmpty();
  }

  @Test
  void eventOfAnotherTenantIsNotFound() {
    TestTenant owner = TestTenant.create(http, "event-owner");
    TestTenant other = TestTenant.create(http, "not-the-event-owner");
    UUID id = accept(owner, ORDER_CREATED);

    expectNotFound(read(other, id));
    read(owner, id).expectStatus().isOk();
  }

  @Test
  void eventThatDoesNotExistIsNotFound() {
    TestTenant tenant = TestTenant.create(http, "unknown-event");

    expectNotFound(read(tenant, UUID.randomUUID()));
  }

  @Test
  void eventRoutesTakeAnApiKey() {
    http.get().uri("/v1/events/{id}", UUID.randomUUID()).exchange().expectStatus().isUnauthorized();
    http.post()
        .uri("/v1/events")
        .header("Idempotency-Key", TestTenant.newIdempotencyKey())
        .contentType(MediaType.APPLICATION_JSON)
        .body(ORDER_CREATED.getBytes(StandardCharsets.UTF_8))
        .exchange()
        .expectStatus()
        .isUnauthorized();
  }

  @Test
  void eventTypeOfOneHundredTwentyEightCharactersIsAccepted() {
    TestTenant tenant = TestTenant.create(http, "longest-type");
    String type = "a".repeat(124) + ".0_-";

    UUID id = accept(tenant, "{\"type\":\"%s\",\"data\":{}}".formatted(type));

    read(tenant, id).expectBody().jsonPath("$.type").isEqualTo(type);
  }

  static Stream<String> bodiesThatAreNotAnEvent() {
    return Stream.of(
        "{\"type\":\"" + "a".repeat(129) + "\",\"data\":{}}",
        "{\"type\":\"Order.Created\",\"data\":{}}",
        "{\"type\":\"\",\"data\":{}}",
        "{\"type\":17,\"data\":{}}",
        "{\"data\":{}}",
        "{\"type\":\"order.created\"}",
        "{\"type\":\"order.created\",\"data\":[17]}",
        "{\"type\":\"order.created\",\"data\":null}",
        "[" + ORDER_CREATED + "]",
        ORDER_CREATED + ORDER_CREATED,
        "{\"type\":\"order.created\",\"data\":{");
  }

  @ParameterizedTest
  @MethodSource("bodiesThatAreNotAnEvent")
  void bodyThatIsNotAnEventIsRejected(String body) {
    TestTenant tenant = TestTenant.create(http, "invalid-event");

    expectBadRequest(tenant.send(http, TestTenant.newIdempotencyKey(), body), INVALID_BODY);

    assertThat(tenant.eventRows()).isZero();
  }

  // The body is the first level and the data the second, so 500 objects from the data inwards
  // are 501 levels.
  static Stream<Arguments> bodiesOutsideTheLimits() {
    return Stream.of(
        arguments(named("number of 1,001 digits", eventWith("{\"n\":" + "7".repeat(1001) + "}"))),
        arguments(named("exponent beyond a BigDecimal", eventWith("{\"n\":1E+99999999999}"))),
        arguments(
            named("member name of 1,025 bytes", eventWith("{\"" + "k".repeat(1025) + "\":1}"))),
        arguments(
            named(
                "nesting of 501 levels",
                eventWith("{\"a\":".repeat(500) + "1" + "}".repeat(500)))));
  }

  @ParameterizedTest
  @MethodSource("bodiesOutsideTheLimits")
  void eventBodyOutsideTheLimitsIsRejected(String body) {
    TestTenant tenant = TestTenant.create(http, "event-outside-the-limits");

    expectBadRequest(tenant.send(http, TestTenant.newIdempotencyKey(), body), INVALID_BODY);

    assertThat(tenant.eventRows()).isZero();
  }

  static Stream<Arguments> eventsInUtf16OrUtf32() {
    return Stream.of("UTF-16BE", "UTF-16LE", "UTF-32BE", "UTF-32LE")
        .map(
            encoding ->
                arguments(named(encoding, ORDER_CREATED.getBytes(Charset.forName(encoding)))));
  }

  // The other tests of this class send the same event in UTF-8, and it is accepted.
  @ParameterizedTest
  @MethodSource("eventsInUtf16OrUtf32")
  void eventBodyInUtf16OrUtf32IsRejected(byte[] body) {
    TestTenant tenant = TestTenant.create(http, "event-not-in-utf-8");

    expectBadRequest(tenant.send(http, TestTenant.newIdempotencyKey(), body), INVALID_BODY);

    assertThat(tenant.eventRows()).isZero();
  }

  // The mark is the character U+FEFF in front of the body.
  @Test
  void eventBodyThatStartsWithAByteOrderMarkIsRejected() {
    TestTenant tenant = TestTenant.create(http, "event-with-a-mark");

    expectBadRequest(
        tenant.send(http, TestTenant.newIdempotencyKey(), "\uFEFF" + ORDER_CREATED), INVALID_BODY);

    assertThat(tenant.eventRows()).isZero();
  }

  // In ISO-8859-1 the é is the one byte E9, which begins a UTF-8 sequence that the quotation
  // mark after it breaks off. The first event is the same text in UTF-8.
  @Test
  void eventBodyWhoseUtf8SequenceBreaksOffIsRejected() {
    TestTenant tenant = TestTenant.create(http, "event-in-iso-8859-1");
    String event = eventWith("{\"note\":\"café\"}");

    accept(tenant, event);
    expectBadRequest(
        tenant.send(
            http, TestTenant.newIdempotencyKey(), event.getBytes(StandardCharsets.ISO_8859_1)),
        INVALID_BODY);

    assertThat(tenant.eventRows()).isEqualTo(1);
  }

  @Test
  void bodyThatIsNotSentAsJsonIsRejected() {
    TestTenant tenant = TestTenant.create(http, "not-sent-as-json");

    http.post()
        .uri("/v1/events")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .header("Idempotency-Key", TestTenant.newIdempotencyKey())
        .contentType(MediaType.TEXT_PLAIN)
        .body(ORDER_CREATED.getBytes(StandardCharsets.UTF_8))
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON);

    assertThat(tenant.eventRows()).isZero();
  }

  // Every printable ASCII character is in the key, the space included.
  @Test
  void idempotencyKeyOfTwoHundredFiftyFivePrintableCharactersIsAccepted() {
    TestTenant tenant = TestTenant.create(http, "longest-key");
    StringBuilder printable = new StringBuilder();
    for (char c = '!'; c <= '~'; c++) {
      printable.append(c);
    }
    String key = (printable + " " + printable + " " + printable).substring(0, 255);

    UUID id = accept(tenant, key, ORDER_CREATED);

    assertThat(storedEvent(tenant, id).idempotencyKey()).isEqualTo(key).hasSize(255);
  }

  static Stream<String> keysOutsideTheLimits() {
    return Stream.of("", "k".repeat(256), "tab\tinside");
  }

  @ParameterizedTest
  @MethodSource("keysOutsideTheLimits")
  void idempotencyKeyOutsideTheLimitsIsRejected(String key) {
    TestTenant tenant = TestTenant.create(http, "invalid-key");

    expectBadRequest(tenant.send(http, key, ORDER_CREATED), INVALID_KEY);

    assertThat(tenant.eventRows()).isZero();
  }

  // The HTTP client of the JDK replaces a character outside ASCII before it sends a header, so
  // this request is written to the socket as bytes.
  @Test
  void idempotencyKeyOutsideAsciiIsRejected() throws IOException {
    TestTenant tenant = TestTenant.create(http, "key-outside-ascii");
    byte[] body = ORDER_CREATED.getBytes(StandardCharsets.UTF_8);
    String head =
        """
        POST /v1/events HTTP/1.1\r
        Host: localhost\r
        Connection: close\r
        Authorization: Bearer %s\r
        Content-Type: application/json\r
        Content-Length: %d\r
        Idempotency-Key: clé\r
        \r
        """
            .formatted(tenant.apiKey(), body.length);

    String response;
    try (Socket socket = new Socket("localhost", port)) {
      socket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);
      OutputStream request = socket.getOutputStream();
      request.write(head.getBytes(StandardCharsets.UTF_8));
      request.write(body);
      request.flush();
      response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    assertThat(response).startsWith("HTTP/1.1 400").contains(INVALID_KEY);
    assertThat(tenant.eventRows()).isZero();
  }

  @Test
  void requestWithoutAnIdempotencyKeyIsRejected() {
    TestTenant tenant = TestTenant.create(http, "missing-key");

    expectBadRequest(
        http.post()
            .uri("/v1/events")
            .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
            .contentType(MediaType.APPLICATION_JSON)
            .body(ORDER_CREATED.getBytes(StandardCharsets.UTF_8))
            .exchange(),
        INVALID_KEY);

    assertThat(tenant.eventRows()).isZero();
  }

  private static void expectBadRequest(RestTestClient.ResponseSpec response, String detail) {
    response
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo(detail)
        .jsonPath("$.type")
        .doesNotExist();
  }

  private static void expectNotFound(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isNotFound()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo("No event has this id")
        .jsonPath("$.type")
        .doesNotExist();
  }

  private static String eventWith(String data) {
    return "{\"type\":\"order.created\",\"data\":%s}".formatted(data);
  }

  private UUID accept(TestTenant tenant, String body) {
    return accept(tenant, TestTenant.newIdempotencyKey(), body);
  }

  private UUID accept(TestTenant tenant, String key, String body) {
    Map<String, String> accepted =
        tenant
            .send(http, key, body)
            .expectStatus()
            .isAccepted()
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    return UUID.fromString(accepted.get("id"));
  }

  private RestTestClient.ResponseSpec read(TestTenant tenant, UUID id) {
    return http.get()
        .uri("/v1/events/{id}", id)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  private static StoredEvent storedEvent(TestTenant tenant, UUID id) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT type, idempotency_key, request_hash, trace_context,
                   convert_from(body, 'UTF8') AS body
            FROM event WHERE id = :id AND tenant_id = :t
            """)
        .param("id", id)
        .param("t", tenant.id())
        .query(
            (row, n) ->
                new StoredEvent(
                    row.getString("type"),
                    row.getString("idempotency_key"),
                    row.getBytes("request_hash"),
                    row.getString("trace_context"),
                    row.getString("body")))
        .single();
  }

  private static Instant createdAtOf(UUID eventId) {
    return TestDatabase.jdbc()
        .sql("SELECT created_at FROM event WHERE id = :id")
        .param("id", eventId)
        .query(OffsetDateTime.class)
        .single()
        .toInstant();
  }

  private static List<String> deliveryIdsByEndpoint(UUID eventId) {
    return TestDatabase.jdbc()
        .sql("SELECT id::text FROM delivery WHERE event_id = :id ORDER BY endpoint_id")
        .param("id", eventId)
        .query(String.class)
        .list();
  }
}
