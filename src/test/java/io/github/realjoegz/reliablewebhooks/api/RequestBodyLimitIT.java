package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class RequestBodyLimitIT {

  private static final int LIMIT = 256 * 1024;
  private static final Duration ANSWER_TIMEOUT = Duration.ofSeconds(10);
  private static final String HOST = "hooks.body-limit.example";
  private static final String FORM = MediaType.APPLICATION_FORM_URLENCODED_VALUE;
  private static final String MULTIPART = MediaType.MULTIPART_FORM_DATA_VALUE + "; boundary=part";

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @LocalServerPort int port;

  @Test
  void eventBodyOfExactlyTheLimitIsAccepted() {
    TestTenant tenant = TestTenant.create(http, "body-at-limit");

    postEvent(tenant, eventOf(LIMIT)).expectStatus().isAccepted();

    assertThat(tenant.eventRows()).isEqualTo(1);
  }

  @Test
  void eventBodyOneByteAboveTheLimitIsRejected() {
    TestTenant tenant = TestTenant.create(http, "body-above-limit");

    expectContentTooLarge(postEvent(tenant, eventOf(LIMIT + 1)));

    assertThat(tenant.eventRows()).isZero();
  }

  // A body sent in chunks declares no length, so only counting what arrives can refuse it.
  @Test
  void bodyAboveTheLimitWithoutADeclaredLengthIsRejected()
      throws IOException, InterruptedException {
    TestTenant tenant = TestTenant.create(http, "chunked-above-limit");
    byte[] body = eventOf(LIMIT + 1);
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/events"))
            .header(HttpHeaders.AUTHORIZATION, "Bearer " + tenant.apiKey())
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .header("Idempotency-Key", TestTenant.newIdempotencyKey())
            .timeout(ANSWER_TIMEOUT)
            .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(body)))
            .build();

    HttpResponse<String> response;
    try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
      response = client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    assertThat(request.bodyPublisher().orElseThrow().contentLength()).isNegative();
    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONTENT_TOO_LARGE.value());
    assertThat(response.headers().firstValue(HttpHeaders.CONTENT_TYPE))
        .contains(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
    assertThat(tenant.eventRows()).isZero();
  }

  // The padding is a member that a registration ignores, so its size is the only thing that
  // differs between the body that is accepted and the body that is refused.
  @Test
  void limitHoldsForARouteOtherThanIngest() {
    TestTenant tenant = TestTenant.create(http, "registration-body-limit");
    dns.answer(HOST, "93.184.216.34");

    register(tenant, registrationOf(LIMIT)).expectStatus().isCreated();
    expectContentTooLarge(register(tenant, registrationOf(LIMIT + 1)));

    assertThat(tenant.endpointRows()).isEqualTo(1);
  }

  // Spring Boot has a filter that reads the form of a PUT, PATCH or DELETE into memory, to offer
  // it as the parameters of the request. It comes before authentication, does not know the limit
  // and is on unless the configuration turns it off.
  //
  // That filter leaves a POST alone, and no single setting decides the answer to one. The filter
  // of Spring Boot for a hidden method, which asks for a parameter of a POST ahead of
  // authentication, is off by default. Turned on, it would find no form, because RequestBodyLimit
  // has the connector read no body for parameters. The POST is sent because a form is to be
  // answered unread whatever its method.
  @ParameterizedTest
  @ValueSource(strings = {"PUT", "PATCH", "DELETE", "POST"})
  void formBodyIsNotReadBeforeAuthentication(String method) throws IOException {
    int status =
        statusWhileTheBodyIsOpen(
            method, "/v1/endpoints/" + UUID.randomUUID(), "Content-Type: " + FORM);

    assertThat(status).isEqualTo(HttpStatus.UNAUTHORIZED.value());
  }

  // Behind authentication, two readers come before a route reads its body through the limit.
  // Tomcat reads the body of a POST that is sent as a form when the parameters of the request are
  // first asked for, which Spring does on its way to the route. Spring parses a multipart body
  // before it looks for the route. No route takes either, so a registration that is sent as one
  // is refused for its media type, and here it is refused before its body has ended.
  @ParameterizedTest
  @ValueSource(strings = {FORM, MULTIPART})
  void bodyThatNoRouteTakesIsRefusedWithoutBeingRead(String contentType) throws IOException {
    TestTenant tenant = TestTenant.create(http, "body-no-route-takes");

    int status =
        statusWhileTheBodyIsOpen(
            "POST",
            "/v1/endpoints",
            "Authorization: Bearer " + tenant.apiKey(),
            "Content-Type: " + contentType);

    assertThat(status).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
  }

  // Spring would parse a multipart body on any path, also on one that takes no key. The scrape
  // is such a path, and it is answered before the body has ended.
  @Test
  void multipartBodyIsNotReadWhereNoKeyIsTaken() throws IOException {
    int status =
        statusWhileTheBodyIsOpen("GET", "/actuator/prometheus", "Content-Type: " + MULTIPART);

    assertThat(status).isEqualTo(HttpStatus.OK.value());
  }

  // The actuator endpoints take no key, and the handler of Spring Boot for them reads a JSON body
  // that is sent to one, also with a GET. That body comes through the limit like the body of a
  // route. The two bodies differ only in the size of their one member.
  @Test
  void limitHoldsWhereNoKeyIsTaken() {
    scrape(jsonOf(LIMIT)).expectStatus().isOk();
    expectContentTooLarge(scrape(jsonOf(LIMIT + 1)));
  }

  /**
   * Sends the head of a request and one chunk of its body, and returns the status of the answer.
   * The chunk that would end the body is never sent, so the body is still open when the answer
   * arrives. A service that read the body to its end before it answered would not answer. The read
   * timeout ends that wait, and the test fails.
   */
  private int statusWhileTheBodyIsOpen(String method, String path, String... headers)
      throws IOException {
    String chunk = "event_types=order.created";
    String sent =
        String.join(
            "\r\n",
            method + " " + path + " HTTP/1.1",
            "Host: localhost:" + port,
            String.join("\r\n", headers),
            "Transfer-Encoding: chunked",
            "Connection: close",
            "",
            Integer.toHexString(chunk.length()),
            chunk,
            "");
    try (Socket socket = new Socket("localhost", port)) {
      socket.setSoTimeout((int) ANSWER_TIMEOUT.toMillis());
      OutputStream out = socket.getOutputStream();
      out.write(sent.getBytes(StandardCharsets.US_ASCII));
      out.flush();
      String statusLine =
          new BufferedReader(
                  new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII))
              .readLine();
      return Integer.parseInt(statusLine.split(" ")[1]);
    } catch (SocketTimeoutException e) {
      throw new AssertionError(
          "%s %s was not answered within %d s while its body was still open"
              .formatted(method, path, ANSWER_TIMEOUT.toSeconds()),
          e);
    }
  }

  private static void expectContentTooLarge(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isEqualTo(HttpStatus.CONTENT_TOO_LARGE)
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo("The request body must not exceed 262144 bytes")
        .jsonPath("$.type")
        .doesNotExist();
  }

  private RestTestClient.ResponseSpec postEvent(TestTenant tenant, byte[] body) {
    return tenant.send(http, TestTenant.newIdempotencyKey(), body);
  }

  private RestTestClient.ResponseSpec register(TestTenant tenant, byte[] body) {
    return http.post()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .exchange();
  }

  private RestTestClient.ResponseSpec scrape(byte[] body) {
    return http.method(HttpMethod.GET)
        .uri("/actuator/prometheus")
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .exchange();
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

  private static byte[] jsonOf(int bytes) {
    return padded("{\"padding\":\"", "\"}", bytes);
  }

  private static byte[] padded(String head, String tail, int bytes) {
    String body = head + "x".repeat(bytes - head.length() - tail.length()) + tail;
    return body.getBytes(StandardCharsets.US_ASCII);
  }
}
