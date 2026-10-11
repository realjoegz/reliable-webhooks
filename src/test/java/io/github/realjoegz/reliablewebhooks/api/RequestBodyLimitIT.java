package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class RequestBodyLimitIT {

  private static final int LIMIT = 256 * 1024;
  private static final Duration ANSWER_TIMEOUT = Duration.ofSeconds(10);
  private static final String HOST = "hooks.body-limit.example";

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
