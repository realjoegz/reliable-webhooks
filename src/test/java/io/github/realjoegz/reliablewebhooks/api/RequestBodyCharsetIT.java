package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class RequestBodyCharsetIT {

  private static final String HOST = "hooks.body-charset.example";
  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String REGISTRATION =
      """
      {"url":"https://hooks.body-charset.example/hook","event_types":["*"]}""";

  @Autowired RestTestClient http;
  @Autowired StubDns dns;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  @Test
  void bodyDeclaredAsUtf8IsRead() {
    TestTenant tenant = TestTenant.create(http, "charset-utf-8");

    postEvent(tenant, json(StandardCharsets.UTF_8), utf8(EVENT)).expectStatus().isAccepted();
    register(tenant, json(StandardCharsets.UTF_8), utf8(REGISTRATION)).expectStatus().isCreated();
  }

  // Each body is the one that the test above sends, in ASCII characters only, so the charset of
  // the Content-Type is the only thing that differs between the request that is accepted and the
  // request that is refused.
  @ParameterizedTest
  @ValueSource(strings = {"US-ASCII", "ISO-8859-1", "windows-1252", "UTF-16", "UTF-16BE", "UTF-32"})
  void bodyDeclaredInACharsetOtherThanUtf8IsRefused(String charset) {
    TestTenant tenant = TestTenant.create(http, "charset-not-utf-8");
    MediaType contentType = json(Charset.forName(charset));

    expectUnsupportedMediaType(postEvent(tenant, contentType, utf8(EVENT)));
    expectUnsupportedMediaType(register(tenant, contentType, utf8(REGISTRATION)));

    assertThat(tenant.eventRows()).isZero();
    assertThat(tenant.endpointRows()).isZero();
  }

  // The padding alone has the 256 KiB of the body limit. Declared as UTF-8, the body is refused
  // for its size, so the second request breaks two rules.
  @Test
  void bodyAboveTheLimitDeclaredInAnotherCharsetIsRefusedForItsCharset() {
    TestTenant tenant = TestTenant.create(http, "charset-before-size");
    byte[] body =
        utf8(
            "{\"type\":\"order.created\",\"data\":{\"padding\":\""
                + "x".repeat(256 * 1024)
                + "\"}}");

    postEvent(tenant, json(StandardCharsets.UTF_8), body)
        .expectStatus()
        .isEqualTo(HttpStatus.CONTENT_TOO_LARGE);
    expectUnsupportedMediaType(postEvent(tenant, json(StandardCharsets.ISO_8859_1), body));

    assertThat(tenant.eventRows()).isZero();
  }

  static Stream<Arguments> registrationsInUtf16OrUtf32() {
    return Stream.of("UTF-16BE", "UTF-16LE", "UTF-32BE", "UTF-32LE")
        .map(
            encoding ->
                arguments(named(encoding, REGISTRATION.getBytes(Charset.forName(encoding)))));
  }

  // The JSON mapper of the API reads a registration, and EventApiIT sends an event in each of
  // these encodings. The first registration is the same text in UTF-8.
  @ParameterizedTest
  @MethodSource("registrationsInUtf16OrUtf32")
  void registrationInUtf16OrUtf32IsRejected(byte[] body) {
    TestTenant tenant = TestTenant.create(http, "registration-not-in-utf-8");

    register(tenant, MediaType.APPLICATION_JSON, utf8(REGISTRATION)).expectStatus().isCreated();
    expectBadRequest(register(tenant, MediaType.APPLICATION_JSON, body));

    assertThat(tenant.endpointRows()).isEqualTo(1);
  }

  // The mark is the character U+FEFF in front of the body.
  @Test
  void registrationThatStartsWithAByteOrderMarkIsRejected() {
    TestTenant tenant = TestTenant.create(http, "registration-with-a-mark");

    expectBadRequest(register(tenant, MediaType.APPLICATION_JSON, utf8("\uFEFF" + REGISTRATION)));

    assertThat(tenant.endpointRows()).isZero();
  }

  private static void expectBadRequest(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON);
  }

  private static void expectUnsupportedMediaType(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo("The request body must be in UTF-8")
        .jsonPath("$.type")
        .doesNotExist();
  }

  private RestTestClient.ResponseSpec postEvent(
      TestTenant tenant, MediaType contentType, byte[] body) {
    return http.post()
        .uri("/v1/events")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .header("Idempotency-Key", TestTenant.newIdempotencyKey())
        .contentType(contentType)
        .body(body)
        .exchange();
  }

  private RestTestClient.ResponseSpec register(
      TestTenant tenant, MediaType contentType, byte[] body) {
    return http.post()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(contentType)
        .body(body)
        .exchange();
  }

  private static MediaType json(Charset charset) {
    return new MediaType(MediaType.APPLICATION_JSON, charset);
  }

  private static byte[] utf8(String body) {
    return body.getBytes(StandardCharsets.UTF_8);
  }
}
