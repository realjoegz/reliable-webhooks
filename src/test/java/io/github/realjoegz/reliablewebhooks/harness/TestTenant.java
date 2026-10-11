package io.github.realjoegz.reliablewebhooks.harness;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * A tenant created through the admin API of a replica, with the API key it was shown. It makes the
 * two requests that most tests start from, and counts the event and delivery rows of the tenant.
 */
public record TestTenant(UUID id, String apiKey) {

  public static TestTenant create(RestTestClient http, String name) {
    Map<String, String> created =
        http.post()
            .uri("/admin/tenants")
            .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .body(Map.of("name", name))
            .exchange()
            .expectStatus()
            .isCreated()
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    return new TestTenant(UUID.fromString(created.get("id")), created.get("api_key"));
  }

  /** An idempotency key that no other request of the build carries. */
  public static String newIdempotencyKey() {
    return "key-" + UUID.randomUUID();
  }

  /** Registers an endpoint for the URL and the event types, and returns its id. */
  public UUID register(RestTestClient http, String url, String... eventTypes) {
    Map<String, String> created =
        http.post()
            .uri("/v1/endpoints")
            .headers(headers -> headers.setBearerAuth(apiKey))
            .contentType(MediaType.APPLICATION_JSON)
            .body(Map.of("url", url, "event_types", List.of(eventTypes)))
            .exchange()
            .expectStatus()
            .isCreated()
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    return UUID.fromString(created.get("id"));
  }

  /** Sends an event. The body goes out as these bytes. */
  public RestTestClient.ResponseSpec send(RestTestClient http, String idempotencyKey, byte[] body) {
    return http.post()
        .uri("/v1/events")
        .headers(headers -> headers.setBearerAuth(apiKey))
        .header("Idempotency-Key", idempotencyKey)
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .exchange();
  }

  public RestTestClient.ResponseSpec send(RestTestClient http, String idempotencyKey, String body) {
    return send(http, idempotencyKey, body.getBytes(StandardCharsets.UTF_8));
  }

  public int eventRows() {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM event WHERE tenant_id = :t")
        .param("t", id)
        .query(Integer.class)
        .single();
  }

  public int deliveryRows() {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM delivery WHERE tenant_id = :t")
        .param("t", id)
        .query(Integer.class)
        .single();
  }
}
