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
 * requests that the tests of several classes send as a tenant: it registers an endpoint, sends an
 * event and replays a delivery. It also has the limits and the status of the tenant set through the
 * admin API, and counts the event, endpoint and delivery rows of the tenant.
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

  /**
   * Sets the given ones of {@code rate_per_sec}, {@code burst}, {@code max_endpoints} and {@code
   * status} through the admin API, and expects the replica to have set them.
   */
  public void patch(RestTestClient http, Map<String, Object> settings) {
    http.patch()
        .uri("/admin/tenants/{id}", id)
        .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
        .contentType(MediaType.APPLICATION_JSON)
        .body(settings)
        .exchange()
        .expectStatus()
        .isNoContent();
  }

  /** An idempotency key that no other request of the build carries. */
  public static String newIdempotencyKey() {
    return "key-" + UUID.randomUUID();
  }

  /** An endpoint with the signing secret that its registration showed. */
  public record Endpoint(UUID id, String secret) {}

  /** Registers an endpoint for the URL and the event types, and returns its id. */
  public UUID register(RestTestClient http, String url, String... eventTypes) {
    return registerForSecret(http, url, eventTypes).id();
  }

  public Endpoint registerForSecret(RestTestClient http, String url, String... eventTypes) {
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
    return new Endpoint(UUID.fromString(created.get("id")), created.get("secret"));
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

  /** Sends an event under a key of its own, expects it to be accepted, and returns its id. */
  public UUID accept(RestTestClient http, String body) {
    Map<String, String> accepted =
        send(http, newIdempotencyKey(), body)
            .expectStatus()
            .isAccepted()
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    return UUID.fromString(accepted.get("id"));
  }

  /** Asks for the replay of a delivery. What the answer has to be is up to the test. */
  public RestTestClient.ResponseSpec replay(RestTestClient http, UUID deliveryId) {
    return http.post()
        .uri("/v1/deliveries/{id}/replay", deliveryId)
        .headers(headers -> headers.setBearerAuth(apiKey))
        .exchange();
  }

  public int eventRows() {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM event WHERE tenant_id = :t")
        .param("t", id)
        .query(Integer.class)
        .single();
  }

  public int endpointRows() {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM endpoint WHERE tenant_id = :t")
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
