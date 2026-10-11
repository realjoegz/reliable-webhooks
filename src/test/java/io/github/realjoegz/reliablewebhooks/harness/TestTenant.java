package io.github.realjoegz.reliablewebhooks.harness;

import java.util.Map;
import java.util.UUID;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/** A tenant created through the admin API of a replica, with the API key it was shown. */
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
}
