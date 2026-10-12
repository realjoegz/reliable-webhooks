package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.api.EndpointController.CreateEndpoint;
import io.github.realjoegz.reliablewebhooks.api.EndpointController.CreatedEndpoint;
import io.github.realjoegz.reliablewebhooks.api.EndpointController.RotatedSecret;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EndpointControllerTest {

  @Test
  void toStringOfARegistrationDoesNotRevealTheUrl() {
    CreateEndpoint registration =
        new CreateEndpoint(
            "https://hooks.example.com/path-marker?token=query-marker", List.of("order.created"));

    assertThat(registration.toString())
        .contains("order.created")
        .doesNotContain("path-marker", "query-marker");
  }

  @Test
  void toStringOfACreatedEndpointDoesNotRevealTheSecret() {
    UUID id = UUID.randomUUID();
    CreatedEndpoint created = new CreatedEndpoint(id, "whsec_secret-marker");

    assertThat(created.toString()).contains(id.toString()).doesNotContain("secret-marker");
  }

  @Test
  void toStringOfARotatedSecretDoesNotRevealIt() {
    assertThat(new RotatedSecret("whsec_secret-marker").toString()).doesNotContain("secret-marker");
  }
}
