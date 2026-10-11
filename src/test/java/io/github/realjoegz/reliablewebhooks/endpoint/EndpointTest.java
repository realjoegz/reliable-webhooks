package io.github.realjoegz.reliablewebhooks.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EndpointTest {

  @Test
  void toStringDoesNotRevealTheUrl() {
    Endpoint endpoint =
        new Endpoint(
            UUID.randomUUID(),
            "https://hooks.example.com/path-marker?token=query-marker",
            List.of("order.created"),
            "closed",
            Instant.EPOCH);

    assertThat(endpoint.toString())
        .contains(endpoint.id().toString(), "order.created", "closed")
        .doesNotContain("path-marker", "query-marker");
  }
}
