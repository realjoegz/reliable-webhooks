package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.api.AdminController.CreatedTenant;
import io.github.realjoegz.reliablewebhooks.api.AdminController.RotatedApiKey;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AdminControllerTest {

  @Test
  void toStringOfACreatedTenantDoesNotRevealTheApiKey() {
    UUID id = UUID.randomUUID();
    CreatedTenant created = new CreatedTenant(id, "rwh_key-marker");

    assertThat(created.toString()).contains(id.toString()).doesNotContain("key-marker");
  }

  @Test
  void toStringOfARotatedApiKeyDoesNotRevealIt() {
    assertThat(new RotatedApiKey("rwh_key-marker").toString()).doesNotContain("key-marker");
  }
}
