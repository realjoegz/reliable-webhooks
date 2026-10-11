package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Database;
import org.junit.jupiter.api.Test;

class WebhooksPropertiesTest {

  @Test
  void toStringDoesNotRevealTheSecrets() {
    Database database =
        new Database(
            "jdbc:postgresql://db/webhooks?password=password-in-the-url",
            "database-user",
            "database-password",
            null,
            null);
    WebhooksProperties properties =
        new WebhooksProperties("admin-token-value", "encryption-key-value", database, null);

    assertThat(properties.toString())
        .contains("database-user")
        .doesNotContain(
            "admin-token-value",
            "encryption-key-value",
            "database-password",
            "password-in-the-url");
  }
}
