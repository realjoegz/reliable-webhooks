package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Database;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Delivery;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Ssrf;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.mock.env.MockEnvironment;

class WebhooksPropertiesTest {

  // The configuration that ships, read without the variables and system properties of the
  // machine the test runs on.
  @Test
  void shippedDeliverySettingsAreTheTimingRulesOfTheDesign() {
    assertThat(shipped("webhooks.delivery", Delivery.class))
        .isEqualTo(
            new Delivery(
                Duration.ofSeconds(15),
                Duration.ofSeconds(60),
                Duration.ofSeconds(35),
                Duration.ofSeconds(55),
                12,
                64));
  }

  @Test
  void shippedConfigurationAllowsNoAddressRangeAndNoHttp() {
    assertThat(shipped("webhooks.ssrf", Ssrf.class)).isEqualTo(new Ssrf(List.of(), false));
  }

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
        new WebhooksProperties(
            "admin-token-value", "encryption-key-value", "replica", database, null, null);

    assertThat(properties.toString())
        .contains("database-user")
        .doesNotContain(
            "admin-token-value",
            "encryption-key-value",
            "database-password",
            "password-in-the-url");
  }

  private static <T> T shipped(String prefix, Class<T> type) {
    ConfigurableEnvironment environment = new MockEnvironment();
    ConfigDataEnvironmentPostProcessor.applyTo(environment);
    return Binder.get(environment).bind(prefix, type).get();
  }
}
