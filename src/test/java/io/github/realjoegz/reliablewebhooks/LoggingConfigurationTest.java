package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.mock.env.MockEnvironment;

class LoggingConfigurationTest {

  private static final String CONSOLE_FORMAT = "logging.structured.format.console";

  @Test
  void structuredConsoleFormatIsConfiguredByDefault() {
    assertThat(shippedConfiguration().getProperty(CONSOLE_FORMAT)).isEqualTo("logstash");
  }

  @Test
  void localProfileConfiguresNoStructuredConsoleFormat() {
    assertThat(shippedConfiguration("local").getProperty(CONSOLE_FORMAT)).isNull();
  }

  // An environment without the variables and system properties of the machine the test runs on.
  private static ConfigurableEnvironment shippedConfiguration(String... profiles) {
    ConfigurableEnvironment environment = new MockEnvironment();
    ConfigDataEnvironmentPostProcessor.applyTo(environment, null, null, profiles);
    return environment;
  }
}
