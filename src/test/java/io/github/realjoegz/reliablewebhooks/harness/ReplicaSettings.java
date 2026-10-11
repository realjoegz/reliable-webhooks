package io.github.realjoegz.reliablewebhooks.harness;

import java.util.Map;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The settings a replica under test needs beyond the shipped configuration. As a context
 * initializer it applies them to a replica that runs inside the test JVM.
 */
public final class ReplicaSettings
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  public static final String ADMIN_TOKEN = "admin-token-for-integration-tests-only";

  /** The application_name of the replica inside the test JVM. {@link PgWait} finds it by this. */
  public static final String IN_PROCESS_REPLICA = "in-process-replica";

  private static final String SECRET_ENCRYPTION_KEY =
      "c2VjcmV0LWtleS1mb3ItaW50ZWdyYXRpb24tdGVzdHM=";

  public static Map<String, String> properties() {
    return Map.of(
        "webhooks.database.url", jdbcUrlAs(IN_PROCESS_REPLICA),
        "webhooks.database.username", TestDatabase.username(),
        "webhooks.database.password", TestDatabase.password(),
        "webhooks.admin-token", ADMIN_TOKEN,
        "webhooks.secret-encryption-key", SECRET_ENCRYPTION_KEY);
  }

  @Override
  public void initialize(ConfigurableApplicationContext context) {
    TestPropertyValues.of(properties()).applyTo(context);
  }

  private static String jdbcUrlAs(String applicationName) {
    String url = TestDatabase.jdbcUrl();
    return url + (url.contains("?") ? "&" : "?") + "ApplicationName=" + applicationName;
  }
}
