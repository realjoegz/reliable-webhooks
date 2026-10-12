package io.github.realjoegz.reliablewebhooks.harness;

import java.util.Map;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The settings a replica under test needs beyond the shipped configuration. As a context
 * initializer it applies them to a replica that runs inside the test JVM, and keeps the delivery
 * worker and the expiry job of that replica from starting with it.
 */
public final class ReplicaSettings
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  public static final String ADMIN_TOKEN = "admin-token-for-integration-tests-only";

  /** The application_name of the replica inside the test JVM. {@link PgWait} finds it by this. */
  public static final String IN_PROCESS_REPLICA = "in-process-replica";

  private static final String SECRET_ENCRYPTION_KEY =
      "c2VjcmV0LWtleS1mb3ItaW50ZWdyYXRpb24tdGVzdHM=";

  /** The settings of a replica with the given id, which is also its application_name. */
  public static Map<String, String> properties(String replicaId) {
    return Map.of(
        "webhooks.replica-id",
        replicaId,
        "webhooks.database.url",
        jdbcUrlAs(replicaId),
        "webhooks.database.username",
        TestDatabase.username(),
        "webhooks.database.password",
        TestDatabase.password(),
        "webhooks.admin-token",
        ADMIN_TOKEN,
        "webhooks.secret-encryption-key",
        SECRET_ENCRYPTION_KEY);
  }

  @Override
  public void initialize(ConfigurableApplicationContext context) {
    TestPropertyValues.of(properties(IN_PROCESS_REPLICA)).applyTo(context);
    context.addBeanFactoryPostProcessor(ReplicaSettings::leaveTheWorkerAndTheExpiryJobToTheTests);
  }

  // Spring starts a lifecycle bean only if it is a singleton. As prototypes, the worker lifecycle
  // and its poller are not started with the replica, and ReplicaWorker gets a new pair of them
  // for every start. The expiry job is not started either, and a test that wants a run of it
  // calls the run.
  private static void leaveTheWorkerAndTheExpiryJobToTheTests(
      ConfigurableListableBeanFactory beans) {
    beans.getBeanDefinition("workerLifecycle").setScope(BeanDefinition.SCOPE_PROTOTYPE);
    beans.getBeanDefinition("deliveryPoller").setScope(BeanDefinition.SCOPE_PROTOTYPE);
    beans.getBeanDefinition("expiryJob").setScope(BeanDefinition.SCOPE_PROTOTYPE);
  }

  private static String jdbcUrlAs(String applicationName) {
    String url = TestDatabase.jdbcUrl();
    return url + (url.contains("?") ? "&" : "?") + "ApplicationName=" + applicationName;
  }
}
