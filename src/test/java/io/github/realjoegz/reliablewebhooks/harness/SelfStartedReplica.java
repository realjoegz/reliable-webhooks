package io.github.realjoegz.reliablewebhooks.harness;

import io.github.realjoegz.reliablewebhooks.ReliableWebhooksApplication;
import io.github.realjoegz.reliablewebhooks.delivery.PollerHeartbeat;
import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

/**
 * A second replica inside the test JVM that starts the way the application starts. Nothing of it is
 * left to the tests: Spring starts its delivery worker and its expiry job, and closing it stops
 * them. A test uses it to show what a replica does by itself, and closes it before it ends.
 */
public final class SelfStartedReplica {

  private SelfStartedReplica() {}

  public static ConfigurableApplicationContext start(String replicaId, String... profiles) {
    return builder(profiles).run(arguments(replicaId));
  }

  /**
   * The same replica with the given heartbeat in place of its own, so that the test owns the clock
   * that the liveness of the replica is read from.
   */
  public static ConfigurableApplicationContext start(String replicaId, PollerHeartbeat heartbeat) {
    return builder()
        .initializers(
            context ->
                ((GenericApplicationContext) context)
                    .registerBean(
                        "heartbeatOfTheTest",
                        PollerHeartbeat.class,
                        () -> heartbeat,
                        definition -> definition.setPrimary(true)))
        .run(arguments(replicaId));
  }

  private static SpringApplicationBuilder builder(String... profiles) {
    return new SpringApplicationBuilder(ReliableWebhooksApplication.class).profiles(profiles);
  }

  // The settings go in as arguments, which count for more than the configuration that ships. The
  // pools are small because the replicas that the build keeps have theirs open already.
  private static String[] arguments(String replicaId) {
    Map<String, String> settings = new HashMap<>(ReplicaSettings.properties(replicaId));
    settings.put("webhooks.database.api.max-size", "4");
    settings.put("webhooks.database.worker.max-size", "4");
    settings.put("server.port", "0");
    return settings.entrySet().stream()
        .map(setting -> "--" + setting.getKey() + "=" + setting.getValue())
        .toArray(String[]::new);
  }
}
