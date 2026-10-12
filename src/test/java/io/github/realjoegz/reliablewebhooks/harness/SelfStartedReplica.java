package io.github.realjoegz.reliablewebhooks.harness;

import io.github.realjoegz.reliablewebhooks.ReliableWebhooksApplication;
import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * A second replica inside the test JVM that starts the way the application starts. Nothing of it is
 * left to the tests: Spring starts its delivery worker and its expiry job, and closing it stops
 * them. A test uses it to show what a replica does by itself, and closes it before it ends.
 */
public final class SelfStartedReplica {

  private SelfStartedReplica() {}

  // The settings go in as arguments, which count for more than the configuration that ships. The
  // pools are small because the replicas that the build keeps have theirs open already, and the
  // database of the tests takes a hundred connections.
  public static ConfigurableApplicationContext start(String replicaId, String... profiles) {
    Map<String, String> settings = new HashMap<>(ReplicaSettings.properties(replicaId));
    settings.put("webhooks.database.api.max-size", "4");
    settings.put("webhooks.database.worker.max-size", "4");
    settings.put("server.port", "0");
    String[] arguments =
        settings.entrySet().stream()
            .map(setting -> "--" + setting.getKey() + "=" + setting.getValue())
            .toArray(String[]::new);
    return new SpringApplicationBuilder(ReliableWebhooksApplication.class)
        .profiles(profiles)
        .run(arguments);
  }
}
