package io.github.realjoegz.reliablewebhooks.harness;

import io.github.realjoegz.reliablewebhooks.ReliableWebhooksApplication;
import io.github.realjoegz.reliablewebhooks.delivery.PollerHeartbeat;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.http.server.LocalTestWebServer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * A second replica inside the test JVM that starts the way the application starts. Nothing of it is
 * left to the tests: Spring starts its delivery worker and its jobs, and closing it stops them. A
 * test uses it to show what a replica does by itself, and closes it before it ends.
 */
public final class SelfStartedReplica {

  private SelfStartedReplica() {}

  public static ConfigurableApplicationContext start(String replicaId, String... profiles) {
    return builder(profiles).run(arguments(replicaId, Map.of()));
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
        .run(arguments(replicaId, Map.of()));
  }

  /**
   * The same replica with the allowance of the {@code unsafe-local} profile, for a test that reads
   * its spans. It samples the given share of its traces, where a replica samples a tenth of them
   * unless it is told otherwise, and it exports its spans to the exporter of the test.
   */
  public static ConfigurableApplicationContext start(
      String replicaId, double sampledShare, SpanExporter exporter) {
    return builder("unsafe-local")
        .initializers(
            context ->
                ((GenericApplicationContext) context)
                    .registerBean("spanExporterOfTheTest", SpanExporter.class, () -> exporter))
        .run(
            arguments(
                replicaId,
                Map.of("management.tracing.sampling.probability", Double.toString(sampledShare))));
  }

  /**
   * The same replica with a collector named, at the given URL. It samples every trace and exports
   * its spans there, as a replica does once its configuration names a collector.
   */
  public static ConfigurableApplicationContext startExportingTo(
      String collectorUrl, String replicaId) {
    return builder()
        .run(
            arguments(
                replicaId,
                Map.of(
                    "management.tracing.sampling.probability",
                    "1.0",
                    "management.opentelemetry.tracing.export.otlp.endpoint",
                    collectorUrl)));
  }

  /** A client for the API of a replica that one of the starts above returned. */
  public static RestTestClient clientOf(ConfigurableApplicationContext replica) {
    return RestTestClient.bindToServer(new JdkClientHttpRequestFactory())
        .uriBuilderFactory(LocalTestWebServer.obtain(replica).uriBuilderFactory())
        .build();
  }

  private static SpringApplicationBuilder builder(String... profiles) {
    return new SpringApplicationBuilder(ReliableWebhooksApplication.class).profiles(profiles);
  }

  // The settings go in as arguments, which count for more than the configuration that ships. The
  // pools are small because the replicas that the build keeps have theirs open already.
  private static String[] arguments(String replicaId, Map<String, String> ofTheTest) {
    Map<String, String> settings = new HashMap<>(ReplicaSettings.properties(replicaId));
    settings.put("webhooks.database.api.max-size", "4");
    settings.put("webhooks.database.worker.max-size", "4");
    settings.put("server.port", "0");
    settings.putAll(ofTheTest);
    return settings.entrySet().stream()
        .map(setting -> "--" + setting.getKey() + "=" + setting.getValue())
        .toArray(String[]::new);
  }
}
