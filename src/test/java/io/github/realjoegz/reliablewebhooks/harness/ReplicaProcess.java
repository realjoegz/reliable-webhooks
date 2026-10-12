package io.github.realjoegz.reliablewebhooks.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * A replica that runs as a process of its own: the packaged jar in a child JVM, connected to {@link
 * TestDatabase}, listening on a free port. It is what a test sends a real signal to. Nothing of it
 * is left to the test: its delivery worker and its jobs run as they do in production, so a test has
 * to expect that it claims whatever is due.
 *
 * <p>It runs with the shipped configuration and the {@code unsafe-local} profile, like the replica
 * of {@link UnsafeLocalReplica}, so it can deliver to {@link FaultReceiver}. The jar does not hold
 * the configuration of that profile, which is a test resource, so the child is told where it lies.
 */
public final class ReplicaProcess implements AutoCloseable {

  /** The build sets this to the jar that it has packaged before the integration tests run. */
  private static final String JAR_PROPERTY = "replica.jar";

  private static final String PROFILE = "unsafe-local";
  private static final Pattern PORT = Pattern.compile("Tomcat started on port (\\d+)");
  private static final Duration STARTUP = Duration.ofMinutes(1);

  private final Process process;
  private final StringBuffer output;
  private final RestTestClient http;

  private ReplicaProcess(Process process, StringBuffer output, RestTestClient http) {
    this.process = process;
    this.output = output;
    this.http = http;
  }

  /** Starts a replica with the given id and returns once its readiness probe answers. */
  public static ReplicaProcess start(String replicaId) {
    return start(replicaId, ReplicaSettings.properties(replicaId), Map.of());
  }

  /** The same for a replica whose every connection to the database goes through the proxy. */
  public static ReplicaProcess startBehind(TcpProxy proxy, String replicaId) {
    return start(
        replicaId,
        ReplicaSettings.properties(replicaId, TestDatabase.jdbcUrlThroughLocalPort(proxy.port())),
        Map.of());
  }

  /** The same for a replica that finds the given variables in its environment. */
  public static ReplicaProcess startWith(Map<String, String> environment, String replicaId) {
    return start(replicaId, ReplicaSettings.properties(replicaId), environment);
  }

  /** A client for the API of this replica. */
  public RestTestClient http() {
    return http;
  }

  /** What the replica has written to its standard output and standard error so far. */
  public String output() {
    return output.toString();
  }

  public boolean isAlive() {
    return process.isAlive();
  }

  /** Sends SIGKILL and returns once the process is gone, so nothing of it runs afterwards. */
  public void kill() {
    process.destroyForcibly();
    try {
      process.waitFor();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while the replica was being killed");
    }
  }

  /**
   * Sends SIGTERM and returns at once. The replica shuts itself down, and what it writes while it
   * does is still captured.
   */
  public void terminate() {
    // Through the handle: Process.destroy also closes the pipe that the output arrives on.
    process.toHandle().destroy();
  }

  /** Waits for the process to end by itself and returns its exit code. */
  public int exitCode(Duration atMost) {
    try {
      assertThat(process.waitFor(atMost.toMillis(), TimeUnit.MILLISECONDS))
          .as("the replica ended within %s", atMost)
          .isTrue();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for the replica to end");
    }
    return process.exitValue();
  }

  /** Kills the replica if it is still running. */
  @Override
  public void close() {
    kill();
  }

  private static ReplicaProcess start(
      String replicaId, Map<String, String> settings, Map<String, String> environment) {
    List<String> command = new ArrayList<>();
    command.add(ProcessHandle.current().info().command().orElseThrow());
    // StartupInvariants refuses to start without it.
    command.add("-XX:+ExitOnOutOfMemoryError");
    command.add("-jar");
    command.add(jar().toString());
    command.add("--server.port=0");
    command.add("--spring.profiles.active=" + PROFILE);
    command.add("--spring.config.additional-location=" + testResources());
    settings.forEach((name, value) -> command.add("--" + name + "=" + value));

    ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
    builder.environment().putAll(environment);
    Process process;
    try {
      process = builder.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    StringBuffer output = new StringBuffer();
    Thread.ofVirtual().start(() -> copy(process.inputReader(), output, replicaId));
    try {
      RestTestClient http =
          RestTestClient.bindToServer(new JdkClientHttpRequestFactory())
              .baseUrl("http://127.0.0.1:" + port(process, output))
              .build();
      awaitReady(process, output, http);
      return new ReplicaProcess(process, output, http);
    } catch (RuntimeException | Error notStarted) {
      process.destroyForcibly();
      throw notStarted;
    }
  }

  private static Path jar() {
    String jar = System.getProperty(JAR_PROPERTY);
    if (jar == null || !Files.isRegularFile(Path.of(jar))) {
      throw new IllegalStateException(
          "The system property %s must name the packaged jar, as ./mvnw verify sets it. It is: %s"
              .formatted(JAR_PROPERTY, jar));
    }
    return Path.of(jar);
  }

  // The directory of the test resources, as the location that Spring Boot reads
  // application-unsafe-local.yaml from.
  private static String testResources() {
    try {
      Path profile =
          Path.of(
              ReplicaProcess.class.getResource("/application-%s.yaml".formatted(PROFILE)).toURI());
      return profile.getParent().toUri().toString();
    } catch (URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  // Every line is kept for the test and also goes to the output of the build, under the id of
  // the replica, where it is read when a test has failed.
  private static void copy(BufferedReader from, StringBuffer to, String replicaId) {
    try (from) {
      for (String line = from.readLine(); line != null; line = from.readLine()) {
        to.append(line).append('\n');
        System.out.println("[" + replicaId + "] " + line);
      }
    } catch (IOException processGone) {
      // The output ends with the process.
    }
  }

  // The replica listens on a port that the operating system chose, and names it when it has it.
  private static int port(Process process, StringBuffer output) {
    return await("the replica names its port")
        .atMost(STARTUP)
        .until(
            () -> {
              assertStillRunning(process, output);
              Matcher port = PORT.matcher(output.toString());
              return port.find()
                  ? OptionalInt.of(Integer.parseInt(port.group(1)))
                  : OptionalInt.empty();
            },
            OptionalInt::isPresent)
        .getAsInt();
  }

  private static void awaitReady(Process process, StringBuffer output, RestTestClient http) {
    await("the replica is ready")
        .atMost(STARTUP)
        .untilAsserted(
            () -> {
              assertStillRunning(process, output);
              http.get().uri("/actuator/health/readiness").exchange().expectStatus().isOk();
            });
  }

  private static void assertStillRunning(Process process, StringBuffer output) {
    if (!process.isAlive()) {
      throw new IllegalStateException("The replica ended while it was starting:\n" + output);
    }
  }
}
