package io.github.realjoegz.reliablewebhooks.harness;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * One PostgreSQL container for every integration test of a build. It starts when a test first needs
 * it, and Testcontainers removes it when the test JVM exits.
 *
 * <p>One test stops the container and starts it again, to restart the database under replicas that
 * keep running. Docker gives a container that it starts again another host port, unless the port
 * was named when the container was created. The container is therefore created with a host port
 * that was free at that moment, and keeps it for the whole build.
 */
public final class TestDatabase {

  private static final int POSTGRES_PORT = 5432;

  // A replica holds two pools of 16 connections. The two replicas that the build keeps inside the
  // test JVM have theirs open all the time, and a test starts up to two more as processes of
  // their own. The default of 100 connections is not enough for that.
  private static final int MAX_CONNECTIONS = 300;

  private static final Duration RESTART = Duration.ofMinutes(1);

  // The command that Testcontainers gives the container turns fsync off, because a test keeps
  // no data. This one adds the limit on connections.
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:18.6-alpine")
          .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=" + MAX_CONNECTIONS);

  private static final JdbcClient JDBC;

  static {
    POSTGRES.setPortBindings(List.of(freePort() + ":" + POSTGRES_PORT));
    POSTGRES.start();
    JDBC =
        JdbcClient.create(
            new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
  }

  private TestDatabase() {}

  public static String jdbcUrl() {
    return POSTGRES.getJdbcUrl();
  }

  /** The URL of the database for a replica that reaches it through a port of this machine. */
  public static String jdbcUrlThroughLocalPort(int port) {
    return "jdbc:postgresql://127.0.0.1:%d/%s".formatted(port, POSTGRES.getDatabaseName());
  }

  public static String host() {
    return POSTGRES.getHost();
  }

  public static int port() {
    return POSTGRES.getMappedPort(POSTGRES_PORT);
  }

  public static String username() {
    return POSTGRES.getUsername();
  }

  public static String password() {
    return POSTGRES.getPassword();
  }

  /** The test's own access, outside the pools of the service under test. */
  public static JdbcClient jdbc() {
    return JDBC;
  }

  /**
   * A connection with autocommit off, for a transaction that the test holds open while the service
   * under test runs into its locks.
   */
  public static Connection connection() throws SQLException {
    Connection connection =
        DriverManager.getConnection(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    connection.setAutoCommit(false);
    return connection;
  }

  /**
   * Stops the container, and with it the database: the server ends every session and then itself.
   * The data stays. Returns once the container has stopped.
   */
  public static void stop() {
    POSTGRES.getDockerClient().stopContainerCmd(POSTGRES.getContainerId()).exec();
  }

  /** Starts the stopped container again. Returns once the database answers on its port. */
  public static void start() {
    POSTGRES.getDockerClient().startContainerCmd(POSTGRES.getContainerId()).exec();
    await("the database answers again")
        .atMost(RESTART)
        .ignoreExceptions()
        .until(() -> JDBC.sql("SELECT 1").query(Integer.class).single() == 1);
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
