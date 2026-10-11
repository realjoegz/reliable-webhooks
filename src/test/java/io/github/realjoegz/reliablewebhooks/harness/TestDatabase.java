package io.github.realjoegz.reliablewebhooks.harness;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * One PostgreSQL container for every integration test of a build. It starts when a test first needs
 * it, and Testcontainers removes it when the test JVM exits.
 */
public final class TestDatabase {

  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:18.6-alpine");

  private static final JdbcClient JDBC;

  static {
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
}
