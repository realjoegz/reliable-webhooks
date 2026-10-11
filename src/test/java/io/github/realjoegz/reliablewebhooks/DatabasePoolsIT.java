package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Database;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class DatabasePoolsIT {

  private record Bounds(
      long borrowWaitMillis,
      long alivenessTestMillis,
      int connectTimeoutSeconds,
      String lockTimeout,
      String statementTimeout,
      int socketTimeoutMillis,
      String idleInTransactionTimeout,
      boolean tcpKeepAlive) {}

  private static final Bounds API = new Bounds(3_000, 1_000, 2, "2s", "5s", 10_000, "10s", true);
  private static final Bounds WORKER =
      new Bounds(3_000, 1_000, 2, "5s", "10s", 15_000, "10s", true);

  @Autowired
  @Qualifier("apiDataSource")
  HikariDataSource api;

  @Autowired
  @Qualifier("workerDataSource")
  HikariDataSource worker;

  @Autowired WebhooksProperties properties;
  @Autowired Flyway flyway;
  @Autowired RestTestClient http;

  @Test
  void apiPoolRunsWithItsTimeBounds() throws SQLException {
    assertThat(api.getMaximumPoolSize()).isEqualTo(16);
    assertThat(boundsOf(api)).isEqualTo(API);
  }

  @Test
  void workerPoolRunsWithItsTimeBounds() throws SQLException {
    assertThat(worker.getMaximumPoolSize()).isEqualTo(16);
    assertThat(boundsOf(worker)).isEqualTo(WORKER);
  }

  // The reset also takes away what the driver set after it had connected, among it the
  // application name that PgWait knows the replica by. The connection is therefore closed at the
  // end and does not go back into the pool, where a later test would get it.
  @Test
  void resetDoesNotRemoveTheSessionLimits() throws SQLException {
    try (Connection connection = api.getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("RESET ALL");

        assertThat(show(statement, "lock_timeout")).isEqualTo(API.lockTimeout());
        assertThat(show(statement, "statement_timeout")).isEqualTo(API.statementTimeout());
        assertThat(show(statement, "idle_in_transaction_session_timeout"))
            .isEqualTo(API.idleInTransactionTimeout());
      } finally {
        api.evictConnection(connection);
      }
    }
  }

  @Test
  void urlParametersCannotRemoveATimeBound() throws SQLException {
    Database database = properties.database();
    String url =
        database.url()
            + (database.url().contains("?") ? "&" : "?")
            + "connectTimeout=0&socketTimeout=0&tcpKeepAlive=false"
            + "&options=-c%20lock_timeout=0%20-c%20statement_timeout=0";
    WebhooksProperties withUrlParameters =
        new WebhooksProperties(
            properties.adminToken(),
            properties.secretEncryptionKey(),
            properties.replicaId(),
            new Database(
                url, database.username(), database.password(), database.api(), database.worker()),
            properties.delivery(),
            properties.ssrf());

    try (HikariDataSource pool = new DatabasePools().apiDataSource(withUrlParameters)) {
      assertThat(boundsOf(pool)).isEqualTo(API);
    }
  }

  @Test
  void flywayConnectsOutsideThePoolsAndWithoutTheirLimits() throws SQLException {
    DataSource migrations = flyway.getConfiguration().getDataSource();

    assertThat(migrations).isNotSameAs(api).isNotSameAs(worker);
    try (Connection connection = migrations.getConnection();
        Statement statement = connection.createStatement()) {
      assertThat(show(statement, "lock_timeout")).isEqualTo("0");
      assertThat(show(statement, "statement_timeout")).isEqualTo("0");
    }
  }

  // With every connection of both pools taken, a health check that needed one would wait for the
  // borrow timeout and then report the service as down.
  @Test
  void healthEndpointBorrowsNoPooledConnection() throws SQLException {
    List<Connection> borrowed = new ArrayList<>();
    try {
      for (HikariDataSource pool : List.of(api, worker)) {
        for (int i = 0; i < pool.getMaximumPoolSize(); i++) {
          borrowed.add(pool.getConnection());
        }
      }

      http.get()
          .uri("/actuator/health")
          .exchange()
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.status")
          .isEqualTo("UP");
    } finally {
      for (Connection connection : borrowed) {
        connection.close();
      }
    }
  }

  private static Bounds boundsOf(HikariDataSource pool) throws SQLException {
    PGSimpleDataSource driver = (PGSimpleDataSource) pool.getDataSource();
    try (Connection connection = pool.getConnection();
        Statement statement = connection.createStatement()) {
      return new Bounds(
          pool.getConnectionTimeout(),
          pool.getValidationTimeout(),
          driver.getConnectTimeout(),
          show(statement, "lock_timeout"),
          show(statement, "statement_timeout"),
          connection.getNetworkTimeout(),
          show(statement, "idle_in_transaction_session_timeout"),
          driver.getTcpKeepAlive());
    }
  }

  private static String show(Statement statement, String setting) throws SQLException {
    try (ResultSet result = statement.executeQuery("SHOW " + setting)) {
      result.next();
      return result.getString(1);
    }
  }
}
