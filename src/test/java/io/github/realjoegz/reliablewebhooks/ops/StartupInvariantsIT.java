package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@InProcessReplica
class StartupInvariantsIT {

  @Autowired WebhooksProperties properties;
  @Autowired SessionSettingsRepository sessions;
  @Autowired Environment environment;

  @Autowired
  @Qualifier("apiDataSource")
  DataSource api;

  @Autowired
  @Qualifier("workerDataSource")
  DataSource worker;

  @Test
  void sessionSettingsAreReadFromASessionOfTheGivenPool() {
    assertThat(sessions.read(api)).isEqualTo(session(2, 5, 10));
    assertThat(sessions.read(worker)).isEqualTo(session(5, 10, 10));
  }

  // The configuration still asks for every limit. Only the session shows that none is in force.
  @Test
  void poolWhoseSessionsHaveNoLimitsIsRefused() {
    DataSource withoutLimits =
        new DriverManagerDataSource(
            TestDatabase.jdbcUrl(), TestDatabase.username(), TestDatabase.password());
    assertThat(sessions.read(withoutLimits)).isEqualTo(session(0, 0, 0));

    StartupInvariants invariants =
        new StartupInvariants(properties, sessions, withoutLimits, worker, environment);

    assertThatThrownBy(invariants::afterPropertiesSet)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "api pool: need 0 < lock_timeout < statement_timeout < socketTimeout,"
                + " got 0 ms, 0 ms, 10000 ms")
        .hasMessageContaining("api pool: idle_in_transaction_session_timeout must be set")
        .hasMessageNotContaining("worker pool:");
  }

  private static SessionSettings session(
      int lockTimeoutSeconds, int statementTimeoutSeconds, int idleInTransactionTimeoutSeconds) {
    return new SessionSettings(
        "read committed",
        "on",
        Duration.ofSeconds(lockTimeoutSeconds),
        Duration.ofSeconds(statementTimeoutSeconds),
        Duration.ofSeconds(idleInTransactionTimeoutSeconds));
  }
}
