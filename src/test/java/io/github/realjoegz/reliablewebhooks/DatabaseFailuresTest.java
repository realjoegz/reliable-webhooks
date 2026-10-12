package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;

class DatabaseFailuresTest {

  // Connection failures and socket timeouts (class 08), a database that is shutting down or
  // starting up, a deadlock, a lock timeout and a statement timeout.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "08000", "08001", "08003", "08006", "57P01", "57P02", "57P03", "40P01", "55P03", "57014"
      })
  void failureWithAStateThatPassesIsTransient(String sqlState) {
    assertThat(DatabaseFailures.isTransient(statementFailure(sqlState))).isTrue();
  }

  // A constraint, a value that does not fit its column, a statement that is wrong, and a server
  // that is out of connections.
  @ParameterizedTest
  @ValueSource(strings = {"23505", "23503", "22001", "42P01", "53300"})
  void failureWithAnyOtherStateIsNotTransient(String sqlState) {
    assertThat(DatabaseFailures.isTransient(statementFailure(sqlState))).isFalse();
  }

  @Test
  void borrowWaitThatRanOutIsTransientWhereverItLiesInTheChain() {
    SQLException borrowTimeout =
        new SQLTransientConnectionException("worker - Connection is not available");

    assertThat(DatabaseFailures.isTransient(borrowTimeout)).isTrue();
    assertThat(
            DatabaseFailures.isTransient(
                new CannotCreateTransactionException(
                    "begin", new CannotGetJdbcConnectionException("borrow", borrowTimeout))))
        .isTrue();
  }

  // What a transaction throws whose statement ran on a connection that the database had closed:
  // the rollback fails on the same connection, and the pool reports that without a state.
  @Test
  void failureOfAStatementIsFoundBehindTheRollbackThatFailedAfterIt() {
    assertThat(DatabaseFailures.isTransient(rollbackFailedAfter(statementFailure("57P01"))))
        .isTrue();
  }

  // The rollback failed, but what made it necessary was a fault. Trying again would repeat it.
  @Test
  void rollbackThatFailedAfterAFaultIsNotTransient() {
    assertThat(
            DatabaseFailures.isTransient(
                rollbackFailedAfter(
                    new DuplicateKeyException("insert", new SQLException("duplicate", "23505")))))
        .isFalse();
  }

  // A commit that failed has no failure of a statement beside it. Its own state decides.
  @Test
  void commitThatFailedOnItsConnectionIsTransient() {
    assertThat(
            DatabaseFailures.isTransient(
                new TransactionSystemException(
                    "JDBC commit failed", new SQLException("I/O error", "08006"))))
        .isTrue();
  }

  @Test
  void failureWithoutAStateIsNotTransient() {
    assertThat(
            DatabaseFailures.isTransient(new IllegalStateException(new SQLException("no state"))))
        .isFalse();
  }

  @Test
  void failureThatIsNoDatabaseErrorIsNotTransient() {
    assertThat(DatabaseFailures.isTransient(new IllegalStateException("a fault of the service")))
        .isFalse();
  }

  private static RuntimeException statementFailure(String sqlState) {
    return new DataAccessResourceFailureException(
        "A statement failed", new SQLException("reported by the database", sqlState));
  }

  private static TransactionSystemException rollbackFailedAfter(RuntimeException statementFailure) {
    TransactionSystemException rollbackFailed =
        new TransactionSystemException(
            "JDBC rollback failed", new SQLException("Connection is closed"));
    rollbackFailed.initApplicationException(statementFailure);
    return rollbackFailed;
  }
}
