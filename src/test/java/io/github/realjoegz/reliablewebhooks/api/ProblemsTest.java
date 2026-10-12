package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.web.ErrorResponseException;

class ProblemsTest {

  private final Problems problems = new Problems();

  @Test
  void lockTimeoutIsDatabaseBusy() {
    SQLException lockTimeout = new SQLException("canceling statement due to lock timeout", "55P03");

    assertDatabaseBusy(problems.unhandled(new CannotAcquireLockException("update", lockTimeout)));
  }

  @Test
  void statementTimeoutIsDatabaseBusy() {
    SQLException statementTimeout =
        new SQLException("canceling statement due to statement timeout", "57014");

    assertDatabaseBusy(problems.unhandled(new QueryTimeoutException("select", statementTimeout)));
  }

  @Test
  void exhaustedBorrowWaitIsDatabaseBusy() {
    SQLException borrowTimeout =
        new SQLTransientConnectionException("api - Connection is not available");

    assertDatabaseBusy(
        problems.unhandled(new CannotGetJdbcConnectionException("borrow", borrowTimeout)));
  }

  @Test
  void exhaustedBorrowWaitAtTransactionStartIsDatabaseBusy() {
    SQLException borrowTimeout =
        new SQLTransientConnectionException("api - Connection is not available");

    assertDatabaseBusy(
        problems.unhandled(
            new CannotCreateTransactionException(
                "begin", new CannotGetJdbcConnectionException("borrow", borrowTimeout))));
  }

  // A statement on a connection that the pool handed out without testing it, because it had been
  // used a moment before, and that the database had closed since.
  @Test
  void failedConnectionIsDatabaseBusy() {
    SQLException connectionFailure =
        new SQLException("An I/O error occurred while sending to the backend.", "08006");

    assertDatabaseBusy(
        problems.unhandled(new DataAccessResourceFailureException("select", connectionFailure)));
  }

  // The same inside a transaction: the rollback fails on that connection too, and Spring throws
  // the failure of the rollback with the failure of the statement beside it.
  @Test
  void statementOfATransactionThatWasEndedByADatabaseShuttingDownIsDatabaseBusy() {
    TransactionSystemException rollbackFailed =
        new TransactionSystemException(
            "JDBC rollback failed", new SQLException("Connection is closed"));
    rollbackFailed.initApplicationException(
        new UncategorizedSQLException(
            "insert",
            "",
            new SQLException("terminating connection due to administrator command", "57P01")));

    assertDatabaseBusy(problems.unhandled(rollbackFailed));
  }

  @Test
  void otherDatabaseErrorIsAnInternalErrorWithoutDetail() {
    SQLException uniqueViolation = new SQLException("duplicate key value", "23505");

    ResponseEntity<ProblemDetail> response =
        problems.unhandled(new DuplicateKeyException("insert", uniqueViolation));

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
    assertThat(response.getBody().getType()).isNull();
    assertThat(response.getBody().getDetail()).isNull();
  }

  @Test
  void failureWithoutSqlStateIsAnInternalError() {
    ResponseEntity<ProblemDetail> response =
        problems.unhandled(new IllegalStateException(new SQLException("no state")));

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  void unauthorizedChallengesForABearerToken() {
    ErrorResponseException rejection = Problems.unauthorized();

    assertThat(rejection.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(rejection.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE)).isEqualTo("Bearer");
    assertThat(rejection.getBody().getType()).isNull();
  }

  @Test
  void suspendedTenantIsForbiddenWithItsOwnType() {
    ErrorResponseException rejection = Problems.tenantSuspended();

    assertThat(rejection.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(rejection.getBody().getType()).isEqualTo(URI.create("tenant_suspended"));
    assertThat(rejection.getBody().getStatus()).isEqualTo(403);
  }

  @Test
  void refusedEndpointUrlIsUnprocessableWithItsOwnType() {
    ErrorResponseException rejection = Problems.endpointUrlNotAllowed();

    assertThat(rejection.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
    assertThat(rejection.getBody().getType()).isEqualTo(URI.create("endpoint_url_not_allowed"));
    assertThat(rejection.getBody().getStatus()).isEqualTo(422);
  }

  @Test
  void busyLookupAsksTheCallerToComeBackInTwoSeconds() {
    ErrorResponseException rejection = Problems.lookupBusy();

    assertThat(rejection.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    assertThat(rejection.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("2");
    assertThat(rejection.getBody().getType()).isEqualTo(URI.create("lookup_busy"));
    assertThat(rejection.getBody().getStatus()).isEqualTo(429);
  }

  @Test
  void reusedIdempotencyKeyIsUnprocessableWithItsOwnType() {
    ErrorResponseException rejection = Problems.idempotencyKeyReused();

    assertThat(rejection.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
    assertThat(rejection.getBody().getType()).isEqualTo(URI.create("idempotency_key_reused"));
    assertThat(rejection.getBody().getStatus()).isEqualTo(422);
  }

  private static void assertDatabaseBusy(ResponseEntity<ProblemDetail> response) {
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
    assertThat(response.getBody().getType()).isEqualTo(URI.create("database_busy"));
    assertThat(response.getBody().getStatus()).isEqualTo(503);
  }
}
