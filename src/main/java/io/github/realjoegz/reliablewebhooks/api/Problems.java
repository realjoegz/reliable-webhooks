package io.github.realjoegz.reliablewebhooks.api;

import java.net.URI;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Renders every error as an RFC 9457 problem document. */
@RestControllerAdvice
public class Problems extends ResponseEntityExceptionHandler {

  private static final String LOCK_TIMEOUT = "55P03";
  private static final String STATEMENT_TIMEOUT = "57014";

  public static ErrorResponseException unauthorized() {
    ErrorResponseException rejection = new ErrorResponseException(HttpStatus.UNAUTHORIZED);
    rejection.setDetail("A valid bearer token is required.");
    rejection.getHeaders().set(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
    return rejection;
  }

  public static ErrorResponseException tenantSuspended() {
    ProblemDetail problem =
        problem(
            HttpStatus.FORBIDDEN,
            "tenant_suspended",
            "Tenant suspended",
            "The tenant is suspended.");
    return new ErrorResponseException(HttpStatus.FORBIDDEN, problem, null);
  }

  @ExceptionHandler(Exception.class)
  ResponseEntity<ProblemDetail> unhandled(Exception failure) {
    if (databaseBusy(failure)) {
      ProblemDetail problem =
          problem(
              HttpStatus.SERVICE_UNAVAILABLE,
              "database_busy",
              "Database busy",
              "The database did not answer in time.");
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .header(HttpHeaders.RETRY_AFTER, "1")
          .body(problem);
    }
    logger.error("Unhandled failure", failure);
    return ResponseEntity.internalServerError()
        .body(ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR));
  }

  private static boolean databaseBusy(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      // HikariCP reports a borrow wait that ran out as a transient connection failure.
      if (cause instanceof SQLTransientConnectionException) {
        return true;
      }
      if (cause instanceof SQLException sql
          && (LOCK_TIMEOUT.equals(sql.getSQLState())
              || STATEMENT_TIMEOUT.equals(sql.getSQLState()))) {
        return true;
      }
    }
    return false;
  }

  // RFC 9457 allows a relative reference as the type. Clients compare it as a string.
  private static ProblemDetail problem(
      HttpStatus status, String type, String title, String detail) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
    problem.setType(URI.create(type));
    problem.setTitle(title);
    return problem;
  }
}
