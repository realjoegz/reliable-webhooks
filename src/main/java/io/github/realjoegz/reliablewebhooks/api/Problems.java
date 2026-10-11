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

  /**
   * One answer for every URL the policy refuses. It does not say whether the host resolved, so a
   * registration cannot be used to look into the network of the service.
   */
  public static ErrorResponseException endpointUrlNotAllowed() {
    ProblemDetail problem =
        problem(
            HttpStatus.UNPROCESSABLE_CONTENT,
            "endpoint_url_not_allowed",
            "Endpoint URL not allowed",
            "The URL must use https, have no userinfo, use port 443 or 1024 to 65535, have at"
                + " most 2048 characters and name a host that resolves to public addresses only.");
    return new ErrorResponseException(HttpStatus.UNPROCESSABLE_CONTENT, problem, null);
  }

  public static ErrorResponseException lookupBusy() {
    ProblemDetail problem =
        problem(
            HttpStatus.TOO_MANY_REQUESTS,
            "lookup_busy",
            "Lookup busy",
            "The host of the URL could not be looked up right now.");
    ErrorResponseException rejection =
        new ErrorResponseException(HttpStatus.TOO_MANY_REQUESTS, problem, null);
    rejection.getHeaders().set(HttpHeaders.RETRY_AFTER, "2");
    return rejection;
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
