package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.DatabaseFailures;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import java.net.URI;
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

  /**
   * The answer to a charge that the bucket of the tenant refused. The refusal was decided by the
   * database. Whether waiting can help is worked out here, from the rate and the burst that
   * authentication loaded with the tenant, which can be up to 30 seconds old.
   *
   * <p>A bucket that fills refills what the charge costs within the time that the answer names. A
   * bucket that does not fill, or that never holds as much as the charge costs, gets no time.
   */
  public static ErrorResponseException rateLimited(Tenant tenant, int cost) {
    if (tenant.ratePerSec() == 0 || cost > tenant.burst()) {
      ProblemDetail problem =
          problem(
              HttpStatus.TOO_MANY_REQUESTS,
              "rate_limit_unsatisfiable",
              "Rate limit unsatisfiable",
              "The request costs more than the rate limit of the tenant ever allows.");
      return new ErrorResponseException(HttpStatus.TOO_MANY_REQUESTS, problem, null);
    }
    ProblemDetail problem =
        problem(
            HttpStatus.TOO_MANY_REQUESTS,
            "rate_limited",
            "Rate limited",
            "The rate limit of the tenant is used up.");
    ErrorResponseException rejection =
        new ErrorResponseException(HttpStatus.TOO_MANY_REQUESTS, problem, null);
    long refillSeconds = (long) Math.ceil(cost / tenant.ratePerSec());
    rejection.getHeaders().set(HttpHeaders.RETRY_AFTER, Long.toString(refillSeconds));
    return rejection;
  }

  public static ErrorResponseException concurrencyLimit() {
    ProblemDetail problem =
        problem(
            HttpStatus.TOO_MANY_REQUESTS,
            "concurrency_limit",
            "Concurrency limit",
            "The tenant has too many requests open on this replica.");
    ErrorResponseException rejection =
        new ErrorResponseException(HttpStatus.TOO_MANY_REQUESTS, problem, null);
    rejection.getHeaders().set(HttpHeaders.RETRY_AFTER, "1");
    return rejection;
  }

  public static ErrorResponseException idempotencyKeyReused() {
    ProblemDetail problem =
        problem(
            HttpStatus.UNPROCESSABLE_CONTENT,
            "idempotency_key_reused",
            "Idempotency key reused",
            "The Idempotency-Key belongs to an event that was sent with a different body.");
    return new ErrorResponseException(HttpStatus.UNPROCESSABLE_CONTENT, problem, null);
  }

  @ExceptionHandler(Exception.class)
  ResponseEntity<ProblemDetail> unhandled(Exception failure) {
    if (DatabaseFailures.isTransient(failure)) {
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

  // RFC 9457 allows a relative reference as the type. Clients compare it as a string.
  private static ProblemDetail problem(
      HttpStatus status, String type, String title, String detail) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
    problem.setType(URI.create(type));
    problem.setTitle(title);
    return problem;
  }
}
