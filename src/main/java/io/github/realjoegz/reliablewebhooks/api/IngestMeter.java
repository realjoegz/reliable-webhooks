package io.github.realjoegz.reliablewebhooks.api;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Counts the requests that the ingest route answered with one of its results, by result. The result
 * is read from the answer once the request is over, because the controller is not the only place
 * that decides it: the body limit refuses a request before the controller runs, and a database that
 * did not answer in time becomes a 503 after it. An answer with any other status has no result and
 * is not counted. A 500 is such an answer.
 */
@Component
class IngestMeter implements HandlerInterceptor, WebMvcConfigurer {

  private final Counter accepted;
  private final Counter replayed;
  private final Counter invalid;
  private final Counter busy;

  IngestMeter(MeterRegistry meters) {
    this.accepted = counter(meters, "accepted");
    this.replayed = counter(meters, "replayed");
    this.invalid = counter(meters, "invalid");
    this.busy = counter(meters, "busy");
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(this).addPathPatterns(EventController.PATH);
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception failure) {
    switch (HttpStatus.resolve(response.getStatus())) {
      case ACCEPTED ->
          (response.containsHeader(EventController.IDEMPOTENT_REPLAY) ? replayed : accepted)
              .increment();
      case BAD_REQUEST, CONTENT_TOO_LARGE, UNPROCESSABLE_CONTENT -> invalid.increment();
      case SERVICE_UNAVAILABLE -> busy.increment();
      case null, default -> {}
    }
  }

  // The Prometheus registry renders this name as webhooks_ingest_requests_total.
  private static Counter counter(MeterRegistry meters, String result) {
    return Counter.builder("webhooks.ingest.requests").tag("result", result).register(meters);
  }
}
