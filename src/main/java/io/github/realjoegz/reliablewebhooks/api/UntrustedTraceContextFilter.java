package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.Traceparent;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Keeps the trace context that a caller sent away from the tracing filter of Spring, which would
 * continue the trace of the caller: the span of the request would get the trace id that the caller
 * chose, and would be sampled whenever the caller said that its own span was. Without the headers
 * that filter starts a trace of its own for every request.
 *
 * <p>It takes out the headers of both formats that the tracing of Spring Boot reads: those of the
 * W3C, and those of B3 in its one header and in its several. Which of them are read is a setting,
 * and the filter does not go by it.
 *
 * <p>A {@code traceparent} that is well formed is kept beside the request, for the ingest to attach
 * to its span as a link. The trace of the caller can then be found from the trace of the event, and
 * decides nothing in it.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class UntrustedTraceContextFilter extends OncePerRequestFilter {

  /** Request attribute holding the {@link SpanContext} of the caller, if it sent a valid one. */
  static final String CALLER = "webhooks.callerTraceContext";

  private static final Set<String> TRACE_CONTEXT_HEADERS =
      Set.of(
          Traceparent.HEADER,
          "tracestate",
          "baggage",
          "b3",
          "x-b3-traceid",
          "x-b3-spanid",
          "x-b3-parentspanid",
          "x-b3-sampled",
          "x-b3-flags");

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    callerContext(request).ifPresent(context -> request.setAttribute(CALLER, context));
    chain.doFilter(new WithoutTraceContext(request), response);
  }

  // A request with two of these headers does not say which of them is meant.
  private static Optional<SpanContext> callerContext(HttpServletRequest request) {
    List<String> sent = Collections.list(request.getHeaders(Traceparent.HEADER));
    return sent.size() == 1 ? Traceparent.parse(sent.getFirst()) : Optional.empty();
  }

  /** The request as it arrived, except that it has none of the trace context headers. */
  private static final class WithoutTraceContext extends HttpServletRequestWrapper {

    private WithoutTraceContext(HttpServletRequest request) {
      super(request);
    }

    @Override
    public String getHeader(String name) {
      return isTraceContext(name) ? null : super.getHeader(name);
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
      return isTraceContext(name) ? Collections.emptyEnumeration() : super.getHeaders(name);
    }

    @Override
    public Enumeration<String> getHeaderNames() {
      return Collections.enumeration(
          Collections.list(super.getHeaderNames()).stream()
              .filter(name -> !isTraceContext(name))
              .toList());
    }

    private static boolean isTraceContext(String header) {
      return TRACE_CONTEXT_HEADERS.contains(header.toLowerCase(Locale.ROOT));
    }
  }
}
