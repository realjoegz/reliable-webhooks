package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.SpanContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class UntrustedTraceContextFilterTest {

  private static final String TRACE = "0af7651916cd43dd8448eb211c80319c";
  private static final String SPAN = "b7ad6b7169203331";
  private static final String SAMPLED = "00-" + TRACE + "-" + SPAN + "-01";

  private final UntrustedTraceContextFilter filter = new UntrustedTraceContextFilter();

  // The headers of the W3C, and those of B3 in its one header and in its several. The names of
  // headers are not case sensitive, and the filter behind this one would read them under any
  // spelling.
  @Test
  void traceContextHeadersAreKeptFromTheRestOfTheChain() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/v1/events");
    request.addHeader("TraceParent", SAMPLED);
    request.addHeader("TRACESTATE", "vendor=value");
    request.addHeader("Baggage", "user=someone");
    request.addHeader("B3", TRACE + "-" + SPAN + "-1");
    request.addHeader("X-B3-TraceId", TRACE);
    request.addHeader("X-B3-SpanId", SPAN);
    request.addHeader("X-B3-ParentSpanId", SPAN);
    request.addHeader("X-B3-Sampled", "1");
    request.addHeader("X-B3-Flags", "1");
    request.addHeader("Idempotency-Key", "a-key");

    HttpServletRequest passedOn = passedOn(request);

    for (String name :
        new String[] {
          "traceparent",
          "TraceParent",
          "tracestate",
          "TRACESTATE",
          "baggage",
          "b3",
          "B3",
          "x-b3-traceid",
          "X-B3-TraceId",
          "x-b3-spanid",
          "x-b3-parentspanid",
          "x-b3-sampled",
          "x-b3-flags"
        }) {
      assertThat(passedOn.getHeader(name)).as(name).isNull();
      assertThat(Collections.list(passedOn.getHeaders(name))).as(name).isEmpty();
    }
    assertThat(Collections.list(passedOn.getHeaderNames())).containsExactly("Idempotency-Key");
    assertThat(passedOn.getHeader("Idempotency-Key")).isEqualTo("a-key");
    assertThat(Collections.list(passedOn.getHeaders("idempotency-key"))).containsExactly("a-key");
  }

  @Test
  void validTraceparentIsKeptBesideTheRequestAsTheContextOfTheCaller() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/v1/events");
    request.addHeader("traceparent", SAMPLED);

    HttpServletRequest passedOn = passedOn(request);

    assertThat(passedOn.getAttribute(UntrustedTraceContextFilter.CALLER))
        .isInstanceOfSatisfying(
            SpanContext.class,
            caller -> {
              assertThat(caller.getTraceId()).isEqualTo(TRACE);
              assertThat(caller.getSpanId()).isEqualTo(SPAN);
              assertThat(caller.isSampled()).isTrue();
              assertThat(caller.isRemote()).isTrue();
            });
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
        "00-00000000000000000000000000000000-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-more",
        "not a traceparent"
      })
  void traceparentThatIsNotValidIsDroppedWithoutATrace(String value) throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/v1/events");
    request.addHeader("traceparent", value);

    HttpServletRequest passedOn = passedOn(request);

    assertThat(passedOn.getAttribute(UntrustedTraceContextFilter.CALLER)).isNull();
    assertThat(passedOn.getHeader("traceparent")).isNull();
  }

  @Test
  void requestWithTwoTraceparentsHasNoContextOfTheCaller() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/v1/events");
    request.addHeader("traceparent", SAMPLED);
    request.addHeader("traceparent", "00-" + TRACE + "-00f067aa0ba902b7-01");

    HttpServletRequest passedOn = passedOn(request);

    assertThat(passedOn.getAttribute(UntrustedTraceContextFilter.CALLER)).isNull();
    assertThat(Collections.list(passedOn.getHeaders("traceparent"))).isEmpty();
  }

  @Test
  void requestWithoutATraceContextKeepsItsHeaders() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest("GET", "/v1/endpoints");
    request.addHeader("Authorization", "Bearer a-key");

    HttpServletRequest passedOn = passedOn(request);

    assertThat(passedOn.getAttribute(UntrustedTraceContextFilter.CALLER)).isNull();
    assertThat(Collections.list(passedOn.getHeaderNames())).containsExactly("Authorization");
  }

  // The request as the filter hands it to the filter behind it.
  private HttpServletRequest passedOn(MockHttpServletRequest request)
      throws ServletException, IOException {
    MockFilterChain chain = new MockFilterChain();
    filter.doFilter(request, new MockHttpServletResponse(), chain);
    return (HttpServletRequest) chain.getRequest();
  }
}
