package io.github.realjoegz.reliablewebhooks;

import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The context of a span as text, in the form of the W3C {@code traceparent} header. An event keeps
 * the context of its ingest span in this form, and a request to a subscriber carries the context of
 * its attempt span in it.
 *
 * <p>Only version 00 is read: 55 characters, in lower-case hex, with a trace id and a span id that
 * are not all zeros. The same rule holds for what a caller sent and for what the service stored.
 */
public final class Traceparent {

  public static final String HEADER = "traceparent";

  private static final Pattern VERSION_00 =
      Pattern.compile("00-(?<trace>[0-9a-f]{32})-(?<span>[0-9a-f]{16})-(?<flags>[0-9a-f]{2})");

  private Traceparent() {}

  /** Empty for a value that is null or not a traceparent of version 00. */
  public static Optional<SpanContext> parse(String value) {
    if (value == null) {
      return Optional.empty();
    }
    Matcher parts = VERSION_00.matcher(value);
    if (!parts.matches()) {
      return Optional.empty();
    }
    SpanContext context =
        SpanContext.createFromRemoteParent(
            parts.group("trace"),
            parts.group("span"),
            TraceFlags.fromHex(parts.group("flags"), 0),
            TraceState.getDefault());
    // An id of zeros has the right form and names nothing.
    return context.isValid() ? Optional.of(context) : Optional.empty();
  }

  /** Empty for a context that names no span, which is what a replica without tracing has. */
  public static Optional<String> of(SpanContext context) {
    if (!context.isValid()) {
      return Optional.empty();
    }
    return Optional.of(
        "00-%s-%s-%s"
            .formatted(context.getTraceId(), context.getSpanId(), context.getTraceFlags().asHex()));
  }
}
