package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TraceparentTest {

  private static final String TRACE = "0af7651916cd43dd8448eb211c80319c";
  private static final String SPAN = "b7ad6b7169203331";

  @Test
  void valueOfVersion00IsReadWithItsIdsAndItsFlags() {
    Optional<SpanContext> sampled = Traceparent.parse("00-" + TRACE + "-" + SPAN + "-01");
    Optional<SpanContext> notSampled = Traceparent.parse("00-" + TRACE + "-" + SPAN + "-00");

    assertThat(sampled).isPresent();
    assertThat(sampled.get().getTraceId()).isEqualTo(TRACE);
    assertThat(sampled.get().getSpanId()).isEqualTo(SPAN);
    assertThat(sampled.get().isSampled()).isTrue();
    assertThat(sampled.get().isRemote()).isTrue();
    assertThat(notSampled).isPresent();
    assertThat(notSampled.get().isSampled()).isFalse();
  }

  // A value of 55 characters in lower-case hex, with the version 00 and ids that are not all
  // zeros, is read. Each of these breaks one part of that rule.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-B7AD6B7169203331-01",
        "00-00000000000000000000000000000000-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
        "00-0af7651916cd43dd8448eb211c80319-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333-01",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-1",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-more",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01 ",
        " 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01",
        "00_0af7651916cd43dd8448eb211c80319c_b7ad6b7169203331_01"
      })
  void valueThatIsNotATraceparentOfVersion00IsNotRead(String value) {
    assertThat(Traceparent.parse(value)).isEmpty();
  }

  @Test
  void noValueIsNotRead() {
    assertThat(Traceparent.parse(null)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
      })
  void contextIsWrittenAsItWasRead(String value) {
    assertThat(Traceparent.parse(value).flatMap(Traceparent::of)).contains(value);
  }

  @Test
  void contextOfASpanIsWrittenWithTheVersionItsIdsAndItsFlags() {
    SpanContext context =
        SpanContext.create(TRACE, SPAN, TraceFlags.getSampled(), TraceState.getDefault());

    assertThat(Traceparent.of(context)).contains("00-" + TRACE + "-" + SPAN + "-01");
  }

  @Test
  void contextThatNamesNoSpanHasNoText() {
    assertThat(Traceparent.of(SpanContext.getInvalid())).isEmpty();
  }
}
