package io.github.realjoegz.reliablewebhooks.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class EventTypeTest {

  static Stream<String> eventTypes() {
    return Stream.of("a", "order.created", "order_created-v2", "0", "a".repeat(128));
  }

  @ParameterizedTest
  @MethodSource("eventTypes")
  void oneToOneHundredTwentyEightCharactersOfTheAlphabetAreAnEventType(String candidate) {
    assertThat(EventType.isValid(candidate)).isTrue();
  }

  static Stream<String> notEventTypes() {
    return Stream.of(
        "",
        "a".repeat(129),
        "Order.created",
        "order created",
        "order/created",
        "*",
        "order.*",
        "order.created\n",
        "ordér.created");
  }

  @ParameterizedTest
  @MethodSource("notEventTypes")
  void anythingElseIsNotAnEventType(String candidate) {
    assertThat(EventType.isValid(candidate)).isFalse();
  }
}
