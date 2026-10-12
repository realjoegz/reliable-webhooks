package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The statements of the worker, of the circuit breaker and of the single replay are the statements
 * that the design writes out. Each is looked for in the design as it is in the code, with its
 * comments, apart from how its lines are laid out. The insert for an abandoned attempt is not among
 * them: the design gives its values in a sentence.
 */
class DeliveryStatementsTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("statements")
  void statementIsWrittenInTheDesign(String name, String statement) throws IOException {
    String design = Files.readString(Path.of("docs/design.md"));

    assertThat(withoutLayout(design)).contains(withoutLayout(statement));
  }

  // The startup invariants bound an attempt by the probe slot, which they read from a constant.
  // The gate holds that time as text, as the design writes it.
  @Test
  void gateTakesTheProbeSlotThatTheStartupInvariantsRead() {
    assertThat(CircuitBreaker.GATE)
        .contains("now() + interval '%d seconds'".formatted(CircuitBreaker.PROBE_SLOT.toSeconds()));
  }

  static Stream<Arguments> statements() {
    return Stream.of(
        arguments("claim", DeliveryRepository.CLAIM),
        arguments("release", DeliveryRepository.RELEASE),
        arguments("load", DeliveryRepository.LOAD),
        arguments("F1", DeliveryRepository.FENCE_AND_CHANGE_STATE),
        arguments("F2", DeliveryRepository.INSERT_ATTEMPT),
        arguments("gate", CircuitBreaker.GATE),
        arguments("F3 success", CircuitBreaker.CLOSE_ON_SUCCESS),
        arguments("F3 failure", CircuitBreaker.COUNT_FAILURE),
        arguments("F3 probe failure", CircuitBreaker.BACK_OFF_PROBE),
        arguments("replay, endpoint lock", TenantDeliveryRepository.LOCK_ENDPOINT_FOR_REPLAY),
        arguments("replay, re-arm", TenantDeliveryRepository.REARM));
  }

  private static String withoutLayout(String text) {
    return text.strip().replaceAll("\\s+", " ");
  }
}
