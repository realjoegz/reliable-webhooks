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
 * The statements of the worker, of the circuit breaker and of the two replays are the statements
 * that the design writes out. Each is looked for in the design as it is in the code, with its
 * comments, apart from how its lines are laid out. The insert for an abandoned attempt is not among
 * them: the design gives its values in a sentence.
 *
 * <p>The design writes the columns that a replay sets once, for the replay of one delivery, and
 * names them for the replay of many. The code has them once as well, and both statements are made
 * of that one text.
 *
 * <p>The load tests have a copy of the claim as well, which they ask the database to explain.
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

  // The copy has the parameters of the claim written in: no endpoint and no tenant is full, 64
  // attempts are free and the lease is 60 s. Apart from that it has to be the claim of the code
  // without its comments, or a result would show the plan of a statement that no replica runs.
  @Test
  void claimThatTheLoadTestsExplainIsTheClaimOfTheCode() throws IOException {
    String explained = Files.readString(Path.of("loadtest/queries/claim-plan.sql"));
    String claim =
        DeliveryRepository.CLAIM
            .replaceAll("--.*", "")
            .replace(":full_endpoints", "'{}'::uuid[]")
            .replace(":full_tenants", "'{}'::uuid[]")
            .replace(":free", "64")
            .replace(":lease", "interval '60 seconds'");

    assertThat(withoutLayout(explained)).contains(withoutLayout(claim));
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
        arguments("replay, re-arm", TenantDeliveryRepository.REARM),
        arguments(
            "bulk replay, endpoint lock", TenantDeliveryRepository.LOCK_ENDPOINT_FOR_BULK_REPLAY),
        arguments(
            "bulk replay, re-arm",
            TenantDeliveryRepository.REARM_OLDEST_DEAD.replace(
                TenantDeliveryRepository.REARM_COLUMNS,
                "SET (the same columns as the single replay)\n")));
  }

  private static String withoutLayout(String text) {
    return text.strip().replaceAll("\\s+", " ");
  }
}
