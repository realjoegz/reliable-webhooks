package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The statements of the expiry, cancel and retention jobs are the statements that the design writes
 * out. Each is looked for in the design as it is in the code, with its comment, apart from how its
 * lines are laid out.
 */
class JobStatementsTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("statements")
  void statementIsWrittenInTheDesign(String name, String statement) throws IOException {
    String design = Files.readString(Path.of("docs/design.md"));

    assertThat(withoutLayout(design)).contains(withoutLayout(statement));
  }

  // The cancel and retention jobs run a statement again when it took a full batch. They know a
  // full batch by a constant, and the statements hold the size as text, as the design writes it.
  @ParameterizedTest(name = "{0}")
  @MethodSource("statementsThatAreRepeated")
  void statementTakesTheBatchThatItsJobCounts(String name, String statement) {
    assertThat(statement).containsOnlyOnce("LIMIT " + PeriodicJob.BATCH);
  }

  static Stream<Arguments> statements() {
    return Stream.concat(
        Stream.of(arguments("expiry", ExpiryRepository.EXPIRE)), statementsThatAreRepeated());
  }

  static Stream<Arguments> statementsThatAreRepeated() {
    return Stream.of(
        arguments("cancel", CancelRepository.CANCEL),
        arguments("retention, deliveries", RetentionRepository.DELETE_TERMINAL_DELIVERIES),
        arguments("retention, events", RetentionRepository.DELETE_EVENTS_WITHOUT_DELIVERIES),
        arguments(
            "retention, endpoints",
            RetentionRepository.DELETE_DELETED_ENDPOINTS_WITHOUT_DELIVERIES),
        arguments(
            "retention, previous secrets", RetentionRepository.CLEAR_EXPIRED_PREVIOUS_SECRETS));
  }

  private static String withoutLayout(String text) {
    return text.strip().replaceAll("\\s+", " ");
  }
}
