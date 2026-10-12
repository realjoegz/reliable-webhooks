package io.github.realjoegz.reliablewebhooks.tenant;

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
 * The statements that create a tenant and the one that patches it are the statements that the
 * design writes out. Each is looked for in the design as it is in the code, apart from how its
 * lines are laid out.
 */
class TenantStatementsTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("statements")
  void statementIsWrittenInTheDesign(String name, String statement) throws IOException {
    String design = Files.readString(Path.of("docs/design.md"));

    assertThat(withoutLayout(design)).contains(withoutLayout(statement));
  }

  static Stream<Arguments> statements() {
    return Stream.of(
        arguments("create, tenant", TenantRepository.INSERT_TENANT),
        arguments("create, bucket", TenantRepository.INSERT_BUCKET),
        arguments("patch", TenantRepository.PATCH));
  }

  private static String withoutLayout(String text) {
    return text.strip().replaceAll("\\s+", " ");
  }
}
