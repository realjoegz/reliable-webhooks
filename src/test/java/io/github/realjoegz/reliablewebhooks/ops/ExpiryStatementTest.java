package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * The statement of the expiry job is the statement that the design writes out. It is looked for in
 * the design as it is in the code, with its comment, apart from how its lines are laid out.
 */
class ExpiryStatementTest {

  @Test
  void statementIsWrittenInTheDesign() throws IOException {
    String design = Files.readString(Path.of("docs/design.md"));

    assertThat(withoutLayout(design)).contains(withoutLayout(ExpiryRepository.EXPIRE));
  }

  private static String withoutLayout(String text) {
    return text.strip().replaceAll("\\s+", " ");
  }
}
