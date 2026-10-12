package io.github.realjoegz.reliablewebhooks.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * The statement of the bucket is the statement that the design writes out. It is looked for in the
 * design as it is in the code, apart from how its lines are laid out.
 */
class TokenBucketTest {

  @Test
  void statementIsWrittenInTheDesign() throws IOException {
    String design = Files.readString(Path.of("docs/design.md"));

    assertThat(withoutLayout(design)).contains(withoutLayout(TokenBucket.CHARGE));
  }

  private static String withoutLayout(String text) {
    return text.strip().replaceAll("\\s+", " ");
  }
}
