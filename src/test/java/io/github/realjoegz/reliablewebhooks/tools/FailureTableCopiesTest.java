package io.github.realjoegz.reliablewebhooks.tools;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The design carries the kill table and the failure table, and so does {@code
 * docs/failure-table.md}, whose links {@link FailureTableCheck} reads. A row that read differently
 * in the design would be a claim or a link that the build does not check.
 */
class FailureTableCopiesTest {

  private static final Path DESIGN = Path.of("docs/design.md");
  private static final Path CHECKED_COPY = Path.of("docs/failure-table.md");

  @Test
  void killTableOfTheDesignIsTheOneThatTheBuildChecks() throws IOException {
    String header = "| # | SIGKILL lands | State left | Recovery | Visible effect | Test |";

    assertThat(table(DESIGN, header)).isEqualTo(table(CHECKED_COPY, header));
  }

  // The last column says how a test forces its failure, and the checked copy says it at more
  // length. The four columns before it are the number, the failure, the proven behaviour and the
  // tests.
  @Test
  void failureTableOfTheDesignDiffersFromTheCheckedOneInItsLastColumnOnly() throws IOException {
    String header = "| # | Failure | Proven behaviour | Test | Forced by |";

    assertThat(firstFourColumns(table(DESIGN, header)))
        .isEqualTo(firstFourColumns(table(CHECKED_COPY, header)));
  }

  private static List<String> table(Path document, String header) throws IOException {
    List<String> lines = Files.readAllLines(document);
    int start = lines.indexOf(header);
    assertThat(start).as("line of %s that reads %s", document, header).isNotNegative();
    return lines.stream().skip(start).takeWhile(line -> line.startsWith("|")).toList();
  }

  // A row begins with a pipe, so the piece in front of its first column is empty.
  private static List<List<String>> firstFourColumns(List<String> rows) {
    return rows.stream().map(row -> List.of(row.split("\\|", 6)).subList(1, 5)).toList();
  }
}
