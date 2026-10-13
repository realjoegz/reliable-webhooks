package io.github.realjoegz.reliablewebhooks.tools;

import static java.nio.file.StandardOpenOption.APPEND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.realjoegz.reliablewebhooks.tools.FailureTableCheck.TableFile;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The reports under {@code src/test/resources/failure-table} are fixtures, one per matching rule:
 * {@code MethodNamesIT} for {@code Class#method}, {@code InheritedClassIT} for {@code #method},
 * {@code WholeClassTest} for a bare class and {@code OutcomesIT} for what counts as passed.
 */
class FailureTableCheckTest {

  private static Path reports;

  @TempDir Path directory;

  @BeforeAll
  static void locateFixtureReports() throws URISyntaxException {
    reports = Path.of(FailureTableCheckTest.class.getResource("/failure-table").toURI());
  }

  @Test
  void tableWithHeaderRowOnlyPasses() throws Exception {
    assertThat(violationsOf("")).isEmpty();
  }

  @Test
  void methodLinkMatchesPlainIndexedAndTypedTestcaseNames() throws Exception {
    assertThat(
            violationsOf(
                """
                | 1 | plain | `MethodNamesIT#plain` |
                | 2 | indexed | `MethodNamesIT#indexed` |
                | 3 | typed | `MethodNamesIT#typed` |
                """))
        .isEmpty();
  }

  @Test
  void everyInvocationOfALinkedMethodMustHavePassed() throws Exception {
    assertThat(violationsOf("| 1 | one of two fails | `MethodNamesIT#partlyFailing` |"))
        .containsExactly(
            "table.md:3: MethodNamesIT#partlyFailing did not pass (partlyFailing(int)[2])");
  }

  @Test
  void methodWithoutClassInheritsTheClassNamedBeforeItInTheCell() throws Exception {
    assertThat(
            violationsOf(
                "| 1 | inherit | `MethodNamesIT#plain`, `InheritedClassIT#first`, `#second` |"))
        .isEmpty();
  }

  @Test
  void methodWithoutClassDoesNotInheritFromAnotherRow() throws Exception {
    assertThat(
            violationsOf(
                """
                | 1 | names the class | `InheritedClassIT#first` |
                | 2 | names no class | `#second` |
                """))
        .containsExactly("table.md:4: `#second` has no class before it in its cell");
  }

  @Test
  void bareClassPassesWhenEveryTestcaseOfTheClassPassed() throws Exception {
    assertThat(violationsOf("| 1 | whole class | `WholeClassTest` |")).isEmpty();
  }

  @Test
  void bareClassFailsForEachTestcaseThatDidNotPass() throws Exception {
    assertThat(violationsOf("| 1 | whole class | `OutcomesIT` |"))
        .containsExactlyInAnyOrder(
            "table.md:3: OutcomesIT did not pass (skipped)",
            "table.md:3: OutcomesIT did not pass (failed)",
            "table.md:3: OutcomesIT did not pass (errored)",
            "table.md:3: OutcomesIT did not pass (flaky)",
            "table.md:3: OutcomesIT did not pass (rerun)",
            "table.md:3: OutcomesIT did not pass (flakyError)",
            "table.md:3: OutcomesIT did not pass (rerunError)");
  }

  @Test
  void testcaseWithoutAFailureChildPassed() throws Exception {
    assertThat(violationsOf("| 1 | passed | `OutcomesIT#passed` |")).isEmpty();
  }

  // A test that passed only when it was run again has a flakyFailure or a flakyError, by how its
  // first run ended, and one that did not pass then either a rerunFailure or a rerunError.
  @ParameterizedTest
  @ValueSource(
      strings = {"skipped", "failed", "errored", "flaky", "rerun", "flakyError", "rerunError"})
  void testcaseWithThisOutcomeDidNotPass(String method) throws Exception {
    assertThat(violationsOf("| 1 | outcome | `OutcomesIT#%s` |".formatted(method)))
        .containsExactly("table.md:3: OutcomesIT#%s did not pass (%s)".formatted(method, method));
  }

  @Test
  void rowWithoutALinkFails() throws Exception {
    assertThat(violationsOf("| 1 | not proven | to be written |"))
        .containsExactly("table.md:3: the row links no test");
  }

  // The first row has code spans around its link that are no links. The second names a test in
  // every column but the one that is read.
  @Test
  void linksAreReadFromTheTestColumnOnly() throws Exception {
    Path file = directory.resolve("columns.md");
    Files.writeString(
        file,
        """
        | # | Failure | Test | Forced by |
        |---|---|---|---|
        | 1 | attempt logged `unknown` | `MethodNamesIT#plain` | `StubDns` |
        | 2 | `WholeClassTest` | to be written | `MethodNamesIT#plain` |
        """);

    assertThat(FailureTableCheck.violations(List.of(new TableFile(file, 1)), List.of(reports)))
        .containsExactly("columns.md:4: the row links no test");
  }

  @Test
  void escapedPipeDoesNotEndACell() throws Exception {
    assertThat(violationsOf("| 1 | a cell with `a\\|b` in it | `MethodNamesIT#plain` |")).isEmpty();
  }

  @Test
  void rowThatEndsBeforeTheTestColumnFails() throws Exception {
    assertThat(violationsOf("| 1 | cut short |"))
        .containsExactly("table.md:3: the row links no test");
  }

  // A blank line ends a table. The rows after it have no header, so nothing says which of their
  // columns links the tests, and none of them could be checked.
  @Test
  void rowsThatABlankLineCutsOffFromTheirHeaderFail() throws Exception {
    assertThat(
            violationsOf(
                """
                | 1 | checked | `MethodNamesIT#plain` |

                | 2 | cut off | `MethodNamesIT#renamed` |
                | 3 | cut off | `MethodNamesIT#renamed` |
                """))
        .containsExactly("table.md:5: table rows without a header row");
  }

  // GitHub renders a line with a pipe in it as a row of the table that it follows, also when it
  // does not begin with one. Read as the end of the table, it would hide itself and the rows
  // after it from the check.
  @Test
  void rowOfACheckedTableThatDoesNotBeginWithAPipeFails() throws Exception {
    assertThat(
            violationsOf(
                """
                | 1 | checked | `MethodNamesIT#plain` |
                2 | no first pipe | `MethodNamesIT#renamed` |
                | 3 | cut off | `MethodNamesIT#renamed` |
                """))
        .containsExactly(
            "table.md:4: a row of a checked table does not begin with a pipe",
            "table.md:5: table rows without a header row");
  }

  // Text that follows a table without a blank line between them is no row of it unless it has
  // a pipe in it.
  @Test
  void lineWithoutAPipeEndsACheckedTable() throws Exception {
    assertThat(
            violationsOf(
                """
                | 1 | checked | `MethodNamesIT#plain` |
                A sentence about `MethodNamesIT#renamed` right below the table.
                """))
        .isEmpty();
  }

  @Test
  void methodThatDoesNotExistFails() throws Exception {
    assertThat(violationsOf("| 1 | renamed | `MethodNamesIT#renamed` |"))
        .containsExactly("table.md:3: MethodNamesIT#renamed did not run");
  }

  @Test
  void classThatDidNotRunFails() throws Exception {
    assertThat(violationsOf("| 1 | excluded | `ExcludedIT#method`, `ExcludedTest` |"))
        .containsExactly(
            "table.md:3: ExcludedIT#method did not run", "table.md:3: ExcludedTest did not run");
  }

  @Test
  void linkedTestFailsWhenNoReportWasWritten() throws Exception {
    Path table = table("| 1 | plain | `MethodNamesIT#plain` |");

    assertThat(
            FailureTableCheck.violations(
                List.of(new TableFile(table, 1)), List.of(directory.resolve("absent"))))
        .containsExactly("table.md:3: MethodNamesIT#plain did not run");
  }

  @Test
  void codeSpanThatIsNotALinkFails() throws Exception {
    assertThat(violationsOf("| 1 | prose | `mvn verify` |"))
        .containsExactly("table.md:3: `mvn verify` is not Class#method, #method or Class");
  }

  // Read as a link, the empty span would name the class before it a second time and pass.
  @Test
  void emptyCodeSpanIsNotALink() throws Exception {
    assertThat(violationsOf("| 1 | empty | `WholeClassTest`, `` |"))
        .containsExactly("table.md:3: `` is not Class#method, #method or Class");
  }

  @Test
  void fileWithoutATestColumnFails() throws Exception {
    Path file = directory.resolve("renamed.md");
    Files.writeString(
        file,
        """
        | # | Failure | Tests |
        |---|---|---|
        | 1 | unchecked | `MethodNamesIT#renamed` |
        """);

    assertThat(FailureTableCheck.violations(List.of(new TableFile(file, 1)), List.of(reports)))
        .containsExactly("renamed.md: tables with a Test column: expected 1, found 0");
  }

  @Test
  void fileWithFewerCheckedTablesThanExpectedFails() throws Exception {
    Path file = directory.resolve("one-renamed.md");
    Files.writeString(
        file,
        """
        | # | SIGKILL lands | Tests |
        |---|---|---|
        | K1 | unchecked | `MethodNamesIT#renamed` |

        | # | Failure | Test |
        |---|---|---|
        | 1 | checked | `MethodNamesIT#plain` |
        """);

    assertThat(FailureTableCheck.violations(List.of(new TableFile(file, 2)), List.of(reports)))
        .containsExactly("one-renamed.md: tables with a Test column: expected 2, found 1");
  }

  @Test
  void fileWithMoreCheckedTablesThanExpectedFails() throws Exception {
    Path file = directory.resolve("one-added.md");
    Files.writeString(
        file,
        """
        | # | Failure | Test |
        |---|---|---|
        | 1 | checked | `MethodNamesIT#plain` |

        | # | Failure | Test |
        |---|---|---|
        | 2 | checked | `WholeClassTest` |
        """);

    assertThat(FailureTableCheck.violations(List.of(new TableFile(file, 1)), List.of(reports)))
        .containsExactly("one-added.md: tables with a Test column: expected 1, found 2");
  }

  @Test
  void otherTablesAndCodeBlocksAreNotChecked() throws Exception {
    Path file = directory.resolve("mixed.md");
    Files.writeString(
        file,
        """
        | # | Guarantee | Enforced by |
        |---|---|---|
        | G1 | Durable accept | `Single ingest transaction` |

        ```
        | # | Failure | Test |
        | 1 | inside a code block | `MethodNamesIT#renamed` |
        ```

        | # | Failure | Test |
        |---|---|---|
        | 1 | checked | `MethodNamesIT#plain` |
        """);

    assertThat(FailureTableCheck.violations(List.of(new TableFile(file, 1)), List.of(reports)))
        .isEmpty();
  }

  @Test
  void mainChecksTheThreeDocumentsAgainstBothReportDirectories() throws Exception {
    Path project = directory.resolve("project");
    Path build = project.resolve("target");
    Files.createDirectories(project.resolve("docs"));
    copyReport("TEST-fixture.WholeClassTest.xml", build.resolve("surefire-reports"));
    copyReport("TEST-fixture.MethodNamesIT.xml", build.resolve("failsafe-reports"));
    Path readme = project.resolve("README.md");
    Files.writeString(
        readme,
        """
        | # | Failure | Test |
        |---|---|---|
        | 1 | read from the Surefire reports | `WholeClassTest` |
        """);
    Path failureTable = project.resolve("docs/failure-table.md");
    Files.writeString(
        failureTable,
        """
        | # | SIGKILL lands | Test |
        |---|---|---|
        | K1 | read from the Failsafe reports | `MethodNamesIT#plain` |

        | # | Failure | Test |
        |---|---|---|
        | 1 | read from the Surefire reports | `WholeClassTest` |
        """);
    Path behaviour = project.resolve("docs/behaviour.md");
    Files.writeString(
        behaviour,
        """
        | Guarantee | Test |
        |---|---|
        | G1 | `MethodNamesIT#plain` |

        | Area | Test |
        |---|---|
        | Ingest | `WholeClassTest` |

        | What the tests show | Test |
        |---|---|
        | shown | `MethodNamesIT#plain` |

        | What is compared | Test |
        |---|---|
        | compared | `WholeClassTest` |
        """);
    String[] arguments = {project.toString(), build.toString()};

    assertThatCode(() -> FailureTableCheck.main(arguments)).doesNotThrowAnyException();

    Files.writeString(readme, "| 2 | not proven | `ExcludedTest` |\n", APPEND);
    Files.writeString(failureTable, "| 2 | not proven | `ExcludedIT#method` |\n", APPEND);
    Files.writeString(behaviour, "| not proven | `MethodNamesIT#renamed` |\n", APPEND);
    assertThatThrownBy(() -> FailureTableCheck.main(arguments))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("README.md:4: ExcludedTest did not run")
        .hasMessageContaining("failure-table.md:8: ExcludedIT#method did not run")
        .hasMessageContaining("behaviour.md:16: MethodNamesIT#renamed did not run");
  }

  private static void copyReport(String name, Path reportDirectory) throws IOException {
    Files.createDirectories(reportDirectory);
    Files.copy(reports.resolve(name), reportDirectory.resolve(name));
  }

  private List<String> violationsOf(String rows) throws Exception {
    return FailureTableCheck.violations(List.of(new TableFile(table(rows), 1)), List.of(reports));
  }

  private Path table(String rows) throws IOException {
    Path table = directory.resolve("table.md");
    Files.writeString(
        table,
        """
        | # | Failure | Test |
        |---|---|---|
        """
            + rows);
    return table;
  }
}
