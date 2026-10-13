package io.github.realjoegz.reliablewebhooks.tools;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Fails the build unless every test that a table of the documents links to ran and passed in this
 * build.
 *
 * <p>A checked table is a Markdown table with a {@code Test} column. Each of its rows must link at
 * least one test in that column, as {@code Class#method}, {@code #method} (the class named before
 * it in the same cell) or {@code Class} (every test of the class). The outcomes are read from the
 * Surefire and Failsafe XML reports, which the build deletes before it runs any test.
 *
 * <p>A table begins with a header row and the divider row below it. Rows that have no such header
 * fail the check, because nothing says which of their columns links the tests: a blank line in the
 * middle of a table leaves such rows. So does a line with a pipe in it that follows a row of a
 * checked table and does not begin with a pipe, which GitHub renders as one more row of it.
 */
public final class FailureTableCheck {

  private static final Pattern CELL_SEPARATOR = Pattern.compile("(?<!\\\\)\\|");
  private static final Pattern DIVIDER_CELL = Pattern.compile(":?-+:?");
  private static final Pattern CODE_SPAN = Pattern.compile("`([^`]*)`");
  private static final Pattern LINK =
      Pattern.compile("(?<class>[A-Z][A-Za-z0-9_]*)?(?:#(?<method>[A-Za-z_][A-Za-z0-9_]*))?");
  private static final Set<String> NOT_PASSED =
      Set.of(
          "skipped",
          "failure",
          "error",
          "flakyFailure",
          "flakyError",
          "rerunFailure",
          "rerunError");

  /**
   * A file and the number of checked tables it must contain. A table whose column was renamed is no
   * longer checked, and only this number turns that into a failure.
   */
  record TableFile(Path path, int checkedTables) {}

  private record Testcase(String className, String name, boolean passed) {}

  private FailureTableCheck() {}

  public static void main(String[] args)
      throws IOException, ParserConfigurationException, SAXException {
    Path project = Path.of(args[0]);
    Path build = Path.of(args[1]);
    List<String> violations =
        violations(
            List.of(
                new TableFile(project.resolve("README.md"), 1),
                new TableFile(project.resolve("docs/failure-table.md"), 2),
                new TableFile(project.resolve("docs/behaviour.md"), 4)),
            List.of(build.resolve("surefire-reports"), build.resolve("failsafe-reports")));
    if (!violations.isEmpty()) {
      throw new IllegalStateException(
          "The tables that link tests are not proven by this build:\n  "
              + String.join("\n  ", violations));
    }
  }

  static List<String> violations(List<TableFile> files, List<Path> reportDirectories)
      throws IOException, ParserConfigurationException, SAXException {
    List<Testcase> testcases = new ArrayList<>();
    for (Path directory : reportDirectories) {
      testcases.addAll(testcasesIn(directory));
    }
    List<String> violations = new ArrayList<>();
    for (TableFile file : files) {
      violations.addAll(violationsIn(file, testcases));
    }
    return violations;
  }

  private static List<Testcase> testcasesIn(Path reportDirectory)
      throws IOException, ParserConfigurationException, SAXException {
    List<Testcase> testcases = new ArrayList<>();
    // No directory means that Surefire or Failsafe ran no test, so there is nothing to read.
    if (!Files.isDirectory(reportDirectory)) {
      return testcases;
    }
    DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    try (DirectoryStream<Path> reports = Files.newDirectoryStream(reportDirectory, "TEST-*.xml")) {
      for (Path report : reports) {
        NodeList elements = parser.parse(report.toFile()).getElementsByTagName("testcase");
        for (int i = 0; i < elements.getLength(); i++) {
          Element testcase = (Element) elements.item(i);
          String className = testcase.getAttribute("classname");
          testcases.add(
              new Testcase(
                  className.substring(className.lastIndexOf('.') + 1),
                  testcase.getAttribute("name"),
                  passed(testcase)));
        }
      }
    }
    return testcases;
  }

  private static boolean passed(Element testcase) {
    for (Node child = testcase.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (NOT_PASSED.contains(child.getNodeName())) {
        return false;
      }
    }
    return true;
  }

  private static List<String> violationsIn(TableFile file, List<Testcase> testcases)
      throws IOException {
    List<String> violations = new ArrayList<>();
    Path name = file.path().getFileName();
    List<String> lines = Files.readAllLines(file.path());
    boolean inCodeBlock = false;
    boolean inTable = false;
    int testColumn = -1;
    int checkedTables = 0;
    for (int i = 0; i < lines.size(); i++) {
      String line = lines.get(i).strip();
      if (line.startsWith("```")) {
        inCodeBlock = !inCodeBlock;
      }
      String where = "%s:%d".formatted(name, i + 1);
      if (inCodeBlock || !line.startsWith("|")) {
        if (!inCodeBlock && inTable && testColumn >= 0 && line.contains("|")) {
          violations.add(
              "%s: a row of a checked table does not begin with a pipe".formatted(where));
        }
        inTable = false;
        continue;
      }
      List<String> cells = cells(line);
      if (!inTable) {
        inTable = true;
        testColumn = -1;
        if (i + 1 == lines.size() || !isDividerRow(lines.get(i + 1).strip())) {
          violations.add("%s: table rows without a header row".formatted(where));
        } else {
          testColumn = cells.indexOf("Test");
          if (testColumn >= 0) {
            checkedTables++;
          }
        }
      } else if (testColumn >= 0 && !isDivider(cells)) {
        String links = testColumn < cells.size() ? cells.get(testColumn) : "";
        violations.addAll(violationsOfRow(where, links, testcases));
      }
    }
    if (checkedTables != file.checkedTables()) {
      violations.add(
          "%s: tables with a Test column: expected %d, found %d"
              .formatted(name, file.checkedTables(), checkedTables));
    }
    return violations;
  }

  private static List<String> cells(String row) {
    String inner = row.substring(1);
    if (inner.endsWith("|")) {
      inner = inner.substring(0, inner.length() - 1);
    }
    List<String> cells = new ArrayList<>();
    for (String cell : CELL_SEPARATOR.split(inner, -1)) {
      cells.add(cell.strip());
    }
    return cells;
  }

  private static boolean isDividerRow(String line) {
    return line.startsWith("|") && isDivider(cells(line));
  }

  private static boolean isDivider(List<String> cells) {
    return cells.stream().allMatch(cell -> DIVIDER_CELL.matcher(cell).matches());
  }

  private static List<String> violationsOfRow(
      String where, String links, List<Testcase> testcases) {
    List<String> violations = new ArrayList<>();
    Matcher codeSpans = CODE_SPAN.matcher(links);
    String currentClass = null;
    boolean linked = false;
    while (codeSpans.find()) {
      linked = true;
      String link = codeSpans.group(1);
      Matcher parts = LINK.matcher(link);
      if (link.isEmpty() || !parts.matches()) {
        violations.add("%s: `%s` is not Class#method, #method or Class".formatted(where, link));
        continue;
      }
      if (parts.group("class") != null) {
        currentClass = parts.group("class");
      }
      if (currentClass == null) {
        violations.add("%s: `%s` has no class before it in its cell".formatted(where, link));
        continue;
      }
      violations.addAll(violationsOfLink(where, currentClass, parts.group("method"), testcases));
    }
    if (!linked) {
      violations.add("%s: the row links no test".formatted(where));
    }
    return violations;
  }

  private static List<String> violationsOfLink(
      String where, String className, String method, List<Testcase> testcases) {
    String link = method == null ? className : className + "#" + method;
    List<Testcase> matches =
        testcases.stream()
            .filter(testcase -> testcase.className().equals(className))
            .filter(testcase -> method == null || isInvocationOf(method, testcase.name()))
            .toList();
    if (matches.isEmpty()) {
      return List.of("%s: %s did not run".formatted(where, link));
    }
    return matches.stream()
        .filter(testcase -> !testcase.passed())
        .map(testcase -> "%s: %s did not pass (%s)".formatted(where, link, testcase.name()))
        .toList();
  }

  // Surefire appends the argument types or the invocation index for a parameterized test.
  private static boolean isInvocationOf(String method, String testcaseName) {
    return testcaseName.equals(method)
        || testcaseName.startsWith(method + "[")
        || testcaseName.startsWith(method + "(");
  }
}
