package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Repository;

/**
 * Every statement of a repository that names a table with tenant data must carry {@code tenant_id =
 * :t}. The tables with tenant data are all tables but {@code tenant}, whose rows are addressed by
 * their own id. An {@code INSERT} that only supplies values is left out: it creates a row and can
 * neither read nor change a row of another tenant.
 */
class SqlTenantScopeTest {

  private static final Path MIGRATIONS = Path.of("src/main/resources/db/migration");
  private static final Pattern CREATE_TABLE = Pattern.compile("CREATE TABLE (\\w+)");
  private static final Pattern TENANT_SCOPE = Pattern.compile("\\btenant_id = :t\\b");
  private static final Pattern VALUES_ONLY_INSERT =
      Pattern.compile("INSERT INTO (?!.*\\b(FROM|UPDATE)\\b).*", Pattern.CASE_INSENSITIVE);

  @Test
  void everyRepositoryStatementOnATenantTableCarriesTheTenantScope() throws Exception {
    Set<String> tenantTables = tenantTables();
    Map<String, String> statements = repositoryStatements();

    assertThat(tenantTables).contains("endpoint", "endpoint_circuit", "delivery");
    assertThat(statements)
        .containsKeys(
            "EndpointRepository.FIND",
            "EventRepository.INSERT_DELIVERIES",
            "TenantRepository.INSERT_BUCKET");
    assertThat(statements)
        .allSatisfy(
            (name, sql) -> assertThat(unscopedTenantTables(sql, tenantTables)).as(name).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id FROM endpoint WHERE id = :id",
        "SELECT id FROM endpoint WHERE tenant_id = :id",
        "SELECT id FROM endpoint WHERE tenant_id = :tenant",
        "UPDATE endpoint SET status = 'deleted' WHERE id = :id",
        "DELETE FROM endpoint WHERE id = :id",
        "SELECT e.id FROM endpoint e JOIN tenant t ON t.id = e.tenant_id WHERE t.id = :t",
        "INSERT INTO endpoint (tenant_id, url) SELECT :t, url FROM endpoint WHERE id = :id",
        "insert into endpoint (tenant_id, url) select :t, url from endpoint where id = :id",
        "INSERT INTO endpoint (id) VALUES (:id) ON CONFLICT (id) DO UPDATE SET url = :url"
      })
  void statementOnATenantTableWithoutTheScopeIsFound(String sql) {
    assertThat(unscopedTenantTables(sql, Set.of("endpoint"))).containsExactly("endpoint");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id FROM endpoint WHERE id = :id AND tenant_id = :t",
        "SELECT e.id FROM endpoint e\nWHERE e.tenant_id\n  = :t",
        "INSERT INTO endpoint (tenant_id, url) VALUES (:id, :url) RETURNING id",
        "SELECT endpoint_id FROM other_table WHERE endpoint_id = :id",
        "SELECT max_endpoints FROM tenant WHERE id = :t"
      })
  void scopedStatementValuesOnlyInsertAndStatementOnAnotherTableAreNotFound(String sql) {
    assertThat(unscopedTenantTables(sql, Set.of("endpoint"))).isEmpty();
  }

  private static Set<String> unscopedTenantTables(String sql, Set<String> tenantTables) {
    Set<String> named = new TreeSet<>();
    String statement = sql.strip().replaceAll("\\s+", " ");
    if (TENANT_SCOPE.matcher(statement).find() || VALUES_ONLY_INSERT.matcher(statement).matches()) {
      return named;
    }
    for (String table : tenantTables) {
      if (Pattern.compile("\\b" + table + "\\b").matcher(statement).find()) {
        named.add(table);
      }
    }
    return named;
  }

  private static Set<String> tenantTables() throws IOException {
    Set<String> tables = new TreeSet<>();
    try (Stream<Path> migrations = Files.list(MIGRATIONS)) {
      for (Path migration : migrations.toList()) {
        Matcher created = CREATE_TABLE.matcher(Files.readString(migration));
        while (created.find()) {
          tables.add(created.group(1));
        }
      }
    }
    tables.remove("tenant");
    return tables;
  }

  /** Every String constant of every repository class, by class and field name. */
  private static Map<String, String> repositoryStatements() throws ReflectiveOperationException {
    ClassPathScanningCandidateComponentProvider scanner =
        new ClassPathScanningCandidateComponentProvider(false);
    scanner.addIncludeFilter(new AnnotationTypeFilter(Repository.class));
    List<Class<?>> repositories = new ArrayList<>();
    for (BeanDefinition repository :
        scanner.findCandidateComponents(ReliableWebhooksApplication.class.getPackageName())) {
      repositories.add(Class.forName(repository.getBeanClassName()));
    }

    Map<String, String> statements = new LinkedHashMap<>();
    for (Class<?> repository : repositories) {
      for (Field field : repository.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
          field.setAccessible(true);
          statements.put(
              repository.getSimpleName() + "." + field.getName(), (String) field.get(null));
        }
      }
    }
    return statements;
  }
}
