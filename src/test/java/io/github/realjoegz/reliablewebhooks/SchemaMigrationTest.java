package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class SchemaMigrationTest {

  private static final String SQL_BLOCK_START = "```sql\n";
  private static final String BLOCK_END = "```\n";

  @Test
  void migrationIsTheSchemaOfTheDesign() throws IOException {
    String design = Files.readString(Path.of("docs/design.md"));
    String dataModel = design.substring(design.indexOf("## 4. Data model"));
    int start = dataModel.indexOf(SQL_BLOCK_START) + SQL_BLOCK_START.length();
    String schema = dataModel.substring(start, dataModel.indexOf(BLOCK_END, start));

    assertThat(Files.readString(Path.of("src/main/resources/db/migration/V1__schema.sql")))
        .isEqualTo(schema);
  }
}
