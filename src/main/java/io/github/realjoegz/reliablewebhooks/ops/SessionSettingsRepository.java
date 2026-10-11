package io.github.realjoegz.reliablewebhooks.ops;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class SessionSettingsRepository {

  static final String SESSION_SETTINGS =
      """
      SELECT name, setting FROM pg_settings
      WHERE name IN ('default_transaction_isolation', 'synchronous_commit', 'lock_timeout',
                     'statement_timeout', 'idle_in_transaction_session_timeout')
      """;

  /** Reads from a session of the given pool, so the answer is what that pool really runs with. */
  public SessionSettings read(DataSource pool) {
    Map<String, String> settings =
        JdbcClient.create(pool)
            .sql(SESSION_SETTINGS)
            .query((row, n) -> Map.entry(row.getString("name"), row.getString("setting")))
            .list()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return new SessionSettings(
        settings.get("default_transaction_isolation"),
        settings.get("synchronous_commit"),
        milliseconds(settings.get("lock_timeout")),
        milliseconds(settings.get("statement_timeout")),
        milliseconds(settings.get("idle_in_transaction_session_timeout")));
  }

  // pg_settings reports these three timeouts in milliseconds.
  private static Duration milliseconds(String setting) {
    return Duration.ofMillis(Long.parseLong(setting));
  }
}
