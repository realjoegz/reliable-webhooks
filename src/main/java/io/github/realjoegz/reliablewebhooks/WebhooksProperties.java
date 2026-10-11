package io.github.realjoegz.reliablewebhooks;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("webhooks")
public record WebhooksProperties(
    String adminToken, String secretEncryptionKey, Database database, Delivery delivery) {

  /**
   * Leaves out the admin token and the secret-encryption key, so that printing cannot leak them.
   */
  @Override
  public String toString() {
    return "WebhooksProperties[database=%s, delivery=%s]".formatted(database, delivery);
  }

  public record Database(String url, String username, String password, Pool api, Pool worker) {

    /** Leaves out the password, and the URL because a JDBC URL can carry one. */
    @Override
    public String toString() {
      return "Database[username=%s, api=%s, worker=%s]".formatted(username, api, worker);
    }
  }

  public record Pool(
      int maxSize,
      Duration borrowTimeout,
      Duration connectTimeout,
      Duration socketTimeout,
      boolean tcpKeepAlive,
      Duration lockTimeout,
      Duration statementTimeout,
      Duration idleInTransactionTimeout) {}

  public record Delivery(Duration attemptDeadline, Duration lease, Duration startCutoff) {}
}
