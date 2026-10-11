package io.github.realjoegz.reliablewebhooks.ops;

import com.sun.management.HotSpotDiagnosticMXBean;
import io.github.realjoegz.reliablewebhooks.DatabasePools;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Delivery;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Pool;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Checks what the design relies on but cannot enforce in code. A violation fails the application
 * context before the web server accepts a request, so the process exits non-zero.
 */
@Component
public class StartupInvariants implements InitializingBean {

  private static final int MIN_ADMIN_TOKEN_LENGTH = 32;
  private static final Duration LEASE_MARGIN = Duration.ofSeconds(30);
  private static final List<String> PROXY_HOST_PROPERTIES =
      List.of("http.proxyHost", "https.proxyHost", "socksProxyHost");

  private final WebhooksProperties properties;
  private final SessionSettingsRepository sessions;
  private final DataSource api;
  private final DataSource worker;

  StartupInvariants(
      WebhooksProperties properties,
      SessionSettingsRepository sessions,
      @Qualifier("apiDataSource") DataSource api,
      @Qualifier("workerDataSource") DataSource worker) {
    this.properties = properties;
    this.sessions = sessions;
    this.api = api;
    this.worker = worker;
  }

  @Override
  public void afterPropertiesSet() {
    List<String> violations =
        violations(
            properties,
            sessions.read(api),
            sessions.read(worker),
            exitsOnOutOfMemoryError(),
            System.getProperties());
    if (!violations.isEmpty()) {
      throw new IllegalStateException(
          "Startup invariants violated:\n  " + String.join("\n  ", violations));
    }
  }

  static List<String> violations(
      WebhooksProperties properties,
      SessionSettings apiSession,
      SessionSettings workerSession,
      boolean exitsOnOutOfMemoryError,
      Properties systemProperties) {
    List<String> violations = new ArrayList<>();

    String adminToken = properties.adminToken();
    if (adminToken == null || adminToken.length() < MIN_ADMIN_TOKEN_LENGTH) {
      violations.add(
          "webhooks.admin-token must have at least %d characters"
              .formatted(MIN_ADMIN_TOKEN_LENGTH));
    }
    if (!StringUtils.hasText(properties.secretEncryptionKey())) {
      violations.add("webhooks.secret-encryption-key must be set");
    }

    Delivery delivery = properties.delivery();
    if (delivery.lease().compareTo(delivery.attemptDeadline().plus(LEASE_MARGIN)) < 0) {
      violations.add(
          "webhooks.delivery.lease must be at least the attempt deadline plus %d s"
              .formatted(LEASE_MARGIN.toSeconds()));
    }

    violations.addAll(
        poolViolations("api", properties.database().api(), apiSession, delivery.startCutoff()));
    violations.addAll(
        poolViolations(
            "worker", properties.database().worker(), workerSession, delivery.startCutoff()));

    if (!exitsOnOutOfMemoryError) {
      violations.add("the JVM must run with -XX:+ExitOnOutOfMemoryError");
    }

    List<String> proxyProperties = new ArrayList<>();
    for (String name : PROXY_HOST_PROPERTIES) {
      if (StringUtils.hasText(systemProperties.getProperty(name))) {
        proxyProperties.add(name);
      }
    }
    if (Boolean.parseBoolean(systemProperties.getProperty("java.net.useSystemProxies"))) {
      proxyProperties.add("java.net.useSystemProxies");
    }
    if (!proxyProperties.isEmpty()) {
      violations.add("JVM proxy properties must not be set: " + String.join(", ", proxyProperties));
    }

    return violations;
  }

  private static List<String> poolViolations(
      String name, Pool pool, SessionSettings session, Duration startCutoff) {
    List<String> violations = new ArrayList<>();
    // pgjdbc applies these two in whole seconds, and zero seconds means no timeout.
    Duration connectTimeout = pool.connectTimeout().truncatedTo(ChronoUnit.SECONDS);
    Duration socketTimeout = pool.socketTimeout().truncatedTo(ChronoUnit.SECONDS);

    if (!"read committed".equals(session.defaultTransactionIsolation())) {
      violations.add(
          "%s pool: default_transaction_isolation must be 'read committed', is '%s'"
              .formatted(name, session.defaultTransactionIsolation()));
    }
    if ("off".equals(session.synchronousCommit())) {
      violations.add("%s pool: synchronous_commit must not be off".formatted(name));
    }
    // HikariCP reads a borrow wait of zero as no limit.
    if (pool.borrowTimeout().isZero()) {
      violations.add("%s pool: borrow wait must be set".formatted(name));
    }
    if (connectTimeout.isZero()) {
      violations.add("%s pool: connectTimeout must be set".formatted(name));
    }
    if (socketTimeout.isZero()) {
      violations.add("%s pool: socketTimeout must be set".formatted(name));
    }
    if (!pool.tcpKeepAlive()) {
      violations.add("%s pool: tcpKeepAlive must be on".formatted(name));
    }
    if (session.lockTimeout().isZero()
        || session.lockTimeout().compareTo(session.statementTimeout()) >= 0
        || session.statementTimeout().compareTo(socketTimeout) >= 0) {
      violations.add(
          "%s pool: need 0 < lock_timeout < statement_timeout < socketTimeout, got %d ms, %d ms, %d ms"
              .formatted(
                  name,
                  session.lockTimeout().toMillis(),
                  session.statementTimeout().toMillis(),
                  socketTimeout.toMillis()));
    }
    if (session.idleInTransactionTimeout().isZero()) {
      violations.add("%s pool: idle_in_transaction_session_timeout must be set".formatted(name));
    }
    Duration stuckCall =
        pool.borrowTimeout().plus(DatabasePools.ALIVENESS_TEST_TIMEOUT).plus(socketTimeout);
    if (stuckCall.compareTo(startCutoff) >= 0) {
      violations.add(
          "%s pool: borrow wait plus aliveness test plus socketTimeout must be below the start cutoff"
              .formatted(name));
    }
    return violations;
  }

  private static boolean exitsOnOutOfMemoryError() {
    HotSpotDiagnosticMXBean hotSpot =
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    return Boolean.parseBoolean(hotSpot.getVMOption("ExitOnOutOfMemoryError").getValue());
  }
}
