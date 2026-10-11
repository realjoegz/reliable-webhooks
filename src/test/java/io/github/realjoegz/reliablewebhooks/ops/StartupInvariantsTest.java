package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Database;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Delivery;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Pool;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Ssrf;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.mock.env.MockEnvironment;

class StartupInvariantsTest {

  private static final String ADMIN_TOKEN = "a".repeat(32);
  private static final String SECRET_KEY = base64OfBytes(32);
  private static final Pool API_POOL = pool(Duration.ofSeconds(3), Duration.ofSeconds(10), true);
  private static final Pool WORKER_POOL = pool(Duration.ofSeconds(3), Duration.ofSeconds(15), true);
  private static final Duration DEADLINE = Duration.ofSeconds(15);
  private static final Duration LEASE = Duration.ofSeconds(60);
  private static final Duration START_CUTOFF = Duration.ofSeconds(35);
  private static final Duration FINALIZE_CUTOFF = Duration.ofSeconds(55);
  private static final Delivery DELIVERY =
      new Delivery(DEADLINE, LEASE, START_CUTOFF, FINALIZE_CUTOFF, 12, 64);
  private static final Ssrf NO_ALLOWANCE = new Ssrf(List.of(), false);
  private static final SessionSettings API_SESSION = session(2, 5, 10);
  private static final SessionSettings WORKER_SESSION = session(5, 10, 10);

  @Test
  void designValuesHold() {
    assertThat(violations(properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY)))
        .isEmpty();
  }

  @Test
  void adminTokenMustBePresent() {
    assertThat(violations(properties(null, SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY)))
        .containsExactly("webhooks.admin-token must have at least 32 characters");
  }

  @Test
  void adminTokenMustHaveThirtyTwoCharacters() {
    assertThat(violations(properties("a".repeat(31), SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY)))
        .containsExactly("webhooks.admin-token must have at least 32 characters");
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "not base64!"})
  void secretEncryptionKeyMustBePresentAndBase64(String key) {
    assertThat(violations(properties(ADMIN_TOKEN, key, API_POOL, WORKER_POOL, DELIVERY)))
        .containsExactly("webhooks.secret-encryption-key must be 32 bytes in base64");
  }

  @ParameterizedTest
  @ValueSource(ints = {31, 33})
  void secretEncryptionKeyMustHaveThirtyTwoBytes(int bytes) {
    assertThat(
            violations(
                properties(ADMIN_TOKEN, base64OfBytes(bytes), API_POOL, WORKER_POOL, DELIVERY)))
        .containsExactly("webhooks.secret-encryption-key must be 32 bytes in base64");
  }

  // The cutoffs of these two are short enough for either lease: a request that starts at 20 s
  // ends by 35 s, and the finalize cutoff of 40 s is below both leases.
  @Test
  void leaseMayEqualAttemptDeadlinePlusThirtySeconds() {
    assertThat(violations(delivery(45, 20, 40))).isEmpty();
  }

  @Test
  void leaseMustCoverAttemptDeadlinePlusThirtySeconds() {
    assertThat(violations(delivery(44, 20, 40)))
        .containsExactly("webhooks.delivery.lease must be at least the attempt deadline plus 30 s");
  }

  @Test
  void requestThatStartsAtTheStartCutoffMayEndAtTheFinalizeCutoff() {
    assertThat(violations(delivery(60, 35, 50))).isEmpty();
  }

  // A request that starts just before 35 s can run until 50 s, and from 49 s on no finalize is
  // tried again.
  @Test
  void startCutoffPlusAttemptDeadlineMustNotBeAboveTheFinalizeCutoff() {
    assertThat(violations(delivery(60, 35, 49)))
        .containsExactly(
            "webhooks.delivery.start-cutoff plus the attempt deadline must not be above the"
                + " finalize cutoff");
  }

  @Test
  void finalizeCutoffMayEndOneSecondBeforeTheLease() {
    assertThat(violations(delivery(60, 35, 59))).isEmpty();
  }

  @Test
  void finalizeCutoffMustBeBelowTheLease() {
    assertThat(violations(delivery(60, 35, 60)))
        .containsExactly("webhooks.delivery.finalize-cutoff must be below the lease");
  }

  @Test
  void atLeastOneAttemptMustBeAllowed() {
    Delivery noAttempt = new Delivery(DEADLINE, LEASE, START_CUTOFF, FINALIZE_CUTOFF, 0, 64);

    assertThat(violations(noAttempt))
        .containsExactly("webhooks.delivery.max-attempts must be at least 1");
  }

  @Test
  void atLeastOneAttemptMustHaveRoomToRun() {
    Delivery noRoom = new Delivery(DEADLINE, LEASE, START_CUTOFF, FINALIZE_CUTOFF, 12, 0);

    assertThat(violations(noRoom))
        .containsExactly("webhooks.delivery.max-in-flight must be at least 1");
  }

  @Test
  void transactionIsolationMustBeReadCommitted() {
    SessionSettings repeatableRead =
        new SessionSettings(
            "repeatable read",
            "on",
            API_SESSION.lockTimeout(),
            API_SESSION.statementTimeout(),
            API_SESSION.idleInTransactionTimeout());

    assertThat(violations(repeatableRead, WORKER_SESSION))
        .containsExactly(
            "api pool: default_transaction_isolation must be 'read committed', is 'repeatable read'");
  }

  @Test
  void synchronousCommitMustNotBeOff() {
    SessionSettings asynchronous =
        new SessionSettings(
            "read committed",
            "off",
            WORKER_SESSION.lockTimeout(),
            WORKER_SESSION.statementTimeout(),
            WORKER_SESSION.idleInTransactionTimeout());

    assertThat(violations(API_SESSION, asynchronous))
        .containsExactly("worker pool: synchronous_commit must not be off");
  }

  @Test
  void borrowWaitMustBeSet() {
    Pool pool = pool(Duration.ZERO, Duration.ofSeconds(10), true);

    assertThat(violations(properties(ADMIN_TOKEN, SECRET_KEY, pool, WORKER_POOL, DELIVERY)))
        .containsExactly("api pool: borrow wait must be set");
  }

  @Test
  void connectTimeoutBelowOneSecondCountsAsUnset() {
    Pool pool = pool(Duration.ofSeconds(3), Duration.ofMillis(999), Duration.ofSeconds(15), true);

    assertThat(violations(properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, pool, DELIVERY)))
        .containsExactly("worker pool: connectTimeout must be set");
  }

  @Test
  void socketTimeoutBelowOneSecondCountsAsUnset() {
    Pool pool = pool(Duration.ofSeconds(3), Duration.ofMillis(999), true);

    assertThat(violations(properties(ADMIN_TOKEN, SECRET_KEY, pool, WORKER_POOL, DELIVERY)))
        .contains("api pool: socketTimeout must be set");
  }

  @Test
  void tcpKeepAliveMustBeOn() {
    Pool pool = pool(Duration.ofSeconds(3), Duration.ofSeconds(15), false);

    assertThat(violations(properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, pool, DELIVERY)))
        .containsExactly("worker pool: tcpKeepAlive must be on");
  }

  @Test
  void lockTimeoutMustBeSet() {
    assertThat(violations(session(0, 5, 10), WORKER_SESSION))
        .containsExactly(
            "api pool: need 0 < lock_timeout < statement_timeout < socketTimeout,"
                + " got 0 ms, 5000 ms, 10000 ms");
  }

  @Test
  void lockTimeoutMustBeBelowStatementTimeout() {
    assertThat(violations(session(5, 5, 10), WORKER_SESSION))
        .containsExactly(
            "api pool: need 0 < lock_timeout < statement_timeout < socketTimeout,"
                + " got 5000 ms, 5000 ms, 10000 ms");
  }

  @Test
  void statementTimeoutMustBeSet() {
    assertThat(violations(session(2, 0, 10), WORKER_SESSION))
        .containsExactly(
            "api pool: need 0 < lock_timeout < statement_timeout < socketTimeout,"
                + " got 2000 ms, 0 ms, 10000 ms");
  }

  @Test
  void statementTimeoutMustBeBelowSocketTimeout() {
    assertThat(violations(API_SESSION, session(5, 15, 10)))
        .containsExactly(
            "worker pool: need 0 < lock_timeout < statement_timeout < socketTimeout,"
                + " got 5000 ms, 15000 ms, 15000 ms");
  }

  @Test
  void idleInTransactionTimeoutMustBeSet() {
    assertThat(violations(API_SESSION, session(5, 10, 0)))
        .containsExactly("worker pool: idle_in_transaction_session_timeout must be set");
  }

  @Test
  void stuckCallMayEndOneSecondBeforeTheStartCutoff() {
    Pool pool = pool(Duration.ofSeconds(18), Duration.ofSeconds(15), true);

    assertThat(violations(properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, pool, DELIVERY))).isEmpty();
  }

  // 19 s of borrow wait, 1 s of aliveness test and 15 s of socket timeout are the 35 s cutoff.
  @Test
  void borrowWaitPlusAlivenessTestPlusSocketTimeoutMustBeBelowTheStartCutoff() {
    Pool pool = pool(Duration.ofSeconds(19), Duration.ofSeconds(15), true);

    assertThat(violations(properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, pool, DELIVERY)))
        .containsExactly(
            "worker pool: borrow wait plus aliveness test plus socketTimeout must be below the"
                + " start cutoff");
  }

  @Test
  void jvmMustExitOnOutOfMemoryError() {
    assertThat(
            StartupInvariants.violations(
                properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY),
                API_SESSION,
                WORKER_SESSION,
                false,
                new Properties(),
                false))
        .containsExactly("the JVM must run with -XX:+ExitOnOutOfMemoryError");
  }

  @ParameterizedTest
  @ValueSource(strings = {"http.proxyHost", "https.proxyHost", "socksProxyHost"})
  void proxyHostMustNotBeSet(String property) {
    Properties systemProperties = new Properties();
    systemProperties.setProperty(property, "proxy.internal");

    assertThat(violations(systemProperties))
        .containsExactly("JVM proxy properties must not be set: " + property);
  }

  @Test
  void systemProxiesMustNotBeUsed() {
    Properties systemProperties = new Properties();
    systemProperties.setProperty("java.net.useSystemProxies", "true");

    assertThat(violations(systemProperties))
        .containsExactly("JVM proxy properties must not be set: java.net.useSystemProxies");
  }

  @Test
  void allowlistMustBeEmptyWithoutTheUnsafeLocalProfile() {
    Ssrf allowlist = new Ssrf(List.of("127.0.0.1/32"), false);

    assertThat(violations(allowlist, false))
        .containsExactly(
            "webhooks.ssrf.allow-cidrs must be empty unless the unsafe-local profile is active");
  }

  @Test
  void httpMustBeOffWithoutTheUnsafeLocalProfile() {
    Ssrf http = new Ssrf(List.of(), true);

    assertThat(violations(http, false))
        .containsExactly(
            "webhooks.ssrf.allow-http must be false unless the unsafe-local profile is active");
  }

  @Test
  void unsafeLocalProfilePermitsTheAllowlistAndHttp() {
    Ssrf allowance = new Ssrf(List.of("127.0.0.1/32"), true);

    assertThat(violations(allowance, true)).isEmpty();
  }

  // The same allowance is refused and accepted. Only the active profile of the environment
  // differs.
  @Test
  void unsafeLocalProfileIsReadFromTheEnvironment() {
    Ssrf allowance = new Ssrf(List.of("127.0.0.1/32"), true);
    WebhooksProperties properties =
        properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY, allowance);
    DataSource api = new DriverManagerDataSource();
    DataSource worker = new DriverManagerDataSource();
    SessionSettingsRepository sessions =
        new SessionSettingsRepository() {
          @Override
          public SessionSettings read(DataSource pool) {
            return pool == worker ? WORKER_SESSION : API_SESSION;
          }
        };
    MockEnvironment production = new MockEnvironment();
    MockEnvironment unsafeLocal = new MockEnvironment();
    unsafeLocal.setActiveProfiles("unsafe-local");

    assertThat(refusalOf(new StartupInvariants(properties, sessions, api, worker, production)))
        .contains(
            "webhooks.ssrf.allow-cidrs must be empty", "webhooks.ssrf.allow-http must be false");
    assertThat(refusalOf(new StartupInvariants(properties, sessions, api, worker, unsafeLocal)))
        .doesNotContain("webhooks.ssrf");
  }

  // The session limits are missing in the worker pool only, so the pool violations show that each
  // pool is judged by its own session. The JVM flag and the proxy property are read from this
  // JVM: the unit tests run without the flag, and the test sets the property.
  @Test
  void refusesToStartAndNamesEveryViolation() {
    DataSource api = new DriverManagerDataSource();
    DataSource worker = new DriverManagerDataSource();
    SessionSettingsRepository sessions =
        new SessionSettingsRepository() {
          @Override
          public SessionSettings read(DataSource pool) {
            return pool == worker ? session(0, 10, 0) : API_SESSION;
          }
        };
    StartupInvariants invariants =
        new StartupInvariants(
            properties("short", SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY),
            sessions,
            api,
            worker,
            new MockEnvironment());

    System.setProperty("http.proxyHost", "proxy.internal");
    try {
      assertThatThrownBy(invariants::afterPropertiesSet)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("webhooks.admin-token must have at least 32 characters")
          .hasMessageContaining("worker pool: need 0 < lock_timeout")
          .hasMessageContaining("worker pool: idle_in_transaction_session_timeout must be set")
          .hasMessageNotContaining("api pool:")
          .hasMessageContaining("the JVM must run with -XX:+ExitOnOutOfMemoryError")
          .hasMessageContaining("JVM proxy properties must not be set: http.proxyHost");
    } finally {
      System.clearProperty("http.proxyHost");
    }
  }

  // The violations that the invariants name, or nothing when they name none. Whether they name
  // the JVM flag depends on the JVM that runs the test, so a test of something else must not need
  // a refusal.
  private static String refusalOf(StartupInvariants invariants) {
    Throwable refusal = catchThrowable(invariants::afterPropertiesSet);
    return refusal == null ? "" : refusal.getMessage();
  }

  private static List<String> violations(WebhooksProperties properties) {
    return StartupInvariants.violations(
        properties, API_SESSION, WORKER_SESSION, true, new Properties(), false);
  }

  private static List<String> violations(Delivery delivery) {
    return violations(properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, WORKER_POOL, delivery));
  }

  private static List<String> violations(SessionSettings api, SessionSettings worker) {
    return StartupInvariants.violations(
        properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY),
        api,
        worker,
        true,
        new Properties(),
        false);
  }

  private static List<String> violations(Ssrf ssrf, boolean unsafeLocal) {
    return StartupInvariants.violations(
        properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY, ssrf),
        API_SESSION,
        WORKER_SESSION,
        true,
        new Properties(),
        unsafeLocal);
  }

  private static List<String> violations(Properties systemProperties) {
    return StartupInvariants.violations(
        properties(ADMIN_TOKEN, SECRET_KEY, API_POOL, WORKER_POOL, DELIVERY),
        API_SESSION,
        WORKER_SESSION,
        true,
        systemProperties,
        false);
  }

  private static String base64OfBytes(int bytes) {
    return Base64.getEncoder().encodeToString(new byte[bytes]);
  }

  private static WebhooksProperties properties(
      String adminToken, String secretEncryptionKey, Pool api, Pool worker, Delivery delivery) {
    return properties(adminToken, secretEncryptionKey, api, worker, delivery, NO_ALLOWANCE);
  }

  private static WebhooksProperties properties(
      String adminToken,
      String secretEncryptionKey,
      Pool api,
      Pool worker,
      Delivery delivery,
      Ssrf ssrf) {
    Database database = new Database("jdbc:postgresql://db/webhooks", "user", "pw", api, worker);
    return new WebhooksProperties(
        adminToken, secretEncryptionKey, "replica", database, delivery, ssrf);
  }

  private static Delivery delivery(
      int leaseSeconds, int startCutoffSeconds, int finalizeCutoffSeconds) {
    return new Delivery(
        DEADLINE,
        Duration.ofSeconds(leaseSeconds),
        Duration.ofSeconds(startCutoffSeconds),
        Duration.ofSeconds(finalizeCutoffSeconds),
        12,
        64);
  }

  private static Pool pool(Duration borrowTimeout, Duration socketTimeout, boolean tcpKeepAlive) {
    return pool(borrowTimeout, Duration.ofSeconds(2), socketTimeout, tcpKeepAlive);
  }

  private static Pool pool(
      Duration borrowTimeout,
      Duration connectTimeout,
      Duration socketTimeout,
      boolean tcpKeepAlive) {
    return new Pool(
        16,
        borrowTimeout,
        connectTimeout,
        socketTimeout,
        tcpKeepAlive,
        Duration.ofSeconds(2),
        Duration.ofSeconds(5),
        Duration.ofSeconds(10));
  }

  private static SessionSettings session(
      int lockTimeoutSeconds, int statementTimeoutSeconds, int idleInTransactionTimeoutSeconds) {
    return new SessionSettings(
        "read committed",
        "on",
        Duration.ofSeconds(lockTimeoutSeconds),
        Duration.ofSeconds(statementTimeoutSeconds),
        Duration.ofSeconds(idleInTransactionTimeoutSeconds));
  }
}
