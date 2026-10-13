package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.delivery.PollerHeartbeat;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.SelfStartedReplica;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class PollerHealthIT {

  private static final String HEALTH = "/actuator/health";
  private static final String LIVENESS = HEALTH + "/liveness";
  private static final String READINESS = HEALTH + "/readiness";
  private static final String STUCK = "replica-with-a-stuck-poller";
  private static final Duration SIXTY_SECONDS = Duration.ofSeconds(60);

  // A piece of the claim, as the database shows it.
  private static final String CLAIM = "WITH eligible AS MATERIALIZED";

  private static final Pattern GAUGE_OF_THE_POLLER =
      Pattern.compile("(?m)^webhooks_poller_last_iteration_age_seconds (\\S+)$");

  // A second replica starts as the application does, with a heartbeat whose clock stands still
  // unless the test moves it. The test locks the delivery table, and the poller of that replica
  // is seen waiting in its claim: its loop does not come by its heartbeat while it waits there.
  // Then the test lets 60 seconds pass on the clock. The liveness probe now answers down, and so
  // does the health of the replica as a whole, which asks every indicator. The readiness probe,
  // which does not ask the poller, still answers up. When the test lets go of the table, the
  // claim ends, the loop beats, and the replica is live again.
  //
  // The gauge of the poller in a scrape of that replica is the time that the probe went by: the
  // 60 seconds while the probe answers down, and none once the loop has beaten.
  //
  // The claim waits for as long as the lock timeout of the worker pool allows, which is 5 s. The
  // test has that long to ask for the health of the replica three times and to scrape it.
  @Test
  void livenessProbeIsDownWhileThePollerDoesNotLoop() throws Exception {
    AtomicLong nanoTime = new AtomicLong();
    PollerHeartbeat heartbeat = new PollerHeartbeat(nanoTime::get);
    try (ConfigurableApplicationContext replica = SelfStartedReplica.start(STUCK, heartbeat)) {
      RestTestClient http = SelfStartedReplica.clientOf(replica);
      expect(http, LIVENESS, HttpStatus.OK, "UP");

      try (Connection blocker = TestDatabase.connection();
          Statement lock = blocker.createStatement()) {
        lock.execute("LOCK TABLE delivery IN ACCESS EXCLUSIVE MODE");
        PgWait.untilBlocked(CLAIM, STUCK);
        nanoTime.addAndGet(SIXTY_SECONDS.toNanos());

        expect(http, LIVENESS, HttpStatus.SERVICE_UNAVAILABLE, "DOWN");
        expect(http, HEALTH, HttpStatus.SERVICE_UNAVAILABLE, "DOWN");
        expect(http, READINESS, HttpStatus.OK, "UP");
        assertThat(secondsOnTheGaugeOfThePoller(http)).isEqualTo(SIXTY_SECONDS.toSeconds());

        blocker.rollback();
      }

      await().untilAsserted(() -> expect(http, LIVENESS, HttpStatus.OK, "UP"));
      assertThat(secondsOnTheGaugeOfThePoller(http)).isZero();
    }
  }

  private static double secondsOnTheGaugeOfThePoller(RestTestClient http) {
    String scrape =
        http.get()
            .uri("/actuator/prometheus")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(String.class)
            .returnResult()
            .getResponseBody();
    Matcher gauge = GAUGE_OF_THE_POLLER.matcher(scrape);
    assertThat(gauge.find()).isTrue();
    return Double.parseDouble(gauge.group(1));
  }

  private static void expect(RestTestClient http, String path, HttpStatus status, String health) {
    http.get()
        .uri(path)
        .exchange()
        .expectStatus()
        .isEqualTo(status)
        .expectBody()
        .jsonPath("$.status")
        .isEqualTo(health);
  }
}
