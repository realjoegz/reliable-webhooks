package io.github.realjoegz.reliablewebhooks.ops;

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
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.http.server.LocalTestWebServer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class PollerHealthIT {

  private static final String STUCK = "replica-with-a-stuck-poller";
  private static final Duration SIXTY_SECONDS = Duration.ofSeconds(60);

  // A piece of the claim, as the database shows it.
  private static final String CLAIM = "WITH eligible AS MATERIALIZED";

  // A second replica starts as the application does, with a heartbeat whose clock stands still
  // unless the test moves it. The test locks the delivery table, and the poller of that replica
  // is seen waiting in its claim: its loop does not come by its heartbeat while it waits there.
  // Then the test lets 60 seconds pass on the clock. The liveness probe now answers down, and the
  // readiness probe, which does not ask the poller, still answers up. When the test lets go of
  // the table, the claim ends, the loop beats, and the replica is live again.
  //
  // The claim waits for as long as the lock timeout of the worker pool allows, which is 5 s. The
  // test has that long to ask the two probes.
  @Test
  void livenessProbeIsDownWhileThePollerDoesNotLoop() throws Exception {
    AtomicLong nanoTime = new AtomicLong();
    PollerHeartbeat heartbeat = new PollerHeartbeat(nanoTime::get);
    try (ConfigurableApplicationContext replica = SelfStartedReplica.start(STUCK, heartbeat)) {
      RestTestClient http = clientOf(replica);
      expect(http, "liveness", HttpStatus.OK, "UP");

      try (Connection blocker = TestDatabase.connection();
          Statement lock = blocker.createStatement()) {
        lock.execute("LOCK TABLE delivery IN ACCESS EXCLUSIVE MODE");
        PgWait.untilBlocked(CLAIM, STUCK);
        nanoTime.addAndGet(SIXTY_SECONDS.toNanos());

        expect(http, "liveness", HttpStatus.SERVICE_UNAVAILABLE, "DOWN");
        expect(http, "readiness", HttpStatus.OK, "UP");

        blocker.rollback();
      }

      await().untilAsserted(() -> expect(http, "liveness", HttpStatus.OK, "UP"));
    }
  }

  private static RestTestClient clientOf(ConfigurableApplicationContext replica) {
    return RestTestClient.bindToServer(new JdkClientHttpRequestFactory())
        .uriBuilderFactory(LocalTestWebServer.obtain(replica).uriBuilderFactory())
        .build();
  }

  private static void expect(RestTestClient http, String probe, HttpStatus status, String health) {
    http.get()
        .uri("/actuator/health/" + probe)
        .exchange()
        .expectStatus()
        .isEqualTo(status)
        .expectBody()
        .jsonPath("$.status")
        .isEqualTo(health);
  }
}
