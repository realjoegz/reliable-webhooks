package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaProcess;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * A replica that runs as a process of its own gets a SIGTERM while it has attempts in flight. The
 * replica inside the test JVM only takes the requests that set the test up. Its worker does not
 * run.
 */
@UnsafeLocalReplica
@Tag("process")
class GracefulShutdownIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int MAX_IN_FLIGHT_FOR_AN_ENDPOINT = 4;
  private static final String DRAINING = "replica-that-drains";
  private static final String POLLER_STOPPED = "\"message\":\"poller_stopped\"";
  private static final int SIGTERM = 15;
  private static final Duration DRAIN_TIME = Duration.ofSeconds(20);

  @Autowired RestTestClient http;

  // Five deliveries are due for one endpoint, and the receiver holds its requests. Four are in
  // flight, which is all that an endpoint may have, so the fifth is unclaimed for that reason
  // only. Then comes the SIGTERM. Once the replica has logged poller_stopped, the receiver
  // answers. A poller that still claimed would take the fifth delivery as soon as one of the four
  // had finished.
  //
  // While the four are held, the replica is in the middle of its drain. Its readiness probe is
  // answered, which shows that the worker stops before the web server does, and it says that the
  // replica takes no more traffic.
  @Test
  void drainsInFlightAndExits() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200));
        ReplicaProcess replica = ReplicaProcess.start(DRAINING)) {
      receiver.answer("/hook", hold);
      TestTenant tenant = TestTenant.create(http, "sigterm");
      tenant.register(http, receiver.url("/hook"), "*");
      List<UUID> deliveries = new ArrayList<>();
      for (int i = 0; i < MAX_IN_FLIGHT_FOR_AN_ENDPOINT + 1; i++) {
        deliveries.add(Rows.deliveryOf(tenant.accept(http, EVENT)));
      }
      await().until(() -> hold.waiting() == MAX_IN_FLIGHT_FOR_AN_ENDPOINT);

      replica.terminate();
      await().until(() -> replica.output().contains(POLLER_STOPPED));

      assertThat(replica.isAlive()).isTrue();
      assertThat(hold.waiting()).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      replica
          .http()
          .get()
          .uri("/actuator/health/readiness")
          .exchange()
          .expectStatus()
          .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
          .expectBody()
          .jsonPath("$.status")
          .isEqualTo("OUT_OF_SERVICE");

      hold.release();

      assertThat(replica.exitCode(DRAIN_TIME)).isEqualTo(128 + SIGTERM);

      List<UUID> inFlight = deliveries.subList(0, MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      for (UUID delivered : inFlight) {
        assertThat(Rows.delivery(delivered))
            .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
        assertThat(Rows.attempts(delivered))
            .containsExactly(
                new Rows.Attempt(0, 1, false, "success", 200, null, "127.0.0.1", DRAINING));
      }
      assertThat(leasesStillRunning(inFlight)).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      UUID fifth = deliveries.getLast();
      assertThat(Rows.delivery(fifth))
          .isEqualTo(new Rows.Delivery("pending", 0, 0, false, null, false));
      assertThat(Rows.attempts(fifth)).isEmpty();
      assertThat(receiver.requests()).hasSize(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
    }
  }

  // A delivery that has succeeded keeps the end of its last lease as its due time. The process
  // has ended by now, so a lease that would still run shows that it ended inside that lease.
  private static int leasesStillRunning(List<UUID> deliveries) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM delivery WHERE id = ANY(:ids) AND next_attempt_at > now()")
        .param("ids", deliveries.toArray(UUID[]::new))
        .query(Integer.class)
        .single();
  }
}
