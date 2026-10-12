package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.SelfStartedReplica;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.web.server.context.WebServerGracefulShutdownLifecycle;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
@ExtendWith(OutputCaptureExtension.class)
class WorkerLifecycleIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int MAX_IN_FLIGHT_FOR_AN_ENDPOINT = 4;
  private static final String STARTED_BY_SPRING = "replica-started-by-spring";
  private static final String POLLER_STOPPED = "\"message\":\"poller_stopped\"";

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired ObjectProvider<WorkerLifecycle> lifecycles;

  // Spring stops the lifecycle with the highest phase first. The web server waits for its open
  // requests in a phase of its own, and the worker has stopped by the time that phase begins.
  @Test
  void lifecycleAsksToBeStoppedBeforeTheWebServer() {
    assertThat(lifecycles.getObject().getPhase())
        .isGreaterThan(WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE);
  }

  // Every other test starts the worker of its replica itself. Here a second replica starts as
  // the application does, with nothing taken from Spring, and Spring starts its worker. The
  // attempt is recorded under the id of that replica, and the replica of this class, whose
  // worker is not running, cannot have sent it. Closing the second replica stops its worker.
  @Test
  void replicaStartsItsWorkerByItselfAndStopsItWhenItCloses(CapturedOutput output)
      throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "started-by-spring");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));

      try (ConfigurableApplicationContext replica =
          SelfStartedReplica.start(STARTED_BY_SPRING, "unsafe-local")) {
        await()
            .untilAsserted(
                () -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
        assertThat(output.getOut()).doesNotContain(POLLER_STOPPED);
      }

      assertThat(output.getOut()).contains(POLLER_STOPPED);
      assertThat(Rows.attempts(delivery))
          .containsExactly(
              new Rows.Attempt(0, 1, false, "success", 200, null, "127.0.0.1", STARTED_BY_SPRING));
    }
  }

  // Spring may stop a lifecycle and start it again. The poller is the one that was stopped, and
  // it claims again.
  @Test
  void lifecycleThatWasStoppedDeliversAgainOnceItIsStarted() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "started-again");
      tenant.register(http, receiver.url("/hook"), "*");
      WorkerLifecycle lifecycle = lifecycles.getObject();
      try {
        lifecycle.start();
        UUID first = Rows.deliveryOf(tenant.accept(http, EVENT));
        await()
            .untilAsserted(() -> assertThat(Rows.delivery(first).state()).isEqualTo("succeeded"));
        lifecycle.stop();
        assertThat(lifecycle.isRunning()).isFalse();

        UUID second = Rows.deliveryOf(tenant.accept(http, EVENT));
        lifecycle.start();
        await()
            .untilAsserted(() -> assertThat(Rows.delivery(second).state()).isEqualTo("succeeded"));
      } finally {
        if (lifecycle.isRunning()) {
          lifecycle.stop();
        }
      }
    }
  }

  // Five deliveries are due for one endpoint, and the receiver holds its requests. Four are in
  // flight, which is all that an endpoint may have, so the fifth is unclaimed for that reason
  // only. The stop then runs as it does when the application shuts down. Once it has logged
  // poller_stopped, the receiver answers. A poller that still claimed would take the fifth
  // delivery as soon as one of the four had finished.
  @Test
  void stopClaimsNothingMoreAndWaitsForTheAttemptsInFlight(CapturedOutput output) throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200));
        ExecutorService shutdown = Executors.newVirtualThreadPerTaskExecutor()) {
      receiver.answer("/hook", hold);
      TestTenant tenant = TestTenant.create(http, "drain");
      tenant.register(http, receiver.url("/hook"), "*");
      List<UUID> deliveries = new ArrayList<>();
      for (int i = 0; i < MAX_IN_FLIGHT_FOR_AN_ENDPOINT + 1; i++) {
        deliveries.add(Rows.deliveryOf(tenant.accept(http, EVENT)));
      }
      worker.start();
      await().until(() -> hold.waiting() == MAX_IN_FLIGHT_FOR_AN_ENDPOINT);

      Future<?> stop = shutdown.submit(worker::stop);
      await().until(() -> output.getOut().contains(POLLER_STOPPED));
      assertThat(stop).isNotDone();
      assertThat(hold.waiting()).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);

      hold.release();
      stop.get(30, TimeUnit.SECONDS);

      for (UUID delivered : deliveries.subList(0, MAX_IN_FLIGHT_FOR_AN_ENDPOINT)) {
        assertThat(Rows.delivery(delivered))
            .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
        assertThat(Rows.attempts(delivered))
            .containsExactly(
                new Rows.Attempt(
                    0,
                    1,
                    false,
                    "success",
                    200,
                    null,
                    "127.0.0.1",
                    ReplicaSettings.IN_PROCESS_REPLICA));
      }
      UUID fifth = deliveries.getLast();
      assertThat(Rows.delivery(fifth))
          .isEqualTo(new Rows.Delivery("pending", 0, 0, false, null, false));
      assertThat(Rows.attempts(fifth)).isEmpty();
      assertThat(receiver.requests()).hasSize(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      assertThat(receiver.peakConcurrency("/hook")).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
    }
  }
}
