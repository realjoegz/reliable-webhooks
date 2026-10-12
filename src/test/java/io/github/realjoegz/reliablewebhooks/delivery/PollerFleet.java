package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.endpoint.SecretCipher;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Starts pollers beside the replica inside the test JVM. Each has a worker and a replica id of its
 * own, and its own count of what it has in flight. They use the connection pools, the HTTP client
 * and the meters of that replica, and they meet where replicas meet: in the database.
 */
@Component
final class PollerFleet {

  private static final Duration DRAIN_WAIT = Duration.ofSeconds(20);

  /** A poller and the thread of its loop. */
  private record Replica(DeliveryPoller poller, Thread thread) {}

  /** The pollers of one start. Closing them ends their loops and waits for their attempts. */
  final class Running implements AutoCloseable {

    private final List<Replica> replicas = new ArrayList<>();

    private Running(String... replicaIds) {
      for (String replicaId : replicaIds) {
        replicas.add(startPoller(replicaId));
      }
    }

    @Override
    public void close() throws InterruptedException {
      for (Replica replica : replicas) {
        replica.poller().stop();
      }
      for (Replica replica : replicas) {
        replica.thread().join();
        assertThat(replica.poller().awaitIdle(DRAIN_WAIT)).isTrue();
      }
    }
  }

  private final DeliveryRepository deliveries;
  private final CircuitBreaker circuits;
  private final WebhookHttpClient client;
  private final SecretCipher secrets;
  private final DeliveryMeters meters;
  private final WebhooksProperties properties;

  PollerFleet(
      DeliveryRepository deliveries,
      CircuitBreaker circuits,
      WebhookHttpClient client,
      SecretCipher secrets,
      DeliveryMeters meters,
      WebhooksProperties properties) {
    this.deliveries = deliveries;
    this.circuits = circuits;
    this.client = client;
    this.secrets = secrets;
    this.meters = meters;
    this.properties = properties;
  }

  /** Starts one poller for each replica id. Each records its attempts under its id. */
  Running start(String... replicaIds) {
    return new Running(replicaIds);
  }

  private Replica startPoller(String replicaId) {
    DeliveryWorker worker =
        new DeliveryWorker(
            deliveries,
            circuits,
            client,
            secrets,
            meters,
            properties.delivery(),
            replicaId,
            System::nanoTime,
            Thread::sleep);
    DeliveryPoller poller =
        new DeliveryPoller(
            deliveries, worker, meters, new PollerHeartbeat(System::nanoTime), properties);
    return new Replica(
        poller, Thread.ofPlatform().name("poller-of-" + replicaId).start(poller::run));
  }
}
