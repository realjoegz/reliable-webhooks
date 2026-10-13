package io.github.realjoegz.reliablewebhooks.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaProcess;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.client.ExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.web.client.ResourceAccessException;
import tools.jackson.databind.json.JsonMapper;

/**
 * A replica that runs as a process of its own is killed with SIGKILL while it ingests an event, and
 * the producer sends the event again, with the same key, to a second replica. The replica inside
 * the test JVM only creates the tenant and its endpoints.
 */
@UnsafeLocalReplica
@Tag("process")
class IngestKillIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String IDEMPOTENT_REPLAY = "Idempotent-Replay";
  private static final int BURST = 10;
  private static final String KILLED = "replica-killed-in-ingest";
  private static final String OTHER = "replica-that-takes-the-retry";

  @Autowired RestTestClient http;
  @Autowired JsonMapper json;

  // The test holds the row of one of the two endpoints, which the fan-out locks. The replica is
  // seen waiting there, inside its transaction and after the insert of the event, and is killed.
  // The backend that outlives it finds nobody to answer and rolls back. The producer got no
  // answer. Its second try finds no event under the key and stores one, with a delivery for each
  // endpoint.
  //
  // The charge is no part of that transaction. The bucket of the tenant does not refill here, so
  // its row shows what was charged: the two units of an event for two endpoints once the replica
  // is gone, although nothing is stored, and two more for the second try.
  @Test
  void killedBeforeCommit() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        ReplicaProcess killed = ReplicaProcess.start(KILLED);
        ReplicaProcess other = ReplicaProcess.start(OTHER)) {
      TestTenant tenant = TestTenant.create(http, "killed-before-commit");
      UUID first = register(tenant, receiver, "/first");
      UUID second = register(tenant, receiver, "/second");
      tenant.patch(http, Map.of("rate_per_sec", 0, "burst", BURST));
      String key = TestTenant.newIdempotencyKey();

      try (ExecutorService producer = Executors.newVirtualThreadPerTaskExecutor();
          Connection blocker = TestDatabase.connection();
          PreparedStatement lock =
              blocker.prepareStatement("SELECT 1 FROM endpoint WHERE id = ? FOR UPDATE")) {
        lock.setObject(1, second);
        lock.execute();
        Future<ExchangeResult> unanswered =
            producer.submit(() -> tenant.send(killed.http(), key, EVENT).returnResult());
        PgWait.untilBlocked("INSERT INTO delivery", KILLED);

        killed.kill();
        blocker.rollback();

        assertThatThrownBy(unanswered::get)
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(ResourceAccessException.class);
      }
      PgWait.untilGone(KILLED);
      assertThat(tenant.eventRows()).isZero();
      assertThat(tenant.deliveryRows()).isZero();
      assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(BURST - 2);

      ExchangeResult retried = tenant.send(other.http(), key, EVENT).returnResult();

      assertThat(retried.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      assertThat(retried.getResponseHeaders().get(IDEMPOTENT_REPLAY)).isNull();
      assertThat(eventsOf(tenant)).containsExactly(eventIdIn(retried));
      assertThat(endpointsWithADeliveryOf(tenant)).containsExactlyInAnyOrder(first, second);
      assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(BURST - 4);
    }
  }

  // No signal can be landed between a commit and the response that follows it. What such a kill
  // leaves is what a lost response leaves: the event is stored and the producer does not know.
  // The first request is therefore answered and its answer thrown away. The test knows from the
  // database that the event is committed, kills the replica and sends the request again.
  @Test
  void killedAfterCommitBeforeResponse() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        ReplicaProcess killed = ReplicaProcess.start(KILLED);
        ReplicaProcess other = ReplicaProcess.start(OTHER)) {
      TestTenant tenant = TestTenant.create(http, "killed-after-commit");
      UUID first = register(tenant, receiver, "/first");
      UUID second = register(tenant, receiver, "/second");
      String key = TestTenant.newIdempotencyKey();

      tenant.send(killed.http(), key, EVENT);
      List<UUID> committed = eventsOf(tenant);
      assertThat(committed).hasSize(1);
      killed.kill();

      ExchangeResult retried = tenant.send(other.http(), key, EVENT).returnResult();

      assertThat(retried.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      assertThat(retried.getResponseHeaders().get(IDEMPOTENT_REPLAY)).containsExactly("true");
      assertThat(eventIdIn(retried)).isEqualTo(committed.getFirst());
      assertThat(eventsOf(tenant)).isEqualTo(committed);
      assertThat(endpointsWithADeliveryOf(tenant)).containsExactlyInAnyOrder(first, second);
    }
  }

  // The replicas of this class deliver what they have accepted, so the endpoints are real ones.
  private UUID register(TestTenant tenant, FaultReceiver receiver, String path) {
    receiver.answer(path, FaultReceiver.status(200));
    return tenant.register(http, receiver.url(path), "*");
  }

  private UUID eventIdIn(ExchangeResult accepted) {
    return UUID.fromString(json.readTree(accepted.getResponseBodyContent()).get("id").asString());
  }

  private static List<UUID> eventsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM event WHERE tenant_id = :t")
        .param("t", tenant.id())
        .query(UUID.class)
        .list();
  }

  private static List<UUID> endpointsWithADeliveryOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT endpoint_id FROM delivery WHERE tenant_id = :t")
        .param("t", tenant.id())
        .query(UUID.class)
        .list();
  }
}
