package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.delivery.TenantDeliveryRepository;
import io.github.realjoegz.reliablewebhooks.delivery.TenantDeliveryRepository.Replay;
import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.SelfStartedReplica;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.client.ExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * The retention job against rows of every kind that it deletes or clears, and against the rows
 * beside them that it has to leave. The job of the replica does not run by itself in these tests,
 * so a run is the one that the test calls. The thirty days pass in the rows: the test moves their
 * times into the past.
 *
 * <p>A row that is aged by thirty days is older than that when the run comes, because it was not
 * written in the same instant. A row that is aged by a minute less is left, which shows where the
 * limit lies. A run that came more than a minute after the ageing would fail and not pass: it would
 * delete the row.
 */
@UnsafeLocalReplica
class RetentionIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String FOR_THE_DELETED_ENDPOINT =
      """
      {"type":"order.cancelled","data":{"order":17}}""";
  private static final String FOR_NO_ENDPOINT =
      """
      {"type":"order.shipped","data":{"order":17}}""";
  private static final Duration RETENTION = Duration.ofDays(30);
  private static final Duration A_MINUTE_LESS = RETENTION.minusMinutes(1);
  private static final Duration BUDGET = Duration.ofHours(72);
  private static final Duration OVERLAP = Duration.ofHours(24);
  private static final int BATCH = 1_000;
  private static final String STARTED_BY_SPRING = "replica-with-its-own-retention-job";

  @Autowired RestTestClient http;
  @Autowired JsonMapper json;
  @Autowired ReplicaWorker worker;
  @Autowired RetentionJob retention;
  @Autowired ExpiryJob expiry;
  @Autowired CancelJob cancel;
  @Autowired TenantDeliveryRepository deliveries;

  @Autowired
  @Qualifier("apiTransactions")
  TransactionTemplate apiTransactions;

  // Three deliveries died more than thirty days ago. The replay of the first runs inside a
  // transaction that the test opens on the api pool and has not committed when the retention run
  // comes: the row is locked, and to every other transaction it is still a dead delivery of that
  // age. The run does not wait for it. A run that waited would be ended by the lock timeout of
  // the worker pool and fail. It leaves the row, and it deletes the second delivery, which is the
  // witness that it ran and took deliveries of that age.
  //
  // Once the replay has committed, the delivery is pending and has no completion time. A second
  // run leaves it again, and deletes the third delivery.
  @Test
  void replayedDeliveryIsNotPurged() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "retention-replay");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID replayed = deadDelivery(tenant);
      UUID besideIt = deadDelivery(tenant);
      UUID afterIt = deadDelivery(tenant);
      Rows.ageDelivery(replayed, RETENTION);
      Rows.ageDelivery(besideIt, RETENTION);

      apiTransactions.executeWithoutResult(
          transaction -> {
            assertThat(deliveries.replay(tenant.id(), replayed)).isEqualTo(Replay.REPLAYED);

            retention.run();

            assertThat(existing(replayed, besideIt, afterIt)).containsExactly(replayed, afterIt);
            assertThat(Rows.delivery(replayed).state()).isEqualTo("dead");
          });

      Rows.Delivery pendingAgain = new Rows.Delivery("pending", 0, 1, false, null, false);
      assertThat(Rows.delivery(replayed)).isEqualTo(pendingAgain);
      Rows.ageDelivery(afterIt, RETENTION);

      retention.run();

      assertThat(existing(replayed, afterIt)).containsExactly(replayed);
      assertThat(Rows.delivery(replayed)).isEqualTo(pendingAgain);
    }
  }

  // Seven deliveries of one tenant. Two succeeded, two died and two were cancelled when their
  // endpoint was deleted. One of each pair ended thirty days ago and one a minute later. The
  // seventh is pending, and its due time and its expiry lie more than thirty days in the past.
  //
  // The run deletes the three that are in a final state and old, with their attempt rows. It
  // leaves the three that ended a minute later, and it leaves the pending one, which no age makes
  // it delete. What it leaves is as it was, column for column. The events are a few seconds old,
  // and all seven are still there.
  @Test
  void purgesOnlyOldTerminalRows() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "retention-deliveries");
      tenant.register(http, receiver.url("/hook"), "order.created");
      UUID deleted = tenant.register(http, receiver.url("/deleted"), "order.cancelled");

      worker.start();
      UUID succeededLongAgo = Rows.deliveryOf(tenant.accept(http, EVENT));
      UUID succeeded = Rows.deliveryOf(tenant.accept(http, EVENT));
      for (UUID delivery : List.of(succeededLongAgo, succeeded)) {
        await()
            .untilAsserted(
                () -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
      }
      worker.stop();
      UUID diedLongAgo = deadDelivery(tenant);
      UUID died = deadDelivery(tenant);
      UUID cancelledLongAgo = Rows.deliveryOf(tenant.accept(http, FOR_THE_DELETED_ENDPOINT));
      UUID cancelled = Rows.deliveryOf(tenant.accept(http, FOR_THE_DELETED_ENDPOINT));
      tenant.delete(http, deleted).expectStatus().isNoContent();
      cancel.run();
      UUID pending = Rows.deliveryOf(tenant.accept(http, EVENT));
      assertThat(statesOf(succeededLongAgo, diedLongAgo, cancelledLongAgo, pending))
          .containsExactly("succeeded", "dead", "cancelled", "pending");
      assertThat(Rows.attempts(succeededLongAgo)).hasSize(1);

      List<UUID> old = List.of(succeededLongAgo, diedLongAgo, cancelledLongAgo);
      List<UUID> left = List.of(succeeded, died, cancelled, pending);
      old.forEach(delivery -> Rows.ageDelivery(delivery, RETENTION));
      List.of(succeeded, died, cancelled)
          .forEach(delivery -> Rows.ageDelivery(delivery, A_MINUTE_LESS));
      Rows.ageDelivery(pending, RETENTION.plusDays(1));
      List<String> asTheyWere = Rows.everyColumnOf(left);

      retention.run();

      assertThat(existing(all(old, left))).containsExactlyInAnyOrderElementsOf(left);
      assertThat(attemptRowsOf(old)).isZero();
      assertThat(Rows.everyColumnOf(left)).isEqualTo(asTheyWere);
      assertThat(tenant.eventRows()).isEqualTo(7);
    }
  }

  // Four events of one tenant, three of them thirty days old.
  //
  // The first went to no endpoint and has no delivery. The run deletes it, and its idempotency
  // key is free: a request with that key and the same body is accepted as a new event. The second
  // is like the first and a minute younger. It stays, and a request with its key is answered as
  // the repeat that it is.
  //
  // The third has a delivery that died thirty days ago. The run deletes deliveries before events,
  // so that delivery is gone when the events are looked at, and the event goes in the same run.
  // The fourth is older than all of them and has a pending delivery. It stays, with its delivery.
  @Test
  void oldEventIsDeletedOnceItHasNoDeliveryAndItsKeyIsFreeThen() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "retention-events");
      tenant.register(http, receiver.url("/hook"), "order.created");
      String keyOfTheOld = TestTenant.newIdempotencyKey();
      String keyOfTheYounger = TestTenant.newIdempotencyKey();
      UUID old = eventIdIn(tenant.send(http, keyOfTheOld, FOR_NO_ENDPOINT).returnResult());
      UUID younger = eventIdIn(tenant.send(http, keyOfTheYounger, FOR_NO_ENDPOINT).returnResult());
      UUID withADeliveryThatDied = tenant.accept(http, EVENT);
      UUID diedLongAgo = Rows.deliveryOf(withADeliveryThatDied);
      Rows.ageDelivery(diedLongAgo, BUDGET);
      expiry.run();
      assertThat(Rows.delivery(diedLongAgo).state()).isEqualTo("dead");
      UUID withAPendingDelivery = tenant.accept(http, EVENT);
      UUID pending = Rows.deliveryOf(withAPendingDelivery);

      Rows.ageDelivery(diedLongAgo, RETENTION);
      Rows.ageEvent(old, RETENTION);
      Rows.ageEvent(younger, A_MINUTE_LESS);
      Rows.ageEvent(withADeliveryThatDied, RETENTION);
      Rows.ageEvent(withAPendingDelivery, RETENTION.plusDays(1));

      retention.run();

      assertThat(eventsOf(tenant)).containsExactlyInAnyOrder(younger, withAPendingDelivery);
      assertThat(existing(diedLongAgo, pending)).containsExactly(pending);

      ExchangeResult underTheFreedKey =
          tenant.send(http, keyOfTheOld, FOR_NO_ENDPOINT).returnResult();
      assertThat(underTheFreedKey.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      assertThat(underTheFreedKey.getResponseHeaders().get("Idempotent-Replay")).isNull();
      assertThat(eventIdIn(underTheFreedKey)).isNotEqualTo(old);
      ExchangeResult underTheKeptKey =
          tenant.send(http, keyOfTheYounger, FOR_NO_ENDPOINT).returnResult();
      assertThat(underTheKeptKey.getResponseHeaders().get("Idempotent-Replay"))
          .containsExactly("true");
      assertThat(eventIdIn(underTheKeptKey)).isEqualTo(younger);
    }
  }

  // Four endpoints of one tenant, three of them deleted.
  //
  // One was deleted without a delivery. The run deletes its row, and its circuit row with it. One
  // has a delivery that was cancelled a moment ago: it stays, and its deliveries are still
  // listed. One has a delivery that was cancelled thirty days ago. The run deletes deliveries
  // before endpoints, so the endpoint goes in the same run, and a list of its deliveries finds no
  // endpoint. The fourth is active and has no delivery, and stays.
  @Test
  void deletedEndpointIsRemovedOnceItHasNoDelivery() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "retention-endpoints");
      UUID active = tenant.register(http, receiver.url("/active"), "order.shipped");
      UUID withoutDeliveries = tenant.register(http, receiver.url("/none"), "order.shipped");
      UUID withARecentDelivery = tenant.register(http, receiver.url("/recent"), "order.created");
      UUID withAnOldDelivery = tenant.register(http, receiver.url("/old"), "order.cancelled");
      UUID recent = Rows.deliveryOf(tenant.accept(http, EVENT));
      UUID cancelledLongAgo = Rows.deliveryOf(tenant.accept(http, FOR_THE_DELETED_ENDPOINT));
      for (UUID endpoint : List.of(withoutDeliveries, withARecentDelivery, withAnOldDelivery)) {
        tenant.delete(http, endpoint).expectStatus().isNoContent();
      }
      cancel.run();
      assertThat(statesOf(recent, cancelledLongAgo)).containsExactly("cancelled", "cancelled");
      Rows.ageDelivery(cancelledLongAgo, RETENTION);

      retention.run();

      assertThat(endpointsOf(tenant)).containsExactlyInAnyOrder(active, withARecentDelivery);
      assertThat(circuitsOf(active, withoutDeliveries, withARecentDelivery, withAnOldDelivery))
          .containsExactlyInAnyOrder(active, withARecentDelivery);
      assertThat(existing(recent, cancelledLongAgo)).containsExactly(recent);
      listCancelled(tenant, withARecentDelivery)
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.deliveries[0].id")
          .isEqualTo(recent.toString());
      listCancelled(tenant, withAnOldDelivery).expectStatus().isNotFound();
    }
  }

  // The secrets of three endpoints. Two were rotated, and the secret from before the rotation of
  // the first stopped counting when its 24 hours ended, which the test brings about by ageing
  // that time. The run clears it and its time, and leaves the secret of the endpoint as it is.
  // The second has a minute of its 24 hours left and keeps both secrets. The third was never
  // rotated.
  @Test
  void secretFromBeforeARotationIsClearedOnceItsTwentyFourHoursAreOver() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "retention-secrets");
      UUID over = tenant.register(http, receiver.url("/over"), "*");
      UUID running = tenant.register(http, receiver.url("/running"), "*");
      UUID neverRotated = tenant.register(http, receiver.url("/never"), "*");
      tenant.rotateSecret(http, over).expectStatus().isOk();
      tenant.rotateSecret(http, running).expectStatus().isOk();
      Rows.agePreviousSecret(over, OVERLAP);
      Rows.agePreviousSecret(running, OVERLAP.minusMinutes(1));
      String secretOfTheFirst = secretColumnsOf(over).get("secret");
      Map<String, String> ofTheSecond = secretColumnsOf(running);
      Map<String, String> ofTheThird = secretColumnsOf(neverRotated);
      assertThat(secretColumnsOf(over).values()).doesNotContainNull();

      retention.run();

      assertThat(secretColumnsOf(over))
          .containsEntry("secret", secretOfTheFirst)
          .containsEntry("prev_secret", null)
          .containsEntry("prev_secret_expires_at", null);
      assertThat(secretColumnsOf(running)).isEqualTo(ofTheSecond).doesNotContainValue(null);
      assertThat(secretColumnsOf(neverRotated)).isEqualTo(ofTheThird);
    }
  }

  // The test holds three rows that the run would take, in a transaction of its own: an old event
  // without a delivery, a deleted endpoint without one, and an endpoint whose old secret no
  // longer counts. Each has a twin that nobody holds. The run does not wait: it takes the three
  // twins and leaves the held rows for the run that follows the rollback. A run that waited would
  // be ended by the lock timeout of the worker pool and fail.
  @Test
  void runPassesOverRowsThatAnotherTransactionHolds() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "retention-skip-locked");
      UUID heldEvent = tenant.accept(http, FOR_NO_ENDPOINT);
      UUID freeEvent = tenant.accept(http, FOR_NO_ENDPOINT);
      UUID heldDeleted = tenant.register(http, receiver.url("/held-deleted"), "*");
      UUID freeDeleted = tenant.register(http, receiver.url("/free-deleted"), "*");
      UUID heldRotated = tenant.register(http, receiver.url("/held-rotated"), "*");
      UUID freeRotated = tenant.register(http, receiver.url("/free-rotated"), "*");
      for (UUID event : List.of(heldEvent, freeEvent)) {
        Rows.ageEvent(event, RETENTION);
      }
      for (UUID endpoint : List.of(heldDeleted, freeDeleted)) {
        tenant.delete(http, endpoint).expectStatus().isNoContent();
      }
      for (UUID endpoint : List.of(heldRotated, freeRotated)) {
        tenant.rotateSecret(http, endpoint).expectStatus().isOk();
        Rows.agePreviousSecret(endpoint, OVERLAP);
      }

      try (Connection holder = TestDatabase.connection();
          PreparedStatement lockEvent =
              holder.prepareStatement("SELECT 1 FROM event WHERE id = ? FOR UPDATE");
          PreparedStatement lockEndpoints =
              holder.prepareStatement("SELECT 1 FROM endpoint WHERE id IN (?, ?) FOR UPDATE")) {
        lockEvent.setObject(1, heldEvent);
        lockEvent.execute();
        lockEndpoints.setObject(1, heldDeleted);
        lockEndpoints.setObject(2, heldRotated);
        lockEndpoints.execute();

        retention.run();

        assertThat(eventsOf(tenant)).containsExactly(heldEvent);
        assertThat(endpointsOf(tenant))
            .containsExactlyInAnyOrder(heldDeleted, heldRotated, freeRotated);
        assertThat(secretColumnsOf(freeRotated)).containsEntry("prev_secret", null);
        assertThat(secretColumnsOf(heldRotated).values()).doesNotContainNull();
        holder.rollback();
      }

      retention.run();

      assertThat(eventsOf(tenant)).isEmpty();
      assertThat(endpointsOf(tenant)).containsExactlyInAnyOrder(heldRotated, freeRotated);
      assertThat(secretColumnsOf(heldRotated)).containsEntry("prev_secret", null);
    }
  }

  // Two and a half batches of deliveries died more than thirty days ago, each the only delivery
  // of an event of that age. One run deletes them all, and then their events: a step goes on for
  // as long as its statement takes a full batch.
  @Test
  void runGoesOnUntilAStatementTakesLessThanABatch() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "retention-batches");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      insertDeliveriesThatDiedLongAgo(tenant, endpoint, 2 * BATCH + BATCH / 2);
      assertThat(tenant.deliveryRows()).isEqualTo(2 * BATCH + BATCH / 2);
      assertThat(tenant.eventRows()).isEqualTo(2 * BATCH + BATCH / 2);

      retention.run();

      assertThat(tenant.deliveryRows()).isZero();
      assertThat(tenant.eventRows()).isZero();
    }
  }

  // Every other test calls the run itself. Here a second replica starts as the application does,
  // and Spring starts its retention job, whose first run comes with the start.
  @Test
  void replicaRunsItsRetentionJobByItself() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "purged-by-a-replica");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID diedLongAgo = deadDelivery(tenant);
      Rows.ageDelivery(diedLongAgo, RETENTION);

      try (ConfigurableApplicationContext replica = SelfStartedReplica.start(STARTED_BY_SPRING)) {
        await().untilAsserted(() -> assertThat(existing(diedLongAgo)).isEmpty());
      }
    }
  }

  // A delivery of the endpoint that takes the event of these tests, which expired while it was
  // pending. No worker runs when this is called, so nothing claims the delivery before the
  // expiry run comes.
  private UUID deadDelivery(TestTenant tenant) {
    UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
    Rows.ageDelivery(delivery, BUDGET);
    expiry.run();
    assertThat(Rows.delivery(delivery).state()).isEqualTo("dead");
    return delivery;
  }

  private UUID eventIdIn(ExchangeResult accepted) {
    return UUID.fromString(json.readTree(accepted.getResponseBodyContent()).get("id").asString());
  }

  private RestTestClient.ResponseSpec listCancelled(TestTenant tenant, UUID endpointId) {
    return http.get()
        .uri("/v1/deliveries?endpoint_id={id}&state=cancelled", endpointId)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  private static List<UUID> all(List<UUID> some, List<UUID> others) {
    List<UUID> all = new ArrayList<>(some);
    all.addAll(others);
    return all;
  }

  /** Those of the deliveries that still have a row, in the order in which they are given. */
  private static List<UUID> existing(UUID... deliveryIds) {
    return existing(List.of(deliveryIds));
  }

  private static List<UUID> existing(List<UUID> deliveryIds) {
    List<UUID> rows =
        TestDatabase.jdbc()
            .sql("SELECT id FROM delivery WHERE id = ANY(:ids)")
            .param("ids", deliveryIds.toArray(UUID[]::new))
            .query(UUID.class)
            .list();
    return deliveryIds.stream().filter(rows::contains).toList();
  }

  private static List<String> statesOf(UUID... deliveryIds) {
    return Stream.of(deliveryIds).map(id -> Rows.delivery(id).state()).toList();
  }

  private static int attemptRowsOf(List<UUID> deliveryIds) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM delivery_attempt WHERE delivery_id = ANY(:ids)")
        .param("ids", deliveryIds.toArray(UUID[]::new))
        .query(Integer.class)
        .single();
  }

  private static List<UUID> eventsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM event WHERE tenant_id = :t")
        .param("t", tenant.id())
        .query(UUID.class)
        .list();
  }

  private static List<UUID> endpointsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM endpoint WHERE tenant_id = :t")
        .param("t", tenant.id())
        .query(UUID.class)
        .list();
  }

  /** Those of the endpoints that have a circuit row. */
  private static List<UUID> circuitsOf(UUID... endpointIds) {
    return TestDatabase.jdbc()
        .sql("SELECT endpoint_id FROM endpoint_circuit WHERE endpoint_id = ANY(:ids)")
        .param("ids", endpointIds)
        .query(UUID.class)
        .list();
  }

  /** The three columns of an endpoint that hold its secrets, each as text or as null. */
  private static Map<String, String> secretColumnsOf(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT encode(secret, 'hex') AS secret, encode(prev_secret, 'hex') AS prev_secret,
                   prev_secret_expires_at::text AS prev_secret_expires_at
            FROM endpoint WHERE id = :id
            """)
        .param("id", endpointId)
        .query(
            (row, n) -> {
              Map<String, String> columns = new HashMap<>();
              for (String column : List.of("secret", "prev_secret", "prev_secret_expires_at")) {
                columns.put(column, row.getString(column));
              }
              return columns;
            })
        .single();
  }

  // Events with one delivery each, as they are once the delivery has expired, but created forty
  // days ago and dead since three days after that. They are written here and not accepted
  // through the API: thousands of requests are no part of what the test is about.
  private static void insertDeliveriesThatDiedLongAgo(
      TestTenant tenant, UUID endpointId, int count) {
    TestDatabase.jdbc()
        .sql(
            """
            WITH old AS (
              INSERT INTO event (id, tenant_id, type, body, idempotency_key, request_hash,
                                 created_at)
              SELECT gen_random_uuid(), :t, 'order.created', :body, 'long-ago-' || n,
                     sha256(:body), now() - interval '40 days'
              FROM generate_series(1, :count) AS n
              RETURNING id, created_at
            )
            INSERT INTO delivery (tenant_id, event_id, endpoint_id, state, dead_reason,
                                  next_attempt_at, expires_at, created_at, completed_at)
            SELECT :t, id, :e, 'dead', 'expired', created_at, created_at + interval '72 hours',
                   created_at, created_at + interval '72 hours'
            FROM old
            """)
        .param("t", tenant.id())
        .param("e", endpointId)
        .param("body", EVENT.getBytes(StandardCharsets.UTF_8))
        .param("count", count)
        .update();
  }
}
