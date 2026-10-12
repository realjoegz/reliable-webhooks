package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.delivery.BulkReplay;
import io.github.realjoegz.reliablewebhooks.delivery.TenantDeliveryRepository;
import io.github.realjoegz.reliablewebhooks.delivery.TenantDeliveryRepository.Replay;
import io.github.realjoegz.reliablewebhooks.endpoint.EndpointRepository;
import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.SelfStartedReplica;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import io.github.realjoegz.reliablewebhooks.ingest.EventRequest;
import io.github.realjoegz.reliablewebhooks.ingest.IngestService;
import io.github.realjoegz.reliablewebhooks.ops.CancelJob;
import io.github.realjoegz.reliablewebhooks.ops.ExpiryJob;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
 * The deletion of an endpoint against the deliveries that it has and against the requests that are
 * creating or re-arming one at that moment. The cancel job of the replica does not run by itself in
 * these tests, so a delivery is cancelled by the run that the test calls and by nothing else.
 *
 * <p>The two tests of a race force each order of the two racers. The one that goes first runs
 * inside a transaction that the test opens on the api pool of the replica: it is the code of the
 * service, and the test decides when it commits. The other one is a request to the replica, and the
 * test commits once the database shows that request waiting for the row of the endpoint. A request
 * that waits is ended by the lock timeout of the api pool after 2 s, so the test has that long. A
 * run that took longer would fail and not pass: the request would be answered with a 503.
 */
@UnsafeLocalReplica
class EndpointDeleteIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String CANARY =
      """
      {"type":"canary.sent","data":{}}""";
  private static final String FOR_THE_ENDPOINT_DELETED_FIRST =
      """
      {"type":"deleted.first","data":{}}""";
  private static final String FOR_THE_ENDPOINT_DELETED_SECOND =
      """
      {"type":"deleted.second","data":{}}""";
  private static final int MAX_IN_FLIGHT_FOR_AN_ENDPOINT = 4;
  private static final int STILL_DUE = 2;
  private static final int BATCH = 1_000;
  private static final Duration BUDGET = Duration.ofHours(72);
  private static final Duration AN_HOUR = Duration.ofHours(1);
  private static final String LOAD = "JOIN event ev";
  private static final String DELETE = "UPDATE endpoint SET status = 'deleted'";
  private static final String FAN_OUT = "INSERT INTO delivery";
  private static final String SHARE_LOCK_OF_A_REPLAY = "FOR SHARE";
  private static final String STARTED_BY_SPRING = "replica-with-its-own-cancel-job";

  private static final Rows.Delivery NEVER_CLAIMED =
      new Rows.Delivery("pending", 0, 0, false, null, false);
  private static final Rows.Delivery LEASED_ONCE =
      new Rows.Delivery("pending", 1, 0, true, null, false);
  private static final Rows.Delivery CANCELLED =
      new Rows.Delivery("cancelled", 0, 0, false, "endpoint_deleted", true);

  @Autowired RestTestClient http;
  @Autowired JsonMapper json;
  @Autowired ReplicaWorker worker;
  @Autowired CancelJob cancel;
  @Autowired ExpiryJob expiry;
  @Autowired MeterRegistry meters;
  @Autowired EndpointRepository endpoints;
  @Autowired IngestService ingest;
  @Autowired TenantDeliveryRepository deliveries;
  @Autowired BulkReplay bulkReplay;

  @Autowired
  @Qualifier("apiTransactions")
  TransactionTemplate apiTransactions;

  // An endpoint has six deliveries due, and the receiver holds every request that it gets for
  // it. Four are in flight, which is all that an endpoint may have, so the other two are due and
  // unclaimed only because the endpoint is full. Then the endpoint is deleted and the receiver
  // answers.
  //
  // The four requests were sent before the delete, and each is recorded as the success that it
  // was. The endpoint now has room, and the two deliveries are still due. That no claim takes
  // them needs a witness: an event that is accepted after the four have succeeded goes to a
  // second endpoint and is delivered, so a claim ran while the two were due and the endpoint had
  // room. The event would have matched the deleted endpoint as well, and has no delivery for it.
  //
  // The two rows are untouched until the cancel run, which cancels them and leaves the four that
  // succeeded as they are. The tenant no longer reads the endpoint, and still lists its
  // deliveries.
  //
  // A held request is cut at the deadline of an attempt, 15 s after it began. A run that took
  // that long to delete the endpoint would fail: the four would be recorded as failed.
  @Test
  void deleteStopsDeliveryAndCancelsPending() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer("/deleted", hold);
      receiver.answer("/kept", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "delete-with-deliveries");
      UUID deleted = tenant.register(http, receiver.url("/deleted"), "*");
      UUID kept = tenant.register(http, receiver.url("/kept"), "canary.sent");
      worker.start();
      for (int i = 0; i < MAX_IN_FLIGHT_FOR_AN_ENDPOINT + STILL_DUE; i++) {
        tenant.accept(http, EVENT);
      }
      await().until(() -> hold.waiting() == MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      List<UUID> inFlight = deliveriesOf(deleted, true);
      List<UUID> stillDue = deliveriesOf(deleted, false);
      assertThat(inFlight).hasSize(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      assertThat(stillDue).hasSize(STILL_DUE);
      List<String> asTheDeleteFoundThem = Rows.everyColumnOf(stillDue);

      tenant.delete(http, deleted).expectStatus().isNoContent();
      hold.release();
      for (UUID delivery : inFlight) {
        await()
            .untilAsserted(
                () -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
      }
      UUID canary = Rows.deliveryOf(tenant.accept(http, CANARY));
      await().untilAsserted(() -> assertThat(Rows.delivery(canary).state()).isEqualTo("succeeded"));

      assertThat(endpointOf(canary)).isEqualTo(kept);
      assertThat(Rows.everyColumnOf(stillDue)).isEqualTo(asTheDeleteFoundThem);
      assertThat(stillDue)
          .allSatisfy(due -> assertThat(Rows.delivery(due)).isEqualTo(NEVER_CLAIMED));
      assertThat(receiver.requests("/deleted")).hasSize(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);

      cancel.run();

      assertThat(stillDue).allSatisfy(due -> assertThat(Rows.delivery(due)).isEqualTo(CANCELLED));
      assertThat(inFlight)
          .allSatisfy(
              sent ->
                  assertThat(Rows.delivery(sent))
                      .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true)));
      assertThat(receiver.requests("/deleted")).hasSize(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      get(tenant, "/v1/endpoints/" + deleted).expectStatus().isNotFound();
      get(tenant, "/v1/deliveries?endpoint_id=" + deleted + "&state=cancelled")
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.deliveries.length()")
          .isEqualTo(STILL_DUE)
          .jsonPath("$.deliveries[0].dead_reason")
          .isEqualTo("endpoint_deleted");
    }
  }

  // A delivery is claimed before its endpoint is deleted and loaded after it. The test locks the
  // event table, which a claim does not read and the load does, so a delivery that is claimed
  // waits in its load. A delivery for a second endpoint is claimed first and is seen waiting
  // there. Only then does the test make the delivery for the first endpoint due, which it had
  // moved an hour ahead. Once both loads wait, the endpoint is deleted and the lock is released.
  //
  // The load of the second delivery finds its endpoint, and the delivery succeeds with its first
  // attempt. The load of the first finds no active endpoint. Nothing is sent for it, and it
  // stays leased, with its attempt counted and nothing recorded, until the cancel run cancels
  // it. The worker is stopped before the rows are read. A stop waits for every attempt in
  // flight, so a request for the first delivery would have arrived by then.
  //
  // A load that waits is ended by the lock timeout of the worker pool after 5 s. The load of the
  // second delivery began to wait first, so the test has that long from then to the release of
  // the lock. A run that took longer would fail and not pass: that load would fail, and the
  // second delivery would not succeed with its first attempt.
  @Test
  void deliveryClaimedBeforeTheDeleteIsNotSentAfterIt() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/deleted", FaultReceiver.status(200));
      receiver.answer("/kept", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "delete-after-claim");
      UUID deleted = tenant.register(http, receiver.url("/deleted"), "deleted.first");
      tenant.register(http, receiver.url("/kept"), "canary.sent");
      UUID claimed = Rows.deliveryOf(tenant.accept(http, FOR_THE_ENDPOINT_DELETED_FIRST));
      Rows.postponeDelivery(claimed, AN_HOUR);
      UUID loadedBesideIt = Rows.deliveryOf(tenant.accept(http, CANARY));

      try (Connection blocker = TestDatabase.connection();
          Statement lock = blocker.createStatement()) {
        lock.execute("LOCK TABLE event IN ACCESS EXCLUSIVE MODE");
        worker.start();
        PgWait.untilBlocked(LOAD, ReplicaSettings.IN_PROCESS_REPLICA);
        Rows.ageDelivery(claimed, AN_HOUR);
        PgWait.untilBlocked(LOAD, ReplicaSettings.IN_PROCESS_REPLICA, 2);

        tenant.delete(http, deleted).expectStatus().isNoContent();
        blocker.rollback();
      }
      await()
          .untilAsserted(
              () -> assertThat(Rows.delivery(loadedBesideIt).state()).isEqualTo("succeeded"));
      worker.stop();

      assertThat(Rows.delivery(loadedBesideIt))
          .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
      assertThat(Rows.delivery(claimed)).isEqualTo(LEASED_ONCE);
      assertThat(Rows.attempts(claimed)).isEmpty();
      assertThat(receiver.requests("/deleted")).isEmpty();

      cancel.run();

      assertThat(Rows.delivery(claimed))
          .isEqualTo(new Rows.Delivery("cancelled", 1, 0, false, "endpoint_deleted", true));
    }
  }

  // The request of a delivery is held when its endpoint is deleted, and the cancel run comes
  // before the answer. The delivery is cancelled while it is leased, and loses its lease token
  // with that. The answer is a 200, and the counter shows that the finalize of the attempt ran
  // and was turned away: the row stays as the cancel left it, and no attempt is logged.
  @Test
  void resultOfARequestInFlightIsNotRecordedOnceItsDeliveryIsCancelled() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer("/hook", hold);
      TestTenant tenant = TestTenant.create(http, "cancel-in-flight");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      double lostLeasesBefore = lostLeases();
      worker.start();
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await().until(() -> hold.waiting() == 1);

      tenant.delete(http, endpoint).expectStatus().isNoContent();
      cancel.run();

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("cancelled", 1, 0, false, "endpoint_deleted", true));
      List<String> asTheCancelLeftIt = Rows.everyColumnOf(List.of(delivery));

      hold.release();
      await().untilAsserted(() -> assertThat(lostLeases() - lostLeasesBefore).isEqualTo(1));

      assertThat(Rows.everyColumnOf(List.of(delivery))).isEqualTo(asTheCancelLeftIt);
      assertThat(Rows.attempts(delivery)).isEmpty();
      assertThat(receiver.requests()).hasSize(1);
    }
  }

  // Delete first: the transaction of the test holds the delete of one endpoint, and an event for
  // it is sent. The request has matched the endpoint, because the delete is not committed, and
  // waits in its fan-out for the row. After the commit it reads the status again under its own
  // lock. The event is accepted with a delivery for the endpoint of the tenant that takes every
  // event, and with none for the deleted one.
  //
  // Ingest first: the transaction of the test holds the ingest of an event for a second endpoint,
  // with its delivery created and the row of the endpoint share-locked. The delete waits for the
  // row. After the commit it deletes the endpoint, and the delivery is there and pending: the
  // cancel run finds it and cancels it.
  //
  // Neither order leaves a delivery that is pending for a deleted endpoint once the cancel job
  // has run.
  @Test
  void ingestRacingDeleteLeavesNoOrphan() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
      TestTenant tenant = TestTenant.create(http, "ingest-races-delete");
      UUID deletedFirst = tenant.register(http, receiver.url("/first"), "deleted.first");
      UUID deletedSecond = tenant.register(http, receiver.url("/second"), "deleted.second");
      UUID kept = tenant.register(http, receiver.url("/kept"), "*");

      Future<ExchangeResult> ingestBehindTheDelete =
          apiTransactions.execute(
              transaction -> {
                assertThat(endpoints.delete(tenant.id(), deletedFirst)).isTrue();
                Future<ExchangeResult> waiting =
                    callers.submit(
                        () ->
                            tenant
                                .send(
                                    http,
                                    TestTenant.newIdempotencyKey(),
                                    FOR_THE_ENDPOINT_DELETED_FIRST)
                                .returnResult());
                PgWait.untilBlocked(FAN_OUT, ReplicaSettings.IN_PROCESS_REPLICA);
                return waiting;
              });

      ExchangeResult accepted = ingestBehindTheDelete.get();
      assertThat(accepted.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      assertThat(endpointsWithADeliveryOf(eventIdIn(accepted))).containsExactly(kept);
      assertThat(deliveriesOf(deletedFirst)).isEmpty();

      EventRequest request =
          EventRequest.parse(FOR_THE_ENDPOINT_DELETED_SECOND.getBytes(StandardCharsets.UTF_8))
              .orElseThrow();
      Future<ExchangeResult> deleteBehindTheIngest =
          apiTransactions.execute(
              transaction -> {
                assertThat(ingest.ingest(tenant.id(), TestTenant.newIdempotencyKey(), request))
                    .isInstanceOf(IngestService.Accepted.class);
                Future<ExchangeResult> waiting =
                    callers.submit(() -> tenant.delete(http, deletedSecond).returnResult());
                PgWait.untilBlocked(DELETE, ReplicaSettings.IN_PROCESS_REPLICA);
                return waiting;
              });

      assertThat(deleteBehindTheIngest.get().getStatus()).isEqualTo(HttpStatus.NO_CONTENT);
      List<UUID> created = deliveriesOf(deletedSecond);
      assertThat(created).hasSize(1);
      assertThat(Rows.delivery(created.getFirst())).isEqualTo(NEVER_CLAIMED);

      cancel.run();

      assertThat(Rows.delivery(created.getFirst())).isEqualTo(CANCELLED);
      assertThat(pendingDeliveriesOf(deletedFirst, deletedSecond)).isZero();
    }
  }

  // Each of two endpoints has two dead deliveries, one for the replay of a single delivery and
  // one for the replay of the dead deliveries of an endpoint. The two replays lock the row of the
  // endpoint with statements of their own, so both race the delete.
  //
  // Delete first: the transaction of the test holds the delete of one endpoint, and both replays
  // are sent. Each has paid and waits for the row. After the commit each reads the status under
  // its own lock and answers 404, and the two deliveries are dead as they were, column for
  // column. Neither charge is given back. The bucket of the tenant does not refill, and it is
  // three units lower: one for the replay of the delivery, and two for the two dead deliveries
  // that the other replay counted.
  //
  // Replay first: the transaction of the test holds both replays for the second endpoint, with
  // its two deliveries pending again and its row share-locked. The delete waits for the row.
  // After the commit it deletes the endpoint, and the cancel run cancels the two deliveries.
  //
  // Neither order leaves a delivery that is pending for a deleted endpoint once the cancel job
  // has run.
  @Test
  void replayRacingDeleteLeavesNoOrphan() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
      TestTenant tenant = TestTenant.create(http, "replay-races-delete");
      tenant.patch(http, Map.of("rate_per_sec", 0));
      UUID deletedFirst = tenant.register(http, receiver.url("/first"), "deleted.first");
      UUID deletedSecond = tenant.register(http, receiver.url("/second"), "deleted.second");
      List<UUID> ofTheFirst = twoDeadDeliveries(tenant, FOR_THE_ENDPOINT_DELETED_FIRST);
      List<UUID> ofTheSecond = twoDeadDeliveries(tenant, FOR_THE_ENDPOINT_DELETED_SECOND);
      List<String> dead = Rows.everyColumnOf(ofTheFirst);
      double tokens = Rows.bucketTokens(tenant.id());

      List<Future<ExchangeResult>> replaysBehindTheDelete =
          apiTransactions.execute(
              transaction -> {
                assertThat(endpoints.delete(tenant.id(), deletedFirst)).isTrue();
                Future<ExchangeResult> ofOne =
                    callers.submit(() -> tenant.replay(http, ofTheFirst.getFirst()).returnResult());
                Future<ExchangeResult> ofMany =
                    callers.submit(() -> tenant.replayDead(http, deletedFirst, 10).returnResult());
                PgWait.untilBlocked(SHARE_LOCK_OF_A_REPLAY, ReplicaSettings.IN_PROCESS_REPLICA, 2);
                return List.of(ofOne, ofMany);
              });

      for (Future<ExchangeResult> replay : replaysBehindTheDelete) {
        assertThat(replay.get().getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
      }
      assertThat(Rows.everyColumnOf(ofTheFirst)).isEqualTo(dead);
      assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(tokens - 3);

      Future<ExchangeResult> deleteBehindTheReplays =
          apiTransactions.execute(
              transaction -> {
                assertThat(deliveries.replay(tenant.id(), ofTheSecond.getFirst()))
                    .isEqualTo(Replay.REPLAYED);
                assertThat(bulkReplay.replay(asAuthenticated(tenant), deletedSecond, 10))
                    .isEqualTo(new BulkReplay.Replayed(1, false));
                Future<ExchangeResult> waiting =
                    callers.submit(() -> tenant.delete(http, deletedSecond).returnResult());
                PgWait.untilBlocked(DELETE, ReplicaSettings.IN_PROCESS_REPLICA);
                return waiting;
              });

      assertThat(deleteBehindTheReplays.get().getStatus()).isEqualTo(HttpStatus.NO_CONTENT);
      assertThat(ofTheSecond)
          .allSatisfy(
              replayed ->
                  assertThat(Rows.delivery(replayed))
                      .isEqualTo(new Rows.Delivery("pending", 0, 1, false, null, false)));

      cancel.run();

      assertThat(ofTheSecond)
          .allSatisfy(
              replayed ->
                  assertThat(Rows.delivery(replayed))
                      .isEqualTo(
                          new Rows.Delivery("cancelled", 0, 1, false, "endpoint_deleted", true)));
      assertThat(pendingDeliveriesOf(deletedFirst, deletedSecond)).isZero();
    }
  }

  // Two and a half batches of deliveries are pending for an endpoint when it is deleted. One run
  // cancels them all: it goes on for as long as its statement cancels a full batch.
  @Test
  void cancelRunGoesOnUntilAStatementCancelsLessThanABatch() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "cancel-batches");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      insertPendingDeliveries(tenant, endpoint, 2 * BATCH + BATCH / 2);
      tenant.delete(http, endpoint).expectStatus().isNoContent();
      assertThat(pendingDeliveriesOf(endpoint)).isEqualTo(2 * BATCH + BATCH / 2);

      cancel.run();

      assertThat(pendingDeliveriesOf(endpoint)).isZero();
      assertThat(cancelledDeliveriesOf(endpoint)).isEqualTo(2 * BATCH + BATCH / 2);
    }
  }

  // Every other test calls the run itself. Here a second replica starts as the application does,
  // and Spring starts its cancel job, whose first run comes with the start.
  @Test
  void replicaRunsItsCancelJobByItself() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "cancelled-by-a-replica");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      tenant.delete(http, endpoint).expectStatus().isNoContent();

      try (ConfigurableApplicationContext replica = SelfStartedReplica.start(STARTED_BY_SPRING)) {
        await()
            .untilAsserted(
                () -> assertThat(Rows.delivery(delivery).state()).isEqualTo("cancelled"));
      }

      assertThat(Rows.delivery(delivery)).isEqualTo(CANCELLED);
    }
  }

  private RestTestClient.ResponseSpec get(TestTenant tenant, String pathAndQuery) {
    return http.get()
        .uri(pathAndQuery)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  // Two deliveries that expired while they were pending, for the one endpoint that takes the
  // event. No worker runs, so nothing claims them before the expiry run comes.
  private List<UUID> twoDeadDeliveries(TestTenant tenant, String event) {
    List<UUID> two =
        List.of(
            Rows.deliveryOf(tenant.accept(http, event)),
            Rows.deliveryOf(tenant.accept(http, event)));
    two.forEach(delivery -> Rows.ageDelivery(delivery, BUDGET));
    expiry.run();
    assertThat(two)
        .allSatisfy(delivery -> assertThat(Rows.delivery(delivery).state()).isEqualTo("dead"));
    return two;
  }

  // The tenant as authentication hands it to a route: with the burst of a new tenant, and with
  // the rate of 0 that the test gave it.
  private static Tenant asAuthenticated(TestTenant tenant) {
    return new Tenant(tenant.id(), "active", 0, 100);
  }

  private UUID eventIdIn(ExchangeResult accepted) {
    return UUID.fromString(json.readTree(accepted.getResponseBodyContent()).get("id").asString());
  }

  private double lostLeases() {
    return meters.get("webhooks.finalize.lost.lease").counter().count();
  }

  private static UUID endpointOf(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql("SELECT endpoint_id FROM delivery WHERE id = :id")
        .param("id", deliveryId)
        .query(UUID.class)
        .single();
  }

  private static List<UUID> endpointsWithADeliveryOf(UUID eventId) {
    return TestDatabase.jdbc()
        .sql("SELECT endpoint_id FROM delivery WHERE event_id = :id")
        .param("id", eventId)
        .query(UUID.class)
        .list();
  }

  private static List<UUID> deliveriesOf(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM delivery WHERE endpoint_id = :e ORDER BY created_at, id")
        .param("e", endpointId)
        .query(UUID.class)
        .list();
  }

  /** The deliveries of an endpoint that have a lease token, or those that have none. */
  private static List<UUID> deliveriesOf(UUID endpointId, boolean leased) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT id FROM delivery
            WHERE endpoint_id = :e AND (lease_token IS NOT NULL) = :leased
            """)
        .param("e", endpointId)
        .param("leased", leased)
        .query(UUID.class)
        .list();
  }

  private static int pendingDeliveriesOf(UUID... endpointIds) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM delivery WHERE endpoint_id = ANY(:ids) AND state = 'pending'")
        .param("ids", endpointIds)
        .query(Integer.class)
        .single();
  }

  private static int cancelledDeliveriesOf(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM delivery
            WHERE endpoint_id = :e AND state = 'cancelled' AND dead_reason = 'endpoint_deleted'
              AND completed_at IS NOT NULL
            """)
        .param("e", endpointId)
        .query(Integer.class)
        .single();
  }

  // Events with one delivery each, as ingest leaves them for one matching endpoint. They are
  // written here and not accepted through the API: thousands of requests are no part of what the
  // test is about.
  private static void insertPendingDeliveries(TestTenant tenant, UUID endpointId, int count) {
    TestDatabase.jdbc()
        .sql(
            """
            WITH written AS (
              INSERT INTO event (id, tenant_id, type, body, idempotency_key, request_hash)
              SELECT gen_random_uuid(), :t, 'order.created', :body, 'pending-' || n, sha256(:body)
              FROM generate_series(1, :count) AS n
              RETURNING id, created_at
            )
            INSERT INTO delivery (tenant_id, event_id, endpoint_id, next_attempt_at, expires_at,
                                  created_at)
            SELECT :t, id, :e, created_at, created_at + interval '72 hours', created_at
            FROM written
            """)
        .param("t", tenant.id())
        .param("e", endpointId)
        .param("body", EVENT.getBytes(StandardCharsets.UTF_8))
        .param("count", count)
        .update();
  }
}
