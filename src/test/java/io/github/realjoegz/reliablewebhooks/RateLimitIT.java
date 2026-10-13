package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.OpenRequests;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.SelfStartedReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.ops.ExpiryJob;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.ExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The token bucket and the request permits of a tenant. A test gives its tenant a rate of 0
 * wherever it counts units, so that the bucket holds what the test put into it and nothing refills
 * while the test runs. The row of the bucket is then read after each charge.
 */
@InProcessReplica
class RateLimitIT {

  private static final String HOST = "hooks.rate-limit.example";
  private static final String HOOK = "https://" + HOST + "/hook";
  private static final String ORDER_CREATED =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String ORDER_PAID =
      """
      {"type":"order.paid","data":{"order":17}}""";
  private static final String ORDER_SHIPPED =
      """
      {"type":"order.shipped","data":{"order":17}}""";

  private static final int PERMITS = 8;
  private static final int REPLICAS = 4;
  private static final int EVENTS_FOR_A_REPLICA = 50;
  private static final int BYTES_OF_A_UNIT = 4096;
  private static final int BODY_LIMIT = 256 * 1024;
  private static final Duration BUDGET = Duration.ofHours(72);

  // What the body that is stored has more than the request it was rendered from: the id of the
  // event and its creation time, which have the same length in every event.
  private static final int ADDED_BY_RENDERING =
      "\"id\":\"00000000-0000-0000-0000-000000000000\",".length()
          + "\"created_at\":\"2026-01-01T00:00:00.000Z\",".length();

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired JsonMapper json;
  @Autowired MeterRegistry meters;
  @Autowired OpenRequests openRequests;
  @Autowired ExpiryJob expiry;

  @Autowired
  @Qualifier("apiDataSource")
  HikariDataSource api;

  @LocalServerPort int port;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  // Four replicas answer one tenant: the replica of this class and three that are started here the
  // way the application starts, each with its own pools, key cache and permits.
  //
  // First the volume. The tenant has 20 units and gets no more. Two hundred events of one unit
  // each go to the four replicas at once, fifty to each, and twenty are accepted. A limit that
  // each replica kept for itself would let 80 events in.
  //
  // A replica is sent eight requests at a time, which is what the permits of a tenant allow, so
  // every refusal comes from the bucket. A caller sends its next event once it has read the
  // answer to the one before to its end, and the end of an answer is sent after the permit of
  // its request is back.
  //
  // Then the charges are made to meet, which the volume leaves to chance. A second tenant has 3
  // units and gets no more, and the test holds its bucket row. Two events go to each replica, and
  // the eight charges are seen waiting for the row. Once the test lets go of it, three events are
  // accepted and five are refused: a charge that has waited decides on what the charge before it
  // left. One that decided on the bucket as it was when the wait began would let all eight in.
  //
  // A charge that waits is ended by the lock timeout of the api pool after 2 s. The test has that
  // long from the first of the eight to the release of the row. A run that took longer would
  // fail and not pass: an event would be answered with a 503.
  @Test
  void limitIsExactAcrossReplicas() throws Exception {
    try (ConfigurableApplicationContext second = SelfStartedReplica.start("second-of-four");
        ConfigurableApplicationContext third = SelfStartedReplica.start("third-of-four");
        ConfigurableApplicationContext fourth = SelfStartedReplica.start("fourth-of-four");
        ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
      List<ConfigurableApplicationContext> started = List.of(second, third, fourth);
      List<Replica> replicas =
          List.of(
              new Replica(ReplicaSettings.IN_PROCESS_REPLICA, http),
              replicaOf(second),
              replicaOf(third),
              replicaOf(fourth));

      int burst = 20;
      TestTenant tenant = TestTenant.create(http, "exact-limit");
      tenant.patch(http, Map.of("rate_per_sec", 0, "burst", burst));
      CountDownLatch start = new CountDownLatch(1);
      List<Future<List<ExchangeResult>>> calls = new ArrayList<>();
      for (Replica replica : replicas) {
        AtomicInteger left = new AtomicInteger(EVENTS_FOR_A_REPLICA);
        for (int caller = 0; caller < PERMITS; caller++) {
          calls.add(
              callers.submit(
                  () -> {
                    start.await();
                    List<ExchangeResult> answered = new ArrayList<>();
                    while (left.getAndDecrement() > 0) {
                      answered.add(send(replica.http(), tenant, ORDER_CREATED));
                    }
                    return answered;
                  }));
        }
      }
      start.countDown();
      List<ExchangeResult> answers = new ArrayList<>();
      for (Future<List<ExchangeResult>> call : calls) {
        answers.addAll(call.get());
      }
      // Each started replica has answered its fifty: its own meter, new with it, has counted them.
      for (ConfigurableApplicationContext replica : started) {
        MeterRegistry itsMeters = replica.getBean(MeterRegistry.class);
        await()
            .untilAsserted(
                () -> assertThat(ingestRequestsCounted(itsMeters)).isEqualTo(EVENTS_FOR_A_REPLICA));
      }
      assertThat(answers).hasSize(REPLICAS * EVENTS_FOR_A_REPLICA);
      assertAdmitted(answers, burst);
      assertThat(tenant.eventRows()).isEqualTo(burst);
      assertThat(Rows.bucketTokens(tenant.id())).isZero();

      int units = 3;
      int chargesOfAReplica = 2;
      TestTenant meeting = TestTenant.create(http, "charges-that-meet");
      meeting.patch(http, Map.of("rate_per_sec", 0, "burst", units));
      List<Future<ExchangeResult>> waiting = new ArrayList<>();
      try (Connection blocker = TestDatabase.connection();
          PreparedStatement lock =
              blocker.prepareStatement(
                  "SELECT 1 FROM rate_bucket WHERE tenant_id = ? FOR UPDATE")) {
        lock.setObject(1, meeting.id());
        lock.execute();

        for (Replica replica : replicas) {
          for (int i = 0; i < chargesOfAReplica; i++) {
            waiting.add(callers.submit(() -> send(replica.http(), meeting, ORDER_CREATED)));
          }
        }
        for (Replica replica : replicas) {
          PgWait.untilBlocked("UPDATE rate_bucket", replica.id(), chargesOfAReplica);
        }
        blocker.rollback();
      }
      List<ExchangeResult> met = new ArrayList<>();
      for (Future<ExchangeResult> charged : waiting) {
        met.add(charged.get());
      }
      assertAdmitted(met, units);
      assertThat(meeting.eventRows()).isEqualTo(units);
      assertThat(Rows.bucketTokens(meeting.id())).isZero();
    }
  }

  // The bucket is emptied first: with a rate of 0 and a burst of 3 it takes three events and no
  // fourth. The tenant then gets a rate of one unit in 100 seconds, and the last refill of the
  // bucket is moved 500 seconds into the past. Five events are accepted and no sixth.
  //
  // The sixth is told to come back after 100 seconds. It is still refused once the refill is 90
  // seconds older, and accepted once it is 100 seconds older. The bucket also refills while the
  // test runs, so a run that took 10 seconds from the emptying to the refusal at 90 seconds would
  // fail and not pass.
  @Test
  void refillIsExact() {
    TestTenant tenant = TestTenant.create(http, "refill");
    tenant.patch(http, Map.of("rate_per_sec", 0, "burst", 3));
    for (int i = 0; i < 3; i++) {
      assertThat(send(http, tenant, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    }
    assertRefused(send(http, tenant, ORDER_CREATED), "rate_limit_unsatisfiable", null);
    assertThat(Rows.bucketTokens(tenant.id())).isZero();

    tenant.patch(http, Map.of("rate_per_sec", 0.01, "burst", 100));
    Rows.ageBucket(tenant.id(), Duration.ofSeconds(500));

    for (int i = 0; i < 5; i++) {
      assertThat(send(http, tenant, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    }
    assertRefused(send(http, tenant, ORDER_CREATED), "rate_limited", "100");

    Rows.ageBucket(tenant.id(), Duration.ofSeconds(90));
    assertRefused(send(http, tenant, ORDER_CREATED), "rate_limited", "100");

    Rows.ageBucket(tenant.id(), Duration.ofSeconds(10));
    assertThat(send(http, tenant, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(tenant.eventRows()).isEqualTo(9);
  }

  // The last refill of a bucket can lie ahead of the clock of a charge: a charge that began later
  // wrote it while this one waited for the row, or the clock of the database was set back. The
  // time since the refill is then negative, and the statement takes tokens away for it.
  //
  // The tenant gets one unit a second, and its bucket holds 9 of 10. With the last refill an hour
  // ahead, an event of one unit is refused and told to come back after a second, and the bucket
  // still holds 9. With the refill back where it was, the event is accepted.
  @Test
  void chargeBehindTheLastRefillIsRefusedAlthoughTheBucketHoldsEnough() {
    TestTenant tenant = TestTenant.create(http, "refill-ahead");
    tenant.patch(http, Map.of("rate_per_sec", 1, "burst", 10));
    assertThat(send(http, tenant, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(9);

    Rows.postponeRefill(tenant.id(), Duration.ofHours(1));
    assertRefused(send(http, tenant, ORDER_CREATED), "rate_limited", "1");
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(9);

    Rows.ageBucket(tenant.id(), Duration.ofHours(1));
    assertThat(send(http, tenant, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(tenant.eventRows()).isEqualTo(2);
  }

  // Three endpoints take order.created, one takes order.paid and none takes order.shipped. An
  // event costs one unit for each endpoint that it goes to, and one unit when it goes to none.
  //
  // The size of a body is added to that and not multiplied by it. The first order.created is
  // stored as 4 KiB and costs four units: three for its endpoints and one for its size. Three
  // endpoints times the two units of that size would be six.
  //
  // The third of the small ones finds two units where it costs three: it is refused, nothing of
  // it is stored and the two units stay.
  @Test
  void fanOutIsChargedPerDelivery() {
    TestTenant tenant = TestTenant.create(http, "fan-out-charge");
    for (int i = 0; i < 3; i++) {
      tenant.register(http, HOOK, "order.created");
    }
    tenant.register(http, HOOK, "order.paid");
    tenant.patch(http, Map.of("rate_per_sec", 0, "burst", 12));

    UUID storedAsAUnit = accept(tenant, eventStoredAs(BYTES_OF_A_UNIT));
    assertThat(storedBytesOf(storedAsAUnit)).isEqualTo(BYTES_OF_A_UNIT);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(8);
    assertThat(tenant.deliveryRows()).isEqualTo(3);

    assertThat(send(http, tenant, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(5);
    assertThat(tenant.deliveryRows()).isEqualTo(6);

    assertThat(send(http, tenant, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(2);
    assertThat(tenant.deliveryRows()).isEqualTo(9);

    assertRefused(send(http, tenant, ORDER_CREATED), "rate_limit_unsatisfiable", null);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(2);
    assertThat(tenant.eventRows()).isEqualTo(3);
    assertThat(tenant.deliveryRows()).isEqualTo(9);

    assertThat(send(http, tenant, ORDER_PAID).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(1);
    assertThat(tenant.deliveryRows()).isEqualTo(10);

    assertThat(send(http, tenant, ORDER_SHIPPED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(Rows.bucketTokens(tenant.id())).isZero();
    assertThat(tenant.deliveryRows()).isEqualTo(10);

    assertRefused(send(http, tenant, ORDER_SHIPPED), "rate_limit_unsatisfiable", null);
    assertThat(tenant.eventRows()).isEqualTo(5);
  }

  // The tenant has no endpoint, so an event costs one unit and one more for each full 4 KiB of
  // the body that is stored. A stored body of one byte less than 4 KiB costs one unit, one of
  // 4 KiB costs two, and the body of the largest request that is read costs 65. The three leave
  // 32 of the 100 units: a second event of the largest size is refused, and a small one is
  // accepted.
  @Test
  void largeBodiesAreChargedBySize() {
    TestTenant tenant = TestTenant.create(http, "body-charge");
    tenant.patch(http, Map.of("rate_per_sec", 0));

    UUID belowAUnit = accept(tenant, eventStoredAs(BYTES_OF_A_UNIT - 1));
    assertThat(storedBytesOf(belowAUnit)).isEqualTo(BYTES_OF_A_UNIT - 1);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(99);

    UUID oneUnit = accept(tenant, eventStoredAs(BYTES_OF_A_UNIT));
    assertThat(storedBytesOf(oneUnit)).isEqualTo(BYTES_OF_A_UNIT);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(97);

    UUID largest = accept(tenant, eventOf(BODY_LIMIT));
    assertThat(storedBytesOf(largest)).isEqualTo(BODY_LIMIT + ADDED_BY_RENDERING);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(32);

    assertRefused(
        tenant.send(http, TestTenant.newIdempotencyKey(), eventOf(BODY_LIMIT)).returnResult(),
        "rate_limit_unsatisfiable",
        null);
    assertThat(send(http, tenant, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(31);
    assertThat(tenant.eventRows()).isEqualTo(4);
  }

  // The bucket of the tenant refills, and it is full. An event of 17 units is refused all the
  // same, because the bucket never holds more than its 16, and the answer names no time to come
  // back after. Nothing is charged for it and nothing is stored. An event of 16 units is
  // accepted and empties the bucket. The small event after it is refused too, and that answer
  // does name a time: one unit refills in 100 seconds.
  @Test
  void costAboveBurstIsUnsatisfiable() {
    int burst = 16;
    TestTenant tenant = TestTenant.create(http, "cost-above-burst");
    tenant.patch(http, Map.of("rate_per_sec", 0.01, "burst", burst));
    String full = bucketRowOf(tenant);

    assertRefused(
        tenant
            .send(http, TestTenant.newIdempotencyKey(), eventStoredAs(burst * BYTES_OF_A_UNIT))
            .returnResult(),
        "rate_limit_unsatisfiable",
        null);
    assertThat(bucketRowOf(tenant)).isEqualTo(full);
    assertThat(tenant.eventRows()).isZero();

    accept(tenant, eventStoredAs(burst * BYTES_OF_A_UNIT - 1));
    assertThat(Rows.bucketTokens(tenant.id())).isZero();

    assertRefused(send(http, tenant, ORDER_CREATED), "rate_limited", "100");
    assertThat(tenant.eventRows()).isEqualTo(1);
  }

  // Eight requests of the tenant are open: the head of each has arrived and half of its body, so
  // the route waits for the rest. Each of them has passed the filters, and none holds a
  // connection. The replica has the key of the tenant in its cache from their authentication.
  // The ninth request of the tenant is refused, and so is a request of the tenant to another
  // route: the permits are those of the tenant, whichever route a request is for. The count of
  // connections that the api pool has handed out is the same before and after the two, and the
  // event of another tenant, accepted while the eight are still open, moves that count.
  //
  // The ninth request is counted under none of the results of the ingest route: the counters go
  // up by the one event of the other tenant.
  //
  // The eight are then sent the rest of their bodies and are accepted, and the tenant is served
  // again. The test reads no more than the status line of each of the eight answers, and a
  // permit comes back a moment after that line has gone out, hence the wait.
  @Test
  void ninthConcurrentRequestTakesNoConnection() throws Exception {
    TestTenant tenant = TestTenant.create(http, "eight-open-requests");
    TestTenant other = TestTenant.create(http, "beside-eight-open-requests");
    List<UnfinishedRequest> open = new ArrayList<>();
    try {
      for (int i = 0; i < PERMITS; i++) {
        open.add(UnfinishedRequest.open(port, tenant, ORDER_CREATED));
      }
      await().until(() -> openRequests.of(tenant.id()) == PERMITS);
      assertThat(api.getHikariPoolMXBean().getActiveConnections()).isZero();
      long acquired = connectionsAcquired();
      double counted = ingestRequestsCounted(meters);

      assertRefused(send(http, tenant, ORDER_CREATED), "concurrency_limit", "1");
      assertRefused(endpointsOf(tenant), "concurrency_limit", "1");
      assertThat(connectionsAcquired()).isEqualTo(acquired);

      assertThat(send(http, other, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      assertThat(connectionsAcquired()).isGreaterThan(acquired);
      await().untilAsserted(() -> assertThat(ingestRequestsCounted(meters)).isEqualTo(counted + 1));
      assertThat(openRequests.of(tenant.id())).isEqualTo(PERMITS);
      assertThat(tenant.eventRows()).isZero();

      for (UnfinishedRequest request : open) {
        assertThat(request.finish()).isEqualTo(HttpStatus.ACCEPTED.value());
      }
    } finally {
      for (UnfinishedRequest request : open) {
        request.close();
      }
    }
    assertThat(tenant.eventRows()).isEqualTo(PERMITS);
    await()
        .untilAsserted(
            () ->
                assertThat(send(http, tenant, ORDER_CREATED).getStatus())
                    .isEqualTo(HttpStatus.ACCEPTED));
  }

  // The test holds the bucket row of a tenant, so each of its requests waits in its charge with a
  // connection of the api pool. Eight of them are seen waiting there. Twenty-four more are
  // refused by the permits, and the pool has handed out no connection for them: eight of its
  // sixteen are in use, and the event of another tenant is accepted.
  //
  // A charge that waits is ended by the lock timeout of the api pool after 2 s. The test has that
  // long from the first of the eight to the release of the row. A run that took longer would
  // fail and not pass: one of the eight would be answered with a 503.
  @Test
  void floodingTenantDoesNotBlockOtherTenant() throws Exception {
    int refusedByThePermits = 24;
    TestTenant flooding = TestTenant.create(http, "flooding");
    TestTenant other = TestTenant.create(http, "beside-flooding");

    try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor();
        Connection blocker = TestDatabase.connection();
        PreparedStatement lock =
            blocker.prepareStatement("SELECT 1 FROM rate_bucket WHERE tenant_id = ? FOR UPDATE")) {
      lock.setObject(1, flooding.id());
      lock.execute();

      List<Future<ExchangeResult>> waiting = new ArrayList<>();
      for (int i = 0; i < PERMITS; i++) {
        waiting.add(callers.submit(() -> send(http, flooding, ORDER_CREATED)));
      }
      PgWait.untilBlocked("UPDATE rate_bucket", ReplicaSettings.IN_PROCESS_REPLICA, PERMITS);
      long acquired = connectionsAcquired();

      List<Future<ExchangeResult>> flood = new ArrayList<>();
      for (int i = 0; i < refusedByThePermits; i++) {
        flood.add(callers.submit(() -> send(http, flooding, ORDER_CREATED)));
      }
      for (Future<ExchangeResult> refused : flood) {
        assertRefused(refused.get(), "concurrency_limit", "1");
      }
      assertThat(connectionsAcquired()).isEqualTo(acquired);
      assertThat(api.getHikariPoolMXBean().getActiveConnections()).isEqualTo(PERMITS);

      assertThat(send(http, other, ORDER_CREATED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      assertThat(waiting).noneMatch(Future::isDone);

      blocker.rollback();
      for (Future<ExchangeResult> charged : waiting) {
        assertThat(charged.get().getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      }
    }
    assertThat(flooding.eventRows()).isEqualTo(PERMITS);
  }

  // Two deliveries are dead, and the tenant has two units. The replay of the first takes one. A
  // second replay of that delivery is refused, because the delivery is pending now, and takes
  // the other unit all the same: the charge comes before the replay and is not given back. The
  // replay of the second delivery then finds the bucket empty, and the delivery stays dead.
  @Test
  void replayIsCharged() {
    TestTenant tenant = TestTenant.create(http, "replay-charge");
    tenant.register(http, HOOK, "*");
    UUID first = Rows.deliveryOf(tenant.accept(http, ORDER_CREATED));
    UUID second = Rows.deliveryOf(tenant.accept(http, ORDER_PAID));
    Rows.ageDelivery(first, BUDGET);
    Rows.ageDelivery(second, BUDGET);
    expiry.run();
    Rows.Delivery dead = new Rows.Delivery("dead", 0, 0, false, "expired", true);
    assertThat(Rows.delivery(first)).isEqualTo(dead);
    assertThat(Rows.delivery(second)).isEqualTo(dead);
    tenant.patch(http, Map.of("rate_per_sec", 0, "burst", 2));

    tenant.replay(http, first).expectStatus().isNoContent();
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(1);
    assertThat(Rows.delivery(first))
        .isEqualTo(new Rows.Delivery("pending", 0, 1, false, null, false));

    tenant.replay(http, first).expectStatus().isEqualTo(HttpStatus.CONFLICT);
    assertThat(Rows.bucketTokens(tenant.id())).isZero();

    assertRefused(tenant.replay(http, second).returnResult(), "rate_limit_unsatisfiable", null);
    assertThat(Rows.delivery(second)).isEqualTo(dead);
  }

  // The bucket holds one unit. The event takes it, and its repeat under the same key is answered
  // as a repeat from the empty bucket: the idempotency lookup comes before the charge. The same
  // event under a new key is refused.
  @Test
  void repeatOfAnAcceptedEventIsNeitherChargedNorRefused() {
    TestTenant tenant = TestTenant.create(http, "repeat-not-charged");
    tenant.patch(http, Map.of("rate_per_sec", 0, "burst", 1));
    String key = TestTenant.newIdempotencyKey();

    ExchangeResult accepted = tenant.send(http, key, ORDER_CREATED).returnResult();
    assertThat(accepted.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(Rows.bucketTokens(tenant.id())).isZero();

    ExchangeResult repeat = tenant.send(http, key, ORDER_CREATED).returnResult();
    assertThat(repeat.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(repeat.getResponseHeaders().get("Idempotent-Replay")).containsExactly("true");
    assertThat(repeat.getResponseBodyContent()).isEqualTo(accepted.getResponseBodyContent());

    assertRefused(send(http, tenant, ORDER_CREATED), "rate_limit_unsatisfiable", null);
    assertThat(tenant.eventRows()).isEqualTo(1);
  }

  // The test holds the endpoint row that the fan-out locks, so the request waits inside its
  // transaction. The row of the bucket shows its charge by then, to another connection: the
  // charge was committed before the transaction began. The transaction does not hold the bucket
  // row either, because a second event of the tenant, which goes to no endpoint, is charged and
  // accepted meanwhile.
  //
  // The lock timeout then ends the first request. Its event is not stored, and its unit is not
  // given back.
  @Test
  void chargeIsCommittedOutsideTheIngestTransactionAndIsNotGivenBack() throws Exception {
    TestTenant tenant = TestTenant.create(http, "charge-outside-transaction");
    UUID endpoint = tenant.register(http, HOOK, "order.created");
    tenant.patch(http, Map.of("rate_per_sec", 0, "burst", 10));

    try (ExecutorService caller = Executors.newVirtualThreadPerTaskExecutor();
        Connection blocker = TestDatabase.connection();
        PreparedStatement lock =
            blocker.prepareStatement("SELECT 1 FROM endpoint WHERE id = ? FOR UPDATE")) {
      lock.setObject(1, endpoint);
      lock.execute();

      Future<ExchangeResult> waiting = caller.submit(() -> send(http, tenant, ORDER_CREATED));
      PgWait.untilBlocked("INSERT INTO delivery", ReplicaSettings.IN_PROCESS_REPLICA);
      assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(9);

      assertThat(send(http, tenant, ORDER_SHIPPED).getStatus()).isEqualTo(HttpStatus.ACCEPTED);
      assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(8);

      assertThat(waiting.get().getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
      blocker.rollback();
    }
    assertThat(tenant.eventRows()).isEqualTo(1);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(8);
  }

  /**
   * A request to the ingest route of which the head and half of the body have been sent. The route
   * has it and waits for the rest.
   */
  private static final class UnfinishedRequest implements AutoCloseable {

    private final Socket socket;
    private final byte[] rest;

    private UnfinishedRequest(Socket socket, byte[] rest) {
      this.socket = socket;
      this.rest = rest;
    }

    static UnfinishedRequest open(int port, TestTenant tenant, String event) throws IOException {
      byte[] body = event.getBytes(StandardCharsets.UTF_8);
      int sent = body.length / 2;
      String head =
          """
          POST /v1/events HTTP/1.1
          Host: localhost:%d
          Authorization: Bearer %s
          Content-Type: application/json
          Idempotency-Key: %s
          Content-Length: %d
          Connection: close

          """
              .formatted(port, tenant.apiKey(), TestTenant.newIdempotencyKey(), body.length)
              .replace("\n", "\r\n");
      Socket socket = new Socket("localhost", port);
      OutputStream out = socket.getOutputStream();
      out.write(head.getBytes(StandardCharsets.US_ASCII));
      out.write(body, 0, sent);
      out.flush();
      return new UnfinishedRequest(socket, Arrays.copyOfRange(body, sent, body.length));
    }

    /** Sends the rest of the body and returns the status of the answer. */
    int finish() throws IOException {
      OutputStream out = socket.getOutputStream();
      out.write(rest);
      out.flush();
      String statusLine =
          new BufferedReader(
                  new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII))
              .readLine();
      return Integer.parseInt(statusLine.split(" ")[1]);
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }

  private static ExchangeResult send(RestTestClient replica, TestTenant tenant, String event) {
    return tenant.send(replica, TestTenant.newIdempotencyKey(), event).returnResult();
  }

  /** The answer to a request for the list of the endpoints of a tenant. */
  private ExchangeResult endpointsOf(TestTenant tenant) {
    return http.get()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange()
        .returnResult();
  }

  private UUID accept(TestTenant tenant, byte[] event) {
    ExchangeResult answer = tenant.send(http, TestTenant.newIdempotencyKey(), event).returnResult();
    assertThat(answer.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    return UUID.fromString(json.readTree(answer.getResponseBodyContent()).get("id").asString());
  }

  // Of the answers, this many are a 202. Every other one is the refusal of a bucket that does not
  // refill.
  private void assertAdmitted(List<ExchangeResult> answers, int admitted) {
    assertThat(answers)
        .filteredOn(answer -> answer.getStatus().equals(HttpStatus.ACCEPTED))
        .hasSize(admitted);
    assertThat(answers)
        .filteredOn(answer -> !answer.getStatus().equals(HttpStatus.ACCEPTED))
        .allSatisfy(refused -> assertRefused(refused, "rate_limit_unsatisfiable", null));
  }

  // A refusal is a problem document with the type of its reason. It names a time to come back
  // after where one is given, and carries no such header where none is.
  private void assertRefused(ExchangeResult answer, String type, String retryAfter) {
    assertThat(answer.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    assertThat(answer.getResponseHeaders().getContentType())
        .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    assertThat(answer.getResponseHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo(retryAfter);
    JsonNode problem = json.readTree(answer.getResponseBodyContent());
    assertThat(problem.get("type").asString()).isEqualTo(type);
    assertThat(problem.get("status").asInt()).isEqualTo(429);
    assertThat(problem.get("title").asString()).isNotBlank();
    assertThat(problem.get("detail").asString()).isNotBlank();
  }

  /** How often the api pool of the replica has handed out a connection. */
  private long connectionsAcquired() {
    return meters.get("hikaricp.connections.acquire").tag("pool", "api").timer().count();
  }

  /** The requests that the ingest meter of a replica has counted, under whichever result. */
  private static double ingestRequestsCounted(MeterRegistry meters) {
    return meters.get("webhooks.ingest.requests").counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }

  /** A replica as a test reaches it: by the name of its connections and through its client. */
  private record Replica(String id, RestTestClient http) {}

  private static Replica replicaOf(ConfigurableApplicationContext started) {
    return new Replica(
        started.getBean(WebhooksProperties.class).replicaId(),
        SelfStartedReplica.clientOf(started));
  }

  private static int storedBytesOf(UUID eventId) {
    return TestDatabase.jdbc()
        .sql("SELECT octet_length(body) FROM event WHERE id = :id")
        .param("id", eventId)
        .query(Integer.class)
        .single();
  }

  /** The bucket row of a tenant as the text of all of its columns. */
  private static String bucketRowOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT b::text FROM rate_bucket b WHERE b.tenant_id = :t")
        .param("t", tenant.id())
        .query(String.class)
        .single();
  }

  /** An event whose body, as the service stores it, has this many bytes. */
  private static byte[] eventStoredAs(int bytes) {
    return eventOf(bytes - ADDED_BY_RENDERING);
  }

  /** An event whose request has this many bytes. */
  private static byte[] eventOf(int bytes) {
    String head = "{\"type\":\"order.created\",\"data\":{\"padding\":\"";
    String tail = "\"}}";
    String body = head + "x".repeat(bytes - head.length() - tail.length()) + tail;
    return body.getBytes(StandardCharsets.US_ASCII);
  }
}
