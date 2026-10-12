package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import io.github.realjoegz.reliablewebhooks.ops.ExpiryJob;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class TenantIsolationIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String FOR_FOUR_ENDPOINTS =
      """
      {"type":"held.sent","data":{}}""";
  private static final String FOR_THE_FIFTH_ENDPOINT =
      """
      {"type":"fifth.sent","data":{}}""";
  private static final int MAX_IN_FLIGHT_FOR_AN_ENDPOINT = 4;
  private static final int MAX_IN_FLIGHT_FOR_A_TENANT = 16;
  private static final int ENDPOINTS_THAT_FILL_A_TENANT =
      MAX_IN_FLIGHT_FOR_A_TENANT / MAX_IN_FLIGHT_FOR_AN_ENDPOINT;
  private static final int DEEP_BACKLOG = 2_000;
  private static final int A_TENTH_OF_THE_BACKLOG = DEEP_BACKLOG / 10;
  private static final Duration BUDGET = Duration.ofHours(72);
  private static final Map<String, List<String>> OTHER_EVENT_TYPES =
      Map.of("event_types", List.of("order.paid"));

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired ExpiryJob expiry;

  // A tenant has five endpoints, and the receiver holds every request that it gets for them. Four
  // of the endpoints have four deliveries each. All sixteen are in flight, which is all that a
  // tenant may have and all that these four endpoints may have. The four deliveries for the
  // fifth endpoint are accepted then. Nothing is in flight for that endpoint, so only the cap of
  // the tenant stands between them and the receiver.
  //
  // The event of another tenant is accepted last and is delivered while the sixteen are held.
  // Its delivery is the witness that the replica went on claiming: the deliveries for the fifth
  // endpoint were due in that claim, and were passed over.
  //
  // The sixteen are made due four at a time, each four after the four before are held. A tenant
  // has four lookups at a time, and a fifth one is refused and its delivery given back for two
  // seconds, also when the four before it are only a moment from their answer. Sixteen
  // deliveries that are due at once can lose one that way, and the tenant then stays below its
  // cap for as long as the others are held.
  //
  // A held request is cut at the deadline of an attempt, 15 s after it began. A run that took
  // that long to get here would fail: the receiver would count the requests that followed.
  @Test
  void backlogOfOneTenantDoesNotStarveAnother() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200))) {
      TestTenant held = TestTenant.create(http, "held-backlog");
      for (int i = 0; i < ENDPOINTS_THAT_FILL_A_TENANT; i++) {
        receiver.answer("/held-" + i, hold);
        held.register(http, receiver.url("/held-" + i), "held.sent");
      }
      receiver.answer("/fifth", hold);
      held.register(http, receiver.url("/fifth"), "fifth.sent");
      receiver.answer("/other-tenant", FaultReceiver.status(200));
      TestTenant other = TestTenant.create(http, "beside-held-backlog");
      other.register(http, receiver.url("/other-tenant"), "*");
      worker.start();
      for (int events = 1; events <= MAX_IN_FLIGHT_FOR_AN_ENDPOINT; events++) {
        held.accept(http, FOR_FOUR_ENDPOINTS);
        int inFlight = events * ENDPOINTS_THAT_FILL_A_TENANT;
        await().until(() -> hold.waiting() == inFlight);
      }

      List<UUID> forTheFifthEndpoint = new ArrayList<>();
      for (int i = 0; i < MAX_IN_FLIGHT_FOR_AN_ENDPOINT; i++) {
        forTheFifthEndpoint.add(Rows.deliveryOf(held.accept(http, FOR_THE_FIFTH_ENDPOINT)));
      }
      UUID ofTheOtherTenant = Rows.deliveryOf(other.accept(http, EVENT));
      await()
          .untilAsserted(
              () -> assertThat(Rows.delivery(ofTheOtherTenant).state()).isEqualTo("succeeded"));

      assertThat(hold.waiting()).isEqualTo(MAX_IN_FLIGHT_FOR_A_TENANT);
      assertThat(receiver.requests()).hasSize(MAX_IN_FLIGHT_FOR_A_TENANT + 1);
      assertThat(receiver.requests("/fifth")).isEmpty();
      for (UUID passedOver : forTheFifthEndpoint) {
        assertThat(Rows.delivery(passedOver))
            .isEqualTo(new Rows.Delivery("pending", 0, 0, false, null, false));
      }
    }
  }

  // Two thousand deliveries have been due for an hour for one endpoint of a tenant, which answers
  // at once. The one delivery of another tenant is newer than all of them, and a claim that went
  // by age alone would reach it last. The log of the receiver holds the requests in the order
  // in which they arrived: the request of the other tenant is among the first two hundred, so it
  // arrived before the two hundredth of the backlog did.
  @Test
  void deepBacklogOnFastEndpointDoesNotStarveAnother() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/backlog", FaultReceiver.status(200));
      receiver.answer("/other-tenant", FaultReceiver.status(200));
      TestTenant backlog = TestTenant.create(http, "deep-backlog");
      UUID endpoint = backlog.register(http, receiver.url("/backlog"), "*");
      TestTenant other = TestTenant.create(http, "beside-deep-backlog");
      other.register(http, receiver.url("/other-tenant"), "*");
      insertDeliveriesDueForAnHour(backlog, endpoint, DEEP_BACKLOG);
      UUID ofTheOtherTenant = Rows.deliveryOf(other.accept(http, EVENT));
      worker.start();

      await()
          .atMost(Duration.ofMinutes(1))
          .until(() -> receiver.requests("/backlog").size() >= A_TENTH_OF_THE_BACKLOG);
      await()
          .untilAsserted(
              () -> assertThat(Rows.delivery(ofTheOtherTenant).state()).isEqualTo("succeeded"));

      assertThat(receiver.requests().subList(0, A_TENTH_OF_THE_BACKLOG))
          .extracting(FaultReceiver.Request::path)
          .contains("/other-tenant");
    }
  }

  // One tenant owns an endpoint, two events and their two dead deliveries. Another tenant, with a
  // key of its own, sends every request that names one of them by its id. Each is answered with
  // 404, and the endpoint, circuit, delivery and bucket rows of the owner are afterwards what
  // they were, column for column.
  //
  // The replay of one delivery is charged before it looks for its delivery. With a foreign id it
  // therefore costs a unit although it is refused, and the unit comes out of the bucket of the
  // tenant that sent it. The replay of the dead deliveries of an endpoint looks for the endpoint
  // first, and with a foreign id it costs nothing. That tenant has a rate of 0, so its bucket
  // holds what its charges left: one unit less.
  //
  // The deliveries are dead, which is the one state in which a replay changes them. The owner
  // then sends the same requests. Each read finds its row, the patch changes the endpoint, each
  // of the two replays makes one delivery pending, the rotation answers with a secret, and the
  // delete comes last and leaves no endpoint to read. What the other tenant sent was therefore
  // refused for its sender and for nothing else.
  @Test
  void mutatingRoutesLeaveForeignTenantUntouched() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant owner = TestTenant.create(http, "owner-of-the-rows");
      TestTenant other = TestTenant.create(http, "holder-of-foreign-ids");
      other.patch(http, Map.of("rate_per_sec", 0));
      double unitsOfTheOther = Rows.bucketTokens(other.id());
      UUID endpoint = owner.register(http, receiver.url("/hook"), "*");
      UUID event = owner.accept(http, EVENT);
      UUID delivery = Rows.deliveryOf(event);
      UUID secondDelivery = Rows.deliveryOf(owner.accept(http, EVENT));
      Rows.ageDelivery(delivery, BUDGET);
      Rows.ageDelivery(secondDelivery, BUDGET);
      expiry.run();
      assertThat(Rows.delivery(delivery).state()).isEqualTo("dead");
      assertThat(Rows.delivery(secondDelivery).state()).isEqualTo("dead");
      List<String> rows = rowsOf(owner);
      List<String> reads =
          List.of(
              "/v1/endpoints/" + endpoint,
              "/v1/events/" + event,
              "/v1/deliveries/" + delivery,
              "/v1/deliveries/" + delivery + "?run=0",
              "/v1/deliveries?endpoint_id=" + endpoint + "&state=dead");

      expectNotFound(patch(other, endpoint));
      expectNotFound(other.replay(http, delivery));
      expectNotFound(other.replayDead(http, endpoint, 10));
      expectNotFound(other.rotateSecret(http, endpoint));
      expectNotFound(other.delete(http, endpoint));
      for (String read : reads) {
        expectNotFound(get(other, read));
      }

      assertThat(rowsOf(owner)).isEqualTo(rows);
      assertThat(Rows.bucketTokens(other.id())).isEqualTo(unitsOfTheOther - 1);

      for (String read : reads) {
        get(owner, read).expectStatus().isOk();
      }
      patch(owner, endpoint).expectStatus().isNoContent();
      get(owner, "/v1/endpoints/" + endpoint)
          .expectBody()
          .jsonPath("$.event_types")
          .isEqualTo(OTHER_EVENT_TYPES.get("event_types"));
      Rows.Delivery replayed = new Rows.Delivery("pending", 0, 1, false, null, false);
      owner.replay(http, delivery).expectStatus().isNoContent();
      assertThat(Rows.delivery(delivery)).isEqualTo(replayed);
      owner
          .replayDead(http, endpoint, 10)
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.replayed")
          .isEqualTo(1)
          .jsonPath("$.remaining")
          .isEqualTo(false);
      assertThat(Rows.delivery(secondDelivery)).isEqualTo(replayed);
      owner
          .rotateSecret(http, endpoint)
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.secret")
          .exists();
      owner.delete(http, endpoint).expectStatus().isNoContent();
      expectNotFound(get(owner, "/v1/endpoints/" + endpoint));
    }
  }

  private RestTestClient.ResponseSpec get(TestTenant tenant, String pathAndQuery) {
    return http.get()
        .uri(pathAndQuery)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  private RestTestClient.ResponseSpec patch(TestTenant tenant, UUID endpointId) {
    return http.patch()
        .uri("/v1/endpoints/{id}", endpointId)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(OTHER_EVENT_TYPES)
        .exchange();
  }

  private static void expectNotFound(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isNotFound()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON);
  }

  /**
   * The endpoint, circuit, delivery and bucket rows of a tenant, each as the text of all of its
   * columns.
   */
  private static List<String> rowsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT line FROM (
              SELECT 'endpoint ' || e::text AS line FROM endpoint e WHERE e.tenant_id = :t
              UNION ALL
              SELECT 'circuit ' || c::text
              FROM endpoint_circuit c JOIN endpoint e ON e.id = c.endpoint_id
              WHERE e.tenant_id = :t
              UNION ALL
              SELECT 'delivery ' || d::text FROM delivery d WHERE d.tenant_id = :t
              UNION ALL
              SELECT 'bucket ' || b::text FROM rate_bucket b WHERE b.tenant_id = :t
            ) lines
            ORDER BY line
            """)
        .param("t", tenant.id())
        .query(String.class)
        .list();
  }

  // Events with one delivery each, as ingest leaves them for one matching endpoint, but an hour
  // old. They are written here and not accepted through the API: no request can be an hour old,
  // and two thousand of them are no part of what the test is about.
  private static void insertDeliveriesDueForAnHour(TestTenant tenant, UUID endpointId, int count) {
    TestDatabase.jdbc()
        .sql(
            """
            WITH backlog AS (
              INSERT INTO event (id, tenant_id, type, body, idempotency_key, request_hash,
                                 created_at)
              SELECT gen_random_uuid(), :t, 'order.created', :body, 'backlog-' || n, sha256(:body),
                     now() - interval '1 hour'
              FROM generate_series(1, :count) AS n
              RETURNING id, created_at
            )
            INSERT INTO delivery (tenant_id, event_id, endpoint_id, next_attempt_at, expires_at,
                                  created_at)
            SELECT :t, id, :e, created_at, created_at + interval '72 hours', created_at
            FROM backlog
            """)
        .param("t", tenant.id())
        .param("e", endpointId)
        .param("body", EVENT.getBytes(StandardCharsets.UTF_8))
        .param("count", count)
        .update();
  }
}
