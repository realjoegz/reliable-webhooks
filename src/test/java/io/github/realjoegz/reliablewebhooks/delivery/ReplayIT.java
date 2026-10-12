package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * The replay of the dead deliveries of an endpoint. The deliveries are written by the tests as
 * dead: a delivery that the service made dead would have taken twelve attempts or seventy-two
 * hours, and hundreds of them are no part of what these tests are about. No worker runs, so a
 * delivery that a replay made pending stays as the replay left it.
 */
@InProcessReplica
class ReplayIT {

  private static final String HOST = "hooks.replay.example";
  private static final String HOOK = "https://" + HOST + "/hook";
  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int MOST_ROWS_OF_A_CALL = 500;
  private static final Duration SPREAD_OF_A_REPLAY = Duration.ofSeconds(60);
  private static final Duration BUDGET = Duration.ofHours(72);
  private static final Duration AN_HOUR = Duration.ofHours(1);

  /** What a call answers: the deliveries that it replayed, and whether it left dead ones. */
  private record Replayed(int replayed, boolean remaining) {}

  @Autowired RestTestClient http;
  @Autowired StubDns dns;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  // An endpoint has 640 dead deliveries, and each call is bounded by something else. After each
  // call the deliveries that it replayed are the oldest that were dead, the bucket has paid one
  // unit for each of them, and every other delivery of the tenant is as it was, column for
  // column: those of the endpoint that are still dead or were never dead, and the dead ones of a
  // second endpoint.
  //
  // First the burst. The tenant has 40 units and gets no more. The call asks for 500 deliveries
  // and replays 40, which takes the whole bucket. Their due times lie in the 60 seconds after the
  // call, and the first and the last of them are more than 30 seconds apart. Forty times that
  // are drawn from 60 seconds are closer together than that less than once in ten thousand
  // million runs. A call for a single delivery is then refused by the bucket and replays nothing.
  //
  // Then the 500. The tenant now has a burst of 1,000, and its bucket is aged until it is full
  // before each of the next three calls, so the bucket row reads 1,000 less the cost of the call
  // after it. A call for 1,000 deliveries replays 500.
  //
  // Then the limit of the call, which is below both. A call for 30 replays 30 and says that more
  // are left. A call for 70 replays the last 70 and says that none is left: it asked for exactly
  // as many as there were. A further call finds nothing, replays nothing and costs nothing. The
  // bucket is not aged before it, and its row is as the call before left it: a charge, also one
  // of no units, would have written what has refilled since then.
  @Test
  void bulkReplayIsBoundedAndCharged() {
    TestTenant tenant = TestTenant.create(http, "bulk-replay");
    UUID endpoint = tenant.register(http, HOOK, "order.created");
    UUID otherEndpoint = tenant.register(http, HOOK, "order.paid");
    tenant.accept(http, EVENT);
    insertDeliveries(tenant, endpoint, "dead", "exhausted", 640);
    insertDeliveries(tenant, endpoint, "succeeded", null, 1);
    insertDeliveries(tenant, endpoint, "cancelled", "endpoint_deleted", 1);
    insertDeliveries(tenant, otherEndpoint, "dead", "expired", 3);
    List<UUID> dead = deadOldestFirst(endpoint);
    assertThat(dead).hasSize(640);

    tenant.patch(http, Map.of("rate_per_sec", 0, "burst", 40));
    List<UUID> paidByTheBurst = dead.subList(0, 40);
    expectReplayed(tenant, endpoint, MOST_ROWS_OF_A_CALL, paidByTheBurst, true);
    assertThat(spreadOfDueTimes(paidByTheBurst)).isGreaterThan(SPREAD_OF_A_REPLAY.dividedBy(2));
    assertThat(Rows.bucketTokens(tenant.id())).isZero();

    Map<UUID, String> rows = deliveryRowsOf(tenant);
    tenant
        .replayDead(http, endpoint, 1)
        .expectStatus()
        .isEqualTo(HttpStatus.TOO_MANY_REQUESTS)
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectHeader()
        .doesNotExist(HttpHeaders.RETRY_AFTER)
        .expectBody()
        .jsonPath("$.type")
        .isEqualTo("rate_limit_unsatisfiable");
    assertThat(deliveryRowsOf(tenant)).isEqualTo(rows);

    tenant.patch(http, Map.of("rate_per_sec", 1, "burst", 1_000));
    Rows.ageBucket(tenant.id(), AN_HOUR);
    expectReplayed(tenant, endpoint, 1_000, dead.subList(40, 540), true);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(1_000 - MOST_ROWS_OF_A_CALL);

    Rows.ageBucket(tenant.id(), AN_HOUR);
    expectReplayed(tenant, endpoint, 30, dead.subList(540, 570), true);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(1_000 - 30);

    Rows.ageBucket(tenant.id(), AN_HOUR);
    expectReplayed(tenant, endpoint, 70, dead.subList(570, 640), false);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(1_000 - 70);

    expectReplayed(tenant, endpoint, 70, List.of(), false);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(1_000 - 70);
    assertThat(deadOldestFirst(endpoint)).isEmpty();
    assertThat(deadOldestFirst(otherEndpoint)).hasSize(3);
  }

  // The test holds one of three dead deliveries, as a replay of that delivery would. The call
  // counted three and paid for three. It does not wait for the held one: it replays the other
  // two and says that it left some. The lock timeout of the api pool would fail a call that
  // waited.
  @Test
  void bulkReplayPassesOverADeliveryThatAnotherTransactionHolds() throws Exception {
    TestTenant tenant = TestTenant.create(http, "bulk-replay-skip-locked");
    UUID endpoint = tenant.register(http, HOOK, "*");
    insertDeliveries(tenant, endpoint, "dead", "exhausted", 3);
    List<UUID> dead = deadOldestFirst(endpoint);
    UUID held = dead.get(1);
    tenant.patch(http, Map.of("rate_per_sec", 0, "burst", 10));

    try (Connection holder = TestDatabase.connection();
        PreparedStatement lock =
            holder.prepareStatement("SELECT 1 FROM delivery WHERE id = ? FOR UPDATE")) {
      lock.setObject(1, held);
      lock.execute();

      assertThat(replayDead(tenant, endpoint, 10)).isEqualTo(new Replayed(2, true));

      holder.rollback();
    }

    assertThat(deadOldestFirst(endpoint)).containsExactly(held);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(10 - 3);

    assertThat(replayDead(tenant, endpoint, 10)).isEqualTo(new Replayed(1, false));
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(10 - 4);
  }

  // A tenant with a burst of 0 can pay for nothing. Its call is refused, and is not answered as
  // if no delivery were left.
  @Test
  void bulkReplayOfATenantWithoutABurstIsRefused() {
    TestTenant tenant = TestTenant.create(http, "bulk-replay-no-burst");
    UUID endpoint = tenant.register(http, HOOK, "*");
    insertDeliveries(tenant, endpoint, "dead", "exhausted", 2);
    tenant.patch(http, Map.of("burst", 0));

    tenant
        .replayDead(http, endpoint, 10)
        .expectStatus()
        .isEqualTo(HttpStatus.TOO_MANY_REQUESTS)
        .expectBody()
        .jsonPath("$.type")
        .isEqualTo("rate_limit_unsatisfiable");

    assertThat(deadOldestFirst(endpoint)).hasSize(2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"{}", "{\"limit\":null}", "{\"limit\":0}", "{\"limit\":-1}"})
  void bulkReplayWithoutALimitOfAtLeastOneIsRejected(String body) {
    TestTenant tenant = TestTenant.create(http, "bulk-replay-limit");
    UUID endpoint = tenant.register(http, HOOK, "*");
    insertDeliveries(tenant, endpoint, "dead", "exhausted", 1);
    tenant.patch(http, Map.of("rate_per_sec", 0));

    post(tenant, endpoint, body)
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo("limit must be 1 or more");

    assertThat(deadOldestFirst(endpoint)).hasSize(1);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(100);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"", "{\"limit\":1.5}", "{\"limit\":\"ten\"}", "{\"limit\":2147483648}", "[10]"})
  void bulkReplayWithABodyThatIsNotALimitIsRejected(String body) {
    TestTenant tenant = TestTenant.create(http, "bulk-replay-body");
    UUID endpoint = tenant.register(http, HOOK, "*");
    insertDeliveries(tenant, endpoint, "dead", "exhausted", 1);

    post(tenant, endpoint, body)
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON);

    assertThat(deadOldestFirst(endpoint)).hasSize(1);
  }

  // The dead deliveries of a deleted endpoint stay dead. The call finds no endpoint before it
  // counts them, so it costs nothing either.
  @Test
  void bulkReplayForAnEndpointThatIsDeletedOrDoesNotExistIsNotFoundAndNotCharged() {
    TestTenant tenant = TestTenant.create(http, "bulk-replay-no-endpoint");
    UUID deleted = tenant.register(http, HOOK, "*");
    insertDeliveries(tenant, deleted, "dead", "exhausted", 2);
    tenant.patch(http, Map.of("rate_per_sec", 0));
    tenant.delete(http, deleted).expectStatus().isNoContent();

    for (UUID endpoint : List.of(deleted, UUID.randomUUID())) {
      tenant
          .replayDead(http, endpoint, 10)
          .expectStatus()
          .isNotFound()
          .expectHeader()
          .contentType(MediaType.APPLICATION_PROBLEM_JSON)
          .expectBody()
          .jsonPath("$.detail")
          .isEqualTo("No endpoint has this id");
    }

    assertThat(deadOldestFirst(deleted)).hasSize(2);
    assertThat(Rows.bucketTokens(tenant.id())).isEqualTo(100);
  }

  // Calls for up to that many deliveries, and expects the call to replay the given ones and no
  // others: each of them is as a replay between the two readings of the database clock leaves a
  // delivery, and every other delivery row of the tenant is as it was before the call.
  private void expectReplayed(
      TestTenant tenant, UUID endpointId, int limit, List<UUID> expected, boolean remaining) {
    Map<UUID, String> rows = deliveryRowsOf(tenant);
    Instant before = databaseClock();
    assertThat(replayDead(tenant, endpointId, limit))
        .isEqualTo(new Replayed(expected.size(), remaining));
    Instant after = databaseClock();

    assertThat(replayedBetween(before, after, expected)).isEqualTo(expected.size());
    assertThat(deliveryRowsOf(tenant)).isEqualTo(with(rows, deliveryRowsOf(expected)));
  }

  private Replayed replayDead(TestTenant tenant, UUID endpointId, int limit) {
    return tenant
        .replayDead(http, endpointId, limit)
        .expectStatus()
        .isOk()
        .expectBody(Replayed.class)
        .returnResult()
        .getResponseBody();
  }

  private RestTestClient.ResponseSpec post(TestTenant tenant, UUID endpointId, String body) {
    return http.post()
        .uri("/v1/endpoints/{id}/replay", endpointId)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .exchange();
  }

  // Events with one delivery each in the given state, created a day ago and a second apart, so
  // that the order of their creation is the order in which they were written. A delivery that is
  // not pending was completed an hour after it was created.
  private static void insertDeliveries(
      TestTenant tenant, UUID endpointId, String state, String deadReason, int count) {
    TestDatabase.jdbc()
        .sql(
            """
            WITH written AS (
              INSERT INTO event (id, tenant_id, type, body, idempotency_key, request_hash,
                                 created_at)
              SELECT gen_random_uuid(), :t, 'order.created', :body,
                     'written-' || gen_random_uuid(), sha256(:body),
                     now() - interval '1 day' + n * interval '1 second'
              FROM generate_series(1, :count) AS n
              RETURNING id, created_at
            )
            INSERT INTO delivery (tenant_id, event_id, endpoint_id, state, attempts, dead_reason,
                                  next_attempt_at, expires_at, created_at, completed_at)
            SELECT :t, id, :e, :state, 12, :dead_reason::text, created_at,
                   created_at + interval '72 hours', created_at, created_at + interval '1 hour'
            FROM written
            """)
        .param("t", tenant.id())
        .param("e", endpointId)
        .param("state", state)
        .param("dead_reason", deadReason)
        .param("body", EVENT.getBytes(StandardCharsets.UTF_8))
        .param("count", count)
        .update();
  }

  private static List<UUID> deadOldestFirst(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT id FROM delivery WHERE endpoint_id = :e AND state = 'dead'
            ORDER BY created_at, id
            """)
        .param("e", endpointId)
        .query(UUID.class)
        .list();
  }

  /** How many of the deliveries are as a replay between the two times leaves a delivery. */
  private static int replayedBetween(Instant before, Instant after, List<UUID> deliveries) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM delivery
            WHERE id = ANY(:ids)
              AND state = 'pending' AND attempts = 0 AND replays = 1
              AND dead_reason IS NULL AND completed_at IS NULL AND lease_token IS NULL
              AND next_attempt_at BETWEEN :before AND :after + make_interval(secs => :spread)
              AND expires_at BETWEEN :before + make_interval(secs => :budget)
                                 AND :after + make_interval(secs => :budget)
            """)
        .param("ids", deliveries.toArray(UUID[]::new))
        .param("before", before.atOffset(ZoneOffset.UTC))
        .param("after", after.atOffset(ZoneOffset.UTC))
        .param("spread", SPREAD_OF_A_REPLAY.toSeconds())
        .param("budget", BUDGET.toSeconds())
        .query(Integer.class)
        .single();
  }

  private static Duration spreadOfDueTimes(List<UUID> deliveries) {
    return Duration.ofMillis(
        TestDatabase.jdbc()
            .sql(
                """
                SELECT (extract(epoch FROM max(next_attempt_at) - min(next_attempt_at)) * 1000)::bigint
                FROM delivery WHERE id = ANY(:ids)
                """)
            .param("ids", deliveries.toArray(UUID[]::new))
            .query(Long.class)
            .single());
  }

  /** Every delivery row of the tenant by its id, as the text of all of its columns. */
  private static Map<UUID, String> deliveryRowsOf(TestTenant tenant) {
    return rows("SELECT d.id, d::text FROM delivery d WHERE d.tenant_id = :t", "t", tenant.id());
  }

  private static Map<UUID, String> deliveryRowsOf(List<UUID> deliveries) {
    return rows(
        "SELECT d.id, d::text FROM delivery d WHERE d.id = ANY(:ids)",
        "ids",
        deliveries.toArray(UUID[]::new));
  }

  private static Map<UUID, String> rows(String sql, String parameter, Object value) {
    return TestDatabase.jdbc()
        .sql(sql)
        .param(parameter, value)
        .query((row, n) -> Map.entry(row.getObject(1, UUID.class), row.getString(2)))
        .list()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** The rows as they were, apart from those that are given as they are now. */
  private static Map<UUID, String> with(Map<UUID, String> rows, Map<UUID, String> changed) {
    Map<UUID, String> expected = new HashMap<>(rows);
    expected.putAll(changed);
    return expected;
  }

  private static Instant databaseClock() {
    return TestDatabase.jdbc().sql("SELECT now()").query(OffsetDateTime.class).single().toInstant();
  }
}
