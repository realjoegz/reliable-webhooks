package io.github.realjoegz.reliablewebhooks.api;

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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class DeliveryApiIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int MAX_PAGE_SIZE = 100;
  private static final int LISTED = 250;
  private static final Duration BUDGET = Duration.ofHours(72);
  private static final Duration SPREAD_OF_A_REPLAY = Duration.ofSeconds(60);

  /** A page as the list route answers it. */
  private record Page(List<String> ids, String next) {}

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired ExpiryJob expiry;

  // An endpoint has 250 pending deliveries. They were created in groups of 30 with one creation
  // time each, so the second and the third page begin in the middle of a group, where only the
  // id tells the rows of a group apart.
  //
  // A request without a limit gets 100 rows and so does the page after it, and no page gets more.
  // The pages hold every delivery once, in the order of creation time and id, and the last one
  // has no cursor. A limit above 100 is refused, so no request reads a longer page. A request
  // without the endpoint or without the state is refused as well, so no request reads more than
  // the deliveries of one endpoint in one state.
  @Test
  void listIsKeysetPagedAndBounded() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "paged-list");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      insertPendingDeliveriesInGroups(tenant, endpoint, LISTED);
      List<String> inOrder = pendingIdsInListOrder(endpoint);
      assertThat(inOrder).hasSize(LISTED);

      List<Page> pages = new ArrayList<>();
      String cursor = null;
      do {
        Page page = page(list(tenant, endpoint, "pending", null, cursor));
        pages.add(page);
        cursor = page.next();
      } while (cursor != null && pages.size() < LISTED);

      assertThat(pages).extracting(page -> page.ids().size()).containsExactly(100, 100, 50);
      assertThat(pages.stream().flatMap(page -> page.ids().stream()))
          .containsExactlyElementsOf(inOrder);

      Page ofSeven = page(list(tenant, endpoint, "pending", 7, null));
      assertThat(ofSeven.ids()).isEqualTo(inOrder.subList(0, 7));
      assertThat(page(list(tenant, endpoint, "pending", 7, ofSeven.next())).ids())
          .isEqualTo(inOrder.subList(7, 14));
      assertThat(page(list(tenant, endpoint, "pending", MAX_PAGE_SIZE, null)).ids())
          .isEqualTo(inOrder.subList(0, MAX_PAGE_SIZE));
      expectBadRequest(
          list(tenant, endpoint, "pending", MAX_PAGE_SIZE + 1, null),
          "limit must be between 1 and 100");
      expectBadRequest(
          list(tenant, endpoint, "pending", 0, null), "limit must be between 1 and 100");
      expectBadRequest(
          list(tenant, null, "pending", null, null),
          "Required parameter 'endpoint_id' is not present.");
      expectBadRequest(
          list(tenant, endpoint, null, null, null), "Required parameter 'state' is not present.");
    }
  }

  // Two endpoints get the same three events. One delivery of the first endpoint is dead. A list
  // holds the deliveries of the endpoint and the state that it was asked for, and no others.
  @Test
  void listHoldsTheDeliveriesOfOneEndpointInOneState() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "filtered-list");
      UUID first = tenant.register(http, receiver.url("/first"), "*");
      UUID second = tenant.register(http, receiver.url("/second"), "*");
      List<UUID> events =
          List.of(
              tenant.accept(http, EVENT), tenant.accept(http, EVENT), tenant.accept(http, EVENT));
      UUID dead = deliveryOf(events.getFirst(), first);
      Rows.ageDelivery(dead, BUDGET);
      expiry.run();

      assertThat(page(list(tenant, first, "dead", null, null)))
          .isEqualTo(new Page(List.of(dead.toString()), null));
      assertThat(page(list(tenant, first, "pending", null, null)).ids())
          .containsExactly(
              deliveryOf(events.get(1), first).toString(),
              deliveryOf(events.get(2), first).toString());
      assertThat(page(list(tenant, second, "pending", null, null)).ids())
          .containsExactly(
              deliveryOf(events.get(0), second).toString(),
              deliveryOf(events.get(1), second).toString(),
              deliveryOf(events.get(2), second).toString());
      assertThat(page(list(tenant, second, "dead", null, null)))
          .isEqualTo(new Page(List.of(), null));
      assertThat(page(list(tenant, first, "succeeded", null, null)).ids()).isEmpty();
      assertThat(page(list(tenant, first, "cancelled", null, null)).ids()).isEmpty();

      list(tenant, first, "dead", null, null)
          .expectBody()
          .jsonPath("$.length()")
          .isEqualTo(2)
          .jsonPath("$.deliveries[0].length()")
          .isEqualTo(9)
          .jsonPath("$.deliveries[0].id")
          .isEqualTo(dead.toString())
          .jsonPath("$.deliveries[0].event_id")
          .isEqualTo(events.getFirst().toString())
          .jsonPath("$.deliveries[0].endpoint_id")
          .isEqualTo(first.toString())
          .jsonPath("$.deliveries[0].state")
          .isEqualTo("dead")
          .jsonPath("$.deliveries[0].attempts")
          .isEqualTo(0)
          .jsonPath("$.deliveries[0].replays")
          .isEqualTo(0)
          .jsonPath("$.deliveries[0].dead_reason")
          .isEqualTo("expired")
          .jsonPath("$.deliveries[0].created_at")
          .value(String.class, at -> assertThat(Instant.parse(at)).isEqualTo(createdAtOf(dead)))
          .jsonPath("$.deliveries[0].completed_at")
          .value(String.class, at -> assertThat(Instant.parse(at)).isEqualTo(completedAtOf(dead)));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/v1/deliveries",
        "/v1/deliveries?state=pending",
        "/v1/deliveries?endpoint_id={endpoint}",
        "/v1/deliveries?endpoint_id=not-an-id&state=pending",
        "/v1/deliveries?endpoint_id={endpoint}&state=delivered",
        "/v1/deliveries?endpoint_id={endpoint}&state=pending&limit=ten",
        "/v1/deliveries?endpoint_id={endpoint}&state=pending&cursor=",
        "/v1/deliveries?endpoint_id={endpoint}&state=pending&cursor=bm8gcG9zaXRpb24"
      })
  void listWithoutBothFiltersOrWithAParameterOutOfFormIsRejected(String request) throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "list-out-of-form");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");

      http.get()
          .uri(request, endpoint)
          .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
          .exchange()
          .expectStatus()
          .isBadRequest()
          .expectHeader()
          .contentType(MediaType.APPLICATION_PROBLEM_JSON);
    }
  }

  @Test
  void listForAnEndpointThatDoesNotExistIsNotFound() {
    TestTenant tenant = TestTenant.create(http, "list-unknown-endpoint");

    expectNotFound(
        list(tenant, UUID.randomUUID(), "pending", null, null), "No endpoint has this id");
  }

  // No route deletes an endpoint yet, so the test sets the status that a delete will set. The
  // endpoint itself is no longer read, and its deliveries still are.
  @Test
  void deliveriesOfADeletedEndpointAreStillListed() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "list-deleted-endpoint");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      markDeleted(endpoint);

      http.get()
          .uri("/v1/endpoints/{id}", endpoint)
          .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
          .exchange()
          .expectStatus()
          .isNotFound();
      assertThat(page(list(tenant, endpoint, "pending", null, null)).ids())
          .containsExactly(delivery.toString());
    }
  }

  // The first attempt of the delivery fails, and the delivery expires while it waits for the
  // second. It is replayed, and the one attempt of its second run succeeds. Each step is read
  // through the route. Without a run the route shows the current one. A run by its number shows
  // that one, also when it is the current one, and a run that the delivery does not have is not
  // found.
  //
  // The answer to the first attempt asks for an hour before the next one. The delivery is
  // therefore not due again before the test has stopped the worker and read it, however long
  // that takes.
  //
  // The worker is stopped whenever the test moves a time: a delivery that is aged past its budget
  // is due as well, and the worker would claim it before the expiry run came.
  @Test
  void deliveryIsReadWithTheAttemptLogOfItsCurrentRunOrOfTheRunAskedFor() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/hook", FaultReceiver.status(500, "Retry-After", "3600"));
      TestTenant tenant = TestTenant.create(http, "read-delivery");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      UUID event = tenant.accept(http, EVENT);
      UUID delivery = Rows.deliveryOf(event);
      worker.start();
      await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(1));
      worker.stop();

      read(tenant, delivery, null)
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.length()")
          .isEqualTo(11)
          .jsonPath("$.id")
          .isEqualTo(delivery.toString())
          .jsonPath("$.event_id")
          .isEqualTo(event.toString())
          .jsonPath("$.endpoint_id")
          .isEqualTo(endpoint.toString())
          .jsonPath("$.state")
          .isEqualTo("pending")
          .jsonPath("$.attempts")
          .isEqualTo(1)
          .jsonPath("$.replays")
          .isEqualTo(0)
          .jsonPath("$.dead_reason")
          .isEmpty()
          .jsonPath("$.created_at")
          .value(String.class, at -> assertThat(Instant.parse(at)).isEqualTo(createdAtOf(delivery)))
          .jsonPath("$.completed_at")
          .isEmpty()
          .jsonPath("$.run")
          .isEqualTo(0)
          .jsonPath("$.attempt_log.length()")
          .isEqualTo(1)
          .jsonPath("$.attempt_log[0].length()")
          .isEqualTo(6)
          .jsonPath("$.attempt_log[0].attempt_no")
          .isEqualTo(1)
          .jsonPath("$.attempt_log[0].outcome")
          .isEqualTo("failure")
          .jsonPath("$.attempt_log[0].http_status")
          .isEqualTo(500)
          .jsonPath("$.attempt_log[0].error")
          .isEmpty()
          .jsonPath("$.attempt_log[0].started_at")
          .value(String.class, at -> assertThat(Instant.parse(at)).isEqualTo(startedAtOf(delivery)))
          .jsonPath("$.attempt_log[0].duration_ms")
          .value(Integer.class, took -> assertThat(took).isNotNegative());

      Rows.ageDelivery(delivery, BUDGET);
      expiry.run();
      read(tenant, delivery, null)
          .expectBody()
          .jsonPath("$.state")
          .isEqualTo("dead")
          .jsonPath("$.dead_reason")
          .isEqualTo("expired")
          .jsonPath("$.completed_at")
          .value(
              String.class, at -> assertThat(Instant.parse(at)).isEqualTo(completedAtOf(delivery)))
          .jsonPath("$.attempt_log[*].http_status")
          .isEqualTo(List.of(500));

      receiver.answer("/hook", FaultReceiver.status(200));
      tenant.replay(http, delivery).expectStatus().isNoContent();
      read(tenant, delivery, null)
          .expectBody()
          .jsonPath("$.state")
          .isEqualTo("pending")
          .jsonPath("$.attempts")
          .isEqualTo(0)
          .jsonPath("$.replays")
          .isEqualTo(1)
          .jsonPath("$.dead_reason")
          .isEmpty()
          .jsonPath("$.completed_at")
          .isEmpty()
          .jsonPath("$.run")
          .isEqualTo(1)
          .jsonPath("$.attempt_log")
          .isEmpty();

      Rows.ageDelivery(delivery, SPREAD_OF_A_REPLAY);
      worker.start();
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));

      for (Integer currentRun : new Integer[] {null, 1}) {
        read(tenant, delivery, currentRun)
            .expectStatus()
            .isOk()
            .expectBody()
            .jsonPath("$.state")
            .isEqualTo("succeeded")
            .jsonPath("$.attempts")
            .isEqualTo(1)
            .jsonPath("$.replays")
            .isEqualTo(1)
            .jsonPath("$.run")
            .isEqualTo(1)
            .jsonPath("$.attempt_log[*].attempt_no")
            .isEqualTo(List.of(1))
            .jsonPath("$.attempt_log[*].outcome")
            .isEqualTo(List.of("success"))
            .jsonPath("$.attempt_log[*].http_status")
            .isEqualTo(List.of(200));
      }
      read(tenant, delivery, 0)
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.state")
          .isEqualTo("succeeded")
          .jsonPath("$.replays")
          .isEqualTo(1)
          .jsonPath("$.run")
          .isEqualTo(0)
          .jsonPath("$.attempt_log[*].attempt_no")
          .isEqualTo(List.of(1))
          .jsonPath("$.attempt_log[*].outcome")
          .isEqualTo(List.of("failure"))
          .jsonPath("$.attempt_log[*].http_status")
          .isEqualTo(List.of(500));
      expectNotFound(read(tenant, delivery, 2), "The delivery has no run 2");
      expectNotFound(read(tenant, delivery, -1), "The delivery has no run -1");
      http.get()
          .uri("/v1/deliveries/{id}?run=first", delivery)
          .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
          .exchange()
          .expectStatus()
          .isBadRequest();
    }
  }

  @Test
  void deliveryThatDoesNotExistIsNeitherReadNorReplayed() {
    TestTenant tenant = TestTenant.create(http, "unknown-delivery");

    expectNotFound(read(tenant, UUID.randomUUID(), null), "No delivery has this id");
    expectNotFound(
        tenant.replay(http, UUID.randomUUID()), "No delivery to an active endpoint has this id");
  }

  // Only a dead delivery is replayed. This one is pending, and the refusal leaves its row as it
  // is. That a delivery which has succeeded is refused in the same way is part of
  // DeadLetterReplayIT.
  @Test
  void replayOfADeliveryThatIsNotDeadIsRefused() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "replay-not-dead");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      String pending = rowOf(delivery);

      tenant
          .replay(http, delivery)
          .expectStatus()
          .isEqualTo(HttpStatus.CONFLICT)
          .expectHeader()
          .contentType(MediaType.APPLICATION_PROBLEM_JSON)
          .expectBody()
          .jsonPath("$.detail")
          .isEqualTo("Only a dead delivery can be replayed")
          .jsonPath("$.type")
          .doesNotExist();

      assertThat(rowOf(delivery)).isEqualTo(pending);
    }
  }

  // No route deletes an endpoint yet, so the test sets the status that a delete will set. The
  // delivery is dead, so the status of its endpoint is all that stands against the replay: once
  // the endpoint is active again, the same request is accepted.
  @Test
  void replayOfADeliveryWhoseEndpointWasDeletedIsNotFound() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "replay-deleted-endpoint");
      UUID endpoint = tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      Rows.ageDelivery(delivery, BUDGET);
      expiry.run();
      markDeleted(endpoint);
      String dead = rowOf(delivery);

      expectNotFound(
          tenant.replay(http, delivery), "No delivery to an active endpoint has this id");

      assertThat(rowOf(delivery)).isEqualTo(dead);
      read(tenant, delivery, null).expectStatus().isOk();

      TestDatabase.jdbc()
          .sql("UPDATE endpoint SET status = 'active', deleted_at = NULL WHERE id = :id")
          .param("id", endpoint)
          .update();
      tenant.replay(http, delivery).expectStatus().isNoContent();
    }
  }

  @Test
  void deliveryRoutesTakeAnApiKey() {
    UUID id = UUID.randomUUID();

    http.get()
        .uri("/v1/deliveries?endpoint_id={id}&state=pending", id)
        .exchange()
        .expectStatus()
        .isUnauthorized();
    http.get().uri("/v1/deliveries/{id}", id).exchange().expectStatus().isUnauthorized();
    http.post().uri("/v1/deliveries/{id}/replay", id).exchange().expectStatus().isUnauthorized();
  }

  // A parameter that is null is left out of the request.
  private RestTestClient.ResponseSpec list(
      TestTenant tenant, UUID endpoint, String state, Integer limit, String cursor) {
    return http.get()
        .uri(
            uri ->
                uri.path("/v1/deliveries")
                    .queryParamIfPresent("endpoint_id", Optional.ofNullable(endpoint))
                    .queryParamIfPresent("state", Optional.ofNullable(state))
                    .queryParamIfPresent("limit", Optional.ofNullable(limit))
                    .queryParamIfPresent("cursor", Optional.ofNullable(cursor))
                    .build())
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  private RestTestClient.ResponseSpec read(TestTenant tenant, UUID delivery, Integer run) {
    return http.get()
        .uri(
            uri -> {
              uri.path("/v1/deliveries/{id}");
              if (run != null) {
                uri.queryParam("run", run);
              }
              return uri.build(delivery);
            })
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  private static Page page(RestTestClient.ResponseSpec response) {
    Map<String, Object> body =
        response
            .expectStatus()
            .isOk()
            .expectBody(new ParameterizedTypeReference<Map<String, Object>>() {})
            .returnResult()
            .getResponseBody();
    assertThat(body).containsOnlyKeys("deliveries", "next");
    List<?> deliveries = (List<?>) body.get("deliveries");
    return new Page(
        deliveries.stream().map(delivery -> (String) ((Map<?, ?>) delivery).get("id")).toList(),
        (String) body.get("next"));
  }

  private static void expectBadRequest(RestTestClient.ResponseSpec response, String detail) {
    response
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo(detail);
  }

  private static void expectNotFound(RestTestClient.ResponseSpec response, String detail) {
    response
        .expectStatus()
        .isNotFound()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo(detail)
        .jsonPath("$.type")
        .doesNotExist();
  }

  // Events with one delivery each, as ingest leaves them for one matching endpoint. They are
  // written here and not accepted through the API, because no two requests give their deliveries
  // the same creation time. Thirty rows share each one. It is the time of this statement less a
  // number of whole seconds, so it has microseconds like a time that ingest writes: a cursor that
  // lost them on its way into the statement would repeat or skip the rest of its group.
  private static void insertPendingDeliveriesInGroups(
      TestTenant tenant, UUID endpointId, int count) {
    TestDatabase.jdbc()
        .sql(
            """
            WITH grouped AS (
              INSERT INTO event (id, tenant_id, type, body, idempotency_key, request_hash,
                                 created_at)
              SELECT gen_random_uuid(), :t, 'order.created', :body, 'listed-' || n, sha256(:body),
                     now() - (n / 30) * interval '1 second'
              FROM generate_series(1, :count) AS n
              RETURNING id, created_at
            )
            INSERT INTO delivery (tenant_id, event_id, endpoint_id, next_attempt_at, expires_at,
                                  created_at)
            SELECT :t, id, :e, created_at, created_at + interval '72 hours', created_at
            FROM grouped
            """)
        .param("t", tenant.id())
        .param("e", endpointId)
        .param("body", EVENT.getBytes(StandardCharsets.UTF_8))
        .param("count", count)
        .update();
  }

  private static List<String> pendingIdsInListOrder(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT id::text FROM delivery
            WHERE endpoint_id = :e AND state = 'pending'
            ORDER BY created_at, id
            """)
        .param("e", endpointId)
        .query(String.class)
        .list();
  }

  private static UUID deliveryOf(UUID eventId, UUID endpointId) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM delivery WHERE event_id = :event AND endpoint_id = :endpoint")
        .param("event", eventId)
        .param("endpoint", endpointId)
        .query(UUID.class)
        .single();
  }

  private static void markDeleted(UUID endpointId) {
    TestDatabase.jdbc()
        .sql("UPDATE endpoint SET status = 'deleted', deleted_at = now() WHERE id = :id")
        .param("id", endpointId)
        .update();
  }

  /** The delivery row as the text of all of its columns. */
  private static String rowOf(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql("SELECT d::text FROM delivery d WHERE d.id = :id")
        .param("id", deliveryId)
        .query(String.class)
        .single();
  }

  private static Instant createdAtOf(UUID deliveryId) {
    return timeOf("SELECT created_at FROM delivery WHERE id = :id", deliveryId);
  }

  private static Instant completedAtOf(UUID deliveryId) {
    return timeOf("SELECT completed_at FROM delivery WHERE id = :id", deliveryId);
  }

  private static Instant startedAtOf(UUID deliveryId) {
    return timeOf("SELECT started_at FROM delivery_attempt WHERE delivery_id = :id", deliveryId);
  }

  private static Instant timeOf(String sql, UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql(sql)
        .param("id", deliveryId)
        .query(OffsetDateTime.class)
        .single()
        .toInstant();
  }
}
