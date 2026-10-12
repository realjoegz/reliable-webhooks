package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * The admin routes. The class runs against the replica that can deliver to the receiver, because
 * two of its tests show what a suspension does to deliveries.
 */
@UnsafeLocalReplica
class AdminApiIT {

  private static final String API_KEY_FORMAT = "rwh_[A-Za-z0-9_-]{43}";
  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String FOR_THE_HELD_ENDPOINT =
      """
      {"type":"held.sent","data":{}}""";
  private static final String FOR_THE_IDLE_ENDPOINT =
      """
      {"type":"idle.sent","data":{}}""";
  private static final int MAX_IN_FLIGHT_FOR_AN_ENDPOINT = 4;
  private static final Duration AN_HOUR = Duration.ofHours(1);

  private record Bucket(int burst, double tokens) {}

  /** What a patch can set, as the tenant row holds it. */
  private record Settings(double ratePerSec, int burst, int maxEndpoints, String status) {}

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;

  @Test
  void createdTenantHasBucketWithTokensEqualToBurst() {
    UUID id = UUID.fromString(createTenant("bucket").get("id"));

    Bucket bucket =
        TestDatabase.jdbc()
            .sql(
                """
                SELECT t.burst, b.tokens FROM tenant t
                JOIN rate_bucket b ON b.tenant_id = t.id
                WHERE t.id = :id
                """)
            .param("id", id)
            .query((row, n) -> new Bucket(row.getInt("burst"), row.getDouble("tokens")))
            .single();

    assertThat(bucket.burst()).isEqualTo(100);
    assertThat(bucket.tokens()).isEqualTo(bucket.burst());
  }

  // The test holds a table lock that the bucket insert needs. The tenant insert has run by then,
  // and the lock timeout of the api pool ends the wait.
  @Test
  void failedBucketInsertLeavesNoTenant() throws SQLException {
    String name = "no-bucket-" + UUID.randomUUID();

    try (Connection blocker = TestDatabase.connection();
        Statement statement = blocker.createStatement()) {
      statement.execute("LOCK TABLE rate_bucket IN ACCESS EXCLUSIVE MODE");

      http.post()
          .uri("/admin/tenants")
          .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
          .contentType(MediaType.APPLICATION_JSON)
          .body(Map.of("name", name))
          .exchange()
          .expectStatus()
          .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
          .expectHeader()
          .contentType(MediaType.APPLICATION_PROBLEM_JSON)
          .expectHeader()
          .valueEquals(HttpHeaders.RETRY_AFTER, "1")
          .expectBody()
          .jsonPath("$.type")
          .isEqualTo("database_busy");

      blocker.rollback();
    }

    assertThat(tenantsNamed(name)).isZero();
  }

  @Test
  void apiKeyIsShownOnceAndOnlyItsSha256IsStored() throws NoSuchAlgorithmException {
    Map<String, String> created = createTenant("key");
    UUID id = UUID.fromString(created.get("id"));
    String apiKey = created.get("api_key");

    assertThat(apiKey).matches(API_KEY_FORMAT);
    assertThat(storedHash(id)).isEqualTo(sha256(apiKey));
    Integer positionOfKeyInRow =
        TestDatabase.jdbc()
            .sql("SELECT strpos(t::text, :key) FROM tenant t WHERE t.id = :id")
            .param("key", apiKey)
            .param("id", id)
            .query(Integer.class)
            .single();
    assertThat(positionOfKeyInRow).isZero();
  }

  @Test
  void rotatedKeyReplacesTheOldOne() throws NoSuchAlgorithmException {
    Map<String, String> created = createTenant("rotate");
    UUID id = UUID.fromString(created.get("id"));
    Map<String, String> other = createTenant("not-rotated");

    Map<String, String> rotated =
        http.post()
            .uri("/admin/tenants/{id}/api-key/rotate", id)
            .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .cacheControl(CacheControl.noStore())
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();

    assertThat(rotated.get("api_key")).matches(API_KEY_FORMAT).isNotEqualTo(created.get("api_key"));
    assertThat(storedHash(id)).isEqualTo(sha256(rotated.get("api_key")));
    assertThat(storedHash(UUID.fromString(other.get("id"))))
        .isEqualTo(sha256(other.get("api_key")));
  }

  @Test
  void rotateForUnknownTenantIsNotFound() throws NoSuchAlgorithmException {
    Map<String, String> existing = createTenant("not-addressed");

    http.post()
        .uri("/admin/tenants/{id}/api-key/rotate", UUID.randomUUID())
        .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
        .exchange()
        .expectStatus()
        .isNotFound()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.status")
        .isEqualTo(404);

    assertThat(storedHash(UUID.fromString(existing.get("id"))))
        .isEqualTo(sha256(existing.get("api_key")));
  }

  // Each of these characters takes two chars of a Java string and counts once.
  @Test
  void nameOfTwoHundredCharactersIsAccepted() {
    String name = Character.toString(0x1F680).repeat(200);

    UUID id = UUID.fromString(createTenant(name).get("id"));

    Integer storedLength =
        TestDatabase.jdbc()
            .sql("SELECT char_length(name) FROM tenant WHERE id = :id")
            .param("id", id)
            .query(Integer.class)
            .single();
    assertThat(storedLength).isEqualTo(200);
  }

  static Stream<Map<String, String>> bodiesWithoutAnAcceptableName() {
    return Stream.of(Map.of(), Map.of("name", ""), Map.of("name", "a".repeat(201)));
  }

  @ParameterizedTest
  @MethodSource("bodiesWithoutAnAcceptableName")
  void nameOutsideOneToTwoHundredCharactersIsRejected(Map<String, String> body) {
    http.post()
        .uri("/admin/tenants")
        .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo("name must have 1 to 200 characters");
  }

  static Stream<Arguments> patchesAndWhatTheySet() {
    return Stream.of(
        arguments(Map.of("rate_per_sec", 2.5), new Settings(2.5, 100, 20, "active")),
        arguments(Map.of("rate_per_sec", 0), new Settings(0, 100, 20, "active")),
        arguments(Map.of("rate_per_sec", 0.000001), new Settings(0.000001, 100, 20, "active")),
        arguments(Map.of("rate_per_sec", 1_000_000), new Settings(1_000_000, 100, 20, "active")),
        arguments(Map.of("burst", 7), new Settings(50, 7, 20, "active")),
        arguments(Map.of("burst", 0), new Settings(50, 0, 20, "active")),
        arguments(Map.of("max_endpoints", 50), new Settings(50, 100, 50, "active")),
        arguments(Map.of("max_endpoints", 0), new Settings(50, 100, 0, "active")),
        arguments(Map.of("status", "suspended"), new Settings(50, 100, 20, "suspended")),
        arguments(
            Map.of("rate_per_sec", 0.5, "burst", 3, "max_endpoints", 1, "status", "suspended"),
            new Settings(0.5, 3, 1, "suspended")));
  }

  // A second tenant is the control: a patch that named no tenant would change its row too.
  @ParameterizedTest
  @MethodSource("patchesAndWhatTheySet")
  void patchSetsWhatItNamesAndNothingElse(Map<String, Object> patch, Settings expected) {
    TestTenant tenant = TestTenant.create(http, "patched");
    TestTenant other = TestTenant.create(http, "not-patched");
    String otherRow = rowOf(other);

    patch(tenant.id(), patch).expectStatus().isNoContent().expectBody().isEmpty();

    assertThat(settingsOf(tenant)).isEqualTo(expected);
    assertThat(rowOf(other)).isEqualTo(otherRow);
  }

  static Stream<Arguments> patchesThatAreRefused() {
    String oneIsRequired = "One of rate_per_sec, burst, max_endpoints and status is required";
    String rate = "rate_per_sec must be 0, or between 0.000001 and 1000000";
    String cap = "max_endpoints must be between 0 and 50";
    return Stream.of(
        arguments("{}", oneIsRequired),
        arguments("{\"name\":\"renamed\"}", oneIsRequired),
        arguments("{\"rate_per_sec\":-0.5}", rate),
        arguments("{\"rate_per_sec\":0.0000009}", rate),
        arguments("{\"rate_per_sec\":1000001}", rate),
        arguments("{\"rate_per_sec\":1e999}", rate),
        arguments("{\"burst\":-1}", "burst must be 0 or more"),
        arguments("{\"max_endpoints\":-1}", cap),
        arguments("{\"max_endpoints\":51}", cap),
        arguments("{\"status\":\"deleted\"}", "status must be active or suspended"),
        arguments("{\"burst\":7,\"status\":\"deleted\"}", "status must be active or suspended"));
  }

  // The last body names a burst that could be set beside a status that cannot. Nothing of it is
  // set.
  @ParameterizedTest
  @MethodSource("patchesThatAreRefused")
  void patchOutsideTheLimitsIsRejectedAndSetsNothing(String body, String detail) {
    TestTenant tenant = TestTenant.create(http, "refused-patch");
    String row = rowOf(tenant);

    http.patch()
        .uri("/admin/tenants/{id}", tenant.id())
        .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body.getBytes(StandardCharsets.UTF_8))
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo(detail);

    assertThat(rowOf(tenant)).isEqualTo(row);
  }

  // The mapper of the API refuses these before the route sees them. Read as whole numbers, they
  // would set a burst of 1 and a cap of 2.
  @ParameterizedTest
  @ValueSource(strings = {"{\"burst\":1.9}", "{\"max_endpoints\":2.5}"})
  void patchWithAFractionWhereAWholeNumberBelongsIsRejected(String body) {
    TestTenant tenant = TestTenant.create(http, "fraction-in-patch");
    String row = rowOf(tenant);

    http.patch()
        .uri("/admin/tenants/{id}", tenant.id())
        .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body.getBytes(StandardCharsets.UTF_8))
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON);

    assertThat(rowOf(tenant)).isEqualTo(row);
  }

  @Test
  void patchForUnknownTenantIsNotFound() {
    TestTenant existing = TestTenant.create(http, "not-addressed-by-patch");
    String row = rowOf(existing);

    patch(UUID.randomUUID(), Map.of("status", "suspended"))
        .expectStatus()
        .isNotFound()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.status")
        .isEqualTo(404);

    assertThat(rowOf(existing)).isEqualTo(row);
  }

  // A tenant that could patch itself could lift its own limits and its own suspension.
  @Test
  void patchTakesTheAdminTokenAndNotTheKeyOfTheTenant() {
    TestTenant tenant = TestTenant.create(http, "patching-itself");
    String row = rowOf(tenant);

    http.patch()
        .uri("/admin/tenants/{id}", tenant.id())
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("burst", 1_000_000))
        .exchange()
        .expectStatus()
        .isUnauthorized();

    assertThat(rowOf(tenant)).isEqualTo(row);
  }

  // The replica has the tenant in its key cache, with the rate and the burst that a refusal of
  // the bucket is answered from. The patch evicts it, so the next request of the tenant is
  // answered from the row as the patch left it.
  @Test
  void patchedRateAndBurstReachTheReplicaThatHandledThePatchAtOnce() {
    TestTenant tenant = TestTenant.create(http, "patched-limits");
    whoAmI(tenant)
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.rate_per_sec")
        .isEqualTo(50.0)
        .jsonPath("$.burst")
        .isEqualTo(100);

    patch(tenant.id(), Map.of("rate_per_sec", 0.25, "burst", 9)).expectStatus().isNoContent();

    whoAmI(tenant)
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.rate_per_sec")
        .isEqualTo(0.25)
        .jsonPath("$.burst")
        .isEqualTo(9);
  }

  @Test
  void patchedEndpointCapIsTheCapOfTheNextRegistration() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "patched-cap");

      patch(tenant.id(), Map.of("max_endpoints", 1)).expectStatus().isNoContent();
      register(tenant, receiver).expectStatus().isCreated();
      register(tenant, receiver).expectStatus().isEqualTo(HttpStatus.CONFLICT);

      patch(tenant.id(), Map.of("max_endpoints", 2)).expectStatus().isNoContent();
      register(tenant, receiver).expectStatus().isCreated();
      register(tenant, receiver).expectStatus().isEqualTo(HttpStatus.CONFLICT);
    }
  }

  // The tenant has four requests in flight, which the receiver holds, when it is suspended. It
  // also has a delivery for a second endpoint, which the test kept from being claimed by moving
  // it an hour ahead. The replica has the key of the tenant in its cache, and rejects it from the
  // suspension on.
  //
  // The delivery is then made due. Nothing is in flight for its endpoint, and its tenant has four
  // requests in flight where it may have sixteen, so only the suspension keeps a claim from
  // taking it. The event of another tenant, accepted after that, is delivered: a claim ran while
  // the delivery was due. The row of the delivery is afterwards what it was, column for column.
  //
  // The four requests that were in flight are not called back. The receiver answers them at the
  // end, and their results are recorded. A held request is cut at the deadline of an attempt,
  // 15 s after it began, so the test has that long to get there. A run that took longer would
  // fail and not pass: the four would be recorded as failed.
  @Test
  void suspendStopsOutboundDeliveries() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer("/held", hold);
      receiver.answer("/idle", FaultReceiver.status(200));
      receiver.answer("/other-tenant", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "suspended-while-delivering");
      tenant.register(http, receiver.url("/held"), "held.sent");
      tenant.register(http, receiver.url("/idle"), "idle.sent");
      TestTenant other = TestTenant.create(http, "beside-the-suspended");
      other.register(http, receiver.url("/other-tenant"), "*");
      UUID forTheIdleEndpoint = Rows.deliveryOf(tenant.accept(http, FOR_THE_IDLE_ENDPOINT));
      Rows.postponeDelivery(forTheIdleEndpoint, AN_HOUR);
      List<UUID> inFlight = new ArrayList<>();
      for (int i = 0; i < MAX_IN_FLIGHT_FOR_AN_ENDPOINT; i++) {
        inFlight.add(Rows.deliveryOf(tenant.accept(http, FOR_THE_HELD_ENDPOINT)));
      }
      worker.start();
      await().until(() -> hold.waiting() == MAX_IN_FLIGHT_FOR_AN_ENDPOINT);

      patch(tenant.id(), Map.of("status", "suspended")).expectStatus().isNoContent();

      expectSuspended(tenant.send(http, TestTenant.newIdempotencyKey(), EVENT));
      Rows.ageDelivery(forTheIdleEndpoint, AN_HOUR);
      List<String> held = everyColumnOf(List.of(forTheIdleEndpoint));
      UUID ofTheOtherTenant = Rows.deliveryOf(other.accept(http, EVENT));
      await()
          .untilAsserted(
              () -> assertThat(Rows.delivery(ofTheOtherTenant).state()).isEqualTo("succeeded"));

      assertThat(everyColumnOf(List.of(forTheIdleEndpoint))).isEqualTo(held);
      assertThat(Rows.delivery(forTheIdleEndpoint))
          .isEqualTo(new Rows.Delivery("pending", 0, 0, false, null, false));
      assertThat(Rows.attempts(forTheIdleEndpoint)).isEmpty();
      assertThat(receiver.requests("/idle")).isEmpty();
      assertThat(hold.waiting()).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);

      hold.release();
      for (UUID delivery : inFlight) {
        await()
            .untilAsserted(
                () ->
                    assertThat(Rows.delivery(delivery))
                        .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true)));
      }
      assertThat(tenant.eventRows()).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT + 1);
    }
  }

  // Three deliveries of a tenant are due when it is suspended, and the worker starts only after
  // that. The event of another tenant, accepted then, is delivered, so claims ran while the
  // three were due. Their rows are what they were, column for column, and the receiver has no
  // request for them.
  //
  // Once the tenant is active again, the three are delivered, each with its first attempt: the
  // time for which they were held has used none. The replica that handled the patch also takes
  // the key of the tenant again at once.
  @Test
  void resumeDeliversHeldDeliveries() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/held", FaultReceiver.status(200));
      receiver.answer("/other-tenant", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "suspended-and-resumed");
      tenant.register(http, receiver.url("/held"), "*");
      TestTenant other = TestTenant.create(http, "beside-the-resumed");
      other.register(http, receiver.url("/other-tenant"), "*");
      List<UUID> held = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        held.add(Rows.deliveryOf(tenant.accept(http, EVENT)));
      }
      patch(tenant.id(), Map.of("status", "suspended")).expectStatus().isNoContent();
      List<String> rows = everyColumnOf(held);
      worker.start();
      UUID ofTheOtherTenant = Rows.deliveryOf(other.accept(http, EVENT));
      await()
          .untilAsserted(
              () -> assertThat(Rows.delivery(ofTheOtherTenant).state()).isEqualTo("succeeded"));

      assertThat(everyColumnOf(held)).isEqualTo(rows);
      assertThat(receiver.requests("/held")).isEmpty();
      expectSuspended(tenant.send(http, TestTenant.newIdempotencyKey(), EVENT));

      patch(tenant.id(), Map.of("status", "active")).expectStatus().isNoContent();

      for (UUID delivery : held) {
        await()
            .untilAsserted(
                () ->
                    assertThat(Rows.delivery(delivery))
                        .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true)));
        assertThat(Rows.attempts(delivery)).containsExactly(Rows.Attempt.answered(1, 200));
      }
      assertThat(receiver.requests("/held")).hasSize(3);
      tenant.send(http, TestTenant.newIdempotencyKey(), EVENT).expectStatus().isAccepted();
    }
  }

  private RestTestClient.ResponseSpec patch(UUID tenantId, Map<String, Object> settings) {
    return http.patch()
        .uri("/admin/tenants/{id}", tenantId)
        .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
        .contentType(MediaType.APPLICATION_JSON)
        .body(settings)
        .exchange();
  }

  private RestTestClient.ResponseSpec whoAmI(TestTenant tenant) {
    return http.get()
        .uri("/v1/whoami")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  private RestTestClient.ResponseSpec register(TestTenant tenant, FaultReceiver receiver) {
    return http.post()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("url", receiver.url("/hook"), "event_types", List.of("*")))
        .exchange();
  }

  private static void expectSuspended(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isForbidden()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.type")
        .isEqualTo("tenant_suspended");
  }

  private static Settings settingsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT rate_per_sec, burst, max_endpoints, status FROM tenant WHERE id = :id")
        .param("id", tenant.id())
        .query(
            (row, n) ->
                new Settings(
                    row.getDouble("rate_per_sec"),
                    row.getInt("burst"),
                    row.getInt("max_endpoints"),
                    row.getString("status")))
        .single();
  }

  /** The tenant row as the text of all of its columns. */
  private static String rowOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT t::text FROM tenant t WHERE t.id = :id")
        .param("id", tenant.id())
        .query(String.class)
        .single();
  }

  /** The delivery rows, each as the text of all of its columns. */
  private static List<String> everyColumnOf(List<UUID> deliveryIds) {
    return TestDatabase.jdbc()
        .sql("SELECT d::text FROM delivery d WHERE d.id = ANY(:ids) ORDER BY d.id")
        .param("ids", deliveryIds.toArray(UUID[]::new))
        .query(String.class)
        .list();
  }

  private Map<String, String> createTenant(String name) {
    return http.post()
        .uri("/admin/tenants")
        .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("name", name))
        .exchange()
        .expectStatus()
        .isCreated()
        .expectHeader()
        .cacheControl(CacheControl.noStore())
        .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
        .returnResult()
        .getResponseBody();
  }

  private static int tenantsNamed(String name) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM tenant WHERE name = :name")
        .param("name", name)
        .query(Integer.class)
        .single();
  }

  private static byte[] storedHash(UUID tenantId) {
    return TestDatabase.jdbc()
        .sql("SELECT api_key_hash FROM tenant WHERE id = :id")
        .param("id", tenantId)
        .query(byte[].class)
        .single();
  }

  private static byte[] sha256(String apiKey) throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("SHA-256").digest(apiKey.getBytes(StandardCharsets.US_ASCII));
  }
}
