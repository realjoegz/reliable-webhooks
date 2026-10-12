package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.endpoint.SecretCipher;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class EndpointApiIT {

  private static final String HOST = "hooks.endpoint-api.example";
  private static final String URL = "https://" + HOST + "/hooks/1";
  private static final String SECRET_FORMAT = "whsec_[A-Za-z0-9+/]{43}=";
  private static final String ISO_INSTANT = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z";
  private static final Duration OVERLAP = Duration.ofHours(24);

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired SecretCipher cipher;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  @Test
  void secretIsShownOnceAndOnlyItsCiphertextIsStored() {
    TestTenant tenant = TestTenant.create(http, "secret");

    Map<String, String> created =
        register(tenant, URL, List.of("*"))
            .expectStatus()
            .isCreated()
            .expectHeader()
            .cacheControl(CacheControl.noStore())
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    UUID id = UUID.fromString(created.get("id"));
    String secret = created.get("secret");

    assertThat(created).containsOnlyKeys("id", "secret");
    assertThat(secret).matches(SECRET_FORMAT);

    // Every value of a later answer is searched for the key, so the secret is not there under
    // another name either. The list holds the endpoint that the read shows and nothing else.
    String keyInBase64 = secret.substring("whsec_".length());
    Map<String, Object> shown =
        read(tenant, id)
            .expectStatus()
            .isOk()
            .expectBody(new ParameterizedTypeReference<Map<String, Object>>() {})
            .returnResult()
            .getResponseBody();
    List<Map<String, Object>> listed =
        list(tenant)
            .expectStatus()
            .isOk()
            .expectBody(new ParameterizedTypeReference<List<Map<String, Object>>>() {})
            .returnResult()
            .getResponseBody();
    assertThat(shown).containsEntry("id", id.toString());
    assertThat(shown.toString()).doesNotContain(keyInBase64);
    assertThat(listed).containsExactly(shown);

    byte[] key = Base64.getDecoder().decode(keyInBase64);
    byte[] stored =
        TestDatabase.jdbc()
            .sql("SELECT secret FROM endpoint WHERE id = :id")
            .param("id", id)
            .query(byte[].class)
            .single();
    assertThat(cipher.decrypt(stored)).isEqualTo(key);
    Integer positionOfKeyInStoredValue =
        TestDatabase.jdbc()
            .sql("SELECT position(:key IN secret) FROM endpoint WHERE id = :id")
            .param("key", key)
            .param("id", id)
            .query(Integer.class)
            .single();
    assertThat(positionOfKeyInStoredValue).isZero();
    Integer positionOfSecretInRow =
        TestDatabase.jdbc()
            .sql("SELECT strpos(e::text, :secret) FROM endpoint e WHERE e.id = :id")
            .param("secret", secret)
            .param("id", id)
            .query(Integer.class)
            .single();
    assertThat(positionOfSecretInRow).isZero();
  }

  @Test
  void twoEndpointsGetDifferentSecrets() {
    TestTenant tenant = TestTenant.create(http, "two-secrets");

    assertThat(secretOf(register(tenant, URL, List.of("*"))))
        .isNotEqualTo(secretOf(register(tenant, URL, List.of("*"))));
  }

  @Test
  void endpointIsCreatedWithAClosedCircuit() {
    TestTenant tenant = TestTenant.create(http, "circuit");

    UUID id = create(tenant, URL, List.of("*"));

    assertThat(Rows.circuit(id)).isEqualTo(Rows.Circuit.CLOSED);
  }

  // The test holds a table lock that the circuit insert needs. The endpoint insert has run by
  // then, and the lock timeout of the api pool ends the wait.
  @Test
  void failedCircuitInsertLeavesNoEndpoint() throws SQLException {
    TestTenant tenant = TestTenant.create(http, "no-circuit");

    try (Connection blocker = TestDatabase.connection();
        Statement statement = blocker.createStatement()) {
      statement.execute("LOCK TABLE endpoint_circuit IN ACCESS EXCLUSIVE MODE");

      register(tenant, URL, List.of("*"))
          .expectStatus()
          .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
          .expectHeader()
          .valueEquals(HttpHeaders.RETRY_AFTER, "1")
          .expectBody()
          .jsonPath("$.type")
          .isEqualTo("database_busy");

      blocker.rollback();
    }

    assertThat(tenant.endpointRows()).isZero();
    create(tenant, URL, List.of("*"));
    assertThat(tenant.endpointRows()).isEqualTo(1);
  }

  @Test
  void endpointIsReadAsItWasRegistered() {
    TestTenant tenant = TestTenant.create(http, "read");
    String url = "https://" + HOST + ":8443/hooks/2?source=orders";

    UUID id = create(tenant, url, List.of("order.created", "order.paid"));

    read(tenant, id)
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.length()")
        .isEqualTo(5)
        .jsonPath("$.id")
        .isEqualTo(id.toString())
        .jsonPath("$.url")
        .isEqualTo(url)
        .jsonPath("$.event_types")
        .isEqualTo(List.of("order.created", "order.paid"))
        .jsonPath("$.circuit")
        .isEqualTo("closed")
        .jsonPath("$.created_at")
        .value(String.class, createdAt -> assertThat(createdAt).matches(ISO_INSTANT));
  }

  @Test
  void listHoldsTheEndpointsOfTheTenantInTheOrderOfTheirCreation() {
    TestTenant tenant = TestTenant.create(http, "list");
    TestTenant other = TestTenant.create(http, "list-other");
    list(tenant).expectStatus().isOk().expectBody().json("[]");

    UUID first = create(tenant, URL, List.of("*"));
    UUID second = create(tenant, URL, List.of("order.created"));
    create(other, URL, List.of("*"));

    list(tenant)
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.length()")
        .isEqualTo(2)
        .jsonPath("$[0].id")
        .isEqualTo(first.toString())
        .jsonPath("$[0].event_types")
        .isEqualTo(List.of("*"))
        .jsonPath("$[1].id")
        .isEqualTo(second.toString())
        .jsonPath("$[1].event_types")
        .isEqualTo(List.of("order.created"));
  }

  // Only closed and open are stored. The time of the next probe is moved in the row, and the
  // database clock decides which side of it now is.
  @Test
  void circuitStateIsDerivedFromTheCircuitRowAndTheDatabaseClock() {
    TestTenant tenant = TestTenant.create(http, "derived-circuit");
    UUID id = create(tenant, URL, List.of("*"));

    openCircuit(id, "now() + interval '30 seconds'");
    read(tenant, id).expectBody().jsonPath("$.circuit").isEqualTo("open");
    list(tenant).expectBody().jsonPath("$[0].circuit").isEqualTo("open");

    openCircuit(id, "now() - interval '1 second'");
    read(tenant, id).expectBody().jsonPath("$.circuit").isEqualTo("half_open");
    list(tenant).expectBody().jsonPath("$[0].circuit").isEqualTo("half_open");
  }

  @Test
  void patchReplacesTheEventTypesAndNothingElse() {
    TestTenant tenant = TestTenant.create(http, "patch");
    UUID id = create(tenant, URL, List.of("*"));
    UUID untouched = create(tenant, URL, List.of("*"));

    patch(tenant, id, Map.of("event_types", List.of("order.paid"), "url", "https://other.example"))
        .expectStatus()
        .isNoContent()
        .expectBody()
        .isEmpty();

    read(tenant, id)
        .expectBody()
        .jsonPath("$.url")
        .isEqualTo(URL)
        .jsonPath("$.event_types")
        .isEqualTo(List.of("order.paid"));
    read(tenant, untouched).expectBody().jsonPath("$.event_types").isEqualTo(List.of("*"));
  }

  @Test
  void endpointOfAnotherTenantIsNotFoundAndNotChanged() {
    TestTenant owner = TestTenant.create(http, "owner");
    TestTenant other = TestTenant.create(http, "not-the-owner");
    UUID id = create(owner, URL, List.of("*"));

    expectNotFound(read(other, id));
    expectNotFound(patch(other, id, Map.of("event_types", List.of("order.paid"))));
    list(other).expectStatus().isOk().expectBody().json("[]");

    read(owner, id).expectStatus().isOk().expectBody().jsonPath("$.event_types[0]").isEqualTo("*");
  }

  @Test
  void endpointThatDoesNotExistIsNotFound() {
    TestTenant tenant = TestTenant.create(http, "unknown-id");

    expectNotFound(read(tenant, UUID.randomUUID()));
    expectNotFound(patch(tenant, UUID.randomUUID(), Map.of("event_types", List.of("*"))));
    expectNotFound(tenant.rotateSecret(http, UUID.randomUUID()));
    expectNotFound(tenant.delete(http, UUID.randomUUID()));
  }

  // The row of a deleted endpoint stays, with its status and the time of the delete. For the
  // tenant the endpoint is gone: no route finds it, and a second delete finds none either.
  @Test
  void deletedEndpointIsNeitherListedNorReadNorPatchedNorDeletedAgain() {
    TestTenant tenant = TestTenant.create(http, "deleted");
    UUID id = create(tenant, URL, List.of("*"));
    UUID kept = create(tenant, URL, List.of("*"));

    tenant.delete(http, id).expectStatus().isNoContent().expectBody().isEmpty();

    assertThat(statusAndWhetherDeletedAtIsSet(id)).isEqualTo("deleted, true");
    assertThat(statusAndWhetherDeletedAtIsSet(kept)).isEqualTo("active, false");
    expectNotFound(read(tenant, id));
    expectNotFound(patch(tenant, id, Map.of("event_types", List.of("order.paid"))));
    expectNotFound(tenant.rotateSecret(http, id));
    expectNotFound(tenant.delete(http, id));
    list(tenant)
        .expectBody()
        .jsonPath("$.length()")
        .isEqualTo(1)
        .jsonPath("$[0].id")
        .isEqualTo(kept.toString());
  }

  // A rotation answers with a new secret, once, and stores it as the secret of the endpoint. The
  // secret that it replaces is kept beside it, for 24 hours of the database clock. A second
  // rotation keeps the secret of the first, and the one from before the first is gone.
  @Test
  void rotationShowsTheNewSecretOnceAndKeepsTheOneBeforeItForTwentyFourHours() {
    TestTenant tenant = TestTenant.create(http, "rotation");
    TestTenant.Endpoint endpoint = tenant.registerForSecret(http, URL, "*");
    assertThat(storedSecretsOf(endpoint.id())).containsExactly(endpoint.secret(), null);
    assertThat(previousSecretExpiresAt(endpoint.id())).isEmpty();

    Instant before = databaseClock();
    Map<String, String> answer =
        tenant
            .rotateSecret(http, endpoint.id())
            .expectStatus()
            .isOk()
            .expectHeader()
            .cacheControl(CacheControl.noStore())
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    Instant after = databaseClock();
    String rotated = answer.get("secret");

    assertThat(answer).containsOnlyKeys("secret");
    assertThat(rotated).matches(SECRET_FORMAT).isNotEqualTo(endpoint.secret());
    assertThat(storedSecretsOf(endpoint.id())).containsExactly(rotated, endpoint.secret());
    assertThat(previousSecretExpiresAt(endpoint.id()))
        .hasValueSatisfying(
            expiresAt ->
                assertThat(expiresAt).isBetween(before.plus(OVERLAP), after.plus(OVERLAP)));
    String shown =
        read(tenant, endpoint.id()).expectBody(String.class).returnResult().getResponseBody();
    assertThat(shown).doesNotContain(rotated.substring("whsec_".length()));

    String rotatedAgain =
        tenant
            .rotateSecret(http, endpoint.id())
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody()
            .get("secret");

    assertThat(storedSecretsOf(endpoint.id())).containsExactly(rotatedAgain, rotated);
  }

  @Test
  void endpointRoutesTakeAnApiKey() {
    http.get().uri("/v1/endpoints").exchange().expectStatus().isUnauthorized();
    http.post()
        .uri("/v1/endpoints")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("url", URL, "event_types", List.of("*")))
        .exchange()
        .expectStatus()
        .isUnauthorized();
  }

  @Test
  void twentyEventTypesOfOneHundredTwentyEightCharactersAreAccepted() {
    TestTenant tenant = TestTenant.create(http, "largest-filter");
    List<String> eventTypes =
        Stream.iterate(0, n -> n + 1)
            .limit(20)
            .map(n -> "a".repeat(125) + ".%02d".formatted(n))
            .toList();

    UUID id = create(tenant, URL, eventTypes);

    read(tenant, id).expectBody().jsonPath("$.event_types").isEqualTo(eventTypes);
  }

  static Stream<List<String>> eventTypesOutsideTheLimits() {
    return Stream.of(
        List.of(),
        Collections.nCopies(21, "order.created"),
        List.of("a".repeat(129)),
        List.of(""),
        List.of("Order.Created"),
        List.of("order created"),
        List.of("order.*"),
        Arrays.asList("order.created", null));
  }

  @ParameterizedTest
  @MethodSource("eventTypesOutsideTheLimits")
  void eventTypesOutsideTheLimitsAreRejectedOnCreateAndOnPatch(List<String> eventTypes) {
    TestTenant tenant = TestTenant.create(http, "invalid-filter");
    UUID existing = create(tenant, URL, List.of("*"));

    expectInvalidEventTypes(register(tenant, URL, eventTypes));
    expectInvalidEventTypes(patch(tenant, existing, Map.of("event_types", eventTypes)));

    assertThat(tenant.endpointRows()).isEqualTo(1);
    read(tenant, existing).expectBody().jsonPath("$.event_types[0]").isEqualTo("*");
  }

  @Test
  void bodyWithoutEventTypesOrWithoutUrlIsRejected() {
    TestTenant tenant = TestTenant.create(http, "missing-members");
    UUID existing = create(tenant, URL, List.of("*"));

    expectInvalidEventTypes(post(tenant, Map.of("url", URL)));
    expectInvalidEventTypes(patch(tenant, existing, Map.of()));
    post(tenant, Map.of("event_types", List.of("*")))
        .expectStatus()
        .isBadRequest()
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo("url is required");

    assertThat(tenant.endpointRows()).isEqualTo(1);
  }

  // A registration ignores the extra member, so the name of that member is the only thing that
  // differs between the body that is accepted and the body that is refused. The name is made of
  // two-byte characters, so the refused one has 513 characters: the bytes are what is counted.
  @Test
  void memberNameAboveOneThousandTwentyFourBytesIsRejected() {
    TestTenant tenant = TestTenant.create(http, "member-name-limit");
    String name = "é".repeat(512);

    post(tenant, Map.of("url", URL, "event_types", List.of("*"), name, 1))
        .expectStatus()
        .isCreated();
    post(tenant, Map.of("url", URL, "event_types", List.of("*"), "k" + name, 1))
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON);

    assertThat(tenant.endpointRows()).isEqualTo(1);
  }

  // The host of each URL is public, so the form of the URL is the only reason to refuse it. The
  // first names a port that is allowed, so its scheme alone refuses it.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://hooks.endpoint-api.example:8443/hooks/1",
        "https://hooks.endpoint-api.example:80/hooks/1",
        "https://user:password@hooks.endpoint-api.example/hooks/1",
        "hooks.endpoint-api.example/hooks/1"
      })
  void urlThatBreaksARuleOfFormIsRefused(String url) {
    TestTenant tenant = TestTenant.create(http, "url-form");

    register(tenant, url, List.of("*"))
        .expectStatus()
        .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.type")
        .isEqualTo("endpoint_url_not_allowed");

    assertThat(tenant.endpointRows()).isZero();
  }

  private static void expectNotFound(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isNotFound()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo("No endpoint has this id")
        .jsonPath("$.type")
        .doesNotExist();
  }

  private static void expectInvalidEventTypes(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isBadRequest()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo(
            "event_types must have 1 to 20 entries, each '*' or 1 to 128 characters of"
                + " [a-z0-9_.-]");
  }

  private UUID create(TestTenant tenant, String url, List<String> eventTypes) {
    Map<String, String> created =
        register(tenant, url, eventTypes)
            .expectStatus()
            .isCreated()
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    return UUID.fromString(created.get("id"));
  }

  private static String secretOf(RestTestClient.ResponseSpec created) {
    return created
        .expectStatus()
        .isCreated()
        .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
        .returnResult()
        .getResponseBody()
        .get("secret");
  }

  private RestTestClient.ResponseSpec register(
      TestTenant tenant, String url, List<String> eventTypes) {
    return post(tenant, Map.of("url", url, "event_types", eventTypes));
  }

  private RestTestClient.ResponseSpec post(TestTenant tenant, Map<String, ?> body) {
    return http.post()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .exchange();
  }

  private RestTestClient.ResponseSpec read(TestTenant tenant, UUID id) {
    return http.get()
        .uri("/v1/endpoints/{id}", id)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  private RestTestClient.ResponseSpec list(TestTenant tenant) {
    return http.get()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange();
  }

  private RestTestClient.ResponseSpec patch(TestTenant tenant, UUID id, Map<String, ?> body) {
    return http.patch()
        .uri("/v1/endpoints/{id}", id)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .exchange();
  }

  /**
   * The secret of an endpoint and the one from before its last rotation, each in the form in which
   * a tenant is shown a secret. The second is null when the endpoint keeps none.
   */
  private List<String> storedSecretsOf(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql("SELECT secret, prev_secret FROM endpoint WHERE id = :id")
        .param("id", endpointId)
        .query(
            (row, n) ->
                Arrays.asList(shown(row.getBytes("secret")), shown(row.getBytes("prev_secret"))))
        .single();
  }

  private String shown(byte[] stored) {
    return stored == null
        ? null
        : "whsec_" + Base64.getEncoder().encodeToString(cipher.decrypt(stored));
  }

  /** Until when the secret from before the last rotation counts. Empty when none is kept. */
  private static Optional<Instant> previousSecretExpiresAt(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql("SELECT prev_secret_expires_at FROM endpoint WHERE id = :id")
        .param("id", endpointId)
        .query(
            (row, n) ->
                Optional.ofNullable(row.getObject(1, OffsetDateTime.class))
                    .map(OffsetDateTime::toInstant))
        .single();
  }

  private static Instant databaseClock() {
    return TestDatabase.jdbc().sql("SELECT now()").query(OffsetDateTime.class).single().toInstant();
  }

  private static String statusAndWhetherDeletedAtIsSet(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql("SELECT status || ', ' || (deleted_at IS NOT NULL) FROM endpoint WHERE id = :id")
        .param("id", endpointId)
        .query(String.class)
        .single();
  }

  private static void openCircuit(UUID endpointId, String nextProbeAt) {
    TestDatabase.jdbc()
        .sql(
            "UPDATE endpoint_circuit SET state = 'open', opened_at = now(), next_probe_at = "
                + nextProbeAt
                + " WHERE endpoint_id = :id")
        .param("id", endpointId)
        .update();
  }
}
