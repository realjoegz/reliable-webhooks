package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class AdminApiIT {

  private static final String API_KEY_FORMAT = "rwh_[A-Za-z0-9_-]{43}";

  private record Bucket(int burst, double tokens) {}

  @Autowired RestTestClient http;

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

    try (Connection blocker =
            DriverManager.getConnection(
                TestDatabase.jdbcUrl(), TestDatabase.username(), TestDatabase.password());
        Statement statement = blocker.createStatement()) {
      blocker.setAutoCommit(false);
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
