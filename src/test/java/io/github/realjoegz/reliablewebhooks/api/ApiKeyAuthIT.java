package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class ApiKeyAuthIT {

  private record NewTenant(UUID id, String apiKey) {}

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @LocalServerPort int port;

  @Test
  void missingKeyIsRejected() {
    expectUnauthorized(http.get().uri("/v1/whoami").exchange());
  }

  // Each header carries a key that exists, so only its form can be the reason for the rejection.
  @ParameterizedTest
  @ValueSource(strings = {"%s", "Basic %s", "Bearer  %s", "Bearer %s=", "Bearer x%s"})
  void malformedKeyIsRejected(String authorization) {
    String apiKey = createTenant("malformed").apiKey();

    expectUnauthorized(whoAmI(authorization.formatted(apiKey)));
  }

  // A tenant exists, so a lookup that did not compare the key would find one.
  @Test
  void unknownKeyIsRejected() {
    createTenant("unknown");

    expectUnauthorized(whoAmI("Bearer rwh_" + "A".repeat(43)));
  }

  @Test
  void rejectionOfAnUnknownKeyIsNotCached() {
    String apiKey = "rwh_" + UUID.randomUUID().toString().replace("-", "") + "A".repeat(11);
    expectUnauthorized(whoAmI("Bearer " + apiKey));

    TestDatabase.jdbc()
        .sql(
            """
            WITH created AS (
              INSERT INTO tenant (name, api_key_hash)
              VALUES ('issued-later', sha256(convert_to(:key, 'UTF8')))
              RETURNING id, burst
            )
            INSERT INTO rate_bucket (tenant_id, tokens) SELECT id, burst FROM created
            """)
        .param("key", apiKey)
        .update();

    whoAmI("Bearer " + apiKey).expectStatus().isOk();
  }

  @Test
  void lookupIsCachedSoAChangeMadeElsewhereIsNotSeenAtOnce() {
    NewTenant tenant = createTenant("cached");
    whoAmI("Bearer " + tenant.apiKey()).expectStatus().isOk();

    TestDatabase.jdbc()
        .sql("UPDATE tenant SET status = 'suspended' WHERE id = :id")
        .param("id", tenant.id())
        .update();

    whoAmI("Bearer " + tenant.apiKey()).expectStatus().isOk();
  }

  @Test
  void validKeyReachesTheRouteAsItsTenant() {
    NewTenant tenant = createTenant("valid");

    whoAmI("Bearer " + tenant.apiKey())
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.id")
        .isEqualTo(tenant.id().toString())
        .jsonPath("$.status")
        .isEqualTo("active")
        .jsonPath("$.rate_per_sec")
        .isEqualTo(50.0)
        .jsonPath("$.burst")
        .isEqualTo(100);
  }

  @Test
  void pathWithoutARouteStillTakesAKey() {
    expectUnauthorized(http.get().uri("/no-such-route").exchange());

    http.get()
        .uri("/no-such-route")
        .headers(headers -> headers.setBearerAuth(createTenant("no-route").apiKey()))
        .exchange()
        .expectStatus()
        .isNotFound()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON);
  }

  @Test
  void authenticationSchemeIsCaseInsensitive() {
    whoAmI("bearer " + createTenant("scheme").apiKey()).expectStatus().isOk();
  }

  @Test
  void rotatedAwayKeyIsRejected() {
    NewTenant tenant = createTenant("rotated-away");
    whoAmI("Bearer " + tenant.apiKey()).expectStatus().isOk();

    String rotated =
        http.post()
            .uri("/admin/tenants/{id}/api-key/rotate", tenant.id())
            .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody()
            .get("api_key");

    expectUnauthorized(whoAmI("Bearer " + tenant.apiKey()));
    whoAmI("Bearer " + rotated).expectStatus().isOk();
  }

  @Test
  void suspendedTenantIsRejected() {
    NewTenant tenant = createTenant("suspended");
    TestDatabase.jdbc()
        .sql("UPDATE tenant SET status = 'suspended' WHERE id = :id")
        .param("id", tenant.id())
        .update();

    whoAmI("Bearer " + tenant.apiKey())
        .expectStatus()
        .isForbidden()
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.type")
        .isEqualTo("tenant_suspended");
  }

  @Test
  void adminRoutesRejectMissingOrWrongToken() {
    String name = "rejected-" + UUID.randomUUID();
    String tenantKey = createTenant("not-an-admin").apiKey();

    expectUnauthorized(createTenantAt("/admin/tenants", null, name));
    expectUnauthorized(
        createTenantAt("/admin/tenants", "Bearer wrong-" + ReplicaSettings.ADMIN_TOKEN, name));
    expectUnauthorized(createTenantAt("/admin/tenants", "Bearer " + tenantKey, name));

    assertThat(tenantsNamed(name)).isZero();
  }

  // Spring MVC routes both spellings to the admin controller, which the last call shows.
  @ParameterizedTest
  @ValueSource(strings = {"/%61dmin/tenants", "/admin;x=1/tenants"})
  void adminRouteSpelledAnotherWayStillTakesOnlyTheAdminToken(String path) {
    String name = "respelled-" + UUID.randomUUID();
    String tenantKey = createTenant("respelling").apiKey();

    expectUnauthorized(createTenantAt(path, "Bearer " + tenantKey, name));
    assertThat(tenantsNamed(name)).isZero();

    createTenantAt(path, "Bearer " + ReplicaSettings.ADMIN_TOKEN, name).expectStatus().isCreated();
  }

  // A replica is live while its poller loops, and the replica of this class leaves its worker to
  // the tests. The probe answers UP once the worker runs and its poller has been through its loop.
  @ParameterizedTest
  @ValueSource(strings = {"/actuator/health/liveness", "/actuator/health/readiness"})
  void healthProbesNeedNoKey(String probe) {
    worker.start();

    await()
        .untilAsserted(
            () ->
                http.get()
                    .uri(probe)
                    .exchange()
                    .expectStatus()
                    .isOk()
                    .expectBody()
                    .jsonPath("$.status")
                    .isEqualTo("UP"));
  }

  // Every rejected credential gets this one answer, so the answer cannot tell which check failed.
  private static void expectUnauthorized(RestTestClient.ResponseSpec response) {
    response
        .expectStatus()
        .isUnauthorized()
        .expectHeader()
        .valueEquals(HttpHeaders.WWW_AUTHENTICATE, "Bearer")
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.status")
        .isEqualTo(401)
        .jsonPath("$.detail")
        .isEqualTo("A valid bearer token is required.")
        .jsonPath("$.type")
        .doesNotExist();
  }

  private RestTestClient.ResponseSpec whoAmI(String authorization) {
    return http.get().uri("/v1/whoami").header(HttpHeaders.AUTHORIZATION, authorization).exchange();
  }

  // The path goes out exactly as written. A URI template would encode the percent sign again.
  private RestTestClient.ResponseSpec createTenantAt(
      String path, String authorization, String name) {
    RestTestClient.RequestBodySpec request =
        http.post()
            .uri(URI.create("http://localhost:" + port + path))
            .contentType(MediaType.APPLICATION_JSON);
    if (authorization != null) {
      request = request.header(HttpHeaders.AUTHORIZATION, authorization);
    }
    return request.body(Map.of("name", name)).exchange();
  }

  private NewTenant createTenant(String name) {
    Map<String, String> created =
        createTenantAt("/admin/tenants", "Bearer " + ReplicaSettings.ADMIN_TOKEN, name)
            .expectStatus()
            .isCreated()
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    return new NewTenant(UUID.fromString(created.get("id")), created.get("api_key"));
  }

  private static int tenantsNamed(String name) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM tenant WHERE name = :name")
        .param("name", name)
        .query(Integer.class)
        .single();
  }
}
