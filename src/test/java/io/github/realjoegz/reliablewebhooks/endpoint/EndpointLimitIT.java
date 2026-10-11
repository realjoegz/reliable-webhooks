package io.github.realjoegz.reliablewebhooks.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@InProcessReplica
class EndpointLimitIT {

  private static final String HOST = "hooks.endpoint-limit.example";
  private static final int CONCURRENT_CREATES = 4;

  @Autowired RestTestClient http;
  @Autowired StubDns dns;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  // The test holds the lock on the tenant row that every create takes first, and lets go only
  // when all four creates are seen waiting for it. Each of them is then inside its transaction
  // and has not counted yet, with room for two. A create that did not take the lock would never
  // be seen waiting.
  @Test
  void concurrentCreatesCannotExceedCap() throws Exception {
    TestTenant tenant = TestTenant.create(http, "concurrent-creates");
    setCap(tenant, 3);
    assertThat(create(tenant)).isEqualTo(HttpStatus.CREATED);

    List<HttpStatusCode> answers = new ArrayList<>();
    try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<HttpStatusCode>> creates = new ArrayList<>();
      try (Connection blocker = TestDatabase.connection();
          PreparedStatement lock =
              blocker.prepareStatement("SELECT 1 FROM tenant WHERE id = ? FOR NO KEY UPDATE")) {
        lock.setObject(1, tenant.id());
        lock.execute();

        for (int i = 0; i < CONCURRENT_CREATES; i++) {
          creates.add(callers.submit(() -> create(tenant)));
        }
        await()
            .pollInterval(Duration.ofMillis(10))
            .until(() -> createsWaitingForATenantRow() == CONCURRENT_CREATES);

        blocker.rollback();
      }
      for (Future<HttpStatusCode> create : creates) {
        answers.add(create.get());
      }
    }

    assertThat(answers)
        .containsExactlyInAnyOrder(
            HttpStatus.CREATED, HttpStatus.CREATED, HttpStatus.CONFLICT, HttpStatus.CONFLICT);
    assertThat(activeEndpointsOf(tenant)).isEqualTo(3);
    assertThat(circuitRowsOf(tenant)).isEqualTo(3);
  }

  @Test
  void tenantWithTheDefaultCapHasRoomForTwentyEndpoints() {
    TestTenant tenant = TestTenant.create(http, "default-cap");

    for (int i = 0; i < 20; i++) {
      assertThat(create(tenant)).as("endpoint %d", i + 1).isEqualTo(HttpStatus.CREATED);
    }

    register(tenant)
        .expectStatus()
        .isEqualTo(HttpStatus.CONFLICT)
        .expectHeader()
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .expectBody()
        .jsonPath("$.detail")
        .isEqualTo("The tenant has reached its endpoint limit")
        .jsonPath("$.type")
        .doesNotExist();
    assertThat(activeEndpointsOf(tenant)).isEqualTo(20);
  }

  // No route deletes an endpoint yet, so the test sets the status that a delete will set.
  @Test
  void deletedEndpointDoesNotCountTowardsTheCap() {
    TestTenant tenant = TestTenant.create(http, "cap-after-delete");
    setCap(tenant, 1);
    assertThat(create(tenant)).isEqualTo(HttpStatus.CREATED);
    assertThat(create(tenant)).isEqualTo(HttpStatus.CONFLICT);

    TestDatabase.jdbc()
        .sql("UPDATE endpoint SET status = 'deleted', deleted_at = now() WHERE tenant_id = :t")
        .param("t", tenant.id())
        .update();

    assertThat(create(tenant)).isEqualTo(HttpStatus.CREATED);
    assertThat(create(tenant)).isEqualTo(HttpStatus.CONFLICT);
  }

  @Test
  void capOfOneTenantDoesNotCountTheEndpointsOfAnother() {
    TestTenant full = TestTenant.create(http, "cap-reached");
    TestTenant other = TestTenant.create(http, "cap-not-reached");
    setCap(full, 1);
    setCap(other, 1);

    assertThat(create(full)).isEqualTo(HttpStatus.CREATED);

    assertThat(create(other)).isEqualTo(HttpStatus.CREATED);
  }

  private HttpStatusCode create(TestTenant tenant) {
    return register(tenant).returnResult().getStatus();
  }

  private RestTestClient.ResponseSpec register(TestTenant tenant) {
    return http.post()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("url", "https://" + HOST + "/hook", "event_types", List.of("*")))
        .exchange();
  }

  // The admin route that sets the cap does not exist yet.
  private static void setCap(TestTenant tenant, int maxEndpoints) {
    TestDatabase.jdbc()
        .sql("UPDATE tenant SET max_endpoints = :cap WHERE id = :id")
        .param("cap", maxEndpoints)
        .param("id", tenant.id())
        .update();
  }

  private static int createsWaitingForATenantRow() {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM pg_stat_activity
            WHERE wait_event_type = 'Lock' AND query LIKE '%FROM tenant%FOR NO KEY UPDATE%'
            """)
        .query(Integer.class)
        .single();
  }

  private static int activeEndpointsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM endpoint WHERE tenant_id = :t AND status = 'active'")
        .param("t", tenant.id())
        .query(Integer.class)
        .single();
  }

  private static int circuitRowsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM endpoint_circuit c
            JOIN endpoint e ON e.id = c.endpoint_id
            WHERE e.tenant_id = :t
            """)
        .param("t", tenant.id())
        .query(Integer.class)
        .single();
  }
}
