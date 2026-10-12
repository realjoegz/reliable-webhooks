package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGInterval;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.web.servlet.client.RestTestClient;

@UnsafeLocalReplica
class ClaimRaceIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int TENANTS = 5;
  private static final int ENDPOINTS_OF_A_TENANT = 10;
  private static final int EVENTS_OF_A_TENANT = 40;
  private static final int DELIVERIES = TENANTS * ENDPOINTS_OF_A_TENANT * EVENTS_OF_A_TENANT;

  @Autowired RestTestClient http;
  @Autowired PollerFleet fleet;
  @Autowired WebhooksProperties properties;

  // The claim runs on two connections that the test holds, with no worker running. The first
  // has claimed the delivery and has not committed. A second delivery for the same endpoint is
  // accepted then, and the second connection claims while the first row is locked. It returns,
  // which a claim that waited for the lock could not do before the commit: its lock timeout
  // turns a claim that waits into an error. And it returns the second delivery, so it saw the
  // rows that were due and passed over the locked one.
  //
  // The lock timeout is committed before anything else happens on the second connection. Its
  // transaction then begins with its claim, and now() in that claim is later than the time at
  // which the deliveries became due.
  @Test
  void secondClaimerSkipsLockedRow() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        Connection first = TestDatabase.connection();
        Connection second = TestDatabase.connection();
        Statement settings = second.createStatement()) {
      settings.execute("SET lock_timeout = '2s'");
      second.commit();
      TestTenant tenant = TestTenant.create(http, "claim-race");
      tenant.register(http, receiver.url("/hook"), "*");
      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));

      assertThat(claim(first)).containsExactly(delivery);
      UUID behindTheLockedRow = Rows.deliveryOf(tenant.accept(http, EVENT));
      assertThat(claim(second)).containsExactly(behindTheLockedRow);

      first.commit();
      second.commit();
      assertThat(claim(second)).isEmpty();
      second.commit();

      Rows.Delivery leasedOnce = new Rows.Delivery("pending", 1, 0, true, null, false);
      assertThat(Rows.delivery(delivery)).isEqualTo(leasedOnce);
      assertThat(Rows.delivery(behindTheLockedRow)).isEqualTo(leasedOnce);
      assertThat(receiver.requests()).isEmpty();
    }
  }

  // Four pollers, each with its own worker and replica id, on the one database. Every delivery
  // is due before the first of them starts.
  //
  // An event of a tenant goes to its ten endpoints and costs ten units, so the forty events cost
  // four times what a new tenant has. Each tenant is given a burst that pays for them, and its
  // bucket is aged until it is full.
  @Test
  void fourReplicasDeliverEachEventOnce() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      List<UUID> tenants = new ArrayList<>();
      for (int t = 0; t < TENANTS; t++) {
        TestTenant tenant = TestTenant.create(http, "four-replicas-" + t);
        tenant.patch(http, Map.of("burst", ENDPOINTS_OF_A_TENANT * EVENTS_OF_A_TENANT));
        Rows.ageBucket(tenant.id(), Duration.ofHours(1));
        tenants.add(tenant.id());
        for (int e = 0; e < ENDPOINTS_OF_A_TENANT; e++) {
          String path = "/tenant-%d/endpoint-%d".formatted(t, e);
          receiver.answer(path, FaultReceiver.status(200));
          tenant.register(http, receiver.url(path), "*");
        }
        for (int i = 0; i < EVENTS_OF_A_TENANT; i++) {
          tenant.accept(http, EVENT);
        }
      }
      List<UUID> queued = deliveriesOf(tenants);
      assertThat(queued).hasSize(DELIVERIES);

      try (PollerFleet.Running replicas =
          fleet.start("replica-1", "replica-2", "replica-3", "replica-4")) {
        await()
            .atMost(Duration.ofMinutes(2))
            .untilAsserted(() -> assertThat(succeeded(tenants)).isEqualTo(DELIVERIES));
      }

      assertThat(receiver.requests()).hasSize(DELIVERIES);
      assertThat(receiver.requests())
          .extracting(request -> UUID.fromString(request.header("webhook-id")))
          .containsExactlyInAnyOrderElementsOf(queued);
      assertThat(attemptsClaimed(tenants)).containsOnly(1);
      assertThat(attemptRows(tenants)).isEqualTo(DELIVERIES);
      assertThat(attemptRowsThatAreNotTheOneSuccess(tenants)).isZero();
      assertThat(replicasThatRecorded(tenants))
          .containsExactlyInAnyOrder("replica-1", "replica-2", "replica-3", "replica-4");
    }
  }

  private List<UUID> claim(Connection connection) {
    Duration lease = properties.delivery().lease();
    return JdbcClient.create(new SingleConnectionDataSource(connection, true))
        .sql(DeliveryRepository.CLAIM)
        .param("full_endpoints", new UUID[0])
        .param("full_tenants", new UUID[0])
        .param("free", properties.delivery().maxInFlight())
        .param("lease", new PGInterval(0, 0, 0, 0, 0, lease.toSeconds()))
        .query((row, n) -> row.getObject("id", UUID.class))
        .list();
  }

  private static List<UUID> deliveriesOf(List<UUID> tenants) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM delivery WHERE tenant_id = ANY(:tenants)")
        .param("tenants", tenants.toArray(UUID[]::new))
        .query(UUID.class)
        .list();
  }

  private static int succeeded(List<UUID> tenants) {
    return TestDatabase.jdbc()
        .sql(
            "SELECT count(*) FROM delivery WHERE tenant_id = ANY(:tenants) AND state = 'succeeded'")
        .param("tenants", tenants.toArray(UUID[]::new))
        .query(Integer.class)
        .single();
  }

  private static List<Integer> attemptsClaimed(List<UUID> tenants) {
    return TestDatabase.jdbc()
        .sql("SELECT DISTINCT attempts FROM delivery WHERE tenant_id = ANY(:tenants)")
        .param("tenants", tenants.toArray(UUID[]::new))
        .query(Integer.class)
        .list();
  }

  private static int attemptRows(List<UUID> tenants) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM delivery_attempt a JOIN delivery d ON d.id = a.delivery_id
            WHERE d.tenant_id = ANY(:tenants)
            """)
        .param("tenants", tenants.toArray(UUID[]::new))
        .query(Integer.class)
        .single();
  }

  private static int attemptRowsThatAreNotTheOneSuccess(List<UUID> tenants) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM delivery_attempt a JOIN delivery d ON d.id = a.delivery_id
            WHERE d.tenant_id = ANY(:tenants)
              AND NOT (a.run = 0 AND a.attempt_no = 1
                       AND a.outcome = 'success' AND a.http_status = 200)
            """)
        .param("tenants", tenants.toArray(UUID[]::new))
        .query(Integer.class)
        .single();
  }

  private static List<String> replicasThatRecorded(List<UUID> tenants) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT DISTINCT a.replica
            FROM delivery_attempt a JOIN delivery d ON d.id = a.delivery_id
            WHERE d.tenant_id = ANY(:tenants)
            """)
        .param("tenants", tenants.toArray(UUID[]::new))
        .query(String.class)
        .list();
  }
}
