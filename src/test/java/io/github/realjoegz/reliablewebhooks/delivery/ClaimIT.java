package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * The claim statement by itself. The worker of the replica does not run in these tests and every
 * test starts with an empty queue, so a claim finds the deliveries of its test and no others, and
 * what it returns is what the statement picked.
 */
@InProcessReplica
class ClaimIT {

  private static final String HOST = "hooks.claim.example";
  private static final String HOOK = "https://" + HOST + "/hook";
  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final UUID[] NONE = {};
  private static final int ROOM_FOR_ALL = 64;

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired DeliveryRepository deliveries;
  @Autowired WebhooksProperties properties;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  // Six endpoints with two due deliveries each. A claim with room for all twelve takes four, each
  // for another endpoint and each the older one of its endpoint.
  @Test
  void claimTakesOneRowForAnEndpointAndFourForATenant() {
    TestTenant tenant = TestTenant.create(http, "one-per-endpoint");
    for (int i = 0; i < 6; i++) {
      tenant.register(http, HOOK, "*");
    }
    UUID older = tenant.accept(http, EVENT);
    UUID newer = tenant.accept(http, EVENT);

    List<Claimed> first = claim(NONE, NONE, ROOM_FOR_ALL);

    assertThat(first).hasSize(4);
    assertThat(first).extracting(Claimed::endpointId).doesNotHaveDuplicates();
    assertThat(first)
        .allSatisfy(
            claimed -> {
              assertThat(claimed.tenantId()).isEqualTo(tenant.id());
              assertThat(claimed.eventId()).isEqualTo(older);
              assertThat(claimed.attempts()).isEqualTo(1);
              assertThat(claimed.replays()).isZero();
              assertThat(claimed.abandoned()).isFalse();
              assertThat(claimed.leaseToken()).isNotNull();
            });
    assertThat(first).extracting(Claimed::leaseToken).doesNotHaveDuplicates();

    // The two endpoints that the first claim left out have the oldest heads now.
    List<Claimed> second = claim(NONE, NONE, ROOM_FOR_ALL);

    assertThat(second).hasSize(4);
    assertThat(second).extracting(Claimed::endpointId).doesNotHaveDuplicates();
    assertThat(second)
        .extracting(Claimed::eventId)
        .containsExactlyInAnyOrder(older, older, newer, newer);
    assertThat(second)
        .filteredOn(claimed -> claimed.eventId().equals(older))
        .extracting(Claimed::endpointId)
        .doesNotContainAnyElementsOf(first.stream().map(Claimed::endpointId).toList());
  }

  // The four deliveries of the first tenant are older than the one of the second. A claim with
  // room for two takes one of each tenant. By age alone it would take two of the first.
  @Test
  void everyTenantGetsItsFirstRowBeforeAnyTenantGetsASecond() {
    TestTenant backlog = TestTenant.create(http, "fairness-backlog");
    for (int i = 0; i < 4; i++) {
      backlog.register(http, HOOK, "*");
    }
    TestTenant latecomer = TestTenant.create(http, "fairness-latecomer");
    latecomer.register(http, HOOK, "*");
    backlog.accept(http, EVENT);
    UUID late = Rows.deliveryOf(latecomer.accept(http, EVENT));

    List<Claimed> claimed = claim(NONE, NONE, 2);

    assertThat(claimed)
        .extracting(Claimed::tenantId)
        .containsExactlyInAnyOrder(backlog.id(), latecomer.id());
    assertThat(claimed).extracting(Claimed::id).contains(late);

    // With the first rows gone, the room goes to the tenant that has rows left.
    assertThat(claim(NONE, NONE, 2))
        .extracting(Claimed::tenantId)
        .containsExactly(backlog.id(), backlog.id());
  }

  // The second claim is the witness that the rows the first one left were there to be claimed.
  @Test
  void claimLeavesOutTheEndpointsAndTenantsThatAreFull() {
    TestTenant tenant = TestTenant.create(http, "bulkhead-lists");
    UUID full = tenant.register(http, HOOK, "*");
    UUID free = tenant.register(http, HOOK, "*");
    TestTenant fullTenant = TestTenant.create(http, "bulkhead-lists-full-tenant");
    UUID ofFullTenant = fullTenant.register(http, HOOK, "*");
    tenant.accept(http, EVENT);
    fullTenant.accept(http, EVENT);

    List<Claimed> withLists = claim(new UUID[] {full}, new UUID[] {fullTenant.id()}, ROOM_FOR_ALL);

    assertThat(withLists).extracting(Claimed::endpointId).containsExactly(free);
    assertThat(claim(NONE, NONE, ROOM_FOR_ALL))
        .extracting(Claimed::endpointId)
        .containsExactlyInAnyOrder(full, ofFullTenant);
  }

  // Nothing in the service suspends a tenant or opens a circuit yet, so the test writes both
  // states. The first claim takes the delivery of the third tenant only, and the held deliveries
  // are as they were. Once the tenant is active again and the probe time of the circuit has
  // passed, a claim takes both.
  @Test
  void claimHoldsTheDeliveriesOfASuspendedTenantAndOfAnOpenCircuit() {
    TestTenant suspended = TestTenant.create(http, "held-suspended");
    suspended.register(http, HOOK, "*");
    TestTenant behindOpenCircuit = TestTenant.create(http, "held-open-circuit");
    UUID openEndpoint = behindOpenCircuit.register(http, HOOK, "*");
    TestTenant healthy = TestTenant.create(http, "held-healthy");
    healthy.register(http, HOOK, "*");
    UUID ofSuspended = Rows.deliveryOf(suspended.accept(http, EVENT));
    UUID ofOpenCircuit = Rows.deliveryOf(behindOpenCircuit.accept(http, EVENT));
    UUID ofHealthy = Rows.deliveryOf(healthy.accept(http, EVENT));
    setStatus(suspended, "suspended");
    openCircuit(openEndpoint, "now() + interval '30 seconds'");

    assertThat(claim(NONE, NONE, ROOM_FOR_ALL)).extracting(Claimed::id).containsExactly(ofHealthy);
    Rows.Delivery untouched = new Rows.Delivery("pending", 0, 0, false, null, false);
    assertThat(Rows.delivery(ofSuspended)).isEqualTo(untouched);
    assertThat(Rows.delivery(ofOpenCircuit)).isEqualTo(untouched);

    setStatus(suspended, "active");
    openCircuit(openEndpoint, "now() - interval '1 second'");

    assertThat(claim(NONE, NONE, ROOM_FOR_ALL))
        .extracting(Claimed::id)
        .containsExactlyInAnyOrder(ofSuspended, ofOpenCircuit);
  }

  @Test
  void claimTakesNoDeliveryThatIsNotDueOrNotPending() {
    TestTenant tenant = TestTenant.create(http, "not-due");
    tenant.register(http, HOOK, "order.created");
    UUID scheduled = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID succeeded = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID due = Rows.deliveryOf(tenant.accept(http, EVENT));
    TestDatabase.jdbc()
        .sql("UPDATE delivery SET next_attempt_at = now() + interval '1 minute' WHERE id = :id")
        .param("id", scheduled)
        .update();
    TestDatabase.jdbc()
        .sql("UPDATE delivery SET state = 'succeeded', completed_at = now() WHERE id = :id")
        .param("id", succeeded)
        .update();

    assertThat(claim(NONE, NONE, ROOM_FOR_ALL)).extracting(Claimed::id).containsExactly(due);
    assertThat(claim(NONE, NONE, ROOM_FOR_ALL)).isEmpty();
  }

  // A leased delivery is not due, so no claim takes it. When the lease has run out, the next
  // claim takes it again and writes the attempt row that its first holder did not.
  @Test
  void claimLeasesForSixtySecondsAndTakesAnAbandonedDeliveryAgain() {
    TestTenant tenant = TestTenant.create(http, "lease");
    tenant.register(http, HOOK, "*");
    UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));

    List<Claimed> first = claim(NONE, NONE, ROOM_FOR_ALL);

    assertThat(first).extracting(Claimed::id).containsExactly(delivery);
    assertThat(Rows.delivery(delivery))
        .isEqualTo(new Rows.Delivery("pending", 1, 0, true, null, false));
    assertThat(dueWithin(delivery, "50 seconds", "60 seconds")).isTrue();
    assertThat(Rows.attempts(delivery)).isEmpty();
    assertThat(claim(NONE, NONE, ROOM_FOR_ALL)).isEmpty();

    Rows.ageDelivery(delivery, Duration.ofSeconds(61));
    List<Claimed> second = claim(NONE, NONE, ROOM_FOR_ALL);

    assertThat(second).hasSize(1);
    Claimed reclaimed = second.getFirst();
    assertThat(reclaimed.id()).isEqualTo(delivery);
    assertThat(reclaimed.attempts()).isEqualTo(2);
    assertThat(reclaimed.abandoned()).isTrue();
    assertThat(reclaimed.leaseToken()).isNotEqualTo(first.getFirst().leaseToken());
    assertThat(Rows.attempts(delivery))
        .containsExactly(
            new Rows.Attempt(0, 1, false, "unknown", null, "lease_expired", null, null));
  }

  // The lease and the attempt row of the holder that recorded nothing are one transaction. The
  // test has put a row where that attempt row belongs, so the insert fails after the delivery
  // was leased. The delivery is then as it was, with one attempt and the token of its first
  // holder, and not leased a second time without the row.
  @Test
  void claimWhoseAbandonedAttemptRowIsRefusedLeasesNothing() {
    TestTenant tenant = TestTenant.create(http, "refused-abandoned-row");
    tenant.register(http, HOOK, "*");
    UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
    assertThat(claim(NONE, NONE, ROOM_FOR_ALL)).extracting(Claimed::id).containsExactly(delivery);
    Rows.ageDelivery(delivery, Duration.ofSeconds(61));
    TestDatabase.jdbc()
        .sql(
            """
            INSERT INTO delivery_attempt (delivery_id, run, attempt_no, outcome)
            VALUES (:id, 0, 1, 'unknown')
            """)
        .param("id", delivery)
        .update();
    String abandoned = rowOf(delivery);

    assertThatThrownBy(() -> claim(NONE, NONE, ROOM_FOR_ALL))
        .isInstanceOf(DuplicateKeyException.class);

    assertThat(rowOf(delivery)).isEqualTo(abandoned);
    assertThat(Rows.delivery(delivery))
        .isEqualTo(new Rows.Delivery("pending", 1, 0, true, null, false));
  }

  @Test
  void releasedDeliveryIsDueAgainAfterItsDelayAndHasUsedNoAttempt() {
    TestTenant tenant = TestTenant.create(http, "release");
    tenant.register(http, HOOK, "*");
    UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
    Claimed claimed = claim(NONE, NONE, ROOM_FOR_ALL).getFirst();

    deliveries.release(claimed, Duration.ofSeconds(30));

    assertThat(Rows.delivery(delivery))
        .isEqualTo(new Rows.Delivery("pending", 0, 0, false, null, false));
    assertThat(dueWithin(delivery, "20 seconds", "30 seconds")).isTrue();
    assertThat(Rows.attempts(delivery)).isEmpty();
    assertThat(claim(NONE, NONE, ROOM_FOR_ALL)).isEmpty();

    Rows.ageDelivery(delivery, Duration.ofSeconds(31));
    List<Claimed> again = claim(NONE, NONE, ROOM_FOR_ALL);

    assertThat(again).hasSize(1);
    assertThat(again.getFirst().attempts()).isEqualTo(1);
    assertThat(again.getFirst().abandoned()).isFalse();
    assertThat(Rows.attempts(delivery)).isEmpty();
  }

  private List<Claimed> claim(UUID[] fullEndpoints, UUID[] fullTenants, int free) {
    return deliveries.claim(fullEndpoints, fullTenants, free, properties.delivery().lease());
  }

  /** The delivery row as the text of all of its columns. */
  private static String rowOf(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql("SELECT d::text FROM delivery d WHERE d.id = :id")
        .param("id", deliveryId)
        .query(String.class)
        .single();
  }

  // Whether the delivery becomes due after more than the one interval and at most the other,
  // counted on the database clock from now. The first leaves the test ten seconds to ask.
  private static boolean dueWithin(UUID deliveryId, String after, String atMost) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT next_attempt_at > now() + :after::interval
               AND next_attempt_at <= now() + :at_most::interval
            FROM delivery WHERE id = :id
            """)
        .param("after", after)
        .param("at_most", atMost)
        .param("id", deliveryId)
        .query(Boolean.class)
        .single();
  }

  private static void setStatus(TestTenant tenant, String status) {
    TestDatabase.jdbc()
        .sql("UPDATE tenant SET status = :status WHERE id = :id")
        .param("status", status)
        .param("id", tenant.id())
        .update();
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
