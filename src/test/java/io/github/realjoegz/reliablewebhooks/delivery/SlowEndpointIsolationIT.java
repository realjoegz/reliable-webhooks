package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * One endpoint that is slow to answer, or whose host is slow to resolve, against the bounds of a
 * replica: four attempts in flight for an endpoint, fifteen seconds for an attempt, two seconds for
 * a lookup and four lookups for a tenant. The replica runs with the timing that ships, so the test
 * of the deadline takes fifteen seconds and the two of the lookup take two.
 */
@UnsafeLocalReplica
class SlowEndpointIsolationIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String FOR_THE_FAULTY_ENDPOINT =
      """
      {"type":"faulty.sent","data":{}}""";
  private static final String FOR_THE_SIBLING =
      """
      {"type":"sibling.sent","data":{}}""";
  private static final int MAX_IN_FLIGHT_FOR_AN_ENDPOINT = 4;
  private static final int LOOKUP_SLOTS_OF_A_TENANT = 4;
  private static final Duration ATTEMPT_DEADLINE = Duration.ofSeconds(15);
  private static final Duration LOOKUP_WAIT = Duration.ofSeconds(2);
  private static final Duration MARGIN = Duration.ofSeconds(5);

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired StubDns dns;

  // Six deliveries are due for an endpoint whose requests the receiver holds. Four of them are in
  // flight, which is all that an endpoint may have. Two events are accepted then, one for another
  // endpoint of the tenant and one of another tenant, and both are delivered while the four are
  // still held. They are the witness that the replica went on claiming: the two deliveries that
  // are left for the slow endpoint were due in those claims, and were passed over. Their due time
  // is still the one that ingest gave them, so no claim took one of them and gave it back.
  //
  // A held request is cut at the deadline of an attempt, 15 s after it began. A run that took
  // that long to get here would fail: the receiver would count the requests of the two that are
  // left.
  @Test
  void slowEndpointDoesNotDelayOthers() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer("/slow", hold);
      receiver.answer("/sibling", FaultReceiver.status(200));
      receiver.answer("/other-tenant", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "slow-endpoint");
      tenant.register(http, receiver.url("/slow"), "faulty.sent");
      tenant.register(http, receiver.url("/sibling"), "sibling.sent");
      TestTenant other = TestTenant.create(http, "beside-slow-endpoint");
      other.register(http, receiver.url("/other-tenant"), "*");
      List<UUID> forTheSlowEndpoint = new ArrayList<>();
      for (int i = 0; i < MAX_IN_FLIGHT_FOR_AN_ENDPOINT + 2; i++) {
        forTheSlowEndpoint.add(Rows.deliveryOf(tenant.accept(http, FOR_THE_FAULTY_ENDPOINT)));
      }
      worker.start();
      await()
          .untilAsserted(() -> assertThat(hold.waiting()).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT));

      UUID forTheSibling = Rows.deliveryOf(tenant.accept(http, FOR_THE_SIBLING));
      UUID ofTheOtherTenant = Rows.deliveryOf(other.accept(http, EVENT));
      await()
          .untilAsserted(
              () -> {
                assertThat(Rows.delivery(forTheSibling).state()).isEqualTo("succeeded");
                assertThat(Rows.delivery(ofTheOtherTenant).state()).isEqualTo("succeeded");
              });

      assertThat(hold.waiting()).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      assertThat(receiver.requests("/slow")).hasSize(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      assertThat(receiver.peakConcurrency("/slow")).isEqualTo(MAX_IN_FLIGHT_FOR_AN_ENDPOINT);
      for (UUID inFlight : forTheSlowEndpoint.subList(0, MAX_IN_FLIGHT_FOR_AN_ENDPOINT)) {
        assertThat(Rows.delivery(inFlight))
            .isEqualTo(new Rows.Delivery("pending", 1, 0, true, null, false));
        assertThat(Rows.attempts(inFlight)).isEmpty();
      }
      for (UUID passedOver :
          forTheSlowEndpoint.subList(MAX_IN_FLIGHT_FOR_AN_ENDPOINT, forTheSlowEndpoint.size())) {
        assertThat(Rows.delivery(passedOver))
            .isEqualTo(new Rows.Delivery("pending", 0, 0, false, null, false));
        assertThat(wasGivenBack(passedOver)).isFalse();
      }
    }
  }

  // A byte of the response arrives every second, and the status line alone has more bytes than
  // the deadline has seconds. No wait between two bytes comes near a timeout, so only a bound on
  // the whole request can end it, and it ends at that bound: the attempt took fifteen seconds
  // and got no status.
  @Test
  void drippingResponseIsCutAtDeadline() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/drip", FaultReceiver.drip(Duration.ofSeconds(1)));
      TestTenant tenant = TestTenant.create(http, "dripping-response");
      tenant.register(http, receiver.url("/drip"), "*");
      worker.start();

      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(1));

      assertThat(Rows.attempts(delivery))
          .containsExactly(Rows.Attempt.unanswered(1, "deadline", "127.0.0.1"));
      assertThat(durationOfTheAttempt(delivery))
          .isBetween(ATTEMPT_DEADLINE, ATTEMPT_DEADLINE.plus(MARGIN));
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 1, 0, false, null, false));
    }
  }

  // The resolver never answers for the host of two endpoints of one tenant, which have six
  // deliveries between them. Neither endpoint is full with its three, so nothing but the lookup
  // slots of the tenant stands between the six and the resolver. Four lookups are stuck, and the
  // delivery of another tenant, accepted then, succeeds.
  //
  // The four attempts whose lookups were admitted fail after the two seconds of the lookup wait,
  // long before the deadline. The other two deliveries were given back: they have used no
  // attempt and are due later than when they were created, which only a release leaves behind.
  // That is the witness that they were claimed and found no slot.
  @Test
  void slowDnsDoesNotStarveOthers() throws Exception {
    String stuckHost = hostOfThisTest();
    dns.answer(stuckHost, "127.0.0.1");
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/other-tenant", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "stuck-dns");
      tenant.register(http, url(stuckHost, receiver, "/first"), "*");
      tenant.register(http, url(stuckHost, receiver, "/second"), "*");
      TestTenant other = TestTenant.create(http, "beside-stuck-dns");
      other.register(http, receiver.url("/other-tenant"), "*");
      for (int i = 0; i < 3; i++) {
        tenant.accept(http, EVENT);
      }
      List<UUID> deliveries = deliveriesOf(tenant);
      assertThat(deliveries).hasSize(LOOKUP_SLOTS_OF_A_TENANT + 2);

      try (StubDns.Hold hold = dns.hold(stuckHost)) {
        worker.start();
        await().untilAsserted(() -> assertThat(hold.waiting()).isEqualTo(LOOKUP_SLOTS_OF_A_TENANT));

        UUID ofTheOtherTenant = Rows.deliveryOf(other.accept(http, EVENT));
        await()
            .untilAsserted(
                () -> assertThat(Rows.delivery(ofTheOtherTenant).state()).isEqualTo("succeeded"));

        await().until(() -> attempted(deliveries).size() == LOOKUP_SLOTS_OF_A_TENANT);
        List<UUID> admitted = attempted(deliveries);
        for (UUID timedOut : admitted) {
          assertThat(Rows.attempts(timedOut))
              .containsExactly(Rows.Attempt.unanswered(1, "timeout", null));
          assertThat(durationOfTheAttempt(timedOut))
              .isBetween(LOOKUP_WAIT, LOOKUP_WAIT.plus(MARGIN));
        }
        for (UUID delivery : deliveries) {
          if (!admitted.contains(delivery)) {
            await().until(() -> wasGivenBack(delivery));
            assertThat(Rows.attempts(delivery)).isEmpty();
          }
        }
        assertThat(hold.waiting()).isEqualTo(LOOKUP_SLOTS_OF_A_TENANT);
        assertThat(receiver.requests())
            .extracting(FaultReceiver.Request::path)
            .containsExactly("/other-tenant");
      }
    }
  }

  // One endpoint of a tenant has a host that the resolver never answers for, and its four
  // deliveries take the four lookup slots of the tenant. The delivery for the sibling endpoint,
  // whose host resolves at once, finds no slot. It is given back, which the later due time
  // shows, and has used no attempt. The four attempts of the stuck endpoint fail meanwhile.
  //
  // Those four failures count toward opening the circuit of the stuck endpoint. The circuit of
  // the sibling has counted nothing: a delivery that is given back is no failure of its endpoint.
  //
  // When the resolver answers again, the delivery for the sibling succeeds, and that attempt is
  // its first. An attempt that the wait had cost it would show as a second one here.
  @Test
  void stuckDnsOfOneEndpointDoesNotFailSiblingEndpoints() throws Exception {
    String stuckHost = hostOfThisTest();
    dns.answer(stuckHost, "127.0.0.1");
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer("/stuck", FaultReceiver.status(200));
      receiver.answer("/sibling", FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "stuck-dns-with-sibling");
      UUID stuck = tenant.register(http, url(stuckHost, receiver, "/stuck"), "faulty.sent");
      UUID sibling = tenant.register(http, receiver.url("/sibling"), "sibling.sent");
      List<UUID> forTheStuckEndpoint = new ArrayList<>();
      for (int i = 0; i < LOOKUP_SLOTS_OF_A_TENANT; i++) {
        forTheStuckEndpoint.add(Rows.deliveryOf(tenant.accept(http, FOR_THE_FAULTY_ENDPOINT)));
      }

      UUID forTheSibling;
      try (StubDns.Hold hold = dns.hold(stuckHost)) {
        worker.start();
        await().untilAsserted(() -> assertThat(hold.waiting()).isEqualTo(LOOKUP_SLOTS_OF_A_TENANT));

        forTheSibling = Rows.deliveryOf(tenant.accept(http, FOR_THE_SIBLING));
        await().until(() -> wasGivenBack(forTheSibling));
        for (UUID timedOut : forTheStuckEndpoint) {
          await()
              .untilAsserted(
                  () ->
                      assertThat(Rows.attempts(timedOut))
                          .containsExactly(Rows.Attempt.unanswered(1, "timeout", null)));
        }

        assertThat(Rows.attempts(forTheSibling)).isEmpty();
        assertThat(Rows.circuit(stuck))
            .isEqualTo(new Rows.Circuit("closed", LOOKUP_SLOTS_OF_A_TENANT, 0, false, false));
        assertThat(Rows.circuit(sibling)).isEqualTo(Rows.Circuit.CLOSED);
        assertThat(receiver.requests()).isEmpty();
        assertThat(hold.waiting()).isEqualTo(LOOKUP_SLOTS_OF_A_TENANT);
      }

      await()
          .untilAsserted(
              () -> assertThat(Rows.delivery(forTheSibling).state()).isEqualTo("succeeded"));
      assertThat(Rows.delivery(forTheSibling))
          .isEqualTo(new Rows.Delivery("succeeded", 1, 0, false, null, true));
      assertThat(Rows.attempts(forTheSibling)).containsExactly(Rows.Attempt.answered(1, 200));
      assertThat(Rows.circuit(sibling)).isEqualTo(Rows.Circuit.CLOSED);
    }
  }

  // A name of its own for every test, because the replica and its resolver are shared.
  private static String hostOfThisTest() {
    return "stuck-%s.slow-dns.example".formatted(UUID.randomUUID());
  }

  private static String url(String host, FaultReceiver receiver, String path) {
    return "http://%s:%d%s".formatted(host, receiver.port(), path);
  }

  private static List<UUID> deliveriesOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM delivery WHERE tenant_id = :t")
        .param("t", tenant.id())
        .query(UUID.class)
        .list();
  }

  /** The deliveries that have an attempt row. */
  private static List<UUID> attempted(List<UUID> deliveries) {
    return deliveries.stream().filter(delivery -> !Rows.attempts(delivery).isEmpty()).toList();
  }

  /** How long the one attempt of the delivery took, as the replica measured and recorded it. */
  private static Duration durationOfTheAttempt(UUID deliveryId) {
    return Duration.ofMillis(
        TestDatabase.jdbc()
            .sql("SELECT duration_ms FROM delivery_attempt WHERE delivery_id = :id")
            .param("id", deliveryId)
            .query(Long.class)
            .single());
  }

  // Ingest makes a delivery due at the time it is created, and a claim counts an attempt. Only a
  // release leaves a delivery without an attempt and due later than that.
  private static boolean wasGivenBack(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT attempts = 0 AND lease_token IS NULL AND next_attempt_at > created_at
            FROM delivery WHERE id = :id
            """)
        .param("id", deliveryId)
        .query(Boolean.class)
        .single();
  }
}
