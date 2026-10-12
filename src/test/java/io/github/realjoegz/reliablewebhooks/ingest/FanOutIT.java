package io.github.realjoegz.reliablewebhooks.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.client.ExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.json.JsonMapper;

@InProcessReplica
class FanOutIT {

  private static final String HOST = "hooks.fan-out.example";
  private static final String HOOK = "https://" + HOST + "/hook";
  private static final String ORDER_CREATED =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String ORDER_SHIPPED =
      """
      {"type":"order.shipped","data":{"order":17}}""";

  /**
   * A delivery row next to the row of its event. The three time columns of a delivery are compared
   * in the database with {@code created_at} of the event, which is the {@code now()} of the
   * transaction that inserted the event.
   */
  private record Delivery(
      UUID endpointId,
      UUID tenantId,
      String state,
      int attempts,
      int replays,
      boolean leased,
      boolean completed,
      boolean createdWithItsEvent,
      boolean dueWhenCreated,
      boolean expiresSeventyTwoHoursLater) {

    static Delivery pendingFor(UUID endpointId, TestTenant tenant) {
      return new Delivery(endpointId, tenant.id(), "pending", 0, 0, false, false, true, true, true);
    }
  }

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired JsonMapper json;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  @Test
  void eachMatchingActiveEndpointGetsOneDelivery() {
    TestTenant tenant = TestTenant.create(http, "fan-out");
    TestTenant other = TestTenant.create(http, "fan-out-other-tenant");
    UUID exact = tenant.register(http, HOOK, "order.created");
    UUID wildcard = tenant.register(http, HOOK, "*");
    UUID oneOfSeveral = tenant.register(http, HOOK, "order.paid", "order.created");
    tenant.register(http, HOOK, "order.paid");
    tenant.register(http, HOOK, "order.created.v2", "order");
    tenant.delete(http, tenant.register(http, HOOK, "*")).expectStatus().isNoContent();
    other.register(http, HOOK, "*");

    UUID event = accept(tenant, ORDER_CREATED);

    assertThat(deliveriesOf(event))
        .containsExactlyInAnyOrder(
            Delivery.pendingFor(exact, tenant),
            Delivery.pendingFor(wildcard, tenant),
            Delivery.pendingFor(oneOfSeveral, tenant));
    assertThat(other.deliveryRows()).isZero();
  }

  @Test
  void eventWithoutAMatchingEndpointIsStoredAndItsKeyHolds() {
    TestTenant tenant = TestTenant.create(http, "no-match");
    tenant.register(http, HOOK, "order.created");
    String key = TestTenant.newIdempotencyKey();

    ExchangeResult accepted = post(tenant, key, ORDER_SHIPPED);

    assertThat(accepted.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    UUID event = eventIdIn(accepted);
    assertThat(storedTypesOf(tenant)).containsExactly("order.shipped");
    assertThat(deliveriesOf(event)).isEmpty();

    ExchangeResult again = post(tenant, key, ORDER_SHIPPED);
    assertThat(again.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(again.getResponseHeaders().get("Idempotent-Replay")).containsExactly("true");
    assertThat(eventIdIn(again)).isEqualTo(event);
    assertThat(post(tenant, key, ORDER_CREATED).getStatus())
        .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
    assertThat(storedTypesOf(tenant)).containsExactly("order.shipped");
    assertThat(tenant.deliveryRows()).isZero();
  }

  @Test
  void tenantWithoutEndpointsCanSendEvents() {
    TestTenant tenant = TestTenant.create(http, "no-endpoints");

    UUID event = accept(tenant, ORDER_CREATED);

    assertThat(storedTypesOf(tenant)).containsExactly("order.created");
    assertThat(deliveriesOf(event)).isEmpty();
  }

  // The test makes the change of a delete in a transaction of its own, and holds it uncommitted
  // until the request is seen waiting for the endpoint row. The request has matched both
  // endpoints by then, and it reads the status of the deleted one again once the commit lets it
  // have the row. EndpointDeleteIT shows the same against the statement of the delete route, and
  // the other order of the two as well.
  @Test
  void endpointDeletedAfterTheMatchGetsNoDelivery() throws Exception {
    TestTenant tenant = TestTenant.create(http, "deleted-after-match");
    UUID deleted = tenant.register(http, HOOK, "*");
    UUID kept = tenant.register(http, HOOK, "*");

    ExchangeResult accepted;
    try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
      Future<ExchangeResult> call;
      try (Connection deleter = TestDatabase.connection();
          PreparedStatement delete =
              deleter.prepareStatement(
                  "UPDATE endpoint SET status = 'deleted', deleted_at = now() WHERE id = ?")) {
        delete.setObject(1, deleted);
        delete.execute();

        call = callers.submit(() -> post(tenant, TestTenant.newIdempotencyKey(), ORDER_CREATED));
        PgWait.untilBlocked("INSERT INTO delivery", ReplicaSettings.IN_PROCESS_REPLICA);

        deleter.commit();
      }
      accepted = call.get();
    }

    assertThat(accepted.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(deliveriesOf(eventIdIn(accepted)))
        .containsExactly(Delivery.pendingFor(kept, tenant));
  }

  private UUID accept(TestTenant tenant, String body) {
    ExchangeResult accepted = post(tenant, TestTenant.newIdempotencyKey(), body);
    assertThat(accepted.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    return eventIdIn(accepted);
  }

  private ExchangeResult post(TestTenant tenant, String key, String body) {
    return tenant.send(http, key, body).returnResult();
  }

  private UUID eventIdIn(ExchangeResult accepted) {
    return UUID.fromString(json.readTree(accepted.getResponseBodyContent()).get("id").asString());
  }

  private static List<Delivery> deliveriesOf(UUID eventId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT d.endpoint_id, d.tenant_id, d.state, d.attempts, d.replays,
                   d.lease_token IS NOT NULL AS leased,
                   d.completed_at IS NOT NULL AS completed,
                   d.created_at = e.created_at AS created_with_its_event,
                   d.next_attempt_at = e.created_at AS due_when_created,
                   d.expires_at = e.created_at + interval '72 hours'
                     AS expires_seventy_two_hours_later
            FROM delivery d JOIN event e ON e.id = d.event_id
            WHERE d.event_id = :id
            """)
        .param("id", eventId)
        .query(
            (row, n) ->
                new Delivery(
                    row.getObject("endpoint_id", UUID.class),
                    row.getObject("tenant_id", UUID.class),
                    row.getString("state"),
                    row.getInt("attempts"),
                    row.getInt("replays"),
                    row.getBoolean("leased"),
                    row.getBoolean("completed"),
                    row.getBoolean("created_with_its_event"),
                    row.getBoolean("due_when_created"),
                    row.getBoolean("expires_seventy_two_hours_later")))
        .list();
  }

  private static List<String> storedTypesOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT type FROM event WHERE tenant_id = :t")
        .param("t", tenant.id())
        .query(String.class)
        .list();
  }
}
