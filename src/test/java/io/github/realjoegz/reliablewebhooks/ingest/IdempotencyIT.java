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
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.ExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.json.JsonMapper;

@InProcessReplica
class IdempotencyIT {

  private static final String HOST = "hooks.idempotency.example";
  private static final String HOOK = "https://" + HOST + "/hook";
  private static final String IDEMPOTENT_REPLAY = "Idempotent-Replay";
  private static final String ORDER_17 =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String ORDER_18 =
      """
      {"type":"order.created","data":{"order":18}}""";
  private static final String ORDER_17_PAID =
      """
      {"type":"order.paid","data":{"order":17}}""";

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired JsonMapper json;

  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  // The second endpoint is registered between the two requests. A second request that was
  // ingested again would fan out to it.
  @Test
  void sameKeyReturnsSameEvent() {
    TestTenant tenant = TestTenant.create(http, "same-key");
    tenant.register(http, HOOK, "*");
    String key = TestTenant.newIdempotencyKey();

    ExchangeResult first = post(tenant, key, ORDER_17);
    tenant.register(http, HOOK, "*");
    ExchangeResult second = post(tenant, key, ORDER_17);

    assertThat(first.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(first.getResponseHeaders().get(IDEMPOTENT_REPLAY)).isNull();
    assertThat(second.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(second.getResponseHeaders().get(IDEMPOTENT_REPLAY)).containsExactly("true");
    assertThat(eventIdIn(second)).isEqualTo(eventIdIn(first));
    assertThat(eventsOf(tenant)).containsExactly(eventIdIn(first));
    assertThat(tenant.deliveryRows()).isEqualTo(1);
  }

  // The test holds the endpoint row that the fan-out of the first request locks, so the first
  // request waits inside its transaction with its event inserted and not committed. The second
  // request is let go only when the first is seen waiting, and the row is released only when the
  // second is seen waiting on the insert of its own event.
  @Test
  void concurrentSameKeyCreatesOneEvent() throws Exception {
    TestTenant tenant = TestTenant.create(http, "concurrent-key");
    UUID endpoint = tenant.register(http, HOOK, "*");
    String key = TestTenant.newIdempotencyKey();

    ExchangeResult first;
    ExchangeResult second;
    try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
      Future<ExchangeResult> firstCall;
      Future<ExchangeResult> secondCall;
      try (Connection blocker = TestDatabase.connection();
          PreparedStatement lock =
              blocker.prepareStatement("SELECT 1 FROM endpoint WHERE id = ? FOR UPDATE")) {
        lock.setObject(1, endpoint);
        lock.execute();

        firstCall = callers.submit(() -> post(tenant, key, ORDER_17));
        PgWait.untilBlocked("INSERT INTO delivery", ReplicaSettings.IN_PROCESS_REPLICA);
        assertThat(eventsOf(tenant)).isEmpty();

        secondCall = callers.submit(() -> post(tenant, key, ORDER_17));
        PgWait.untilBlocked("INSERT INTO event", ReplicaSettings.IN_PROCESS_REPLICA);

        blocker.rollback();
      }
      first = firstCall.get();
      second = secondCall.get();
    }

    assertThat(first.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(first.getResponseHeaders().get(IDEMPOTENT_REPLAY)).isNull();
    assertThat(second.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(second.getResponseHeaders().get(IDEMPOTENT_REPLAY)).containsExactly("true");
    assertThat(eventIdIn(second)).isEqualTo(eventIdIn(first));
    assertThat(eventsOf(tenant)).containsExactly(eventIdIn(first));
    assertThat(tenant.deliveryRows()).isEqualTo(1);
  }

  @Test
  void sameKeyDifferentBodyIsRejected() {
    TestTenant tenant = TestTenant.create(http, "reused-key");
    tenant.register(http, HOOK, "*");
    String key = TestTenant.newIdempotencyKey();
    ExchangeResult first = post(tenant, key, ORDER_17);
    assertThat(first.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    byte[] storedBody = storedBodyOf(eventIdIn(first));

    for (String otherBody : List.of(ORDER_18, ORDER_17_PAID)) {
      ExchangeResult rejected = post(tenant, key, otherBody);

      assertThat(rejected.getStatus()).as(otherBody).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
      assertThat(rejected.getResponseHeaders().getContentType())
          .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
      assertThat(json.readTree(rejected.getResponseBodyContent()).get("type").asString())
          .isEqualTo("idempotency_key_reused");
    }

    assertThat(eventsOf(tenant)).containsExactly(eventIdIn(first));
    assertThat(storedBodyOf(eventIdIn(first))).isEqualTo(storedBody);
    assertThat(tenant.deliveryRows()).isEqualTo(1);
    ExchangeResult replayed = post(tenant, key, ORDER_17);
    assertThat(replayed.getResponseHeaders().get(IDEMPOTENT_REPLAY)).containsExactly("true");
    assertThat(eventIdIn(replayed)).isEqualTo(eventIdIn(first));
  }

  @Test
  void sameKeyOfAnotherTenantIsAnotherEvent() {
    TestTenant tenant = TestTenant.create(http, "key-owner");
    TestTenant other = TestTenant.create(http, "same-key-other-tenant");
    String key = TestTenant.newIdempotencyKey();

    ExchangeResult first = post(tenant, key, ORDER_17);
    ExchangeResult ofOther = post(other, key, ORDER_18);

    assertThat(ofOther.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(ofOther.getResponseHeaders().get(IDEMPOTENT_REPLAY)).isNull();
    assertThat(eventIdIn(ofOther)).isNotEqualTo(eventIdIn(first));
    assertThat(eventsOf(tenant)).containsExactly(eventIdIn(first));
    assertThat(eventsOf(other)).containsExactly(eventIdIn(ofOther));
  }

  @Test
  void sameBodyUnderAnotherKeyIsAnotherEvent() {
    TestTenant tenant = TestTenant.create(http, "two-keys");

    ExchangeResult first = post(tenant, TestTenant.newIdempotencyKey(), ORDER_17);
    ExchangeResult second = post(tenant, TestTenant.newIdempotencyKey(), ORDER_17);

    assertThat(second.getResponseHeaders().get(IDEMPOTENT_REPLAY)).isNull();
    assertThat(eventsOf(tenant)).containsExactlyInAnyOrder(eventIdIn(first), eventIdIn(second));
  }

  private ExchangeResult post(TestTenant tenant, String key, String body) {
    return tenant.send(http, key, body).returnResult();
  }

  private UUID eventIdIn(ExchangeResult accepted) {
    return UUID.fromString(json.readTree(accepted.getResponseBodyContent()).get("id").asString());
  }

  private static List<UUID> eventsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM event WHERE tenant_id = :t")
        .param("t", tenant.id())
        .query(UUID.class)
        .list();
  }

  private static byte[] storedBodyOf(UUID eventId) {
    return TestDatabase.jdbc()
        .sql("SELECT body FROM event WHERE id = :id")
        .param("id", eventId)
        .query(byte[].class)
        .single();
  }
}
