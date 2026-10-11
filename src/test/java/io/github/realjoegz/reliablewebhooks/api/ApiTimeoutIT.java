package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Pool;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.ExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.json.JsonMapper;

@InProcessReplica
class ApiTimeoutIT {

  private static final String HOST = "hooks.api-timeout.example";
  private static final String BODY =
      """
      {"type":"order.created","data":{"order":17}}""";

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired JsonMapper json;
  @Autowired WebhooksProperties properties;

  // The test holds the endpoint row that the fan-out locks, and keeps it until the request has
  // been answered. The request is seen waiting in the fan-out, which its transaction runs after
  // the insert of the event, so the answer comes from a transaction that had an event to undo.
  // An answer before the statement timeout shows that the lock timeout gave it.
  @Test
  void blockedStatementAnswers503() throws Exception {
    TestTenant tenant = TestTenant.create(http, "blocked-statement");
    dns.answer(HOST, "93.184.216.34");
    UUID endpoint = tenant.register(http, "https://" + HOST + "/hook", "*");
    String key = TestTenant.newIdempotencyKey();
    Pool api = properties.database().api();

    try (ExecutorService caller = Executors.newVirtualThreadPerTaskExecutor();
        Connection blocker = TestDatabase.connection();
        PreparedStatement lock =
            blocker.prepareStatement("SELECT 1 FROM endpoint WHERE id = ? FOR UPDATE")) {
      lock.setObject(1, endpoint);
      lock.execute();

      long start = System.nanoTime();
      Future<ExchangeResult> call = caller.submit(() -> post(tenant, key));
      PgWait.untilBlocked("INSERT INTO delivery", ReplicaSettings.IN_PROCESS_REPLICA);
      ExchangeResult answer = call.get();
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(answer.getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
      assertThat(answer.getResponseHeaders().getContentType())
          .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
      assertThat(answer.getResponseHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
      assertThat(json.readTree(answer.getResponseBodyContent()).get("type").asString())
          .isEqualTo("database_busy");
      assertThat(took).isBetween(api.lockTimeout(), api.statementTimeout());

      blocker.rollback();
    }
    assertThat(tenant.eventRows()).isZero();

    ExchangeResult retried = post(tenant, key);
    assertThat(retried.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(retried.getResponseHeaders().get("Idempotent-Replay")).isNull();
    assertThat(tenant.eventRows()).isEqualTo(1);
    assertThat(tenant.deliveryRows()).isEqualTo(1);
  }

  private ExchangeResult post(TestTenant tenant, String key) {
    return tenant.send(http, key, BODY).returnResult();
  }
}
