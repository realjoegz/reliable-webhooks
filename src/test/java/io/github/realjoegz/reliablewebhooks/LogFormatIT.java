package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaProcess;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.opentelemetry.api.trace.SpanContext;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * What a replica writes to its console. The two tests about the fields of a line read the output of
 * a replica that runs as a process of its own, which is every line that such a replica wrote and no
 * line of anything else. That replica takes every request of the test, the registration of the
 * endpoint among them, and its worker sends the delivery.
 */
@InProcessReplica
@ExtendWith(OutputCaptureExtension.class)
class LogFormatIT {

  private static final Logger log = LoggerFactory.getLogger(LogFormatIT.class);

  private static final String LOGGING_ATTEMPTS = "replica-that-logs-attempts";
  private static final String LOGGING_A_FAILURE = "replica-that-logs-a-failed-request";
  private static final String TRACE_ID = "[0-9a-f]{32}";
  private static final String SPAN_ID = "[0-9a-f]{16}";

  @Autowired JsonMapper json;

  @Test
  void consoleLineIsAJsonObject(CapturedOutput output) {
    String message = "logged-by-the-test-" + UUID.randomUUID();

    log.info(message);

    String line =
        output
            .getOut()
            .lines()
            .filter(written -> written.contains(message))
            .findFirst()
            .orElseThrow();
    JsonNode entry = json.readTree(line);
    assertThat(entry.get("message").asString()).isEqualTo(message);
    assertThat(entry.get("level").asString()).isEqualTo("INFO");
  }

  // The URL of the subscriber has a marker in its path and in its query. The first request to
  // it is answered with a 503 and the second with a 200, so the replica logs a failed attempt
  // and a successful one.
  //
  // Each of the two lines names the delivery by its ids, the attempt, its outcome, the state
  // that it left the delivery in and the replica. Of the URL it names the host. Its trace id is
  // the one of the event, and its span id is the one that the request of that attempt carried to
  // the subscriber.
  //
  // No line that the replica wrote holds the marker. None holds the API key of the tenant, the
  // admin token, the signing secret of the endpoint, a signature that a request carried or the
  // text in the payload of the event either.
  @Test
  @Tag("process")
  void subscriberUrlIsReducedToHost() throws Exception {
    String marker = "marker" + UUID.randomUUID().toString().replace("-", "");
    String payload = "payload" + UUID.randomUUID().toString().replace("-", "");
    try (FaultReceiver receiver = FaultReceiver.start();
        ReplicaProcess replica = ReplicaProcess.start(LOGGING_ATTEMPTS)) {
      String path = "/hook-" + marker + "?token=" + marker;
      receiver.answerOnce(path, FaultReceiver.status(503));
      receiver.answer(path, FaultReceiver.status(200));
      RestTestClient api = replica.http();
      TestTenant tenant = TestTenant.create(api, "logged-attempts");
      TestTenant.Endpoint endpoint = tenant.registerForSecret(api, receiver.url(path), "*");

      UUID event =
          tenant.accept(
              api,
              """
              {"type":"order.created","data":{"note":"%s"}}"""
                  .formatted(payload));
      UUID delivery = Rows.deliveryOf(event);
      await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(1));
      Rows.ageDelivery(delivery, Duration.ofSeconds(10));
      // The line of an attempt is written after the commit that makes its row visible.
      List<JsonNode> attempts =
          await().until(() -> linesOf(replica, "delivery_attempt"), lines -> lines.size() == 2);

      List<FaultReceiver.Request> requests = receiver.requests();
      assertThat(requests).hasSize(2);
      SpanContext ingest = Traceparent.parse(traceContextOf(event)).orElseThrow();
      for (int i = 0; i < 2; i++) {
        JsonNode line = attempts.get(i);
        SpanContext sent = Traceparent.parse(requests.get(i).header("traceparent")).orElseThrow();
        assertThat(line.get("level").asString()).isEqualTo("INFO");
        assertThat(line.get("tenant_id").asString()).isEqualTo(tenant.id().toString());
        assertThat(line.get("event_id").asString()).isEqualTo(event.toString());
        assertThat(line.get("delivery_id").asString()).isEqualTo(delivery.toString());
        assertThat(line.get("endpoint_id").asString()).isEqualTo(endpoint.id().toString());
        assertThat(line.get("attempt").asInt()).isEqualTo(i + 1);
        assertThat(line.get("replica").asString()).isEqualTo(LOGGING_ATTEMPTS);
        assertThat(line.get("host").asString()).isEqualTo("127.0.0.1");
        assertThat(line.get("trace_id").asString()).isEqualTo(ingest.getTraceId());
        assertThat(line.get("span_id").asString()).isEqualTo(sent.getSpanId());
      }
      assertThat(attempts.get(0).get("outcome").asString()).isEqualTo("failure");
      assertThat(attempts.get(0).get("reason").asString()).isEqualTo("http_5xx");
      assertThat(attempts.get(0).get("state").asString()).isEqualTo("pending");
      assertThat(attempts.get(1).get("outcome").asString()).isEqualTo("success");
      assertThat(attempts.get(1).get("reason").asString()).isEqualTo("http_2xx");
      assertThat(attempts.get(1).get("state").asString()).isEqualTo("succeeded");

      assertThat(replica.output())
          .doesNotContain(marker)
          .doesNotContain(payload)
          .doesNotContain(tenant.apiKey())
          .doesNotContain(ReplicaSettings.ADMIN_TOKEN)
          .doesNotContain(endpoint.secret().substring("whsec_".length()));
      for (FaultReceiver.Request request : requests) {
        assertThat(replica.output())
            .doesNotContain(request.header("webhook-signature").substring("v1,".length()));
      }
    }
  }

  // The tenant has a rate that the admin route would refuse, written by the test, so the charge
  // of its event fails in the database and the request is answered with a 500. The replica logs
  // that failure, and the line names the tenant, the replica and the trace of the request.
  //
  // The caller sent a trace context of its own. The trace id of the line is another one, which
  // the replica gave the request.
  @Test
  @Tag("process")
  void lineOfARequestNamesItsTraceItsTenantAndTheReplica() {
    String traceOfTheCaller = "0af7651916cd43dd8448eb211c80319c";
    try (ReplicaProcess replica = ReplicaProcess.start(LOGGING_A_FAILURE)) {
      RestTestClient api = replica.http();
      TestTenant tenant = TestTenant.create(api, "logged-request");
      TestDatabase.jdbc()
          .sql("UPDATE tenant SET rate_per_sec = 1e308 WHERE id = :id")
          .param("id", tenant.id())
          .update();
      Rows.ageBucket(tenant.id(), Duration.ofHours(1));

      api.post()
          .uri("/v1/events")
          .headers(
              headers -> {
                headers.setBearerAuth(tenant.apiKey());
                headers.set("Idempotency-Key", TestTenant.newIdempotencyKey());
                headers.set("traceparent", "00-" + traceOfTheCaller + "-b7ad6b7169203331-01");
              })
          .contentType(MediaType.APPLICATION_JSON)
          .body(
              """
              {"type":"order.created","data":{"order":17}}""")
          .exchange()
          .expectStatus()
          .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);

      List<JsonNode> failures =
          await().until(() -> linesOf(replica, "Unhandled failure"), lines -> lines.size() == 1);
      JsonNode line = failures.getFirst();
      assertThat(line.get("level").asString()).isEqualTo("ERROR");
      assertThat(line.get("tenant_id").asString()).isEqualTo(tenant.id().toString());
      assertThat(line.get("replica").asString()).isEqualTo(LOGGING_A_FAILURE);
      assertThat(line.get("trace_id").asString()).matches(TRACE_ID).isNotEqualTo(traceOfTheCaller);
      assertThat(line.get("span_id").asString()).matches(SPAN_ID);
      assertThat(replica.output()).doesNotContain(tenant.apiKey());
    }
  }

  // The lines of the replica with that message. A replica writes a banner before its first
  // line, which is no JSON.
  private List<JsonNode> linesOf(ReplicaProcess replica, String message) {
    return replica
        .output()
        .lines()
        .filter(line -> line.startsWith("{"))
        .map(json::readTree)
        .filter(line -> message.equals(line.path("message").asString("")))
        .toList();
  }

  private static String traceContextOf(UUID event) {
    return TestDatabase.jdbc()
        .sql("SELECT trace_context FROM event WHERE id = :id")
        .param("id", event)
        .query(String.class)
        .single();
  }
}
