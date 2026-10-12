package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaProcess;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.SelfStartedReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * The spans of a replica, as an exporter of the test gets them. The share of its traces that a
 * replica samples is a setting that it reads when it starts. A test about spans therefore starts a
 * replica of its own, the way the application starts, with the share that the test is about and
 * with an exporter that keeps the spans in memory. That replica takes the requests of the test, and
 * its worker sends the deliveries.
 *
 * <p>A replica hands its spans to an exporter in batches. A test asks for the batch that is due
 * before it reads the exporter, once the request or the delivery whose spans it wants is over.
 */
@UnsafeLocalReplica
class TracingIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final String SAMPLES_EVERY_TRACE = "replica-that-samples-every-trace";
  private static final String SAMPLES_NO_TRACE = "replica-that-samples-no-trace";
  private static final String NAMES_A_COLLECTOR = "replica-that-names-a-collector";
  private static final String SAMPLES_BY_ITS_SETTING = "replica-that-samples-by-its-setting";
  private static final String COLLECTOR_PATH = "/v1/traces";

  private static final String TRACE_OF_THE_CALLER = "0af7651916cd43dd8448eb211c80319c";
  private static final String SPAN_OF_THE_CALLER = "b7ad6b7169203331";
  private static final String SAMPLED_BY_THE_CALLER =
      "00-" + TRACE_OF_THE_CALLER + "-" + SPAN_OF_THE_CALLER + "-01";

  private static final AttributeKey<String> DELIVERY_ID =
      AttributeKey.stringKey("webhooks.delivery_id");
  private static final AttributeKey<Long> ATTEMPT = AttributeKey.longKey("webhooks.attempt");

  @Autowired ObjectProvider<SpanExporter> exportersOfTheReplicaOfTheClass;

  private final InMemorySpanExporter spans = InMemorySpanExporter.create();

  // The replica of the class runs with the configuration that ships, which names no collector,
  // and it has no exporter for its spans.
  //
  // The witness is a second replica whose configuration names a collector, which is the receiver
  // of the test, and which samples every trace. That replica has an exporter, and the span of a
  // request to it arrives at the receiver as OTLP sends it, in a POST of a protobuf message.
  @Test
  void replicaExportsItsSpansNowhereUnlessACollectorIsNamed() throws Exception {
    assertThat(exportersOfTheReplicaOfTheClass.stream()).isEmpty();

    try (FaultReceiver collector = FaultReceiver.start()) {
      collector.answer(COLLECTOR_PATH, FaultReceiver.status(200));
      try (ConfigurableApplicationContext replica =
          SelfStartedReplica.startExportingTo(collector.url(COLLECTOR_PATH), NAMES_A_COLLECTOR)) {
        assertThat(replica.getBeanProvider(SpanExporter.class).stream()).hasSize(1);

        TestTenant.create(SelfStartedReplica.clientOf(replica), "traced-to-a-collector");
        flush(replica);

        assertThat(collector.requests(COLLECTOR_PATH))
            .isNotEmpty()
            .allSatisfy(
                export -> {
                  assertThat(export.method()).isEqualTo("POST");
                  assertThat(export.header("content-type")).isEqualTo("application/x-protobuf");
                  assertThat(export.body()).isNotEmpty();
                });
      }
    }
  }

  // Spring Boot maps the OTEL_ variables of the environment onto its settings unless it is told
  // not to, and the configuration that ships tells it not to. Only a process has an environment
  // of its own, so this replica is one. Its environment says two things about sampling: under
  // the name of the setting of Spring Boot, that every trace is sampled, and in a variable of
  // OpenTelemetry, that none is.
  //
  // The replica goes by its setting: the event that it accepts is stored with the context of a
  // sampled span.
  @Test
  @Tag("process")
  void replicaDoesNotReadTheOpenTelemetryVariablesOfItsEnvironment() {
    Map<String, String> environment =
        Map.of(
            "MANAGEMENT_TRACING_SAMPLING_PROBABILITY", "1.0",
            "OTEL_TRACES_SAMPLER", "always_off");
    try (ReplicaProcess replica = ReplicaProcess.startWith(environment, SAMPLES_BY_ITS_SETTING)) {
      RestTestClient api = replica.http();
      TestTenant tenant = TestTenant.create(api, "sampled-by-the-setting");

      UUID event = tenant.accept(api, EVENT);

      assertThat(Traceparent.parse(traceContextOf(event)).orElseThrow().isSampled()).isTrue();
    }
  }

  // The replica samples no trace of its own. The caller says that its span is sampled, in the
  // W3C header and in both forms of the B3 headers, which Spring Boot reads unless it is told
  // not to. A replica that continued the trace of the caller would sample the span of the
  // request, and with it the span of the attempt.
  //
  // The event is delivered, so both spans have ended when the exporter is read. The context
  // that was stored for the event says that its span is not sampled and names another trace
  // than the one of the caller, and the request to the subscriber says the same.
  //
  // Nothing was exported. The witness is a span that the test starts in the replica as a child
  // of the context of the caller: it is exported, by the same sampler and through the same
  // exporter, and it is the only span there.
  @Test
  void callerCannotForceSampling() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        ConfigurableApplicationContext replica =
            SelfStartedReplica.start(SAMPLES_NO_TRACE, 0.0, spans)) {
      receiver.answer("/hook", FaultReceiver.status(200));
      RestTestClient api = SelfStartedReplica.clientOf(replica);
      TestTenant tenant = TestTenant.create(api, "caller-forces-sampling");
      tenant.register(api, receiver.url("/hook"), "*");

      UUID event =
          accept(
              api,
              tenant,
              headers -> {
                headers.set("traceparent", SAMPLED_BY_THE_CALLER);
                headers.set("b3", TRACE_OF_THE_CALLER + "-" + SPAN_OF_THE_CALLER + "-1");
                headers.set("X-B3-TraceId", TRACE_OF_THE_CALLER);
                headers.set("X-B3-SpanId", SPAN_OF_THE_CALLER);
                headers.set("X-B3-Sampled", "1");
              });
      UUID delivery = Rows.deliveryOf(event);
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));

      SpanContext ingest = Traceparent.parse(traceContextOf(event)).orElseThrow();
      assertThat(ingest.isSampled()).isFalse();
      assertThat(ingest.getTraceId()).isNotEqualTo(TRACE_OF_THE_CALLER);
      assertThat(receiver.requests()).hasSize(1);
      SpanContext attempt =
          Traceparent.parse(receiver.requests().getFirst().header("traceparent")).orElseThrow();
      assertThat(attempt.isSampled()).isFalse();
      assertThat(attempt.getTraceId()).isEqualTo(ingest.getTraceId());

      Context ofTheCaller =
          Context.root().with(Span.wrap(Traceparent.parse(SAMPLED_BY_THE_CALLER).orElseThrow()));
      replica
          .getBean(OpenTelemetry.class)
          .getTracer("tracing-it")
          .spanBuilder("witness")
          .setParent(ofTheCaller)
          .startSpan()
          .end();

      assertThat(exported(replica)).extracting(SpanData::getName).containsExactly("witness");
    }
  }

  // The replica samples every trace. The span of the ingest request is the root of a trace of
  // its own: it has no parent, and its trace id is not the one that the caller sent. What the
  // caller sent is on it as a link, and nowhere else: no span that the replica exported is in
  // the trace of the caller or names the span of the caller as its parent.
  //
  // The caller also sent a tracestate, and the span has none: nothing of the caller is carried
  // on. The context that is stored with the event is the context of that span. A second event,
  // sent without a traceparent, has a span without a link.
  @Test
  void callerTraceIdBecomesLinkOnly() {
    try (ConfigurableApplicationContext replica =
        SelfStartedReplica.start(SAMPLES_EVERY_TRACE, 1.0, spans)) {
      RestTestClient api = SelfStartedReplica.clientOf(replica);
      TestTenant tenant = TestTenant.create(api, "caller-sends-a-trace");

      UUID withATrace =
          accept(
              api,
              tenant,
              headers -> {
                headers.set("traceparent", SAMPLED_BY_THE_CALLER);
                headers.set("tracestate", "vendor=value");
              });
      UUID withoutOne = accept(api, tenant, headers -> {});

      List<SpanData> exported = exported(replica);
      SpanContext stored = Traceparent.parse(traceContextOf(withATrace)).orElseThrow();
      assertThat(stored.isSampled()).isTrue();
      assertThat(stored.getTraceId()).isNotEqualTo(TRACE_OF_THE_CALLER);
      SpanData ingest = spanOf(exported, stored);
      assertThat(ingest.getKind()).isEqualTo(SpanKind.SERVER);
      assertThat(ingest.getTraceId()).isEqualTo(stored.getTraceId());
      assertThat(ingest.getParentSpanContext().isValid()).isFalse();
      assertThat(ingest.getLinks())
          .extracting(
              link -> link.getSpanContext().getTraceId(), link -> link.getSpanContext().getSpanId())
          .containsExactly(tuple(TRACE_OF_THE_CALLER, SPAN_OF_THE_CALLER));
      assertThat(ingest.getSpanContext().getTraceState().isEmpty()).isTrue();
      assertThat(exported)
          .noneMatch(span -> span.getTraceId().equals(TRACE_OF_THE_CALLER))
          .noneMatch(span -> span.getParentSpanId().equals(SPAN_OF_THE_CALLER));

      SpanData ingestWithoutATrace =
          spanOf(exported, Traceparent.parse(traceContextOf(withoutOne)).orElseThrow());
      assertThat(ingestWithoutATrace.getParentSpanContext().isValid()).isFalse();
      assertThat(ingestWithoutATrace.getLinks()).isEmpty();
      assertThat(ingestWithoutATrace.getTraceId()).isNotEqualTo(ingest.getTraceId());
    }
  }

  // The URL of the subscriber has a marker in its path and in its query, and its host is a name
  // that only this test uses. The first request is answered with a 500 and the second with a
  // 200, so the delivery has two attempts.
  //
  // Each attempt has one span of kind CLIENT, a child of the span of the ingest request, with
  // six attributes and no other. The host is one of them, and no span that the replica exported
  // holds the marker. Each request carried the traceparent of its own span.
  @Test
  void attemptSpanCarriesHostOnly() throws Exception {
    String marker = "marker" + UUID.randomUUID().toString().replace("-", "");
    String host = "subscriber-" + UUID.randomUUID() + ".test";
    try (FaultReceiver receiver = FaultReceiver.start();
        ConfigurableApplicationContext replica =
            SelfStartedReplica.start(SAMPLES_EVERY_TRACE, 1.0, spans)) {
      replica.getBean(StubDns.class).answer(host, "127.0.0.1");
      String path = "/hook-" + marker + "?token=" + marker;
      receiver.answerOnce(path, FaultReceiver.status(500));
      receiver.answer(path, FaultReceiver.status(200));
      RestTestClient api = SelfStartedReplica.clientOf(replica);
      TestTenant tenant = TestTenant.create(api, "attempt-spans");
      tenant.register(api, "http://%s:%d%s".formatted(host, receiver.port(), path), "*");

      UUID event = tenant.accept(api, EVENT);
      UUID delivery = Rows.deliveryOf(event);
      await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(1));
      Rows.ageDelivery(delivery, Duration.ofSeconds(10));
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));

      List<SpanData> exported = exported(replica);
      SpanContext stored = Traceparent.parse(traceContextOf(event)).orElseThrow();
      SpanData ingest = spanOf(exported, stored);
      List<SpanData> attempts =
          exported.stream()
              .filter(span -> delivery.toString().equals(span.getAttributes().get(DELIVERY_ID)))
              .sorted(Comparator.comparing((SpanData span) -> span.getAttributes().get(ATTEMPT)))
              .toList();
      assertThat(attempts).hasSize(2);
      assertThat(attempts)
          .allSatisfy(
              attempt -> {
                assertThat(attempt.getKind()).isEqualTo(SpanKind.CLIENT);
                assertThat(attempt.getTraceId()).isEqualTo(ingest.getTraceId());
                assertThat(attempt.getParentSpanId()).isEqualTo(ingest.getSpanId());
              });
      assertThat(attempts.get(0).getAttributes().asMap())
          .isEqualTo(attributes(delivery, 1, "failure", 500, host));
      assertThat(attempts.get(1).getAttributes().asMap())
          .isEqualTo(attributes(delivery, 2, "success", 200, host));
      assertThat(receiver.requests())
          .extracting(request -> request.header("traceparent"))
          .containsExactly(
              traceparentOf(attempts.get(0).getSpanContext()),
              traceparentOf(attempts.get(1).getSpanContext()));

      // The text of a span holds what the span carries: the host is found in it.
      assertThat(attempts).allSatisfy(span -> assertThat(span.toString()).contains(host));
      assertThat(exported).allSatisfy(span -> assertThat(span.toString()).doesNotContain(marker));
    }
  }

  private static Map<AttributeKey<?>, Object> attributes(
      UUID delivery, long attempt, String outcome, long status, String host) {
    return Map.<AttributeKey<?>, Object>of(
        DELIVERY_ID,
        delivery.toString(),
        ATTEMPT,
        attempt,
        AttributeKey.booleanKey("webhooks.probe"),
        false,
        AttributeKey.stringKey("webhooks.outcome"),
        outcome,
        AttributeKey.longKey("http.response.status_code"),
        status,
        AttributeKey.stringKey("server.address"),
        host);
  }

  // The header as the W3C writes it: the version, the two ids and the flags.
  private static String traceparentOf(SpanContext span) {
    return "00-%s-%s-%s"
        .formatted(span.getTraceId(), span.getSpanId(), span.getTraceFlags().asHex());
  }

  /** Sends an event with the trace headers of the test, and returns the id of the event. */
  private static UUID accept(
      RestTestClient api, TestTenant tenant, Consumer<HttpHeaders> traceHeaders) {
    Map<String, String> accepted =
        api.post()
            .uri("/v1/events")
            .headers(
                headers -> {
                  headers.setBearerAuth(tenant.apiKey());
                  headers.set("Idempotency-Key", TestTenant.newIdempotencyKey());
                  traceHeaders.accept(headers);
                })
            .contentType(MediaType.APPLICATION_JSON)
            .body(EVENT)
            .exchange()
            .expectStatus()
            .isAccepted()
            .expectBody(new ParameterizedTypeReference<Map<String, String>>() {})
            .returnResult()
            .getResponseBody();
    return UUID.fromString(accepted.get("id"));
  }

  // The spans that have ended are with the exporter once the replica has handed over the batch
  // that it was collecting.
  private List<SpanData> exported(ConfigurableApplicationContext replica) {
    flush(replica);
    return spans.getFinishedSpanItems();
  }

  private static void flush(ConfigurableApplicationContext replica) {
    assertThat(
            replica
                .getBean(SdkTracerProvider.class)
                .forceFlush()
                .join(10, TimeUnit.SECONDS)
                .isSuccess())
        .isTrue();
  }

  private static SpanData spanOf(List<SpanData> exported, SpanContext context) {
    return exported.stream()
        .filter(span -> span.getSpanId().equals(context.getSpanId()))
        .findFirst()
        .orElseThrow();
  }

  private static String traceContextOf(UUID event) {
    return TestDatabase.jdbc()
        .sql("SELECT trace_context FROM event WHERE id = :id")
        .param("id", event)
        .query(String.class)
        .single();
  }
}
