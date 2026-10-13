package io.github.realjoegz.reliablewebhooks.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Delivery;
import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.AttemptSpan;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Failed;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.LookupBusy;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Outcome;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Reason;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Response;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Traced;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The client against a receiver on the loopback address, which its address policy lets through
 * here. The deadline of the client is the one that ships, apart from the three tests that run into
 * it: theirs is short, so that they do not take fifteen seconds each. The test that runs into the
 * wait for a lookup has a short wait for the same reason.
 *
 * <p>Most of the clients trace nothing. The tests about the span of a request have a client whose
 * spans are sampled and handed to an exporter of the test as soon as they end.
 */
class WebhookHttpClientIT {

  private static final UUID TENANT = UUID.randomUUID();
  private static final byte[] BODY = "{\"id\":\"1\"}".getBytes(StandardCharsets.UTF_8);
  private static final Duration DEADLINE = Duration.ofSeconds(15);
  private static final Duration SHORT_DEADLINE = Duration.ofMillis(500);
  private static final Duration HANDSHAKE_TIMEOUT = Duration.ofSeconds(3);
  private static final int CONNECTIONS = 4;
  private static final int LOOKUP_SLOTS_OF_A_TENANT = 4;
  private static final int KIB = 1024;

  // What the span of a request says. An attempt without a parent starts a trace of its own.
  private static final AttemptSpan ATTEMPT = new AttemptSpan(UUID.randomUUID(), 1, false, null);
  private static final String MARKER = "marker-that-no-span-may-hold";

  // Every duration but the attempt deadline is the one that ships. The client that is built from
  // these settings, as the application builds its client, has to take the short one as its bound.
  private static final WebhooksProperties SETTINGS_WITH_A_SHORT_DEADLINE =
      new WebhooksProperties(
          null,
          null,
          null,
          null,
          new Delivery(
              SHORT_DEADLINE,
              Duration.ofSeconds(60),
              Duration.ofSeconds(35),
              Duration.ofSeconds(55),
              12,
              CONNECTIONS),
          null);

  private final StubDns dns = new StubDns();
  private final MeterRegistry meters = new SimpleMeterRegistry();
  private final DnsLookups lookups =
      new DnsLookups(dns, new AddressPolicy(List.of("127.0.0.1/32")), meters);
  private final WebhookHttpClient client =
      new WebhookHttpClient(lookups, DEADLINE, CONNECTIONS, OpenTelemetry.noop());
  private final WebhookHttpClient impatient =
      new WebhookHttpClient(lookups, SETTINGS_WITH_A_SHORT_DEADLINE, OpenTelemetry.noop());
  private final InMemorySpanExporter spans = InMemorySpanExporter.create();
  private final SdkTracerProvider tracing =
      SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build();
  private final WebhookHttpClient traced =
      new WebhookHttpClient(
          lookups,
          DEADLINE,
          CONNECTIONS,
          OpenTelemetrySdk.builder().setTracerProvider(tracing).build());
  private FaultReceiver receiver;

  @BeforeEach
  void startReceiver() throws IOException {
    receiver = FaultReceiver.start();
  }

  @AfterEach
  void closeEverything() throws IOException {
    client.close();
    impatient.close();
    traced.close();
    tracing.close();
    lookups.close();
    receiver.close();
  }

  @Test
  void sendsOneJsonPostWithTheGivenHeadersAndReportsTheStatus() throws UnknownHostException {
    receiver.answer("/hook", FaultReceiver.status(204));

    Outcome outcome =
        client
            .send(
                TENANT,
                ATTEMPT,
                url("/hook"),
                Map.of("webhook-id", "an-id", "webhook-timestamp", "17"),
                BODY)
            .outcome();

    assertThat(outcome).isEqualTo(new Response(204, null, InetAddress.getByName("127.0.0.1")));
    assertThat(receiver.requests()).hasSize(1);
    FaultReceiver.Request request = receiver.requests().getFirst();
    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.path()).isEqualTo("/hook");
    assertThat(request.body()).isEqualTo(BODY);
    assertThat(request.headers())
        .containsEntry("webhook-id", "an-id")
        .containsEntry("webhook-timestamp", "17")
        .containsEntry("content-type", "application/json")
        .containsEntry("content-length", Integer.toString(BODY.length))
        .containsEntry("user-agent", "reliable-webhooks")
        .doesNotContainKeys("accept-encoding", "cookie", "authorization", "expect");
  }

  // The default of the library sends a request again when the answer is 503 or 429. An attempt
  // is one request.
  @Test
  void failureStatusIsReportedWithItsRetryAfterAndTheRequestIsNotSentAgain() throws Exception {
    receiver.answer("/hook", FaultReceiver.status(503, "Retry-After", "120"));
    receiver.answer("/limited", FaultReceiver.status(429, "Retry-After", "1"));

    assertThat(client.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY).outcome())
        .isEqualTo(new Response(503, "120", InetAddress.getByName("127.0.0.1")));
    assertThat(client.send(TENANT, ATTEMPT, url("/limited"), Map.of(), BODY).outcome())
        .isEqualTo(new Response(429, "1", InetAddress.getByName("127.0.0.1")));

    assertThat(receiver.requests("/hook")).hasSize(1);
    assertThat(receiver.requests("/limited")).hasSize(1);
  }

  // No resolver but the one of the test knows this name. The request arrives at the receiver, so
  // the client connected to the address that the guarded lookup gave it. A client that had the
  // name checked first and then resolved it again by itself would find no address.
  @Test
  void hostIsConnectedAtTheAddressThatTheGuardedLookupReturned() throws UnknownHostException {
    dns.answer("receiver.example", "127.0.0.1");
    receiver.answer("/hook", FaultReceiver.status(200));
    String authority = "receiver.example:" + receiver.port();

    Outcome outcome =
        client
            .send(TENANT, ATTEMPT, URI.create("http://" + authority + "/hook"), Map.of(), BODY)
            .outcome();

    assertThat(outcome).isEqualTo(new Response(200, null, InetAddress.getByName("127.0.0.1")));
    assertThat(receiver.requests()).hasSize(1);
    assertThat(receiver.requests().getFirst().header("host")).isEqualTo(authority);
  }

  // One client serves every tenant. The second request goes out on the connection that carried
  // the cookie of the first, and carries none.
  @Test
  void cookieOfOneResponseIsNotSentWithTheNextRequest() {
    receiver.answerOnce("/hook", FaultReceiver.status(200, "Set-Cookie", "session=of-a-tenant"));
    receiver.answer("/hook", FaultReceiver.status(200));

    client.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY);
    client.send(UUID.randomUUID(), ATTEMPT, url("/hook"), Map.of(), BODY);

    assertThat(receiver.requests()).hasSize(2);
    assertThat(receiver.requests())
        .allSatisfy(request -> assertThat(request.headers()).doesNotContainKey("cookie"));
    assertThat(receiver.acceptedConnections()).isEqualTo(1);
  }

  @Test
  void connectionThatEndsWithoutAResponseIsAFailedConnectionAndNotSentAgain() throws Exception {
    receiver.answer("/hook", FaultReceiver.closeWithoutResponse());

    assertThat(client.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY).outcome())
        .isEqualTo(new Failed(Reason.CONNECT, InetAddress.getByName("127.0.0.1")));
    assertThat(receiver.requests()).hasSize(1);
  }

  @Test
  void refusedConnectionIsAFailedConnection() throws IOException {
    URI closedPort = url("/hook");
    receiver.close();

    assertThat(client.send(TENANT, ATTEMPT, closedPort, Map.of(), BODY).outcome())
        .isEqualTo(new Failed(Reason.CONNECT, null));
  }

  // The receiver speaks no TLS and ends a connection that does not begin with an HTTP request.
  @Test
  void handshakeThatFailsIsATlsFailure() {
    URI https = URI.create("https://127.0.0.1:%d/hook".formatted(receiver.port()));

    Outcome outcome = client.send(TENANT, ATTEMPT, https, Map.of(), BODY).outcome();

    assertThat(outcome).isInstanceOf(Failed.class);
    assertThat(((Failed) outcome).reason()).isEqualTo(Reason.TLS);
    assertThat(receiver.requests()).isEmpty();
  }

  // The first header line is 8 KiB long with the two bytes that end it.
  @Test
  void headerLineAboveEightKibibytesIsResponseTooLarge() throws Exception {
    String withinTheLimit = "a".repeat(8 * KIB - "X-Padding: \r\n".length());
    receiver.answer("/within", FaultReceiver.status(200, "X-Padding", withinTheLimit));
    receiver.answer("/above", FaultReceiver.status(200, "X-Padding", withinTheLimit + "a"));

    assertThat(client.send(TENANT, ATTEMPT, url("/within"), Map.of(), BODY).outcome())
        .isEqualTo(new Response(200, null, InetAddress.getByName("127.0.0.1")));
    assertThat(client.send(TENANT, ATTEMPT, url("/above"), Map.of(), BODY).outcome())
        .isEqualTo(new Failed(Reason.RESPONSE_TOO_LARGE, InetAddress.getByName("127.0.0.1")));
  }

  // The script adds a Content-Length header to the ones that it is given.
  @Test
  void responseWithMoreThanAHundredHeadersIsResponseTooLarge() throws Exception {
    receiver.answer("/within", FaultReceiver.status(200, headers(99)));
    receiver.answer("/above", FaultReceiver.status(200, headers(100)));

    assertThat(client.send(TENANT, ATTEMPT, url("/within"), Map.of(), BODY).outcome())
        .isEqualTo(new Response(200, null, InetAddress.getByName("127.0.0.1")));
    assertThat(client.send(TENANT, ATTEMPT, url("/above"), Map.of(), BODY).outcome())
        .isEqualTo(new Failed(Reason.RESPONSE_TOO_LARGE, InetAddress.getByName("127.0.0.1")));
  }

  // The header line never ends, so only the limit can end the read. The client reports the head
  // as too large and not the deadline as passed, and it is the client that closes the
  // connection. By then the receiver has written what the buffers between the two took, and not
  // the gigabytes that fifteen seconds of a reading client would let it write.
  @Test
  void headerLineWithoutAnEndIsCutAtTheLimit() throws Exception {
    FaultReceiver.Endless endless = FaultReceiver.endlessHeader();
    receiver.answer("/hook", endless);

    assertThat(client.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY).outcome())
        .isEqualTo(new Failed(Reason.RESPONSE_TOO_LARGE, InetAddress.getByName("127.0.0.1")));

    await().until(endless::ended);
    assertThat(endless.peerClosedFirst()).isTrue();
    assertThat(endless.bytesWritten()).isLessThan(FaultReceiver.Endless.MORE_THAN_BUFFERS_TAKE);
  }

  static Stream<Arguments> bodiesWithoutAnEnd() {
    return Stream.of(
        arguments(named("until the connection closes", FaultReceiver.endlessBody())),
        arguments(named("in chunks", FaultReceiver.endlessChunkedBody())));
  }

  // The status line decides, and the body is not drained: the client reads up to its limit and
  // closes the connection, as it does under a header line without an end.
  @ParameterizedTest
  @MethodSource("bodiesWithoutAnEnd")
  void bodyWithoutAnEndIsCutAtTheLimitAndTheStatusStillCounts(FaultReceiver.Endless endless)
      throws Exception {
    receiver.answer("/hook", endless);

    assertThat(client.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY).outcome())
        .isEqualTo(new Response(200, null, InetAddress.getByName("127.0.0.1")));

    await().until(endless::ended);
    assertThat(endless.peerClosedFirst()).isTrue();
    assertThat(endless.bytesWritten()).isLessThan(FaultReceiver.Endless.MORE_THAN_BUFFERS_TAKE);
  }

  // A body of 64 KiB is read to its end, and its connection takes the next request. With one
  // byte more the client closes the connection under the body, and the next request needs a new
  // one. The status counts either way.
  @Test
  void bodyOfSixtyFourKibibytesIsReadAndOneByteMoreCostsTheConnection() throws Exception {
    receiver.answer("/within", FaultReceiver.body(64 * KIB));
    receiver.answer("/above", FaultReceiver.body(64 * KIB + 1));
    Response ok = new Response(200, null, InetAddress.getByName("127.0.0.1"));

    assertThat(client.send(TENANT, ATTEMPT, url("/within"), Map.of(), BODY).outcome())
        .isEqualTo(ok);
    assertThat(client.send(TENANT, ATTEMPT, url("/within"), Map.of(), BODY).outcome())
        .isEqualTo(ok);
    assertThat(receiver.acceptedConnections()).isEqualTo(1);

    assertThat(client.send(TENANT, ATTEMPT, url("/above"), Map.of(), BODY).outcome()).isEqualTo(ok);
    assertThat(client.send(TENANT, ATTEMPT, url("/within"), Map.of(), BODY).outcome())
        .isEqualTo(ok);
    assertThat(receiver.acceptedConnections()).isEqualTo(2);
  }

  @Test
  void requestThatGetsNoAnswerIsCutAtTheDeadline() throws Exception {
    try (FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer("/hook", hold);

      long start = System.nanoTime();
      Outcome outcome = impatient.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY).outcome();
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(outcome)
          .isEqualTo(new Failed(Reason.DEADLINE, InetAddress.getByName("127.0.0.1")));
      assertThat(took).isBetween(SHORT_DEADLINE, SHORT_DEADLINE.plusSeconds(5));
      assertThat(receiver.requests()).hasSize(1);
    }
  }

  // Every byte arrives well inside any timeout between two reads. Only a bound on the whole
  // request ends it: the response takes six times the deadline to arrive.
  @Test
  void responseThatDripsInIsCutAtTheDeadline() throws Exception {
    receiver.answer("/hook", FaultReceiver.drip(Duration.ofMillis(75)));

    long start = System.nanoTime();
    Outcome outcome = impatient.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY).outcome();
    Duration took = Duration.ofNanos(System.nanoTime() - start);

    assertThat(outcome).isEqualTo(new Failed(Reason.DEADLINE, InetAddress.getByName("127.0.0.1")));
    assertThat(took).isBetween(SHORT_DEADLINE, SHORT_DEADLINE.plusSeconds(5));
  }

  // The host has two addresses, and the listener behind them takes a connection and then says
  // nothing, so the TLS handshake of the client waits for a byte that never comes. The deadline
  // ends that wait. A client that held both addresses would then go on to the second one, where
  // nothing bounds it but the timeout of the handshake, three seconds after the deadline. This
  // one was given one address: the attempt ends at the deadline, and the listener saw one
  // connection.
  @Test
  void hostWithTwoAddressesIsConnectedAtOneOfThemAndCutAtTheDeadline() throws Exception {
    dns.answer("silent.example", "127.0.0.1", "127.0.0.1");
    try (SilentListener silent = SilentListener.start()) {
      URI https = URI.create("https://silent.example:%d/hook".formatted(silent.port()));

      long start = System.nanoTime();
      Outcome outcome = impatient.send(TENANT, ATTEMPT, https, Map.of(), BODY).outcome();
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(outcome).isEqualTo(new Failed(Reason.DEADLINE, null));
      assertThat(took).isBetween(SHORT_DEADLINE, HANDSHAKE_TIMEOUT);
      assertThat(silent.acceptedConnections()).isEqualTo(1);
    }
  }

  // The address literal of the URL goes through the same lookup as a name. This client has no
  // allowance, so the lookup is refused and the receiver sees no connection.
  @Test
  void addressThatThePolicyDeniesIsBlockedBeforeAnyConnection() {
    try (DnsLookups productionLookups = new DnsLookups(dns, new AddressPolicy(List.of()), meters);
        WebhookHttpClient production =
            new WebhookHttpClient(productionLookups, DEADLINE, CONNECTIONS, OpenTelemetry.noop())) {

      assertThat(production.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY).outcome())
          .isEqualTo(new Failed(Reason.BLOCKED, null));
    }
    assertThat(receiver.acceptedConnections()).isZero();
  }

  @Test
  void hostThatDoesNotResolveIsADnsFailure() {
    URI unknown = URI.create("http://unknown.example:%d/hook".formatted(receiver.port()));

    assertThat(client.send(TENANT, ATTEMPT, unknown, Map.of(), BODY).outcome())
        .isEqualTo(new Failed(Reason.DNS, null));
    assertThat(receiver.acceptedConnections()).isZero();
  }

  @Test
  void lookupThatDoesNotAnswerInTimeIsATimeout() {
    URI slow = URI.create("http://slow.example:%d/hook".formatted(receiver.port()));
    try (StubDns.Hold hold = dns.hold("slow.example");
        DnsLookups impatientLookups =
            new DnsLookups(
                dns, new AddressPolicy(List.of("127.0.0.1/32")), meters, Duration.ofMillis(20));
        WebhookHttpClient withImpatientLookups =
            new WebhookHttpClient(impatientLookups, DEADLINE, CONNECTIONS, OpenTelemetry.noop())) {

      assertThat(withImpatientLookups.send(TENANT, ATTEMPT, slow, Map.of(), BODY).outcome())
          .isEqualTo(new Failed(Reason.TIMEOUT, null));
    }
    assertThat(receiver.acceptedConnections()).isZero();
  }

  // The four lookups of the tenant that are stuck are made by the test. The request finds no
  // slot for its own, which is no failure of the endpoint, and another tenant is served.
  @Test
  void lookupThatIsNotAdmittedSendsNothingAndIsNoFailure() {
    receiver.answer("/hook", FaultReceiver.status(200));
    try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor();
        StubDns.Hold hold = dns.hold("stuck.example")) {
      for (int i = 0; i < LOOKUP_SLOTS_OF_A_TENANT; i++) {
        callers.submit(() -> lookups.lookup(TENANT, "stuck.example"));
      }
      await().until(() -> hold.waiting() == LOOKUP_SLOTS_OF_A_TENANT);

      assertThat(client.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY).outcome())
          .isEqualTo(new LookupBusy());
      assertThat(receiver.requests()).isEmpty();
      assertThat(client.send(UUID.randomUUID(), ATTEMPT, url("/hook"), Map.of(), BODY).outcome())
          .isInstanceOf(Response.class);
    }
  }

  // The stored context is the one of a sampled ingest span. The span of the request is its child,
  // of kind CLIENT, and the request carries the traceparent of that span and not the stored one.
  // It carries no other trace header. The URL has a marker in its path and in its query, and the
  // span holds the host and nothing else of it.
  @Test
  void spanOfARequestIsAChildOfTheStoredContextAndTheRequestCarriesItsTraceparent() {
    String path = "/hook-" + MARKER + "?token=" + MARKER;
    receiver.answer(path, FaultReceiver.status(204));
    UUID delivery = UUID.randomUUID();
    String traceOfTheIngest = "0af7651916cd43dd8448eb211c80319c";
    String spanOfTheIngest = "b7ad6b7169203331";
    AttemptSpan attempt =
        new AttemptSpan(
            delivery, 3, true, "00-%s-%s-01".formatted(traceOfTheIngest, spanOfTheIngest));

    Traced sent = traced.send(TENANT, attempt, url(path), Map.of(), BODY);

    assertThat(sent.outcome()).isInstanceOf(Response.class);
    assertThat(spans.getFinishedSpanItems()).hasSize(1);
    SpanData span = spans.getFinishedSpanItems().getFirst();
    assertThat(span.getSpanContext()).isEqualTo(sent.span());
    assertThat(span.getKind()).isEqualTo(SpanKind.CLIENT);
    assertThat(span.getTraceId()).isEqualTo(traceOfTheIngest);
    assertThat(span.getParentSpanId()).isEqualTo(spanOfTheIngest);
    assertThat(span.getSpanId()).isNotEqualTo(spanOfTheIngest);
    assertThat(span.getAttributes().asMap())
        .isEqualTo(
            Map.<AttributeKey<?>, Object>of(
                AttributeKey.stringKey("webhooks.delivery_id"),
                delivery.toString(),
                AttributeKey.longKey("webhooks.attempt"),
                3L,
                AttributeKey.booleanKey("webhooks.probe"),
                true,
                AttributeKey.stringKey("webhooks.outcome"),
                "success",
                AttributeKey.longKey("http.response.status_code"),
                204L,
                AttributeKey.stringKey("server.address"),
                "127.0.0.1"));
    // The text of a span holds what the span carries: the host is found in it.
    assertThat(span.toString()).contains("127.0.0.1").doesNotContain(MARKER);
    assertThat(span.getSpanContext().isSampled()).isTrue();
    FaultReceiver.Request request = receiver.requests().getFirst();
    assertThat(request.header("traceparent")).isEqualTo(traceparentOf(span.getSpanContext()));
    assertThat(request.headers())
        .doesNotContainKeys(
            "tracestate",
            "baggage",
            "b3",
            "x-b3-traceid",
            "x-b3-spanid",
            "x-b3-parentspanid",
            "x-b3-sampled",
            "x-b3-flags");
  }

  // An event that was stored without a trace context: each of its attempts is the root of a trace
  // of its own, and the request carries the traceparent of that root.
  @Test
  void spanOfARequestWithoutAStoredContextStartsATraceOfItsOwn() {
    receiver.answer("/hook", FaultReceiver.status(500));

    Traced sent = traced.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY);

    SpanData span = spans.getFinishedSpanItems().getFirst();
    assertThat(span.getSpanContext()).isEqualTo(sent.span());
    assertThat(span.getParentSpanContext().isValid()).isFalse();
    assertThat(span.getAttributes().get(AttributeKey.stringKey("webhooks.outcome")))
        .isEqualTo("failure");
    assertThat(span.getAttributes().get(AttributeKey.longKey("http.response.status_code")))
        .isEqualTo(500);
    assertThat(receiver.requests().getFirst().header("traceparent"))
        .isEqualTo(traceparentOf(span.getSpanContext()));
  }

  @Test
  void spanOfARequestThatGotNoResponseIsAFailureWithoutAStatus() throws IOException {
    URI closedPort = url("/hook");
    receiver.close();

    traced.send(TENANT, ATTEMPT, closedPort, Map.of(), BODY);

    SpanData span = spans.getFinishedSpanItems().getFirst();
    assertThat(span.getAttributes().get(AttributeKey.stringKey("webhooks.outcome")))
        .isEqualTo("failure");
    assertThat(span.getAttributes().get(AttributeKey.longKey("http.response.status_code")))
        .isNull();
  }

  // A stored context that says its span was not sampled: the span of the request is not
  // exported. The request still carries a traceparent, which says the same about that span.
  @Test
  void spanOfARequestIsNotExportedWhenTheStoredContextWasNotSampled() {
    receiver.answer("/hook", FaultReceiver.status(200));
    String traceOfTheIngest = "0af7651916cd43dd8448eb211c80319c";
    AttemptSpan attempt =
        new AttemptSpan(
            UUID.randomUUID(), 1, false, "00-%s-b7ad6b7169203331-00".formatted(traceOfTheIngest));

    Traced sent = traced.send(TENANT, attempt, url("/hook"), Map.of(), BODY);

    assertThat(spans.getFinishedSpanItems()).isEmpty();
    assertThat(sent.span().getTraceId()).isEqualTo(traceOfTheIngest);
    assertThat(sent.span().isSampled()).isFalse();
    assertThat(receiver.requests().getFirst().header("traceparent"))
        .isEqualTo(traceparentOf(sent.span()));
  }

  // The tenant has every lookup slot taken, so nothing is sent. The span ends all the same and
  // says that the request was not sent.
  @Test
  void spanOfARequestThatIsNotSentSaysSo() {
    try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor();
        StubDns.Hold hold = dns.hold("stuck.example")) {
      for (int i = 0; i < LOOKUP_SLOTS_OF_A_TENANT; i++) {
        callers.submit(() -> lookups.lookup(TENANT, "stuck.example"));
      }
      await().until(() -> hold.waiting() == LOOKUP_SLOTS_OF_A_TENANT);

      Traced notSent = traced.send(TENANT, ATTEMPT, url("/hook"), Map.of(), BODY);

      assertThat(notSent.outcome()).isEqualTo(new LookupBusy());
      assertThat(spans.getFinishedSpanItems()).hasSize(1);
      assertThat(
              spans
                  .getFinishedSpanItems()
                  .getFirst()
                  .getAttributes()
                  .get(AttributeKey.stringKey("webhooks.outcome")))
          .isEqualTo("not_sent");
    }
    assertThat(receiver.requests()).isEmpty();
  }

  // The header as the W3C writes it: the version, the two ids and the flags.
  private static String traceparentOf(SpanContext span) {
    return "00-%s-%s-%s"
        .formatted(span.getTraceId(), span.getSpanId(), span.getTraceFlags().asHex());
  }

  private URI url(String path) {
    return URI.create(receiver.url(path));
  }

  private static String[] headers(int count) {
    List<String> namesAndValues = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      namesAndValues.add("X-Header-" + i);
      namesAndValues.add("value");
    }
    return namesAndValues.toArray(String[]::new);
  }

  /**
   * Listens on the loopback address, takes every connection and keeps it open without reading from
   * it or writing to it.
   */
  private static final class SilentListener implements AutoCloseable {

    private final ServerSocket server;
    private final List<Socket> connections = new CopyOnWriteArrayList<>();

    private SilentListener(ServerSocket server) {
      this.server = server;
    }

    static SilentListener start() throws IOException {
      SilentListener listener =
          new SilentListener(new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1")));
      Thread.ofVirtual().start(listener::accept);
      return listener;
    }

    int port() {
      return server.getLocalPort();
    }

    int acceptedConnections() {
      return connections.size();
    }

    @Override
    public void close() throws IOException {
      server.close();
      for (Socket connection : connections) {
        connection.close();
      }
    }

    private void accept() {
      while (true) {
        try {
          connections.add(server.accept());
        } catch (IOException closed) {
          return;
        }
      }
    }
  }
}
