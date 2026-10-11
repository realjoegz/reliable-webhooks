package io.github.realjoegz.reliablewebhooks.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Response;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.ExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.json.JsonMapper;

@InProcessReplica
class SsrfIT {

  private static final String PUBLIC_ADDRESS = "93.184.216.34";
  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final int MAX_UNRETURNED_LOOKUPS_PER_TENANT = 4;
  private static final Duration DNS_WAIT = Duration.ofSeconds(2);
  private static final Duration ANSWER_MARGIN = Duration.ofSeconds(8);

  private record Timed(ExchangeResult result, Duration took) {}

  private static final List<String> NON_PUBLIC_ADDRESS_LITERALS =
      List.of(
          "0.0.0.0",
          "10.0.0.5",
          "100.64.0.1",
          "127.0.0.1",
          "169.254.169.254",
          "172.16.0.1",
          "192.168.1.10",
          "224.0.0.1",
          "255.255.255.255",
          "[::1]",
          "[::ffff:127.0.0.1]",
          "[fd00:ec2::254]",
          "[fe80::1]",
          "[64:ff9b::7f00:1]",
          "[2001:db8::1]");

  @Autowired RestTestClient http;
  @Autowired StubDns dns;
  @Autowired MeterRegistry meters;
  @Autowired JsonMapper json;
  @Autowired ReplicaWorker worker;
  @Autowired WebhooksProperties properties;

  @Autowired
  @Qualifier("apiDataSource")
  HikariDataSource api;

  // The first registration is the control: the same tenant, route and port are accepted for a
  // host with a public address. Every refusal is compared with one reference answer, so a
  // registration cannot tell a private address from a host that does not exist.
  @Test
  void registrationRejectsPrivateTargets() {
    TestTenant tenant = TestTenant.create(http, "private-targets");
    String publicHost = host("public");
    dns.answer(publicHost, PUBLIC_ADDRESS, "2606:4700:4700::1111");
    assertThat(register(tenant, publicHost).getStatus()).isEqualTo(HttpStatus.CREATED);

    String unknownHost = host("unknown");
    double failedBefore = lookups("failed");
    ExchangeResult reference = register(tenant, unknownHost);
    assertRefusedAsUrlNotAllowed(reference);
    assertThat(lookups("failed") - failedBefore).isEqualTo(1);

    for (String literal : NON_PUBLIC_ADDRESS_LITERALS) {
      assertBlockedLike(reference, tenant, literal);
    }

    String privateHost = host("private");
    dns.answer(privateHost, "10.1.2.3");
    assertBlockedLike(reference, tenant, privateHost);

    String loopbackHost = host("loopback");
    dns.answer(loopbackHost, "::1");
    assertBlockedLike(reference, tenant, loopbackHost);

    String mixedHost = host("mixed");
    dns.answer(mixedHost, PUBLIC_ADDRESS, "192.168.1.10");
    assertBlockedLike(reference, tenant, mixedHost);

    String movingHost = host("moving");
    dns.answerOnce(movingHost, PUBLIC_ADDRESS);
    dns.answer(movingHost, "127.0.0.1");
    assertThat(register(tenant, movingHost).getStatus()).isEqualTo(HttpStatus.CREATED);
    assertBlockedLike(reference, tenant, movingHost);

    assertThat(registeredUrlsOf(tenant))
        .containsExactlyInAnyOrder(urlOf(publicHost), urlOf(movingHost));
  }

  // The resolver never answers for one host. The four registrations for it are refused while
  // their lookups are still stuck, and the lookups keep the slots of the tenant after that,
  // until the resolver lets them return.
  @Test
  void registrationDnsLookupIsBounded() throws Exception {
    TestTenant tenant = TestTenant.create(http, "stuck-dns");
    TestTenant other = TestTenant.create(http, "other-than-stuck-dns");
    String stuckHost = host("stuck");
    String answeringHost = host("answering");
    dns.answer(answeringHost, PUBLIC_ADDRESS);
    ExchangeResult unresolvable = register(other, host("unknown"));
    double busyBefore = lookups("busy");
    double timeoutBefore = lookups("timeout");

    try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor();
        StubDns.Hold hold = dns.hold(stuckHost)) {
      List<Future<Timed>> stuck = new ArrayList<>();
      for (int i = 0; i < MAX_UNRETURNED_LOOKUPS_PER_TENANT; i++) {
        stuck.add(callers.submit(() -> timed(() -> register(tenant, stuckHost))));
      }
      await()
          .pollInterval(Duration.ofMillis(10))
          .until(() -> hold.waiting() == MAX_UNRETURNED_LOOKUPS_PER_TENANT);
      assertThat(api.getHikariPoolMXBean().getActiveConnections()).isZero();

      assertLookupBusy(register(tenant, answeringHost));
      // The witness that the pool was read and the fifth registration was refused while the four
      // were still waiting. The service answers them by itself after 2 s, hence the short poll.
      assertThat(stuck).noneMatch(Future::isDone);
      assertThat(register(other, answeringHost).getStatus()).isEqualTo(HttpStatus.CREATED);

      // The hold is closed only after this loop. A service that waited for its resolver without
      // a limit would never answer here, and the bound makes that a failure and not a hang.
      for (Future<Timed> registration : stuck) {
        Timed refused =
            registration.get(DNS_WAIT.plus(ANSWER_MARGIN).toMillis(), TimeUnit.MILLISECONDS);
        assertRefusedAsUrlNotAllowed(refused.result());
        assertThat(body(refused.result())).isEqualTo(body(unresolvable));
        assertThat(refused.took()).isBetween(DNS_WAIT, DNS_WAIT.plus(ANSWER_MARGIN));
      }
      assertThat(hold.waiting()).isEqualTo(MAX_UNRETURNED_LOOKUPS_PER_TENANT);
      assertLookupBusy(register(tenant, answeringHost));

      assertThat(lookups("timeout") - timeoutBefore).isEqualTo(MAX_UNRETURNED_LOOKUPS_PER_TENANT);
      assertThat(lookups("busy") - busyBefore).isEqualTo(2);
      assertThat(registeredUrlsOf(tenant)).isEmpty();
    }

    await()
        .untilAsserted(
            () ->
                assertThat(register(tenant, answeringHost).getStatus())
                    .isEqualTo(HttpStatus.CREATED));
  }

  // The replica of this class has no allowance for the loopback address, where the receiver
  // listens. The endpoint is registered while its host resolves to a public address, and by the
  // time of the delivery the host resolves to the receiver. The lookup that the connection
  // would use is refused, so no connection is made.
  //
  // The last step is the control for the count of zero: a connection from the test to the
  // address that the host resolves to is counted by the same listener.
  @Test
  void rebindingIsBlockedAtConnect() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      TestTenant tenant = TestTenant.create(http, "rebinding");
      String host = host("rebinding");
      dns.answerOnce(host, PUBLIC_ADDRESS);
      dns.answer(host, "127.0.0.1");
      tenant.register(http, "https://%s:%d/hook".formatted(host, receiver.port()), "*");
      double blockedBefore = lookups("blocked");
      worker.start();

      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(1));

      assertThat(Rows.attempts(delivery))
          .containsExactly(
              new Rows.Attempt(
                  0,
                  1,
                  false,
                  "failure",
                  null,
                  "destination_not_allowed",
                  null,
                  ReplicaSettings.IN_PROCESS_REPLICA));
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 1, 0, false, null, false));
      assertThat(lookups("blocked") - blockedBefore).isEqualTo(1);
      assertThat(receiver.acceptedConnections()).isZero();

      try (Socket fromTheTest = new Socket(InetAddress.getByName("127.0.0.1"), receiver.port())) {
        await().until(() -> receiver.acceptedConnections() == 1);
      }
      assertThat(receiver.requests()).isEmpty();
    }
  }

  // The client of this test is built like the one of the service, with the loopback address
  // allowed so that it can reach the receiver. The receiver answers with a redirect to another
  // of its paths. The client reports the 302, and the receiver has seen one request.
  @Test
  void redirectIsNotFollowed() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        DnsLookups lookups =
            new DnsLookups(
                dns, new AddressPolicy(List.of("127.0.0.1/32")), new SimpleMeterRegistry());
        WebhookHttpClient client = new WebhookHttpClient(lookups, properties)) {
      receiver.answer("/moved", FaultReceiver.redirect(receiver.url("/target")));
      receiver.answer("/target", FaultReceiver.status(200));

      WebhookHttpClient.Outcome outcome =
          client.send(
              UUID.randomUUID(),
              URI.create(receiver.url("/moved")),
              Map.of(),
              EVENT.getBytes(StandardCharsets.UTF_8));

      assertThat(outcome).isEqualTo(new Response(302, null, InetAddress.getByName("127.0.0.1")));
      assertThat(receiver.requests())
          .extracting(FaultReceiver.Request::path)
          .containsExactly("/moved");
    }
  }

  private void assertBlockedLike(ExchangeResult reference, TestTenant tenant, String host) {
    double blockedBefore = lookups("blocked");

    ExchangeResult refused = register(tenant, host);

    assertThat(refused.getStatus()).as(host).isEqualTo(reference.getStatus());
    assertThat(refused.getResponseHeaders().getContentType())
        .as(host)
        .isEqualTo(reference.getResponseHeaders().getContentType());
    assertThat(body(refused)).as(host).isEqualTo(body(reference));
    assertThat(lookups("blocked") - blockedBefore).as(host).isEqualTo(1);
  }

  private void assertRefusedAsUrlNotAllowed(ExchangeResult result) {
    assertThat(result.getStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
    assertThat(result.getResponseHeaders().getContentType())
        .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    assertThat(problemTypeOf(result)).isEqualTo("endpoint_url_not_allowed");
  }

  private void assertLookupBusy(ExchangeResult result) {
    assertThat(result.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    assertThat(result.getResponseHeaders().getContentType())
        .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    assertThat(result.getResponseHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("2");
    assertThat(problemTypeOf(result)).isEqualTo("lookup_busy");
  }

  private String problemTypeOf(ExchangeResult result) {
    return json.readTree(result.getResponseBodyContent()).get("type").asString();
  }

  private ExchangeResult register(TestTenant tenant, String host) {
    return http.post()
        .uri("/v1/endpoints")
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("url", urlOf(host), "event_types", List.of("*")))
        .exchange()
        .returnResult();
  }

  private static Timed timed(Supplier<ExchangeResult> request) {
    long start = System.nanoTime();
    ExchangeResult result = request.get();
    return new Timed(result, Duration.ofNanos(System.nanoTime() - start));
  }

  private double lookups(String result) {
    return meters.get("webhooks.dns.lookups").tag("result", result).counter().count();
  }

  // A name of its own for every test run, because the replica and its resolver are shared.
  private static String host(String label) {
    return "%s-%s.ssrf.example".formatted(label, UUID.randomUUID());
  }

  private static String urlOf(String host) {
    return "https://" + host + ":8443/hook";
  }

  private static String body(ExchangeResult result) {
    return new String(result.getResponseBodyContent(), StandardCharsets.UTF_8);
  }

  private static List<String> registeredUrlsOf(TestTenant tenant) {
    return TestDatabase.jdbc()
        .sql("SELECT url FROM endpoint WHERE tenant_id = :t")
        .param("t", tenant.id())
        .query(String.class)
        .list();
  }
}
