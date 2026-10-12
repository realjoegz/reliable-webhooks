package io.github.realjoegz.reliablewebhooks;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.SelfStartedReplica;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import io.github.realjoegz.reliablewebhooks.ops.QueueGauges;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * What a replica answers a scrape with. Every number that a test asserts is read from the answer of
 * {@code /actuator/prometheus} to a request over HTTP, which is what Prometheus reads.
 *
 * <p>The gauges of the queue count the whole queue, whatever its tenant. A test of this build
 * begins with no pending delivery, so the pending deliveries of a test are the ones that it made.
 */
@UnsafeLocalReplica
class MetricsIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";

  // An endpoint for deliveries that nobody sends: the worker of the replica does not run.
  private static final String NOT_SENT_TO = "http://127.0.0.1:8080/hook";

  private static final String STARTED_BY_SPRING = "replica-with-its-own-queue-gauges";
  private static final Duration HOUR = Duration.ofHours(1);

  // Every sample of the service that a scrape holds, with the names of its labels. A timer and a
  // summary come with a count, a sum and their largest value, and a histogram with its buckets.
  private static final Map<String, Set<String>> SAMPLES_OF_THE_DESIGN =
      Map.ofEntries(
          entry("webhooks_ingest_requests_total", Set.of("result")),
          entry("webhooks_delivery_attempts_total", Set.of("outcome", "reason")),
          entry("webhooks_delivery_released_total", Set.of("reason")),
          entry("webhooks_delivery_attempt_seconds_bucket", Set.of("outcome", "le")),
          entry("webhooks_delivery_attempt_seconds_count", Set.of("outcome")),
          entry("webhooks_delivery_attempt_seconds_sum", Set.of("outcome")),
          entry("webhooks_delivery_attempt_seconds_max", Set.of("outcome")),
          entry("webhooks_delivery_terminal_total", Set.of("state", "reason")),
          entry("webhooks_delivery_end_to_end_seconds_bucket", Set.of("le")),
          entry("webhooks_delivery_end_to_end_seconds_count", Set.of()),
          entry("webhooks_delivery_end_to_end_seconds_sum", Set.of()),
          entry("webhooks_delivery_end_to_end_seconds_max", Set.of()),
          entry("webhooks_claim_seconds_count", Set.of()),
          entry("webhooks_claim_seconds_sum", Set.of()),
          entry("webhooks_claim_seconds_max", Set.of()),
          entry("webhooks_claim_rows_count", Set.of()),
          entry("webhooks_claim_rows_sum", Set.of()),
          entry("webhooks_claim_rows_max", Set.of()),
          entry("webhooks_claim_errors_total", Set.of()),
          entry("webhooks_finalize_lost_lease_total", Set.of()),
          entry("webhooks_finalize_errors_total", Set.of()),
          entry("webhooks_poller_last_iteration_age_seconds", Set.of()),
          entry("webhooks_deliveries", Set.of("state")),
          entry("webhooks_oldest_due_age_seconds", Set.of()),
          entry("webhooks_circuits_open", Set.of()),
          entry("webhooks_dns_lookups_total", Set.of("result")),
          entry("webhooks_ssrf_allowlist_entries", Set.of()));

  // The routes that take an id in their path, by the pattern that Spring MVC matches a request
  // with, and the number of routes that share the pattern: an endpoint is read, patched and
  // deleted at one.
  private static final Map<String, Long> ROUTES_WITH_AN_ID =
      Map.of(
          "/v1/endpoints/{id}", 3L,
          "/v1/endpoints/{id}/secret/rotate", 1L,
          "/v1/endpoints/{id}/replay", 1L,
          "/v1/events/{id}", 1L,
          "/v1/deliveries/{id}", 1L,
          "/v1/deliveries/{id}/replay", 1L,
          "/admin/tenants/{id}", 1L,
          "/admin/tenants/{id}/api-key/rotate", 1L);

  private static final Pattern SAMPLE =
      Pattern.compile("(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(\\{(?<labels>.*)})? (?<value>\\S+)");
  private static final Pattern LABEL =
      Pattern.compile("(?<name>[a-zA-Z_][a-zA-Z0-9_]*)=\"(?<value>(?:[^\"\\\\]|\\\\.)*)\"");

  /** A line of a scrape that holds a value. */
  private record Sample(String name, Map<String, String> labels, double value) {}

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired StubDns dns;
  @Autowired QueueGauges queueGauges;
  @Autowired ApplicationContext replica;

  // The meters exist from the start of the replica, so a scrape holds each of them whether or
  // not anything was counted. The comparison is of the whole set: a sample of the service that
  // the design does not list fails it, and so does a label that the design does not give.
  @Test
  void scrapeHasEveryMeterOfTheDesignWithItsLabelNames() {
    List<Sample> samples = samples(scrape());

    Map<String, Set<Set<String>>> labelNames =
        samples.stream()
            .filter(sample -> sample.name().startsWith("webhooks_"))
            .collect(
                Collectors.groupingBy(
                    Sample::name,
                    Collectors.mapping(sample -> sample.labels().keySet(), Collectors.toSet())));
    Map<String, Set<Set<String>>> ofTheDesign =
        SAMPLES_OF_THE_DESIGN.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, sample -> Set.of(sample.getValue())));
    assertThat(labelNames).isEqualTo(ofTheDesign);

    assertThat(values(samples, "webhooks_ingest_requests_total", "result"))
        .containsExactlyInAnyOrder(
            "accepted", "replayed", "rate_limited", "unsatisfiable", "busy", "invalid");
    assertThat(values(samples, "webhooks_delivery_attempts_total", "outcome"))
        .containsExactlyInAnyOrder("success", "failure", "unknown");
    assertThat(values(samples, "webhooks_delivery_attempts_total", "reason"))
        .containsExactlyInAnyOrder(
            "http_2xx",
            "http_3xx",
            "http_4xx",
            "http_5xx",
            "timeout",
            "connect",
            "tls",
            "dns",
            "blocked",
            "deadline",
            "response_too_large",
            "lease_expired");
    assertThat(values(samples, "webhooks_delivery_released_total", "reason"))
        .containsExactlyInAnyOrder("gate_lost", "dns_busy");
    assertThat(values(samples, "webhooks_delivery_attempt_seconds_count", "outcome"))
        .containsExactlyInAnyOrder("success", "failure");
    // The attempt deadline that ships, and the 72 hours of a delivery.
    assertThat(largestBound(samples, "webhooks_delivery_attempt_seconds_bucket")).isEqualTo(15);
    assertThat(largestBound(samples, "webhooks_delivery_end_to_end_seconds_bucket"))
        .isEqualTo(Duration.ofHours(72).toSeconds());
    assertThat(
            samples.stream()
                .filter(sample -> sample.name().equals("webhooks_delivery_terminal_total"))
                .map(sample -> sample.labels().get("state") + " " + sample.labels().get("reason")))
        .containsExactlyInAnyOrder(
            "succeeded delivered", "dead exhausted", "dead expired", "cancelled endpoint_deleted");
    assertThat(values(samples, "webhooks_deliveries", "state"))
        .containsExactlyInAnyOrder("due", "scheduled", "leased", "held");
    assertThat(values(samples, "webhooks_dns_lookups_total", "result"))
        .containsExactlyInAnyOrder("ok", "timeout", "busy", "failed", "blocked");
  }

  // The meters of a replica are scraped. Nothing pushes them anywhere: the registry of the
  // endpoint is the only one that the replica has.
  @Test
  void prometheusRegistryIsTheOnlyRegistryOfAReplica() {
    assertThat(replica.getBeansOfType(MeterRegistry.class).values())
        .singleElement()
        .isInstanceOf(PrometheusMeterRegistry.class);
  }

  // The URL of the subscriber has a marker in its path and in its query, and its host is a name
  // that only this test uses. The delivery fails once and then succeeds, so that the meters of a
  // failure and of a success have counted it. Every route that takes an id in its path is called
  // with one, and the routes of the replica itself say that these are all of them. The scrape
  // after all that holds none of it: not the marker, not the host, and not the id of the tenant,
  // the endpoint, the event or the delivery.
  //
  // The counts are the witness that the scrape is the one that would hold them: the attempts of
  // the delivery and its end are counted in it, and so is the request to each of those routes,
  // under the pattern of its route.
  @Test
  void noMeterHasHostOrUrlLabel() throws Exception {
    String marker = "marker" + UUID.randomUUID().toString().replace("-", "");
    String host = "subscriber-" + UUID.randomUUID() + ".test";
    dns.answer(host, "127.0.0.1");
    try (FaultReceiver receiver = FaultReceiver.start()) {
      String path = "/hook-" + marker + "?token=" + marker;
      receiver.answerOnce(path, FaultReceiver.status(503));
      receiver.answer(path, FaultReceiver.status(200));
      TestTenant tenant = TestTenant.create(http, "no-url-in-a-meter");
      UUID endpoint =
          tenant.register(http, "http://%s:%d%s".formatted(host, receiver.port(), path), "*");
      List<Sample> before = samples(scrape());
      worker.start();

      UUID event = tenant.accept(http, EVENT);
      UUID delivery = Rows.deliveryOf(event);
      await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(1));
      Rows.ageDelivery(delivery, Duration.ofSeconds(10));
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));
      worker.stop();
      read(tenant, "/v1/endpoints/{id}", endpoint);
      read(tenant, "/v1/events/{id}", event);
      read(tenant, "/v1/deliveries/{id}", delivery);
      patchEventTypes(tenant, endpoint);
      tenant.rotateSecret(http, endpoint).expectStatus().isOk();
      tenant.replayDead(http, endpoint, 1).expectStatus().isOk();
      // The delivery has succeeded, so its replay is refused. The route has handled it.
      tenant.replay(http, delivery).expectStatus().isEqualTo(HttpStatus.CONFLICT);
      tenant.patch(http, Map.of("burst", 100));
      tenant.delete(http, endpoint).expectStatus().isNoContent();
      rotateApiKey(tenant);
      queueGauges.run();

      String scrape = scrape();
      List<Sample> after = samples(scrape);
      assertThat(receiver.requests()).hasSize(2);
      assertThat(increase(before, after, "webhooks_delivery_attempts_total", "reason", "http_5xx"))
          .isEqualTo(1);
      assertThat(increase(before, after, "webhooks_delivery_attempts_total", "reason", "http_2xx"))
          .isEqualTo(1);
      assertThat(increase(before, after, "webhooks_delivery_terminal_total", "state", "succeeded"))
          .isEqualTo(1);
      assertThat(increase(before, after, "webhooks_delivery_end_to_end_seconds_count"))
          .isEqualTo(1);
      assertThat(routesWithAnId()).isEqualTo(ROUTES_WITH_AN_ID);
      assertThat(ROUTES_WITH_AN_ID)
          .allSatisfy(
              (pattern, routes) ->
                  assertThat(requests(after, pattern) - requests(before, pattern))
                      .as(pattern)
                      .isEqualTo(routes.doubleValue()));

      assertThat(scrape)
          .doesNotContain(marker)
          .doesNotContain(host)
          .doesNotContain(tenant.id().toString())
          .doesNotContain(endpoint.toString())
          .doesNotContain(event.toString())
          .doesNotContain(delivery.toString());
      // The client of the service is not instrumented, so no meter is made from its requests.
      assertThat(after).noneMatch(sample -> sample.name().startsWith("http_client"));
    }
  }

  // The deliveries of one endpoint in every state that a pending delivery can be in, and the
  // deliveries of endpoints that no claim serves. The worker of the replica does not run, so
  // each delivery stays as the test left it, and a run of the gauges reads them.
  //
  // A claim could take three: one that was never claimed, one that waited an hour for it, and
  // one whose lease ran out with nothing recorded. One is scheduled an hour ahead, and one is
  // under a lease. Three are held: two behind an open circuit whose probe is not due, and one of
  // a suspended tenant. The delivery of a deleted endpoint is none of these, and the open
  // circuit of that endpoint is not counted.
  //
  // Two more deliveries of the first endpoint are no longer pending, and the gauges leave them
  // out. One has succeeded and keeps the end of its last lease, an hour ahead, as its due time.
  // The other is dead, and its due time has passed. Counted, they would be scheduled and due.
  //
  // Then the probe of the open circuit comes due. A claim could take a delivery of that endpoint
  // again, and the gauges say so: its two deliveries are due and no longer held.
  @Test
  void queueGaugesCountPendingDeliveriesByWhatAClaimWouldDoWithThem() {
    TestTenant tenant = TestTenant.create(http, "queue-gauges");
    tenant.register(http, NOT_SENT_TO, "*");
    UUID neverClaimed = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID dueForAnHour = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID abandoned = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID scheduled = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID leased = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID succeeded = Rows.deliveryOf(tenant.accept(http, EVENT));
    UUID dead = Rows.deliveryOf(tenant.accept(http, EVENT));
    Rows.ageDelivery(dueForAnHour, HOUR);
    Rows.postponeDelivery(scheduled, HOUR);
    lease(abandoned, Duration.ofSeconds(-1));
    lease(leased, HOUR);
    Rows.postponeDelivery(succeeded, HOUR);
    end(succeeded, "succeeded", null);
    end(dead, "dead", "exhausted");

    TestTenant behindACircuit = TestTenant.create(http, "queue-gauges-open-circuit");
    UUID failing = behindACircuit.register(http, NOT_SENT_TO, "*");
    behindACircuit.accept(http, EVENT);
    behindACircuit.accept(http, EVENT);
    double openBefore = value(samples(scrapeAfterARun()), "webhooks_circuits_open");
    openCircuit(failing, HOUR);

    TestTenant suspended = TestTenant.create(http, "queue-gauges-suspended");
    suspended.register(http, NOT_SENT_TO, "*");
    suspended.accept(http, EVENT);
    suspended.patch(http, Map.of("status", "suspended"));

    TestTenant withADeletedEndpoint = TestTenant.create(http, "queue-gauges-deleted");
    UUID deleted = withADeletedEndpoint.register(http, NOT_SENT_TO, "*");
    withADeletedEndpoint.accept(http, EVENT);
    openCircuit(deleted, HOUR);
    withADeletedEndpoint.delete(http, deleted).expectStatus().isNoContent();

    List<Sample> held = samples(scrapeAfterARun());

    assertThat(value(held, "webhooks_deliveries", "state", "due")).isEqualTo(3);
    assertThat(value(held, "webhooks_deliveries", "state", "scheduled")).isEqualTo(1);
    assertThat(value(held, "webhooks_deliveries", "state", "leased")).isEqualTo(1);
    assertThat(value(held, "webhooks_deliveries", "state", "held")).isEqualTo(3);
    assertThat(value(held, "webhooks_circuits_open") - openBefore).isEqualTo(1);
    // The oldest of the three became due an hour ago, and the test has taken some time since.
    assertThat(value(held, "webhooks_oldest_due_age_seconds"))
        .isCloseTo(HOUR.toSeconds(), within(60.0));
    assertThat(Rows.delivery(neverClaimed).state()).isEqualTo("pending");

    Rows.ageCircuit(failing, HOUR);
    List<Sample> probeDue = samples(scrapeAfterARun());

    assertThat(value(probeDue, "webhooks_deliveries", "state", "due")).isEqualTo(5);
    assertThat(value(probeDue, "webhooks_deliveries", "state", "held")).isEqualTo(1);
    assertThat(value(probeDue, "webhooks_circuits_open") - openBefore).isEqualTo(1);
  }

  // Every other test calls the run of the gauges itself. Here a second replica starts as the
  // application does, and its first run comes with the start. That replica runs its worker too,
  // so the delivery belongs to a suspended tenant: no claim takes it, and the gauges of that
  // replica show it as held.
  @Test
  void replicaReadsTheQueueForItsGaugesByItself() {
    TestTenant tenant = TestTenant.create(http, "gauges-of-a-replica");
    tenant.register(http, NOT_SENT_TO, "*");
    tenant.accept(http, EVENT);
    tenant.patch(http, Map.of("status", "suspended"));

    try (ConfigurableApplicationContext started = SelfStartedReplica.start(STARTED_BY_SPRING)) {
      RestTestClient itsApi = SelfStartedReplica.clientOf(started);

      await()
          .untilAsserted(
              () ->
                  assertThat(value(samples(scrape(itsApi)), "webhooks_deliveries", "state", "held"))
                      .isEqualTo(1));
    }
  }

  private void read(TestTenant tenant, String route, UUID id) {
    http.get()
        .uri(route, id)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .exchange()
        .expectStatus()
        .isOk();
  }

  private void patchEventTypes(TestTenant tenant, UUID endpoint) {
    http.patch()
        .uri("/v1/endpoints/{id}", endpoint)
        .headers(headers -> headers.setBearerAuth(tenant.apiKey()))
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("event_types", List.of("order.created")))
        .exchange()
        .expectStatus()
        .isNoContent();
  }

  private void rotateApiKey(TestTenant tenant) {
    http.post()
        .uri("/admin/tenants/{id}/api-key/rotate", tenant.id())
        .headers(headers -> headers.setBearerAuth(ReplicaSettings.ADMIN_TOKEN))
        .exchange()
        .expectStatus()
        .isOk();
  }

  // The routes of the replica with a variable in their path, counted by their pattern.
  private Map<String, Long> routesWithAnId() {
    return replica
        .getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class)
        .getHandlerMethods()
        .keySet()
        .stream()
        .flatMap(route -> route.getPatternValues().stream())
        .filter(pattern -> pattern.contains("{"))
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
  }

  private String scrapeAfterARun() {
    queueGauges.run();
    return scrape();
  }

  private String scrape() {
    return scrape(http);
  }

  // Without a key, as Prometheus asks.
  private static String scrape(RestTestClient api) {
    return api.get()
        .uri("/actuator/prometheus")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(String.class)
        .returnResult()
        .getResponseBody();
  }

  private static List<Sample> samples(String scrape) {
    List<Sample> samples = new ArrayList<>();
    for (String line : scrape.lines().filter(line -> !line.startsWith("#")).toList()) {
      Matcher sample = SAMPLE.matcher(line);
      assertThat(sample.matches()).as(line).isTrue();
      Map<String, String> labels = new LinkedHashMap<>();
      if (sample.group("labels") != null) {
        Matcher label = LABEL.matcher(sample.group("labels"));
        while (label.find()) {
          labels.put(label.group("name"), label.group("value"));
        }
      }
      samples.add(
          new Sample(sample.group("name"), labels, Double.parseDouble(sample.group("value"))));
    }
    return samples;
  }

  private static List<String> values(List<Sample> samples, String name, String label) {
    return samples.stream()
        .filter(sample -> sample.name().equals(name))
        .map(sample -> sample.labels().get(label))
        .distinct()
        .toList();
  }

  // The sum over the samples of that name, with that label where one is given.
  private static double value(List<Sample> samples, String name, String... label) {
    List<Sample> named =
        samples.stream()
            .filter(sample -> sample.name().equals(name))
            .filter(sample -> label.length == 0 || label[1].equals(sample.labels().get(label[0])))
            .toList();
    assertThat(named).as(name).isNotEmpty();
    return named.stream().mapToDouble(Sample::value).sum();
  }

  private static double increase(
      List<Sample> before, List<Sample> after, String name, String... label) {
    return value(after, name, label) - value(before, name, label);
  }

  // The requests that a scrape counts under the pattern of a route, whatever their method and
  // their status. A route that has had no request yet has no sample.
  private static double requests(List<Sample> samples, String pattern) {
    return samples.stream()
        .filter(sample -> sample.name().equals("http_server_requests_seconds_count"))
        .filter(sample -> pattern.equals(sample.labels().get("uri")))
        .mapToDouble(Sample::value)
        .sum();
  }

  // The bound of the last bucket of a histogram before the one that takes every value.
  private static double largestBound(List<Sample> samples, String buckets) {
    List<String> bounds = values(samples, buckets, "le");
    assertThat(bounds).contains("+Inf");
    return bounds.stream()
        .filter(bound -> !bound.equals("+Inf"))
        .mapToDouble(Double::parseDouble)
        .max()
        .orElseThrow();
  }

  // What a claim leaves in the row of a delivery, with a lease that ends that long from now.
  private static void lease(UUID delivery, Duration endsIn) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE delivery
            SET attempts = attempts + 1, lease_token = gen_random_uuid(),
                next_attempt_at = now() + make_interval(secs => :seconds)
            WHERE id = :id
            """)
        .param("seconds", endsIn.toNanos() / 1e9)
        .param("id", delivery)
        .update();
  }

  // What a finalize leaves in the row of a delivery that it ends. The due time stays as it was.
  private static void end(UUID delivery, String state, String deadReason) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE delivery
            SET state = :state, dead_reason = :dead_reason, lease_token = NULL, completed_at = now()
            WHERE id = :id
            """)
        .param("state", state)
        .param("dead_reason", deadReason)
        .param("id", delivery)
        .update();
  }

  // What the fifth failure in a row leaves in the circuit of an endpoint, with the first probe
  // that long from now.
  private static void openCircuit(UUID endpoint, Duration probeIn) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE endpoint_circuit
            SET state = 'open', consecutive_failures = 5, opened_at = now(),
                next_probe_at = now() + make_interval(secs => :seconds)
            WHERE endpoint_id = :id
            """)
        .param("seconds", probeIn.toNanos() / 1e9)
        .param("id", endpoint)
        .update();
  }
}
