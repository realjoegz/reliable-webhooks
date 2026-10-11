package io.github.realjoegz.reliablewebhooks.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.http.DnsLookups.Refusal;
import io.github.realjoegz.reliablewebhooks.http.DnsLookups.Resolved;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DnsLookupsTest {

  private static final String HOST = "hooks.example.com";
  private static final int THREADS = 32;
  private static final int SLOTS_OF_A_TENANT = 4;
  private static final UUID TENANT = UUID.randomUUID();

  private final StubDns dns = new StubDns();
  private final MeterRegistry meters = new SimpleMeterRegistry();
  private final DnsLookups lookups = new DnsLookups(dns, new AddressPolicy(), meters);

  // A lookup that is held never answers, however long the wait. The short wait only keeps the
  // tests of that case short.
  private final DnsLookups impatient =
      new DnsLookups(dns, new AddressPolicy(), meters, Duration.ofMillis(20));

  private final ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor();

  @AfterEach
  void stopThreads() {
    callers.shutdownNow();
    lookups.close();
    impatient.close();
  }

  @Test
  void hostWithAllowedAddressesResolvesToAllOfThem() throws UnknownHostException {
    dns.answer(HOST, "93.184.216.34", "2606:4700:4700::1111");

    assertThat(lookups.lookup(TENANT, HOST))
        .isEqualTo(
            new Resolved(
                List.of(
                    InetAddress.getByName("93.184.216.34"),
                    InetAddress.getByName("2606:4700:4700::1111"))));
  }

  @Test
  void hostThatDoesNotResolveIsFailed() {
    assertThat(lookups.lookup(TENANT, HOST)).isEqualTo(Refusal.FAILED);
  }

  @Test
  void answerWithoutAnAddressIsFailed() {
    try (DnsLookups empty =
        new DnsLookups(host -> new InetAddress[0], new AddressPolicy(), meters)) {
      assertThat(empty.lookup(TENANT, HOST)).isEqualTo(Refusal.FAILED);
    }
  }

  @Test
  void faultOfTheResolverIsThrownAndNotCountedAsAFailedLookup() {
    IllegalArgumentException fault = new IllegalArgumentException("resolver fault");
    HostResolver faulty =
        host -> {
          throw fault;
        };

    try (DnsLookups broken = new DnsLookups(faulty, new AddressPolicy(), meters)) {
      assertThatThrownBy(() -> broken.lookup(TENANT, HOST))
          .isInstanceOf(IllegalStateException.class)
          .cause()
          .isSameAs(fault);
    }
    assertThat(count("failed")).isZero();
  }

  @Test
  void oneDeniedAddressBlocksTheWholeAnswer() {
    dns.answer(HOST, "93.184.216.34", "10.0.0.1");

    assertThat(lookups.lookup(TENANT, HOST)).isEqualTo(Refusal.BLOCKED);
  }

  @ParameterizedTest
  @ValueSource(strings = {"93.184.216.34", "[2606:4700:4700::1111]"})
  void systemResolverAnswersAnAllowedAddressLiteralWithItself(String literal)
      throws UnknownHostException {
    try (DnsLookups system = systemLookups()) {
      assertThat(system.lookup(TENANT, literal))
          .isEqualTo(new Resolved(List.of(InetAddress.getByName(literal))));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"127.0.0.1", "169.254.169.254", "[::1]", "[::ffff:10.0.0.1]"})
  void systemResolverDoesNotLetADeniedAddressLiteralPast(String literal) {
    try (DnsLookups system = systemLookups()) {
      assertThat(system.lookup(TENANT, literal)).isEqualTo(Refusal.BLOCKED);
    }
  }

  @Test
  void lookupThatDoesNotAnswerInsideTheWaitIsATimeout() {
    try (StubDns.Hold hold = dns.hold(HOST)) {
      assertThat(impatient.lookup(TENANT, HOST)).isEqualTo(Refusal.TIMEOUT);
    }
  }

  @Test
  void fifthUnreturnedLookupOfATenantIsBusyWhileAnotherTenantIsAdmitted() {
    dns.answer("other.example.com", "93.184.216.34");

    try (StubDns.Hold hold = dns.hold(HOST)) {
      for (int i = 0; i < SLOTS_OF_A_TENANT; i++) {
        callers.submit(() -> lookups.lookup(TENANT, HOST));
      }
      await().until(() -> hold.waiting() == SLOTS_OF_A_TENANT);

      assertThat(lookups.lookup(TENANT, "other.example.com")).isEqualTo(Refusal.BUSY);
      assertThat(lookups.lookup(UUID.randomUUID(), "other.example.com"))
          .isInstanceOf(Resolved.class);
    }
  }

  // The four callers have their answer, a timeout. The lookups themselves have not returned.
  @Test
  void lookupWhoseCallerGaveUpKeepsItsSlotUntilItReturns() {
    dns.answer(HOST, "93.184.216.34");

    try (StubDns.Hold hold = dns.hold(HOST)) {
      for (int i = 0; i < SLOTS_OF_A_TENANT; i++) {
        assertThat(impatient.lookup(TENANT, HOST)).isEqualTo(Refusal.TIMEOUT);
      }

      assertThat(impatient.lookup(TENANT, HOST)).isEqualTo(Refusal.BUSY);
    }

    await().until(() -> impatient.lookup(TENANT, HOST) instanceof Resolved);
  }

  @Test
  void lookupBeyondTheThreadsIsBusyForATenantWithFreeSlots() {
    dns.answer("other.example.com", "93.184.216.34");

    try (StubDns.Hold hold = dns.hold(HOST)) {
      for (int i = 0; i < THREADS; i++) {
        UUID tenant = new UUID(0, i / SLOTS_OF_A_TENANT);
        callers.submit(() -> lookups.lookup(tenant, HOST));
      }
      await().until(() -> hold.waiting() == THREADS);

      assertThat(lookups.lookup(UUID.randomUUID(), "other.example.com")).isEqualTo(Refusal.BUSY);
    }

    await().until(() -> lookups.lookup(UUID.randomUUID(), "other.example.com") instanceof Resolved);
  }

  @Test
  void everyLookupIsCountedOnceByItsResult() {
    dns.answer("public.example.com", "93.184.216.34");
    dns.answer("private.example.com", "10.0.0.1");

    lookups.lookup(TENANT, "public.example.com");
    lookups.lookup(TENANT, "private.example.com");
    lookups.lookup(TENANT, "unknown.example.com");
    try (StubDns.Hold hold = dns.hold(HOST)) {
      for (int i = 0; i < SLOTS_OF_A_TENANT; i++) {
        impatient.lookup(TENANT, HOST);
      }
      impatient.lookup(TENANT, HOST);
    }

    assertThat(count("ok")).isEqualTo(1);
    assertThat(count("blocked")).isEqualTo(1);
    assertThat(count("failed")).isEqualTo(1);
    assertThat(count("timeout")).isEqualTo(SLOTS_OF_A_TENANT);
    assertThat(count("busy")).isEqualTo(1);
  }

  private DnsLookups systemLookups() {
    return new DnsLookups(new SystemHostResolver(), new AddressPolicy(), meters);
  }

  private double count(String result) {
    return meters.get("webhooks.dns.lookups").tag("result", result).counter().count();
  }
}
