package io.github.realjoegz.reliablewebhooks.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.endpoint.UrlPolicy.Verdict;
import io.github.realjoegz.reliablewebhooks.harness.StubDns;
import io.github.realjoegz.reliablewebhooks.http.AddressPolicy;
import io.github.realjoegz.reliablewebhooks.http.DnsLookups;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UrlPolicyTest {

  private static final String HOST = "hooks.example.com";
  private static final UUID TENANT = UUID.randomUUID();

  private final StubDns dns = new StubDns();
  private final MeterRegistry meters = new SimpleMeterRegistry();
  private final DnsLookups lookups = new DnsLookups(dns, new AddressPolicy(), meters);
  private final UrlPolicy policy = new UrlPolicy(lookups);

  // The host of nearly every URL below is public, so a URL is refused for its form alone.
  @BeforeEach
  void hostResolvesToAPublicAddress() {
    dns.answer(HOST, "93.184.216.34");
  }

  @AfterEach
  void stopLookupThreads() {
    lookups.close();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://hooks.example.com",
        "https://hooks.example.com/",
        "https://hooks.example.com/hooks/7?token=abc#section",
        "https://hooks.example.com:443/hook",
        "https://hooks.example.com:1024/hook",
        "https://hooks.example.com:8443/hook",
        "https://hooks.example.com:65535/hook",
        "HTTPS://hooks.example.com/hook",
        "https://93.184.216.34/hook",
        "https://[2606:4700:4700::1111]/hook"
      })
  void httpsUrlToAPublicHostIsAllowed(String url) {
    assertThat(policy.check(TENANT, url)).isEqualTo(Verdict.ALLOWED);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://hooks.example.com/hook",
        "ftp://hooks.example.com/hook",
        "//hooks.example.com/hook",
        "hooks.example.com/hook",
        "https:hooks.example.com",
        "https:///hook",
        ""
      })
  void urlThatIsNotAnHttpsUrlWithAHostIsRefusedWithoutALookup(String url) {
    assertRefusedWithoutALookup(url);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 80, 1023, 65536})
  void portThatIsNeither443NorFrom1024To65535IsRefusedWithoutALookup(int port) {
    assertRefusedWithoutALookup("https://hooks.example.com:%d/hook".formatted(port));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://user@hooks.example.com/hook",
        "https://user:password@hooks.example.com/hook",
        "https://@hooks.example.com/hook"
      })
  void urlWithUserinfoIsRefusedWithoutALookup(String url) {
    assertRefusedWithoutALookup(url);
  }

  @Test
  void urlOfTwoThousandFortyEightCharactersIsTheLongestAllowed() {
    String prefix = "https://hooks.example.com/";
    String longest = prefix + "a".repeat(2048 - prefix.length());

    assertThat(policy.check(TENANT, longest)).isEqualTo(Verdict.ALLOWED);
    assertThat(policy.check(TENANT, longest + "a")).isEqualTo(Verdict.NOT_ALLOWED);
    assertThat(lookupsMade()).isEqualTo(1);
  }

  // Forms that another URL parser may read as a different host than the one written.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://hooks.example.com /hook",
        "https://hooks.example.com\\@127.0.0.1/hook",
        "https://hooks.example.com:port/hook",
        "https://hooks_internal.example.com/hook",
        "https://0x7f.0.0.1/hook",
        "https://127.1/hook",
        "https://bücher.example/hook"
      })
  void urlWhoseAuthorityIsNotAPlainHostAndPortIsRefusedWithoutALookup(String url) {
    dns.answer("hooks_internal.example.com", "93.184.216.34");
    dns.answer("0x7f.0.0.1", "93.184.216.34");
    dns.answer("127.1", "93.184.216.34");
    dns.answer("bücher.example", "93.184.216.34");

    assertRefusedWithoutALookup(url);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://127.0.0.1/hook",
        "https://10.0.0.5:8443/hook",
        "https://169.254.169.254/latest/meta-data",
        "https://[::1]/hook",
        "https://[fd00:ec2::254]/hook",
        "https://[::ffff:127.0.0.1]/hook"
      })
  void addressLiteralThatIsNotPublicIsNotAllowed(String url) {
    assertThat(policy.check(TENANT, url)).isEqualTo(Verdict.NOT_ALLOWED);
  }

  @Test
  void hostThatResolvesToAPrivateAddressIsNotAllowed() {
    dns.answer("internal.example.com", "10.1.2.3");

    assertThat(policy.check(TENANT, "https://internal.example.com/hook"))
        .isEqualTo(Verdict.NOT_ALLOWED);
  }

  @Test
  void hostWithOnePrivateAddressAmongPublicOnesIsNotAllowed() {
    dns.answer("mixed.example.com", "93.184.216.34", "192.168.1.10", "2606:4700:4700::1111");

    assertThat(policy.check(TENANT, "https://mixed.example.com/hook"))
        .isEqualTo(Verdict.NOT_ALLOWED);
  }

  @Test
  void hostThatDoesNotResolveIsNotAllowed() {
    assertThat(policy.check(TENANT, "https://unknown.example.com/hook"))
        .isEqualTo(Verdict.NOT_ALLOWED);
  }

  @Test
  void verdictIsNotRememberedForTheNextCheck() {
    dns.answerOnce("moving.example.com", "93.184.216.34");
    dns.answer("moving.example.com", "127.0.0.1");

    assertThat(policy.check(TENANT, "https://moving.example.com/hook")).isEqualTo(Verdict.ALLOWED);
    assertThat(policy.check(TENANT, "https://moving.example.com/hook"))
        .isEqualTo(Verdict.NOT_ALLOWED);
  }

  @Test
  void lookupThatIsNotAdmittedIsBusyAndNotAVerdictOnTheUrl() {
    String url = "https://hooks.example.com/hook";

    try (ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor();
        StubDns.Hold hold = dns.hold(HOST)) {
      for (int i = 0; i < 4; i++) {
        callers.submit(() -> policy.check(TENANT, url));
      }
      await().until(() -> hold.waiting() == 4);

      assertThat(policy.check(TENANT, url)).isEqualTo(Verdict.LOOKUP_BUSY);
    }
  }

  private void assertRefusedWithoutALookup(String url) {
    assertThat(policy.check(TENANT, url)).isEqualTo(Verdict.NOT_ALLOWED);
    assertThat(lookupsMade()).isZero();
  }

  private double lookupsMade() {
    return meters.get("webhooks.dns.lookups").counters().stream().mapToDouble(Counter::count).sum();
  }
}
