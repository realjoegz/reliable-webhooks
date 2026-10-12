package io.github.realjoegz.reliablewebhooks.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ReferenceVerifierTest {

  private static final byte[] KEY =
      "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
  private static final String SECRET = "whsec_" + Base64.getEncoder().encodeToString(KEY);
  private static final UUID DELIVERY = UUID.fromString("0b6ae7e4-0f4c-4d3a-9d7e-3f1d2a5c8e11");
  private static final long SIGNED_AT = 1_800_000_000;
  private static final Instant NOW = Instant.ofEpochSecond(SIGNED_AT);
  private static final byte[] BODY =
      "{\"id\":\"1\",\"type\":\"order.created\"}".getBytes(StandardCharsets.UTF_8);

  private final Map<String, String> headers =
      Signer.headers(DELIVERY, SIGNED_AT, BODY, List.of(KEY));

  // The example of the Standard Webhooks specification. The verifier accepts a signature that
  // this service did not make, so the format is the one of the specification.
  @Test
  void acceptsTheExampleOfStandardWebhooks() throws GeneralSecurityException {
    assertThat(
            ReferenceVerifier.isGenuine(
                "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
                "msg_p5jXN8AQM9LWM0D4loKWxJek",
                "1614265330",
                "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=",
                "{\"test\": 2432232314}".getBytes(StandardCharsets.UTF_8),
                Instant.ofEpochSecond(1614265330)))
        .isTrue();
  }

  @Test
  void acceptsWhatTheSignerSigned() throws GeneralSecurityException {
    assertThat(headers.get(Signer.ID)).isEqualTo(DELIVERY.toString());
    assertThat(headers.get(Signer.TIMESTAMP)).isEqualTo("1800000000");
    assertThat(headers.get(Signer.SIGNATURE)).startsWith("v1,");
    assertThat(isGenuine(SECRET, BODY, NOW)).isTrue();
  }

  @Test
  void rejectsABodyThatWasChanged() throws GeneralSecurityException {
    byte[] changed = BODY.clone();
    changed[changed.length - 2] ^= 1;

    assertThat(isGenuine(SECRET, changed, NOW)).isFalse();
  }

  @Test
  void rejectsAnotherSecret() throws GeneralSecurityException {
    String other = "whsec_" + Base64.getEncoder().encodeToString(new byte[32]);

    assertThat(isGenuine(other, BODY, NOW)).isFalse();
  }

  @Test
  void rejectsAnotherDeliveryId() throws GeneralSecurityException {
    assertThat(
            ReferenceVerifier.isGenuine(
                SECRET,
                UUID.randomUUID().toString(),
                headers.get(Signer.TIMESTAMP),
                headers.get(Signer.SIGNATURE),
                BODY,
                NOW))
        .isFalse();
  }

  // A recorded request with its timestamp moved to the present. The timestamp is signed.
  @Test
  void rejectsATimestampThatWasChanged() throws GeneralSecurityException {
    Instant later = NOW.plus(Duration.ofHours(1));

    assertThat(
            ReferenceVerifier.isGenuine(
                SECRET,
                headers.get(Signer.ID),
                Long.toString(later.getEpochSecond()),
                headers.get(Signer.SIGNATURE),
                BODY,
                later))
        .isFalse();
  }

  @Test
  void acceptsARequestUpToFiveMinutesOldAndRejectsAnOlderOne() throws GeneralSecurityException {
    assertThat(isGenuine(SECRET, BODY, NOW.plus(Duration.ofMinutes(5)))).isTrue();
    assertThat(isGenuine(SECRET, BODY, NOW.plus(Duration.ofMinutes(5)).plusSeconds(1))).isFalse();
  }

  @Test
  void rejectsATimestampMoreThanFiveMinutesAhead() throws GeneralSecurityException {
    assertThat(isGenuine(SECRET, BODY, NOW.minus(Duration.ofMinutes(5)))).isTrue();
    assertThat(isGenuine(SECRET, BODY, NOW.minus(Duration.ofMinutes(5)).minusSeconds(1))).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "soon",
        "1800000000.5",
        "2026-10-11T00:00:00Z",
        "9223372036854775807",
        "99999999999999999999"
      })
  void rejectsATimestampThatIsNoTimeInSeconds(String timestamp) throws GeneralSecurityException {
    assertThat(
            ReferenceVerifier.isGenuine(
                SECRET,
                headers.get(Signer.ID),
                timestamp,
                headers.get(Signer.SIGNATURE),
                BODY,
                NOW))
        .isFalse();
  }

  // What the header looks like while a rotated secret is still valid: one signature for each.
  @Test
  void acceptsWhenOneOfSeveralSignaturesMatches() throws GeneralSecurityException {
    String signatures = "v1,bm90IHRoZSBzaWduYXR1cmU= " + headers.get(Signer.SIGNATURE);

    assertThat(
            ReferenceVerifier.isGenuine(
                SECRET,
                headers.get(Signer.ID),
                headers.get(Signer.TIMESTAMP),
                signatures,
                BODY,
                NOW))
        .isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "v1,", "v2,AAAA", "bm90IHRoZSBzaWduYXR1cmU="})
  void rejectsAHeaderWithoutAMatchingSignature(String signatures) throws GeneralSecurityException {
    assertThat(
            ReferenceVerifier.isGenuine(
                SECRET,
                headers.get(Signer.ID),
                headers.get(Signer.TIMESTAMP),
                signatures,
                BODY,
                NOW))
        .isFalse();
  }

  // The signature of this request without its version. A verifier that took it would also take
  // a signature of a version it does not know.
  @Test
  void rejectsASignatureWithoutTheVersion() throws GeneralSecurityException {
    String unversioned = headers.get(Signer.SIGNATURE).substring("v1,".length());

    assertThat(
            ReferenceVerifier.isGenuine(
                SECRET,
                headers.get(Signer.ID),
                headers.get(Signer.TIMESTAMP),
                unversioned,
                BODY,
                NOW))
        .isFalse();
  }

  @Test
  void readmeQuotesTheVerifier() throws IOException {
    String source =
        Files.readString(
            Path.of(
                "src/test/java/io/github/realjoegz/reliablewebhooks/http/ReferenceVerifier.java"));
    String quoted = source.substring(source.indexOf("public final class ReferenceVerifier"));

    assertThat(Files.readString(Path.of("README.md"))).contains("```java\n" + quoted + "```\n");
  }

  private boolean isGenuine(String secret, byte[] body, Instant now)
      throws GeneralSecurityException {
    return ReferenceVerifier.isGenuine(
        secret,
        headers.get(Signer.ID),
        headers.get(Signer.TIMESTAMP),
        headers.get(Signer.SIGNATURE),
        body,
        now);
  }
}
