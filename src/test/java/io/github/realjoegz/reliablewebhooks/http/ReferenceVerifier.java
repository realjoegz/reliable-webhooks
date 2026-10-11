package io.github.realjoegz.reliablewebhooks.http;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * How a subscriber checks that a request comes from this service. The README quotes this class, and
 * the tests run it against requests that the service sent.
 */
public final class ReferenceVerifier {

  private static final Duration TOLERANCE = Duration.ofMinutes(5);

  private ReferenceVerifier() {}

  /**
   * Whether a request is genuine and recent.
   *
   * @param secret the signing secret of the endpoint, as its registration returned it
   * @param id the value of the webhook-id header
   * @param timestamp the value of the webhook-timestamp header
   * @param signatures the value of the webhook-signature header
   * @param body the body of the request, byte for byte as it arrived
   * @param now the current time
   */
  public static boolean isGenuine(
      String secret, String id, String timestamp, String signatures, byte[] body, Instant now)
      throws GeneralSecurityException {
    Instant signedAt;
    try {
      signedAt = Instant.ofEpochSecond(Long.parseLong(timestamp));
    } catch (NumberFormatException | DateTimeException notATime) {
      return false;
    }
    // A request that was recorded and is sent again later carries a valid signature. Its age is
    // what gives it away.
    if (Duration.between(signedAt, now).abs().compareTo(TOLERANCE) > 0) {
      return false;
    }

    byte[] key = Base64.getDecoder().decode(secret.substring("whsec_".length()));
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(key, "HmacSHA256"));
    mac.update((id + "." + timestamp + ".").getBytes(StandardCharsets.UTF_8));
    byte[] expected = Base64.getEncoder().encode(mac.doFinal(body));

    // The header can carry several signatures, separated by spaces. One match is enough.
    for (String signature : signatures.split(" ")) {
      if (signature.startsWith("v1,")
          && MessageDigest.isEqual(
              expected, signature.substring(3).getBytes(StandardCharsets.UTF_8))) {
        return true;
      }
    }
    return false;
  }
}
