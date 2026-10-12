package io.github.realjoegz.reliablewebhooks.http;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Signs a delivery the way Standard Webhooks defines it. The MAC covers the id, the timestamp and
 * the body, so a subscriber that checks it knows which delivery the request is, when it was signed
 * and that the body is the one that was signed.
 */
public final class Signer {

  public static final String ID = "webhook-id";
  public static final String TIMESTAMP = "webhook-timestamp";
  public static final String SIGNATURE = "webhook-signature";

  private static final String ALGORITHM = "HmacSHA256";
  private static final String VERSION = "v1,";

  private Signer() {}

  /**
   * The three headers of a request for the delivery. The timestamp is in Unix seconds, and a key is
   * the 32 bytes that a secret of the endpoint carries. The signature header holds one signature
   * for each key, in the order of the keys and separated by spaces. A subscriber that knows one of
   * the secrets finds its signature among them.
   */
  public static Map<String, String> headers(
      UUID deliveryId, long timestamp, byte[] body, List<byte[]> keys) {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put(ID, deliveryId.toString());
    headers.put(TIMESTAMP, Long.toString(timestamp));
    headers.put(
        SIGNATURE,
        keys.stream()
            .map(key -> VERSION + signature(deliveryId, timestamp, body, key))
            .collect(Collectors.joining(" ")));
    return headers;
  }

  private static String signature(UUID deliveryId, long timestamp, byte[] body, byte[] key) {
    try {
      Mac mac = Mac.getInstance(ALGORITHM);
      mac.init(new SecretKeySpec(key, ALGORITHM));
      mac.update((deliveryId + "." + timestamp + ".").getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(mac.doFinal(body));
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new IllegalStateException("Could not sign a delivery", e);
    }
  }
}
