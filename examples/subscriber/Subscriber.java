import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
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
 * The smallest subscriber: one file without a build, started with {@code java Subscriber.java
 * <port>}. It checks every request with the reference verifier of docs/api.md, prints what it
 * received and answers 200. A request that is not genuine is answered with 401, and of such a
 * request only the headers are printed.
 *
 * <p>Two routes exist so that it can be shown without a restart. A subscriber in production has
 * neither, and reads its secret from its configuration.
 *
 * <ul>
 *   <li>{@code PUT /secret} takes the signing secret that the registration of the endpoint
 *       returned. Until then no request is genuine.
 *   <li>{@code PUT /status} takes the status that a genuine request is answered with from then on.
 *       With 503 it is a subscriber that fails.
 * </ul>
 */
public final class Subscriber {

  private static final Duration TOLERANCE = Duration.ofMinutes(5);

  private static volatile String signingSecret;
  private static volatile int statusOfAnswer = 200;

  private Subscriber() {}

  public static void main(String[] args) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(Integer.parseInt(args[0])), 0);
    server.createContext("/", Subscriber::handle);
    server.start();
  }

  private static void handle(HttpExchange exchange) throws IOException {
    try (exchange) {
      byte[] body = exchange.getRequestBody().readAllBytes();
      String route = exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath();
      int status =
          switch (route) {
            case "PUT /secret" -> takeSecret(new String(body, StandardCharsets.UTF_8));
            case "PUT /status" -> takeStatus(new String(body, StandardCharsets.UTF_8));
            default -> receive(exchange.getRequestHeaders(), body);
          };
      exchange.sendResponseHeaders(status, -1);
    }
  }

  // A signing secret is whsec_ and 32 bytes in base64.
  private static int takeSecret(String given) {
    if (!given.matches("whsec_[A-Za-z0-9+/]{43}=")) {
      return 400;
    }
    signingSecret = given;
    return 204;
  }

  private static int takeStatus(String given) {
    if (!given.matches("[2-5][0-9][0-9]")) {
      return 400;
    }
    statusOfAnswer = Integer.parseInt(given);
    return 204;
  }

  private static int receive(Headers headers, byte[] body) {
    String id = headers.getFirst("webhook-id");
    String timestamp = headers.getFirst("webhook-timestamp");
    String signatures = headers.getFirst("webhook-signature");
    String secret = signingSecret;
    boolean genuine;
    try {
      genuine =
          secret != null
              && id != null
              && timestamp != null
              && signatures != null
              && isGenuine(secret, id, timestamp, signatures, body, Instant.now());
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Every JDK has HmacSHA256, and the key is not empty", e);
    }
    if (!genuine) {
      System.out.printf(
          "signature=rejected webhook-id=%s webhook-timestamp=%s answered=401%n", id, timestamp);
      return 401;
    }
    int status = statusOfAnswer;
    System.out.printf(
        "signature=verified webhook-id=%s webhook-timestamp=%s answered=%d body=%s%n",
        id, timestamp, status, new String(body, StandardCharsets.UTF_8));
    return status;
  }

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
