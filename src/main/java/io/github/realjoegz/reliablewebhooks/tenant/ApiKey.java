package io.github.realjoegz.reliablewebhooks.tenant;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;
import java.util.regex.Pattern;

public final class ApiKey {

  private static final String PREFIX = "rwh_";
  private static final int SECRET_BYTES = 32;
  private static final Pattern FORMAT = Pattern.compile("rwh_[A-Za-z0-9_-]{43}");
  private static final SecureRandom RANDOM = new SecureRandom();

  private final String value;

  private ApiKey(String value) {
    this.value = value;
  }

  public static ApiKey generate() {
    byte[] secret = new byte[SECRET_BYTES];
    RANDOM.nextBytes(secret);
    return new ApiKey(PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(secret));
  }

  /** Empty for anything that cannot be a key, so that no database lookup is spent on it. */
  public static Optional<ApiKey> parse(String candidate) {
    return FORMAT.matcher(candidate).matches()
        ? Optional.of(new ApiKey(candidate))
        : Optional.empty();
  }

  public String value() {
    return value;
  }

  public byte[] sha256() {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.US_ASCII));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String toString() {
    return "ApiKey[redacted]";
  }
}
