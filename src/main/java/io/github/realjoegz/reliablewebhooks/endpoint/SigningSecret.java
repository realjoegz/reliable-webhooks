package io.github.realjoegz.reliablewebhooks.endpoint;

import java.security.SecureRandom;
import java.util.Base64;

/**
 * The secret an endpoint's deliveries are signed with, in the form of Standard Webhooks: {@code
 * whsec_} and the key in base64. The key bytes are what the service stores, encrypted.
 */
public final class SigningSecret {

  private static final String PREFIX = "whsec_";
  private static final int KEY_BYTES = 32;
  private static final SecureRandom RANDOM = new SecureRandom();

  private final byte[] key;

  private SigningSecret(byte[] key) {
    this.key = key;
  }

  public static SigningSecret generate() {
    byte[] key = new byte[KEY_BYTES];
    RANDOM.nextBytes(key);
    return new SigningSecret(key);
  }

  public String value() {
    return PREFIX + Base64.getEncoder().encodeToString(key);
  }

  public byte[] key() {
    return key.clone();
  }

  @Override
  public String toString() {
    return "SigningSecret[redacted]";
  }
}
