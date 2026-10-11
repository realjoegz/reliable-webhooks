package io.github.realjoegz.reliablewebhooks.endpoint;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * Encrypts signing secrets for storage with AES-256-GCM. A stored value is the key version (one
 * byte), a random nonce (12 bytes), then the ciphertext with its tag. The version says which key
 * encrypted the value, so that a later key can be told apart from this one.
 */
@Component
public class SecretCipher {

  public static final String KEY_REQUIREMENT =
      "webhooks.secret-encryption-key must be 32 bytes in base64";

  private static final byte KEY_VERSION = 1;
  private static final int KEY_BYTES = 32;
  private static final int NONCE_BYTES = 12;
  private static final int TAG_BYTES = 16;
  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  private static final SecureRandom RANDOM = new SecureRandom();

  private final SecretKey key;

  public SecretCipher(WebhooksProperties properties) {
    String configured = properties.secretEncryptionKey();
    Assert.isTrue(isUsableKey(configured), KEY_REQUIREMENT);
    this.key = new SecretKeySpec(Base64.getDecoder().decode(configured), "AES");
  }

  public static boolean isUsableKey(String configured) {
    if (configured == null) {
      return false;
    }
    try {
      return Base64.getDecoder().decode(configured).length == KEY_BYTES;
    } catch (IllegalArgumentException notBase64) {
      return false;
    }
  }

  public byte[] encrypt(byte[] secret) {
    byte[] nonce = new byte[NONCE_BYTES];
    RANDOM.nextBytes(nonce);
    try {
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BYTES * 8, nonce));
      return ByteBuffer.allocate(1 + NONCE_BYTES + cipher.getOutputSize(secret.length))
          .put(KEY_VERSION)
          .put(nonce)
          .put(cipher.doFinal(secret))
          .array();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Could not encrypt a secret", e);
    }
  }

  /** Fails on a value of another key version and on one that was not encrypted with this key. */
  public byte[] decrypt(byte[] stored) {
    if (stored.length < 1 + NONCE_BYTES + TAG_BYTES) {
      throw new IllegalStateException("Stored secret is truncated");
    }
    if (stored[0] != KEY_VERSION) {
      throw new IllegalStateException("Stored secret has unknown key version " + stored[0]);
    }
    try {
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(
          Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BYTES * 8, stored, 1, NONCE_BYTES));
      return cipher.doFinal(stored, 1 + NONCE_BYTES, stored.length - 1 - NONCE_BYTES);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Could not decrypt a stored secret", e);
    }
  }
}
