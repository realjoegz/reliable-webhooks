package io.github.realjoegz.reliablewebhooks.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class SecretCipherTest {

  private static final byte[] SECRET = "a signing secret".getBytes(StandardCharsets.UTF_8);
  private static final int VERSION_BYTES = 1;
  private static final int NONCE_BYTES = 12;
  private static final int TAG_BYTES = 16;

  private final SecretCipher cipher = cipherWithKey(keyOf(1, 32));

  @Test
  void decryptReturnsWhatWasEncrypted() {
    assertThat(cipher.decrypt(cipher.encrypt(SECRET))).isEqualTo(SECRET);
  }

  // The value is read back with the JDK alone. Reading it back with SecretCipher would only show
  // that the class agrees with itself, whatever the algorithm and the layout.
  @Test
  void storedValueIsVersionNonceCiphertextAndTag() throws GeneralSecurityException {
    byte[] stored = cipher.encrypt(SECRET);

    assertThat(stored).hasSize(VERSION_BYTES + NONCE_BYTES + SECRET.length + TAG_BYTES);
    assertThat(stored[0]).isEqualTo((byte) 1);
    assertThat(new String(stored, StandardCharsets.ISO_8859_1))
        .doesNotContain(new String(SECRET, StandardCharsets.ISO_8859_1));

    Cipher aesGcm = Cipher.getInstance("AES/GCM/NoPadding");
    aesGcm.init(
        Cipher.DECRYPT_MODE,
        new SecretKeySpec(Base64.getDecoder().decode(keyOf(1, 32)), "AES"),
        new GCMParameterSpec(TAG_BYTES * 8, stored, VERSION_BYTES, NONCE_BYTES));
    int ciphertextStart = VERSION_BYTES + NONCE_BYTES;
    assertThat(aesGcm.doFinal(stored, ciphertextStart, stored.length - ciphertextStart))
        .isEqualTo(SECRET);
  }

  @Test
  void sameSecretEncryptsToADifferentValueEachTime() {
    byte[] first = cipher.encrypt(SECRET);
    byte[] second = cipher.encrypt(SECRET);

    assertThat(nonceOf(first)).isNotEqualTo(nonceOf(second));
    assertThat(first).isNotEqualTo(second);
  }

  @Test
  void valueEncryptedWithAnotherKeyIsRefused() {
    byte[] stored = cipherWithKey(keyOf(2, 32)).encrypt(SECRET);

    assertThatThrownBy(() -> cipher.decrypt(stored))
        .isInstanceOf(IllegalStateException.class)
        .hasCauseInstanceOf(AEADBadTagException.class);
  }

  @Test
  void valueChangedInAnyByteAfterTheVersionIsRefused() {
    byte[] stored = cipher.encrypt(SECRET);

    for (int i = VERSION_BYTES; i < stored.length; i++) {
      byte[] changed = stored.clone();
      changed[i] ^= 1;

      assertThatThrownBy(() -> cipher.decrypt(changed))
          .as("byte %d", i)
          .isInstanceOf(IllegalStateException.class)
          .hasCauseInstanceOf(AEADBadTagException.class);
    }
  }

  @Test
  void valueOfAnotherKeyVersionIsRefused() {
    byte[] stored = cipher.encrypt(SECRET);
    stored[0] = 2;

    assertThatThrownBy(() -> cipher.decrypt(stored))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Stored secret has unknown key version 2");
  }

  @Test
  void valueTooShortToHoldNonceAndTagIsRefused() {
    byte[] stored = cipher.encrypt(new byte[0]);
    assertThat(cipher.decrypt(stored)).isEmpty();

    assertThatThrownBy(() -> cipher.decrypt(Arrays.copyOf(stored, stored.length - 1)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Stored secret is truncated");
  }

  @Test
  void keyOfThirtyTwoBytesInBase64IsUsable() {
    assertThat(SecretCipher.isUsableKey(keyOf(1, 32))).isTrue();
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "not base64!"})
  void keyThatIsMissingOrNotBase64IsNotUsable(String key) {
    assertThat(SecretCipher.isUsableKey(key)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(ints = {16, 24, 31, 33})
  void keyOfAnotherLengthIsNotUsable(int bytes) {
    assertThat(SecretCipher.isUsableKey(keyOf(1, bytes))).isFalse();
  }

  @Test
  void cipherIsNotBuiltWithAnUnusableKey() {
    assertThatThrownBy(() -> cipherWithKey(keyOf(1, 16)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("webhooks.secret-encryption-key must be 32 bytes in base64");
  }

  private static SecretCipher cipherWithKey(String key) {
    return new SecretCipher(new WebhooksProperties(null, key, null, null));
  }

  private static String keyOf(int fill, int bytes) {
    byte[] key = new byte[bytes];
    Arrays.fill(key, (byte) fill);
    return Base64.getEncoder().encodeToString(key);
  }

  private static byte[] nonceOf(byte[] stored) {
    return Arrays.copyOfRange(stored, VERSION_BYTES, VERSION_BYTES + NONCE_BYTES);
  }
}
