package io.github.realjoegz.reliablewebhooks.tenant;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ApiKeyTest {

  @Test
  void generatedKeyIsThePrefixAndThirtyTwoRandomBytesInBase64Url() {
    String key = ApiKey.generate().value();

    assertThat(key).startsWith("rwh_");
    assertThat(Base64.getUrlDecoder().decode(key.substring(4))).hasSize(32);
    assertThat(key).doesNotContain("=", "+", "/");
  }

  @Test
  void generatedKeysDiffer() {
    assertThat(ApiKey.generate().value()).isNotEqualTo(ApiKey.generate().value());
  }

  @Test
  void generatedKeyParses() {
    String key = ApiKey.generate().value();

    assertThat(ApiKey.parse(key)).map(ApiKey::value).contains(key);
  }

  static Stream<String> notKeys() {
    return Stream.of(
        "",
        "rwh_",
        "rwh_" + "A".repeat(42),
        "rwh_" + "A".repeat(44),
        "rwh_" + "A".repeat(42) + "=",
        "rwh_" + "A".repeat(42) + "+",
        "key_" + "A".repeat(43),
        " rwh_" + "A".repeat(43),
        "rwh_" + "A".repeat(43) + "\n");
  }

  @ParameterizedTest
  @MethodSource("notKeys")
  void anythingElseDoesNotParse(String candidate) {
    assertThat(ApiKey.parse(candidate)).isEmpty();
  }

  @Test
  void hashIsTheSha256OfTheWholeKey() throws NoSuchAlgorithmException {
    ApiKey key = ApiKey.generate();

    byte[] expected =
        MessageDigest.getInstance("SHA-256")
            .digest(key.value().getBytes(StandardCharsets.US_ASCII));
    assertThat(key.sha256()).isEqualTo(expected);
  }

  @Test
  void toStringDoesNotRevealTheKey() {
    ApiKey key = ApiKey.generate();

    assertThat(key.toString()).doesNotContain(key.value());
  }
}
