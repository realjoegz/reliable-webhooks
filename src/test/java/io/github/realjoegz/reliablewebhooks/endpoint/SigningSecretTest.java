package io.github.realjoegz.reliablewebhooks.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Base64;
import org.junit.jupiter.api.Test;

class SigningSecretTest {

  @Test
  void toStringDoesNotRevealTheSecret() {
    SigningSecret secret = SigningSecret.generate();

    assertThat(secret.toString()).doesNotContain(Base64.getEncoder().encodeToString(secret.key()));
  }
}
