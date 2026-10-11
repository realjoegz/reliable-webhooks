package io.github.realjoegz.reliablewebhooks.endpoint;

import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class EndpointService {

  public sealed interface Registration permits Registered, Refused {}

  /** Carries the secret in the clear, for the one time the tenant is shown it. */
  public record Registered(UUID id, SigningSecret secret) implements Registration {}

  public enum Refused implements Registration {
    URL_NOT_ALLOWED,
    LOOKUP_BUSY,
    LIMIT_REACHED
  }

  private final UrlPolicy urls;
  private final SecretCipher cipher;
  private final EndpointRepository endpoints;

  EndpointService(UrlPolicy urls, SecretCipher cipher, EndpointRepository endpoints) {
    this.urls = urls;
    this.cipher = cipher;
    this.endpoints = endpoints;
  }

  public Registration register(UUID tenantId, String url, List<String> eventTypes) {
    // The lookup can take seconds. It runs before the transaction opens, so it holds neither a
    // connection nor the lock on the tenant row.
    return switch (urls.check(tenantId, url)) {
      case NOT_ALLOWED -> Refused.URL_NOT_ALLOWED;
      case LOOKUP_BUSY -> Refused.LOOKUP_BUSY;
      case ALLOWED -> create(tenantId, url, eventTypes);
    };
  }

  private Registration create(UUID tenantId, String url, List<String> eventTypes) {
    SigningSecret secret = SigningSecret.generate();
    return endpoints
        .create(tenantId, url, eventTypes, cipher.encrypt(secret.key()))
        .<Registration>map(id -> new Registered(id, secret))
        .orElse(Refused.LIMIT_REACHED);
  }
}
