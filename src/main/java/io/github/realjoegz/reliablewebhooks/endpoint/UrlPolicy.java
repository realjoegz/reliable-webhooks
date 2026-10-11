package io.github.realjoegz.reliablewebhooks.endpoint;

import io.github.realjoegz.reliablewebhooks.http.DnsLookups;
import io.github.realjoegz.reliablewebhooks.http.DnsLookups.Refusal;
import io.github.realjoegz.reliablewebhooks.http.DnsLookups.Resolved;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Decides whether an endpoint may be registered for a URL: {@code https} only, port 443 or 1024 to
 * 65535, no userinfo, at most 2048 characters, and a host that resolves to allowed addresses only.
 * The verdict holds for the moment of the check. A host can resolve to other addresses later, so an
 * allowed URL is not by that a safe one to connect to.
 */
@Component
public class UrlPolicy {

  private static final int MAX_LENGTH = 2048;
  private static final int HTTPS_PORT = 443;
  private static final int FIRST_UNPRIVILEGED_PORT = 1024;
  private static final int LAST_PORT = 65535;

  public enum Verdict {
    ALLOWED,
    /** One answer for every reason, so that a refusal does not tell what a host resolves to. */
    NOT_ALLOWED,
    LOOKUP_BUSY
  }

  private final DnsLookups lookups;

  UrlPolicy(DnsLookups lookups) {
    this.lookups = lookups;
  }

  public Verdict check(UUID tenantId, String url) {
    Optional<String> host = hostOfWellFormed(url);
    if (host.isEmpty()) {
      return Verdict.NOT_ALLOWED;
    }
    return switch (lookups.lookup(tenantId, host.get())) {
      case Resolved allowed -> Verdict.ALLOWED;
      case Refusal.BUSY -> Verdict.LOOKUP_BUSY;
      case Refusal.TIMEOUT, Refusal.FAILED, Refusal.BLOCKED -> Verdict.NOT_ALLOWED;
    };
  }

  private static Optional<String> hostOfWellFormed(String url) {
    if (url.length() > MAX_LENGTH) {
      return Optional.empty();
    }
    URI uri;
    try {
      uri = new URI(url);
    } catch (URISyntaxException malformed) {
      return Optional.empty();
    }
    // The host is null for an authority that is not host and port, such as a host with an
    // underscore or a port that is not a number.
    if (!"https".equalsIgnoreCase(uri.getScheme())
        || uri.getHost() == null
        || uri.getRawUserInfo() != null
        || !allowedPort(uri.getPort())) {
      return Optional.empty();
    }
    return Optional.of(uri.getHost());
  }

  // -1 means the URL names no port, which is 443 for https.
  private static boolean allowedPort(int port) {
    return port == -1
        || port == HTTPS_PORT
        || (port >= FIRST_UNPRIVILEGED_PORT && port <= LAST_PORT);
  }
}
