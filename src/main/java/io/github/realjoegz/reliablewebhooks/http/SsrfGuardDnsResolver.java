package io.github.realjoegz.reliablewebhooks.http;

import io.github.realjoegz.reliablewebhooks.http.DnsLookups.Refusal;
import io.github.realjoegz.reliablewebhooks.http.DnsLookups.Resolved;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.apache.hc.client5.http.DnsResolver;

/**
 * The resolver of the outbound client. The addresses it returns are the addresses the client
 * connects to, and they come from {@link DnsLookups}, which checks every one of them against the
 * address policy. A host is therefore judged by the addresses of the lookup that the connection
 * uses, not by those of an earlier one.
 */
final class SsrfGuardDnsResolver implements DnsResolver {

  /** A lookup that gave no address to connect to, with the reason. */
  static final class Refused extends UnknownHostException {

    private final Refusal refusal;

    private Refused(String host, Refusal refusal) {
      super(host);
      this.refusal = refusal;
    }

    Refusal refusal() {
      return refusal;
    }
  }

  private final DnsLookups lookups;

  // The client calls the resolver on the thread that sends the request, and its interface has no
  // room for the tenant whose lookup slots the lookup counts against.
  private final ThreadLocal<UUID> tenant = new ThreadLocal<>();

  SsrfGuardDnsResolver(DnsLookups lookups) {
    this.lookups = lookups;
  }

  void resolveFor(UUID tenantId) {
    tenant.set(tenantId);
  }

  void clear() {
    tenant.remove();
  }

  // Never null: the client would take that as leave to resolve the host by itself.
  @Override
  public InetAddress[] resolve(String host) throws UnknownHostException {
    UUID tenantId = tenant.get();
    if (tenantId == null) {
      throw new IllegalStateException("A lookup outside WebhookHttpClient.send");
    }
    return switch (lookups.lookup(tenantId, host)) {
      case Resolved resolved -> resolved.addresses().toArray(InetAddress[]::new);
      case Refusal refusal -> throw new Refused(host, refusal);
    };
  }

  // Asked for only by authentication schemes, which the client does not use. No lookup is made.
  @Override
  public String resolveCanonicalHostname(String host) {
    return host;
  }
}
