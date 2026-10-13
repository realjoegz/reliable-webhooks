package io.github.realjoegz.reliablewebhooks.http;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Every DNS lookup of the service. A lookup can block for as long as the operating system lets it,
 * so it runs on one of 32 threads, the caller waits at most 2 seconds for it, and a tenant has at
 * most 4 lookups running at a time. The answer is checked against the {@link AddressPolicy} here,
 * so a caller gets addresses it may connect to or a refusal.
 */
@Component
public class DnsLookups implements AutoCloseable {

  private static final int THREADS = 32;
  private static final int MAX_UNRETURNED_PER_TENANT = 4;
  private static final Duration WAIT = Duration.ofSeconds(2);

  public sealed interface Answer permits Resolved, Refusal {}

  /**
   * Every address of the host, in the order of the answer, and the policy allows each of them. A
   * connection is made to the first.
   */
  public record Resolved(List<InetAddress> addresses) implements Answer {}

  public enum Refusal implements Answer {
    /** This replica did not admit the lookup. It says nothing about the host. */
    BUSY,
    /** The lookup was admitted and did not answer inside the wait. */
    TIMEOUT,
    /** The host does not resolve, or its answer holds no address. */
    FAILED,
    /** The host resolves, and the policy denies at least one of its addresses. */
    BLOCKED
  }

  private final HostResolver resolver;
  private final AddressPolicy policy;
  private final Duration wait;
  private final Counter resolved;
  private final Map<Refusal, Counter> refused = new EnumMap<>(Refusal.class);
  private final ExecutorService threads =
      Executors.newFixedThreadPool(
          THREADS, Thread.ofPlatform().daemon().name("dns-lookup-", 0).factory());

  private final Map<UUID, Integer> unreturnedByTenant = new HashMap<>();
  private int unreturned;

  @Autowired
  public DnsLookups(HostResolver resolver, AddressPolicy policy, MeterRegistry meters) {
    this(resolver, policy, meters, WAIT);
  }

  DnsLookups(HostResolver resolver, AddressPolicy policy, MeterRegistry meters, Duration wait) {
    this.resolver = resolver;
    this.policy = policy;
    this.wait = wait;
    this.resolved = counter(meters, "ok");
    for (Refusal refusal : Refusal.values()) {
      refused.put(refusal, counter(meters, refusal.name().toLowerCase(Locale.ROOT)));
    }
  }

  public Answer lookup(UUID tenantId, String host) {
    Answer answer = answer(tenantId, host);
    switch (answer) {
      case Resolved allowed -> resolved.increment();
      case Refusal refusal -> refused.get(refusal).increment();
    }
    return answer;
  }

  @Override
  public void close() {
    threads.shutdownNow();
  }

  private Answer answer(UUID tenantId, String host) {
    if (!admit(tenantId)) {
      return Refusal.BUSY;
    }
    Future<InetAddress[]> lookup =
        threads.submit(
            () -> {
              try {
                return resolver.resolve(host);
              } finally {
                release(tenantId);
              }
            });
    InetAddress[] addresses;
    try {
      addresses = lookup.get(wait.toNanos(), TimeUnit.NANOSECONDS);
    } catch (TimeoutException unanswered) {
      return Refusal.TIMEOUT;
    } catch (ExecutionException failure) {
      if (failure.getCause() instanceof UnknownHostException) {
        return Refusal.FAILED;
      }
      // Anything else is a fault of the resolver, not a fact about the host.
      throw new IllegalStateException("The resolver failed on a DNS lookup", failure.getCause());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for a DNS lookup", interrupted);
    }
    // allMatch holds for no address at all, and such an answer names nothing to connect to.
    if (addresses.length == 0) {
      return Refusal.FAILED;
    }
    return Stream.of(addresses).allMatch(policy::allows)
        ? new Resolved(List.of(addresses))
        : Refusal.BLOCKED;
  }

  // Admission counts lookups that have not returned, not callers that still wait. A lookup whose
  // caller gave up keeps its slot until the resolver comes back, because it keeps its thread
  // until then. No more lookups are admitted than there are threads, so an admitted lookup never
  // queues behind a stuck one.
  private synchronized boolean admit(UUID tenantId) {
    int ofTenant = unreturnedByTenant.getOrDefault(tenantId, 0);
    if (unreturned == THREADS || ofTenant == MAX_UNRETURNED_PER_TENANT) {
      return false;
    }
    unreturned++;
    unreturnedByTenant.put(tenantId, ofTenant + 1);
    return true;
  }

  private synchronized void release(UUID tenantId) {
    unreturned--;
    unreturnedByTenant.computeIfPresent(tenantId, (id, count) -> count == 1 ? null : count - 1);
  }

  // The Prometheus registry renders this name as webhooks_dns_lookups_total.
  private static Counter counter(MeterRegistry meters, String result) {
    return Counter.builder("webhooks.dns.lookups").tag("result", result).register(meters);
  }
}
