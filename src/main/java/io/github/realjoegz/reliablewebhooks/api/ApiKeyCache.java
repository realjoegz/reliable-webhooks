package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * Tenants by API-key hash, per replica. Only keys that exist are stored, and an entry is served for
 * 30 seconds from the start of the lookup that loaded it, so a change made on another replica is
 * seen within that time. A change made on this replica evicts the tenant.
 */
@Component
public class ApiKeyCache {

  private static final long TTL_NANOS = Duration.ofSeconds(30).toNanos();

  private record Entry(Tenant tenant, long loadedAt) {}

  private final ConcurrentHashMap<ByteBuffer, Entry> entries = new ConcurrentHashMap<>();
  private final AtomicLong evictions = new AtomicLong();
  private final LongSupplier nanoTime;

  public ApiKeyCache() {
    this(System::nanoTime);
  }

  ApiKeyCache(LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
  }

  /** Answers from the cache, or else runs the lookup and stores the tenant it finds. */
  public Optional<Tenant> get(byte[] apiKeyHash, Supplier<Optional<Tenant>> lookup) {
    ByteBuffer key = ByteBuffer.wrap(apiKeyHash);
    long now = nanoTime.getAsLong();
    Entry cached = entries.get(key);
    if (cached != null && !expired(cached, now)) {
      return Optional.of(cached.tenant());
    }
    long evictionsBefore = evictions.get();
    Optional<Tenant> found = lookup.get();
    found.ifPresent(tenant -> store(key, new Entry(tenant, now), evictionsBefore));
    return found;
  }

  public synchronized void evict(UUID tenantId) {
    evictions.incrementAndGet();
    entries.values().removeIf(entry -> entry.tenant().id().equals(tenantId));
  }

  // A lookup that overlapped an eviction may have read its row before the change that caused the
  // eviction, so its result is not stored. The check and the insert share the lock of evict:
  // otherwise such an entry could be read between its insert and its removal.
  private synchronized void store(ByteBuffer key, Entry entry, long evictionsBefore) {
    if (evictions.get() != evictionsBefore) {
      return;
    }
    // The entry of a rotated-away key is never refreshed, so expired entries are dropped here.
    entries.values().removeIf(stored -> expired(stored, entry.loadedAt()));
    entries.put(key, entry);
  }

  private static boolean expired(Entry entry, long now) {
    return now - entry.loadedAt() >= TTL_NANOS;
  }
}
