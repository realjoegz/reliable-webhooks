package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ApiKeyCacheTest {

  private static final byte[] KEY_HASH = {1, 2, 3};
  private static final byte[] OTHER_KEY_HASH = {4, 5, 6};

  // System.nanoTime may be anywhere in the long range, so this clock starts just before it wraps.
  private final AtomicLong nanoTime = new AtomicLong(Long.MAX_VALUE - 5);
  private final ApiKeyCache cache = new ApiKeyCache(nanoTime::get);
  private final Tenant tenant = new Tenant(UUID.randomUUID(), "active", 50, 100);

  @Test
  void unknownHashIsAMiss() {
    assertThat(cached(KEY_HASH)).isEmpty();
  }

  @Test
  void keyThatDoesNotExistIsLookedUpAgain() {
    assertThat(cache.get(KEY_HASH, Optional::empty)).isEmpty();

    assertThat(cache.get(KEY_HASH, () -> Optional.of(tenant))).contains(tenant);
  }

  @Test
  void entryIsServedUntilThirtySecondsAfterItWasLoaded() {
    load(KEY_HASH, tenant);

    advance(Duration.ofSeconds(30).minusNanos(1));

    assertThat(cached(KEY_HASH)).contains(tenant);
  }

  @Test
  void entryExpiresThirtySecondsAfterItWasLoaded() {
    load(KEY_HASH, tenant);

    advance(Duration.ofSeconds(30));

    assertThat(cached(KEY_HASH)).isEmpty();
  }

  @Test
  void thirtySecondsCountFromTheStartOfTheLookup() {
    cache.get(
        KEY_HASH,
        () -> {
          advance(Duration.ofSeconds(5));
          return Optional.of(tenant);
        });

    advance(Duration.ofSeconds(25));

    assertThat(cached(KEY_HASH)).isEmpty();
  }

  @Test
  void aReadDoesNotExtendTheLifeOfAnEntry() {
    load(KEY_HASH, tenant);

    advance(Duration.ofSeconds(20));
    assertThat(cached(KEY_HASH)).contains(tenant);
    advance(Duration.ofSeconds(10));

    assertThat(cached(KEY_HASH)).isEmpty();
  }

  @Test
  void evictRemovesTheTenantAndLeavesOthers() {
    Tenant other = new Tenant(UUID.randomUUID(), "active", 50, 100);
    load(KEY_HASH, tenant);
    load(OTHER_KEY_HASH, other);

    cache.evict(tenant.id());

    assertThat(cached(KEY_HASH)).isEmpty();
    assertThat(cached(OTHER_KEY_HASH)).contains(other);
  }

  // The lookup read the row, and the eviction ran before the result could be stored. This is a
  // request with the old key that overlaps the rotation of that key.
  @Test
  void lookupThatOverlapsAnEvictionIsNotCached() {
    Optional<Tenant> found =
        cache.get(
            KEY_HASH,
            () -> {
              cache.evict(tenant.id());
              return Optional.of(tenant);
            });

    assertThat(found).contains(tenant);
    assertThat(cached(KEY_HASH)).isEmpty();
  }

  private void load(byte[] apiKeyHash, Tenant found) {
    cache.get(apiKeyHash, () -> Optional.of(found));
  }

  /** What the cache answers by itself: the lookup finds nothing. */
  private Optional<Tenant> cached(byte[] apiKeyHash) {
    return cache.get(apiKeyHash, Optional::empty);
  }

  private void advance(Duration duration) {
    nanoTime.addAndGet(duration.toNanos());
  }
}
