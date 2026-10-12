package io.github.realjoegz.reliablewebhooks.delivery;

import io.github.realjoegz.reliablewebhooks.ingest.TokenBucket;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import java.util.OptionalInt;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Replays the dead deliveries of one endpoint, a bounded number in each call. A caller with more of
 * them calls again until none is left. It needs no cursor, because a delivery that was replayed is
 * no longer dead.
 */
@Service
public class BulkReplay {

  private static final int MAX_ROWS = 500;

  public sealed interface Result permits Replayed, Refused, RateLimited {}

  /** How many deliveries the call made pending, and whether it left dead ones behind. */
  public record Replayed(int replayed, boolean remaining) implements Result {}

  public enum Refused implements Result {
    /** The tenant has no active endpoint with this id. */
    ENDPOINT_NOT_FOUND
  }

  /** The bucket of the tenant did not hold what the call costs, which is this many units. */
  public record RateLimited(int cost) implements Result {}

  private final TenantDeliveryRepository deliveries;
  private final TokenBucket bucket;

  BulkReplay(TenantDeliveryRepository deliveries, TokenBucket bucket) {
    this.deliveries = deliveries;
    this.bucket = bucket;
  }

  public Result replay(Tenant tenant, UUID endpointId, int limit) {
    // A bucket never holds more than the burst, so a call for more rows than that could not be
    // paid for however long the tenant waited. With a burst of 0 the call still asks for one
    // row: the bucket then refuses it, where a call for no row would answer that none is left.
    int rows = Math.min(limit, Math.min(MAX_ROWS, Math.max(1, tenant.burst())));
    if (!deliveries.activeEndpointExists(tenant.id(), endpointId)) {
      return Refused.ENDPOINT_NOT_FOUND;
    }
    // One delivery beyond the rows of this call is counted and not replayed. It tells the caller
    // that there is more.
    int dead = deliveries.countDead(tenant.id(), endpointId, rows + 1);
    int cost = Math.min(dead, rows);
    if (cost == 0) {
      return new Replayed(0, false);
    }
    // A replayed delivery costs one unit, as it did at ingest. The charge comes before the
    // transaction, which makes no more deliveries pending than were paid for.
    if (!bucket.charge(tenant.id(), cost)) {
      return new RateLimited(cost);
    }
    OptionalInt replayed = deliveries.replayOldestDead(tenant.id(), endpointId, cost);
    if (replayed.isEmpty()) {
      return Refused.ENDPOINT_NOT_FOUND;
    }
    // Fewer than were counted, when another call replayed some of them meanwhile or holds them.
    return new Replayed(replayed.getAsInt(), dead > replayed.getAsInt());
  }
}
