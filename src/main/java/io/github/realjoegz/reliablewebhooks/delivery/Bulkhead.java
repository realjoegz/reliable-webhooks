package io.github.realjoegz.reliablewebhooks.delivery;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The attempts that this replica has in flight, by endpoint and by tenant. It names the endpoints
 * and tenants that a claim must leave out, so that the replica never runs more than 4 attempts for
 * an endpoint or 16 for a tenant. Nothing is claimed and then given back for lack of room.
 */
final class Bulkhead {

  static final int MAX_PER_ENDPOINT = 4;
  static final int MAX_PER_TENANT = 16;

  // What one claim can add: the claim statement takes one row for an endpoint and four for a
  // tenant. An endpoint or a tenant is full when one more claim could take it past its cap.
  private static final int CLAIMED_PER_ENDPOINT = 1;
  private static final int CLAIMED_PER_TENANT = 4;

  private final Map<UUID, Integer> byEndpoint = new HashMap<>();
  private final Map<UUID, Integer> byTenant = new HashMap<>();

  synchronized void enter(UUID tenantId, UUID endpointId) {
    byEndpoint.merge(endpointId, 1, Integer::sum);
    byTenant.merge(tenantId, 1, Integer::sum);
  }

  synchronized void leave(UUID tenantId, UUID endpointId) {
    byEndpoint.computeIfPresent(endpointId, (id, inFlight) -> inFlight == 1 ? null : inFlight - 1);
    byTenant.computeIfPresent(tenantId, (id, inFlight) -> inFlight == 1 ? null : inFlight - 1);
  }

  synchronized UUID[] fullEndpoints() {
    return above(byEndpoint, MAX_PER_ENDPOINT - CLAIMED_PER_ENDPOINT);
  }

  synchronized UUID[] fullTenants() {
    return above(byTenant, MAX_PER_TENANT - CLAIMED_PER_TENANT);
  }

  private static UUID[] above(Map<UUID, Integer> inFlight, int room) {
    return inFlight.entrySet().stream()
        .filter(entry -> entry.getValue() > room)
        .map(Map.Entry::getKey)
        .toArray(UUID[]::new);
  }
}
