package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class BulkheadTest {

  private static final UUID TENANT = UUID.randomUUID();

  private final Bulkhead bulkhead = new Bulkhead();

  @Test
  void nothingIsFullWhileNothingIsInFlight() {
    assertThat(bulkhead.fullEndpoints()).isEmpty();
    assertThat(bulkhead.fullTenants()).isEmpty();
  }

  @Test
  void endpointIsFullWithFourAttemptsInFlight() {
    UUID endpoint = UUID.randomUUID();

    enter(endpoint, 3);
    assertThat(bulkhead.fullEndpoints()).isEmpty();

    enter(endpoint, 1);
    assertThat(bulkhead.fullEndpoints()).containsExactly(endpoint);
    assertThat(bulkhead.fullTenants()).isEmpty();
  }

  // A claim can bring four rows of a tenant, so the tenant is left out from thirteen in flight:
  // twelve and four are the sixteen it may have.
  @Test
  void tenantIsFullWithMoreThanTwelveAttemptsInFlight() {
    for (int i = 0; i < 12; i++) {
      bulkhead.enter(TENANT, UUID.randomUUID());
    }
    assertThat(bulkhead.fullTenants()).isEmpty();

    bulkhead.enter(TENANT, UUID.randomUUID());
    assertThat(bulkhead.fullTenants()).containsExactly(TENANT);
    assertThat(bulkhead.fullEndpoints()).isEmpty();
  }

  @Test
  void finishedAttemptMakesRoomAgain() {
    UUID endpoint = UUID.randomUUID();
    enter(endpoint, 4);

    bulkhead.leave(TENANT, endpoint);

    assertThat(bulkhead.fullEndpoints()).isEmpty();
  }

  @Test
  void endpointsAndTenantsAreCountedApart() {
    UUID full = UUID.randomUUID();
    UUID other = UUID.randomUUID();
    UUID otherTenant = UUID.randomUUID();
    enter(full, 4);
    bulkhead.enter(TENANT, other);
    for (int i = 0; i < 13; i++) {
      bulkhead.enter(otherTenant, UUID.randomUUID());
    }

    assertThat(bulkhead.fullEndpoints()).containsExactly(full);
    assertThat(bulkhead.fullTenants()).containsExactly(otherTenant);
  }

  private void enter(UUID endpoint, int attempts) {
    for (int i = 0; i < attempts; i++) {
      bulkhead.enter(TENANT, endpoint);
    }
  }
}
