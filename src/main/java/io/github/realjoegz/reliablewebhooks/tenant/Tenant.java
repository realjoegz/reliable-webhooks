package io.github.realjoegz.reliablewebhooks.tenant;

import java.util.UUID;

/** A tenant as authentication loads it: what every tenant route needs to know about its caller. */
public record Tenant(UUID id, String status, double ratePerSec, int burst) {

  public boolean suspended() {
    return "suspended".equals(status);
  }
}
