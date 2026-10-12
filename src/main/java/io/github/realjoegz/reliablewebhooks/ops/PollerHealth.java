package io.github.realjoegz.reliablewebhooks.ops;

import io.github.realjoegz.reliablewebhooks.delivery.PollerHeartbeat;
import java.time.Duration;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports the replica as not live when the loop of its poller has stopped looping: it is down once
 * 60 seconds have passed without an iteration. The configuration puts it into the liveness group,
 * so the answer of the liveness probe is what restarts such a replica.
 *
 * <p>A database that is down does not make it report down, because a claim that fails ends its
 * iteration like any other. No call of the loop can take 60 seconds either: the time bounds of the
 * {@code worker} pool end a claim long before that.
 */
// Spring Boot names an indicator after its bean without this suffix, which makes this one poller.
@Component("pollerHealthIndicator")
public class PollerHealth implements HealthIndicator {

  static final Duration LONGEST_SILENCE = Duration.ofSeconds(60);

  private final PollerHeartbeat heartbeat;

  PollerHealth(PollerHeartbeat heartbeat) {
    this.heartbeat = heartbeat;
  }

  @Override
  public Health health() {
    boolean looping = heartbeat.sinceLastIteration().compareTo(LONGEST_SILENCE) < 0;
    return (looping ? Health.up() : Health.down()).build();
  }
}
