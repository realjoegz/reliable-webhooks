package io.github.realjoegz.reliablewebhooks.delivery;

import java.time.Duration;
import java.util.function.LongSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * When the loop of the poller last finished an iteration, on the monotonic clock of the replica.
 * The loop beats after every claim, also after one that failed, so the time since the last beat
 * grows only while the loop does not loop. The liveness check reads it.
 *
 * <p>A heartbeat that has not been beaten yet counts from its creation.
 */
@Component
public class PollerHeartbeat {

  private final LongSupplier nanoTime;
  private volatile long lastIterationNanos;

  @Autowired
  PollerHeartbeat() {
    this(System::nanoTime);
  }

  public PollerHeartbeat(LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
    this.lastIterationNanos = nanoTime.getAsLong();
  }

  public void beat() {
    lastIterationNanos = nanoTime.getAsLong();
  }

  public Duration sinceLastIteration() {
    return Duration.ofNanos(nanoTime.getAsLong() - lastIterationNanos);
  }
}
