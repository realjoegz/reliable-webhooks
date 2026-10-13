package io.github.realjoegz.reliablewebhooks.delivery;

import java.time.Duration;

/**
 * How long the poller waits before it claims again after a claim that brought no row: 50 ms the
 * first time, twice as long each time after that, and never more than 1 s. A claim that brought a
 * row starts it over.
 */
final class PollerBackoff {

  private static final Duration FIRST = Duration.ofMillis(50);
  private static final Duration LONGEST = Duration.ofSeconds(1);

  private Duration next = FIRST;

  Duration nextWait() {
    Duration wait = next;
    Duration doubled = next.multipliedBy(2);
    next = doubled.compareTo(LONGEST) < 0 ? doubled : LONGEST;
    return wait;
  }

  void reset() {
    next = FIRST;
  }
}
