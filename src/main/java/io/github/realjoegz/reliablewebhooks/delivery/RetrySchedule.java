package io.github.realjoegz.reliablewebhooks.delivery;

import java.time.Duration;
import java.util.regex.Pattern;

/**
 * When a failed delivery is tried again. With {@code d(n) = min(4 h, 10 s * 2^(n-1))}, the wait
 * after failed attempt {@code n} is one half of {@code d(n)} plus a random part of the other half.
 * The cap is not reached within twelve attempts. It keeps a larger number of attempts from waiting
 * for days.
 */
final class RetrySchedule {

  private static final Duration BASE = Duration.ofSeconds(10);
  private static final Duration CAP = Duration.ofHours(4);
  private static final Duration LONGEST_RETRY_AFTER = Duration.ofHours(1);

  // The base doubled this often is above the cap, and still far inside what a Duration holds.
  private static final int MAX_DOUBLINGS = 30;

  private static final Pattern SECONDS = Pattern.compile("\\d+");

  // A number with more digits than this is longer than the hour that is honoured at most.
  private static final int MAX_DIGITS_PARSED = 9;

  private RetrySchedule() {}

  /**
   * The wait after failed attempt {@code attempt}, which is 1 for the first. {@code random} is a
   * number from 0 to 1 and picks the wait from the window of that attempt.
   */
  static Duration delay(int attempt, double random) {
    Duration uncapped = BASE.multipliedBy(1L << Math.min(attempt - 1, MAX_DOUBLINGS));
    Duration window = uncapped.compareTo(CAP) < 0 ? uncapped : CAP;
    Duration half = window.dividedBy(2);
    return half.plusNanos((long) (half.toNanos() * random));
  }

  /**
   * The wait when the failure response carried a {@code Retry-After}: the longer of the own delay
   * and what the header asks for, where it cannot ask for more than one hour. The header can
   * therefore lengthen a wait and never shorten it. Only a number of seconds is read. A date would
   * have to be compared with the clock of this replica, so it counts as no header.
   */
  static Duration honouring(String retryAfter, Duration ownDelay) {
    String seconds = retryAfter == null ? "" : retryAfter.strip();
    if (!SECONDS.matcher(seconds).matches()) {
      return ownDelay;
    }
    Duration asked =
        seconds.length() > MAX_DIGITS_PARSED
            ? LONGEST_RETRY_AFTER
            : Duration.ofSeconds(Long.parseLong(seconds));
    Duration honoured = asked.compareTo(LONGEST_RETRY_AFTER) < 0 ? asked : LONGEST_RETRY_AFTER;
    return honoured.compareTo(ownDelay) > 0 ? honoured : ownDelay;
  }
}
