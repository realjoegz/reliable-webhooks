package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class RetryScheduleTest {

  private static final Duration HOUR = Duration.ofHours(1);
  private static final double NEARLY_ONE = Math.nextDown(1.0);

  // The eleven waits that twelve attempts have between them, in seconds.
  @ParameterizedTest
  @CsvSource({
    "1,  5,    10",
    "2,  10,   20",
    "3,  20,   40",
    "4,  40,   80",
    "5,  80,   160",
    "6,  160,  320",
    "7,  320,  640",
    "8,  640,  1280",
    "9,  1280, 2560",
    "10, 2560, 5120",
    "11, 5120, 10240"
  })
  void waitAfterAFailedAttemptLiesInTheWindowOfThatAttempt(
      int attempt, long shortest, long longest) {
    assertThat(RetrySchedule.delay(attempt, 0)).isEqualTo(Duration.ofSeconds(shortest));
    assertThat(RetrySchedule.delay(attempt, 0.5))
        .isEqualTo(Duration.ofSeconds(shortest).multipliedBy(3).dividedBy(2));
    assertThat(RetrySchedule.delay(attempt, NEARLY_ONE))
        .isGreaterThan(Duration.ofSeconds(longest).minusMillis(1))
        .isLessThan(Duration.ofSeconds(longest));
  }

  @Test
  void twelveAttemptsSpanBetweenTwoHoursFiftyOneAndFiveHoursFortyOne() {
    Duration shortest = Duration.ZERO;
    Duration longest = Duration.ZERO;
    for (int attempt = 1; attempt <= 11; attempt++) {
      shortest = shortest.plus(RetrySchedule.delay(attempt, 0));
      longest = longest.plus(RetrySchedule.delay(attempt, 1));
    }

    assertThat(shortest).isEqualTo(Duration.ofHours(2).plusMinutes(50).plusSeconds(35));
    assertThat(longest).isEqualTo(Duration.ofHours(5).plusMinutes(41).plusSeconds(10));
  }

  // The window of attempt 12 would be 5 h 41 min. It is the first that the cap of 4 h cuts.
  @ParameterizedTest
  @ValueSource(ints = {12, 13, 31, 32, 64, 1000, Integer.MAX_VALUE})
  void windowIsCappedAtFourHours(int attempt) {
    assertThat(RetrySchedule.delay(attempt, 0)).isEqualTo(Duration.ofHours(2));
    assertThat(RetrySchedule.delay(attempt, 1)).isEqualTo(Duration.ofHours(4));
  }

  @ParameterizedTest
  @CsvSource({
    "0,          10, 10",
    "9,          10, 10",
    "10,         10, 10",
    "11,         10, 11",
    "120,        10, 120",
    "' 120 ',    10, 120",
    "007,        5,  7",
    "3599,       10, 3599",
    "3600,       10, 3600",
    "3601,       10, 3600",
    "1000000000, 10, 3600",
    "999999999999999999999999999999, 10, 3600"
  })
  void retryAfterLengthensTheWaitByUpToOneHour(String retryAfter, long own, long honoured) {
    assertThat(RetrySchedule.honouring(retryAfter, Duration.ofSeconds(own)))
        .isEqualTo(Duration.ofSeconds(honoured));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "60", "3600", "1000000000"})
  void retryAfterNeverShortensAWait(String retryAfter) {
    Duration own = HOUR.plusSeconds(1);

    assertThat(RetrySchedule.honouring(retryAfter, own)).isEqualTo(own);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "-5", "+5", "1.5", "1e3", "soon", "Wed, 21 Oct 2026 07:28:00 GMT"})
  void retryAfterThatIsNoNumberOfSecondsCountsAsNone(String retryAfter) {
    Duration own = Duration.ofSeconds(10);

    assertThat(RetrySchedule.honouring(retryAfter, own)).isEqualTo(own);
  }
}
