package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClaimerBackoffTest {

  private final ClaimerBackoff backoff = new ClaimerBackoff();

  @Test
  void waitDoublesFromFiftyMillisecondsAndStaysAtOneSecond() {
    assertThat(nextWaits(8))
        .containsExactly(
            Duration.ofMillis(50),
            Duration.ofMillis(100),
            Duration.ofMillis(200),
            Duration.ofMillis(400),
            Duration.ofMillis(800),
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            Duration.ofSeconds(1));
  }

  @Test
  void resetStartsTheWaitOver() {
    nextWaits(8);

    backoff.reset();

    assertThat(nextWaits(2)).containsExactly(Duration.ofMillis(50), Duration.ofMillis(100));
  }

  private List<Duration> nextWaits(int count) {
    List<Duration> waits = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      waits.add(backoff.nextWait());
    }
    return waits;
  }
}
