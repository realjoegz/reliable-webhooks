package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.delivery.PollerHeartbeat;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

/**
 * The rule of the liveness check, on a clock that stands still unless the test moves it: a replica
 * is down once its poller has finished no iteration for 60 seconds.
 */
class PollerHeartbeatTest {

  private static final Duration SIXTY_SECONDS = Duration.ofSeconds(60);

  // A monotonic clock starts anywhere. This one is a second away from running over.
  private final AtomicLong nanoTime =
      new AtomicLong(Long.MAX_VALUE - Duration.ofSeconds(1).toNanos());
  private final PollerHeartbeat heartbeat = new PollerHeartbeat(nanoTime::get);
  private final PollerHealth health = new PollerHealth(heartbeat);

  @Test
  void replicaThatHasJustStartedIsUp() {
    assertThat(heartbeat.sinceLastIteration()).isZero();
    assertThat(health.health().getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void replicaIsUpUntilSixtySecondsHavePassedWithoutAnIteration() {
    heartbeat.beat();

    pass(SIXTY_SECONDS.minusNanos(1));
    assertThat(health.health().getStatus()).isEqualTo(Status.UP);

    pass(Duration.ofNanos(1));
    assertThat(heartbeat.sinceLastIteration()).isEqualTo(SIXTY_SECONDS);
    assertThat(health.health().getStatus()).isEqualTo(Status.DOWN);
  }

  // A replica whose poller was never started is not live either: the time counts from its start.
  @Test
  void replicaWhosePollerNeverFinishedAnIterationIsDownAfterSixtySeconds() {
    pass(SIXTY_SECONDS);

    assertThat(health.health().getStatus()).isEqualTo(Status.DOWN);
  }

  @Test
  void iterationStartsTheSixtySecondsOver() {
    pass(Duration.ofMinutes(5));
    assertThat(health.health().getStatus()).isEqualTo(Status.DOWN);

    heartbeat.beat();

    assertThat(heartbeat.sinceLastIteration()).isZero();
    assertThat(health.health().getStatus()).isEqualTo(Status.UP);
    pass(SIXTY_SECONDS.minusNanos(1));
    assertThat(health.health().getStatus()).isEqualTo(Status.UP);
  }

  private void pass(Duration time) {
    nanoTime.addAndGet(time.toNanos());
  }
}
