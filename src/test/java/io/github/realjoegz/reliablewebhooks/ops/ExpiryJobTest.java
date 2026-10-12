package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.realjoegz.reliablewebhooks.delivery.DeliveryMeters;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ExpiryJobTest {

  private static final Duration SIXTY_SECONDS = Duration.ofSeconds(60);
  private static final Duration FAR_LONGER_THAN_A_TEST_WAITS = Duration.ofMinutes(5);
  private static final Duration TIME_LIMIT = Duration.ofSeconds(10);

  private final MeterRegistry registry = new SimpleMeterRegistry();
  private final Runs runs = new Runs();
  private final Waits waits = new Waits();
  private final ExpiryJob job = new ExpiryJob(runs, new DeliveryMeters(registry), waits);

  /**
   * Counts the runs, and answers each with the attempt rows or the failure that a test set. A run
   * that a test gave an end stays in progress until that end gets a permit.
   */
  private static final class Runs extends ExpiryRepository {

    private final AtomicInteger count = new AtomicInteger();
    private volatile int attemptRowsOfARun;
    private volatile RuntimeException failureOfTheNextRun;
    private volatile Semaphore endOfTheNextRun;

    private Runs() {
      super(null, null);
    }

    @Override
    int expire() {
      count.incrementAndGet();
      Semaphore end = endOfTheNextRun;
      if (end != null) {
        endOfTheNextRun = null;
        end.acquireUninterruptibly();
      }
      RuntimeException failure = failureOfTheNextRun;
      if (failure != null) {
        failureOfTheNextRun = null;
        throw failure;
      }
      return attemptRowsOfARun;
    }
  }

  /**
   * Keeps how long the loop wanted to wait, and waits far longer than that. A wait therefore ends
   * when the job is stopped or when the test says that its time is up, and not because the time
   * that the loop asked for has passed.
   */
  private static final class Waits implements ExpiryJob.Wait {

    private final List<Duration> asked = new CopyOnWriteArrayList<>();
    private final AtomicBoolean timeIsUp = new AtomicBoolean();
    private volatile Semaphore permits;

    // A permit from the test stands for the time that has passed. Only a permit from the job is a
    // stop.
    @Override
    public boolean forPermit(Semaphore permits, Duration atMost) throws InterruptedException {
      this.permits = permits;
      asked.add(atMost);
      boolean permitted =
          permits.tryAcquire(FAR_LONGER_THAN_A_TEST_WAITS.toNanos(), TimeUnit.NANOSECONDS);
      return permitted && !timeIsUp.getAndSet(false);
    }

    private void timeIsUp() {
      timeIsUp.set(true);
      permits.release();
    }
  }

  @AfterEach
  void stopTheJob() {
    if (job.isRunning()) {
      job.stop();
    }
  }

  // The first run comes with the start and not after a wait. Every later run follows a wait of
  // sixty seconds, which begins when the run before it has ended.
  @Test
  void jobRunsWhenItStartsAndThenSixtySecondsAfterEachRun() {
    job.start();
    await().until(() -> waits.asked.size() == 1);
    assertThat(runs.count).hasValue(1);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);
    assertThat(runs.count).hasValue(2);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 3);
    assertThat(runs.count).hasValue(3);
    assertThat(waits.asked).containsOnly(SIXTY_SECONDS);
  }

  // A run whose statement failed is followed by the usual wait and then by the next run.
  @Test
  void runThatFailedIsFollowedByTheNextRun() {
    runs.failureOfTheNextRun = new IllegalStateException("the database did not answer");
    job.start();
    await().until(() -> waits.asked.size() == 1);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);

    assertThat(runs.count).hasValue(2);
    assertThat(waits.asked).containsOnly(SIXTY_SECONDS);
  }

  // The wait of this test does not end by time, so the stop that returns has ended it.
  @Test
  void stopEndsTheWaitAndNoRunFollows() {
    job.start();
    await().until(() -> waits.asked.size() == 1);

    job.stop();

    assertThat(job.isRunning()).isFalse();
    assertThat(runs.count).hasValue(1);
    assertThat(waits.asked).hasSize(1);
  }

  // Spring calls this stop, on the thread that stops the worker as well. The run of the test ends
  // only when the test lets it, which is after the stop has returned: a stop that waited for the
  // run would wait for ever, and the time limit would fail it. The job says that it has ended
  // only once the run is over, and no run follows.
  @Test
  void stopDuringARunReturnsAtOnceAndTheEndIsReportedAfterTheRun() {
    Semaphore endOfTheRun = new Semaphore(0);
    runs.endOfTheNextRun = endOfTheRun;
    job.start();
    await().until(() -> runs.count.get() == 1);
    AtomicBoolean endReported = new AtomicBoolean();

    assertTimeoutPreemptively(TIME_LIMIT, () -> job.stop(() -> endReported.set(true)));

    assertThat(job.isRunning()).isFalse();
    assertThat(endReported).isFalse();

    endOfTheRun.release();

    await().untilTrue(endReported);
    assertThat(runs.count).hasValue(1);
  }

  // A caller of this stop waits for the loop to end. The stop comes during a run, and its thread
  // is seen waiting before the test lets the run end. It returns after that, and no run follows.
  @Test
  void stopThatIsWaitedForReturnsAfterTheRunThatIsInProgress() {
    Semaphore endOfTheRun = new Semaphore(0);
    runs.endOfTheNextRun = endOfTheRun;
    job.start();
    await().until(() -> runs.count.get() == 1);

    Thread stopping = Thread.ofPlatform().daemon().start(job::stop);
    await().until(() -> stopping.getState() == Thread.State.WAITING);

    endOfTheRun.release();

    await().until(() -> stopping.getState() == Thread.State.TERMINATED);
    assertThat(runs.count).hasValue(1);
  }

  // Spring may stop a lifecycle and start it again. The second start runs at once, like the
  // first, and then waits.
  @Test
  void jobThatWasStoppedRunsAgainOnceItIsStarted() {
    job.start();
    await().until(() -> waits.asked.size() == 1);
    job.stop();

    job.start();
    await().until(() -> waits.asked.size() == 2);

    assertThat(job.isRunning()).isTrue();
    assertThat(runs.count).hasValue(2);
  }

  @Test
  void runCountsTheAttemptRowsThatItWrote() {
    runs.attemptRowsOfARun = 3;

    job.run();
    job.run();

    assertThat(
            registry
                .get("webhooks.delivery.attempts")
                .tag("outcome", "unknown")
                .tag("reason", "lease_expired")
                .counter()
                .count())
        .isEqualTo(6);
  }
}
