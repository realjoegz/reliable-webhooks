package io.github.realjoegz.reliablewebhooks.ops;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The wait between the runs of a job under test. It keeps how long the loop wanted to wait, and
 * waits far longer than that. A wait therefore ends when the job is stopped or when the test says
 * that its time is up, and not because the time that the loop asked for has passed.
 */
final class Waits implements PeriodicJob.Wait {

  private static final Duration FAR_LONGER_THAN_A_TEST_WAITS = Duration.ofMinutes(5);

  final List<Duration> asked = new CopyOnWriteArrayList<>();
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

  void timeIsUp() {
    timeIsUp.set(true);
    permits.release();
  }
}
