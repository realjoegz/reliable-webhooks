package io.github.realjoegz.reliablewebhooks.ops;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * A job that every replica runs by itself: once when it starts, and then a fixed time after the end
 * of each run. No replica leads. The statements of a job pass over the rows that another
 * transaction holds, so two runs at the same time do not wait for each other and do nothing twice.
 *
 * <p>A job keeps the default phase, like the worker: it stops while the connection pools are still
 * open. A stop starts no further run, and a run in progress starts no further statement. Spring
 * stops the lifecycles of a phase one after the other and then waits until each has said that it
 * has ended. A stop that waited for a run in progress would add that wait to the stop of the
 * worker, so the stop that Spring calls returns at once, and Spring is told when the loop has
 * ended.
 */
abstract class PeriodicJob implements SmartLifecycle {

  /**
   * How many rows one statement of a job takes at most. The statements hold the number as text, as
   * the limit of the CTE that picks their rows. Postgres runs such a CTE once, so the number bounds
   * the statement whatever its plan. A statement that took this many may have left rows behind, and
   * one that took fewer has not.
   */
  static final int BATCH = 1_000;

  /** How the loop waits for its next run. The tests end a wait themselves. */
  interface Wait {

    /** Returns true when a permit came before the time was up. */
    boolean forPermit(Semaphore permits, Duration atMost) throws InterruptedException;
  }

  /** The loop of one start: the permit that ends its wait, and its end after its last run. */
  private record Loop(Semaphore stopRequested, CompletableFuture<Void> ended) {}

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final String name;
  private final Duration interval;
  private final Wait betweenRuns;

  // Every start has a loop of its own, so a stop can leave nothing behind for the next start.
  private volatile Loop loop;

  PeriodicJob(String name, Duration interval) {
    this(
        name,
        interval,
        (permits, atMost) -> permits.tryAcquire(atMost.toNanos(), TimeUnit.NANOSECONDS));
  }

  PeriodicJob(String name, Duration interval, Wait betweenRuns) {
    this.name = name;
    this.interval = interval;
    this.betweenRuns = betweenRuns;
  }

  /**
   * One run of the job. A run of more than one statement asks before each of them whether a stop
   * has been requested, and ends without it if so.
   */
  abstract void run(BooleanSupplier stopRequested);

  /** A whole run, which no stop ends early. */
  public void run() {
    run(() -> false);
  }

  /**
   * Runs a statement that takes one batch of rows and returns how many it took, again and again
   * until a batch is not full or a stop has been requested.
   */
  static void inBatches(IntSupplier statement, BooleanSupplier stopRequested) {
    boolean full = true;
    while (full && !stopRequested.getAsBoolean()) {
      full = statement.getAsInt() == BATCH;
    }
  }

  @Override
  public void start() {
    Loop started = new Loop(new Semaphore(0), new CompletableFuture<>());
    loop = started;
    Thread.ofPlatform().daemon().name(name + "-job").start(() -> runUntilStopped(started));
  }

  @Override
  public void stop(Runnable callback) {
    stopLoop().thenRun(callback);
  }

  @Override
  public void stop() {
    stopLoop().join();
  }

  @Override
  public boolean isRunning() {
    return loop != null;
  }

  private CompletableFuture<Void> stopLoop() {
    Loop stopping = loop;
    loop = null;
    stopping.stopRequested().release();
    return stopping.ended();
  }

  private void runUntilStopped(Loop ofThisStart) {
    Semaphore stopRequested = ofThisStart.stopRequested();
    try {
      do {
        try {
          // The permit of a stop stays until the wait below takes it, so a run sees it as well.
          run(() -> stopRequested.availablePermits() > 0);
        } catch (RuntimeException failure) {
          // Nothing is lost: what this run left undone is still there for the next one. The
          // logger is that of the job, so the line says which one failed.
          log.warn("job_run_failed", failure);
        }
      } while (!stoppedWhileWaiting(stopRequested));
    } finally {
      ofThisStart.ended().complete(null);
    }
  }

  private boolean stoppedWhileWaiting(Semaphore stopRequested) {
    try {
      return betweenRuns.forPermit(stopRequested, interval);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return true;
    }
  }
}
