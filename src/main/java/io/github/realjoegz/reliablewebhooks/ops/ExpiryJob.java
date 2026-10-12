package io.github.realjoegz.reliablewebhooks.ops;

import io.github.realjoegz.reliablewebhooks.delivery.DeliveryMeters;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Dead-letters the deliveries that are still pending when their 72 hours are over. Every replica
 * runs it, once when it starts and then 60 seconds after the end of each run. No replica leads: a
 * run is one statement that passes over the rows another transaction holds, so two runs at the same
 * time do not wait for each other and do nothing twice.
 *
 * <p>It keeps the default phase, like the worker: it stops while the connection pools are still
 * open. A stop starts no further run. Spring stops the lifecycles of a phase one after the other
 * and then waits until each has said that it has ended. A stop that waited for a run in progress
 * would add that wait to the stop of the worker, so the stop that Spring calls returns at once, and
 * Spring is told when the loop has ended.
 */
@Component
public class ExpiryJob implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(ExpiryJob.class);

  private static final Duration INTERVAL = Duration.ofSeconds(60);

  /** How the loop waits for its next run. The tests end a wait themselves. */
  interface Wait {

    /** Returns true when a permit came before the time was up. */
    boolean forPermit(Semaphore permits, Duration atMost) throws InterruptedException;
  }

  /** The loop of one start: the permit that ends its wait, and its end after its last run. */
  private record Loop(Semaphore stopRequested, CompletableFuture<Void> ended) {}

  private final ExpiryRepository deliveries;
  private final DeliveryMeters meters;
  private final Wait betweenRuns;

  // Every start has a loop of its own, so a stop can leave nothing behind for the next start.
  private volatile Loop loop;

  @Autowired
  ExpiryJob(ExpiryRepository deliveries, DeliveryMeters meters) {
    this(
        deliveries,
        meters,
        (permits, atMost) -> permits.tryAcquire(atMost.toNanos(), TimeUnit.NANOSECONDS));
  }

  ExpiryJob(ExpiryRepository deliveries, DeliveryMeters meters, Wait betweenRuns) {
    this.deliveries = deliveries;
    this.meters = meters;
    this.betweenRuns = betweenRuns;
  }

  @Override
  public void start() {
    Loop started = new Loop(new Semaphore(0), new CompletableFuture<>());
    loop = started;
    Thread.ofPlatform().daemon().name("expiry-job").start(() -> runUntilStopped(started));
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

  public void run() {
    meters.abandonedAttemptsRecorded(deliveries.expire());
  }

  private CompletableFuture<Void> stopLoop() {
    Loop stopping = loop;
    loop = null;
    stopping.stopRequested().release();
    return stopping.ended();
  }

  private void runUntilStopped(Loop ofThisStart) {
    try {
      do {
        try {
          run();
        } catch (RuntimeException failure) {
          // Nothing is lost: what this run did not dead-letter is still there for the next one.
          log.warn("expiry_run_failed", failure);
        }
      } while (!stoppedWhileWaiting(ofThisStart.stopRequested()));
    } finally {
      ofThisStart.ended().complete(null);
    }
  }

  private boolean stoppedWhileWaiting(Semaphore stopRequested) {
    try {
      return betweenRuns.forPermit(stopRequested, INTERVAL);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return true;
    }
  }
}
