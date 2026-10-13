package io.github.realjoegz.reliablewebhooks.delivery;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The loop of a replica that claims due deliveries and starts their attempts, each on a virtual
 * thread. It claims no more rows than it has room to run, so a claimed row starts at once and
 * nothing waits in memory.
 */
@Component
public class DeliveryPoller {

  private static final Logger log = LoggerFactory.getLogger(DeliveryPoller.class);

  /** How the loop waits for a permit. The tests end a wait themselves. */
  interface Wait {
    void forPermit(Semaphore permits, Duration atMost) throws InterruptedException;
  }

  private final DeliveryRepository deliveries;
  private final DeliveryWorker worker;
  private final DeliveryMeters meters;
  private final PollerHeartbeat heartbeat;
  private final Duration lease;
  private final int maxInFlight;
  private final Wait betweenClaims;

  // One permit for each attempt that may be in flight.
  private final Semaphore room;

  // Gets a permit whenever an attempt has finished, which ends the wait of the loop.
  private final Semaphore attemptFinished = new Semaphore(0);

  private final Bulkhead bulkhead = new Bulkhead();
  private final PollerBackoff backoff = new PollerBackoff();
  private final ThreadFactory attempts = Thread.ofVirtual().name("delivery-attempt-", 0).factory();
  private volatile boolean stopped;

  @Autowired
  DeliveryPoller(
      DeliveryRepository deliveries,
      DeliveryWorker worker,
      DeliveryMeters meters,
      PollerHeartbeat heartbeat,
      WebhooksProperties properties) {
    this(
        deliveries,
        worker,
        meters,
        heartbeat,
        properties,
        (permits, atMost) -> permits.tryAcquire(atMost.toNanos(), TimeUnit.NANOSECONDS));
  }

  DeliveryPoller(
      DeliveryRepository deliveries,
      DeliveryWorker worker,
      DeliveryMeters meters,
      PollerHeartbeat heartbeat,
      WebhooksProperties properties,
      Wait betweenClaims) {
    this.deliveries = deliveries;
    this.worker = worker;
    this.meters = meters;
    this.heartbeat = heartbeat;
    this.lease = properties.delivery().lease();
    this.maxInFlight = properties.delivery().maxInFlight();
    this.room = new Semaphore(maxInFlight);
    this.betweenClaims = betweenClaims;
  }

  /**
   * Claims until {@link #stop()} is called. A claim that brought a row is followed by the next one
   * at once. After any other claim the loop waits, at most for the backoff and no longer than until
   * an attempt has finished, because that is when there is room for a row again.
   *
   * <p>Every pass beats the heartbeat, also one whose claim failed. A database that is down
   * therefore does not make the replica look stuck. Only a loop that no longer comes by does.
   */
  public void run() {
    while (!stopped) {
      attemptFinished.drainPermits();
      int free = room.availablePermits();
      boolean claimedARow = free > 0 && claimAndStart(free);
      heartbeat.beat();
      if (claimedARow) {
        backoff.reset();
      } else if (!waitedForNextClaim()) {
        return;
      }
    }
  }

  /** Ends the loop. Attempts that are in flight go on. */
  public void stop() {
    stopped = true;
    attemptFinished.release();
  }

  /**
   * Lets {@link #run()} loop again after a stop. It is called before the thread of the loop is
   * started, and not by the loop: a stop that came before the first step of the loop would be lost.
   */
  public void clearStop() {
    stopped = false;
  }

  /** Waits until no attempt is in flight. Returns false when that took longer than the wait. */
  public boolean awaitIdle(Duration wait) throws InterruptedException {
    if (!room.tryAcquire(maxInFlight, wait.toNanos(), TimeUnit.NANOSECONDS)) {
      return false;
    }
    room.release(maxInFlight);
    return true;
  }

  private boolean claimAndStart(int free) {
    // Taken before the claim is sent: the lease starts later than this, so the worker's view of
    // how long the lease has left is the shorter one.
    long claimStartedNanos = System.nanoTime();
    List<Claimed> claimed;
    try {
      claimed = deliveries.claim(bulkhead.fullEndpoints(), bulkhead.fullTenants(), free, lease);
    } catch (RuntimeException failure) {
      meters.claimError();
      log.warn("claim_failed", failure);
      return false;
    }
    meters.claimed(claimed.size(), Duration.ofNanos(System.nanoTime() - claimStartedNanos));
    for (Claimed delivery : claimed) {
      if (delivery.abandoned()) {
        meters.abandonedAttemptsRecorded(1);
      }
      room.acquireUninterruptibly();
      bulkhead.enter(delivery.tenantId(), delivery.endpointId());
      attempts.newThread(() -> run(delivery, claimStartedNanos)).start();
    }
    return !claimed.isEmpty();
  }

  private void run(Claimed delivery, long claimStartedNanos) {
    try {
      worker.attempt(delivery, claimStartedNanos);
    } finally {
      bulkhead.leave(delivery.tenantId(), delivery.endpointId());
      room.release();
      attemptFinished.release();
    }
  }

  private boolean waitedForNextClaim() {
    try {
      betweenClaims.forPermit(attemptFinished, backoff.nextWait());
      return true;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
