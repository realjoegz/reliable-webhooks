package io.github.realjoegz.reliablewebhooks.delivery;

import io.github.realjoegz.reliablewebhooks.DatabaseFailures;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the finalize of a delivery until the database has answered it. A failure that can pass, such
 * as a restart of the database, is tried again until the cutoff, which lies before the end of the
 * lease. Any other failure is a fault in the service or in the data, and trying again would not
 * change it. A delivery whose finalize is given up keeps its lease and is reclaimed when the lease
 * has run out.
 *
 * <p>Trying again is safe when the first try did commit and only its answer was lost: the lease
 * token is gone by then, so the second try changes nothing.
 */
final class FinalizeRetry {

  private static final Logger log = LoggerFactory.getLogger(FinalizeRetry.class);

  private static final Duration PAUSE = Duration.ofMillis(200);

  /** How the retry waits. The tests wait on their own clock. */
  interface Pause {
    void of(Duration duration) throws InterruptedException;
  }

  private final DeliveryMeters meters;
  private final DeliveryLog fields;
  private final Duration cutoff;
  private final LongSupplier nanoTime;
  private final Pause pause;

  FinalizeRetry(
      DeliveryMeters meters,
      DeliveryLog fields,
      Duration cutoff,
      LongSupplier nanoTime,
      Pause pause) {
    this.meters = meters;
    this.fields = fields;
    this.cutoff = cutoff;
    this.nanoTime = nanoTime;
    this.pause = pause;
  }

  /**
   * Returns true when the finalize was recorded. The finalize itself answers false when the lease
   * was lost. The cutoff counts from {@code claimStartedNanos}, and no try starts after it.
   */
  boolean run(Claimed delivery, long claimStartedNanos, BooleanSupplier finalize) {
    while (true) {
      try {
        if (finalize.getAsBoolean()) {
          return true;
        }
        meters.finalizeLostLease();
        fields.about(delivery, log.atWarn()).log("finalize_lost_lease");
        return false;
      } catch (RuntimeException failure) {
        if (!DatabaseFailures.isTransient(failure)) {
          meters.finalizeError();
          fields.about(delivery, log.atError()).setCause(failure).log("finalize_failed");
          return false;
        }
        if (!pausedBeforeCutoff(claimStartedNanos)) {
          fields.about(delivery, log.atWarn()).setCause(failure).log("finalize_given_up");
          return false;
        }
      }
    }
  }

  private boolean pausedBeforeCutoff(long claimStartedNanos) {
    try {
      pause.of(PAUSE);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    }
    return nanoTime.getAsLong() - claimStartedNanos < cutoff.toNanos();
  }
}
