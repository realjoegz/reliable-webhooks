package io.github.realjoegz.reliablewebhooks.ops;

import io.github.realjoegz.reliablewebhooks.delivery.DeliveryMeters;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Cancels the pending deliveries of deleted endpoints. The deletion of an endpoint writes the
 * endpoint row only, however many deliveries wait for it, and stops their delivery by that. This
 * job gives them their final state afterwards. Every replica runs it, once when it starts and then
 * 30 seconds after the end of each run. A run cancels 1,000 deliveries in a statement, and goes on
 * for as long as a statement finds that many.
 */
@Component
public class CancelJob extends PeriodicJob {

  private static final String NAME = "cancel";
  private static final Duration INTERVAL = Duration.ofSeconds(30);

  private final CancelRepository deliveries;
  private final DeliveryMeters meters;

  @Autowired
  CancelJob(CancelRepository deliveries, DeliveryMeters meters) {
    super(NAME, INTERVAL);
    this.deliveries = deliveries;
    this.meters = meters;
  }

  CancelJob(CancelRepository deliveries, DeliveryMeters meters, Wait betweenRuns) {
    super(NAME, INTERVAL, betweenRuns);
    this.deliveries = deliveries;
    this.meters = meters;
  }

  @Override
  void run(BooleanSupplier stopRequested) {
    inBatches(
        () -> {
          int cancelled = deliveries.cancel();
          meters.cancelled(cancelled);
          return cancelled;
        },
        stopRequested);
  }
}
