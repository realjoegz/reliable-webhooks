package io.github.realjoegz.reliablewebhooks.ops;

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

  @Autowired
  CancelJob(CancelRepository deliveries) {
    super(NAME, INTERVAL);
    this.deliveries = deliveries;
  }

  CancelJob(CancelRepository deliveries, Wait betweenRuns) {
    super(NAME, INTERVAL, betweenRuns);
    this.deliveries = deliveries;
  }

  @Override
  void run(BooleanSupplier stopRequested) {
    inBatches(deliveries::cancel, stopRequested);
  }
}
