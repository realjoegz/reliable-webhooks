package io.github.realjoegz.reliablewebhooks.ops;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Deletes what the service no longer needs. Every replica runs it, once when it starts and then an
 * hour after the end of each run. A run takes four steps, each in statements of 1,000 rows for as
 * long as a statement finds that many.
 *
 * <p>The order matters. Deliveries that ended more than 30 days ago go first. Only then can their
 * events and their deleted endpoints go, which the next two steps delete once they have no delivery
 * left. The last step removes the secrets from before a rotation that are no longer signed with.
 */
@Component
public class RetentionJob extends PeriodicJob {

  private static final String NAME = "retention";
  private static final Duration INTERVAL = Duration.ofHours(1);

  private final RetentionRepository rows;

  @Autowired
  RetentionJob(RetentionRepository rows) {
    super(NAME, INTERVAL);
    this.rows = rows;
  }

  RetentionJob(RetentionRepository rows, Wait betweenRuns) {
    super(NAME, INTERVAL, betweenRuns);
    this.rows = rows;
  }

  @Override
  void run(BooleanSupplier stopRequested) {
    inBatches(rows::deleteTerminalDeliveries, stopRequested);
    inBatches(rows::deleteEventsWithoutDeliveries, stopRequested);
    inBatches(rows::deleteDeletedEndpointsWithoutDeliveries, stopRequested);
    inBatches(rows::clearExpiredPreviousSecrets, stopRequested);
  }
}
