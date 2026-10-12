package io.github.realjoegz.reliablewebhooks.ops;

import io.github.realjoegz.reliablewebhooks.delivery.DeliveryMeters;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Dead-letters the deliveries that are still pending when their 72 hours are over. Every replica
 * runs it, once when it starts and then 60 seconds after the end of each run. A run is one
 * statement, which takes up to 1,000 deliveries, those that expired first.
 */
@Component
public class ExpiryJob extends PeriodicJob {

  private static final String NAME = "expiry";
  private static final Duration INTERVAL = Duration.ofSeconds(60);

  private final ExpiryRepository deliveries;
  private final DeliveryMeters meters;

  @Autowired
  ExpiryJob(ExpiryRepository deliveries, DeliveryMeters meters) {
    super(NAME, INTERVAL);
    this.deliveries = deliveries;
    this.meters = meters;
  }

  ExpiryJob(ExpiryRepository deliveries, DeliveryMeters meters, Wait betweenRuns) {
    super(NAME, INTERVAL, betweenRuns);
    this.deliveries = deliveries;
    this.meters = meters;
  }

  @Override
  void run(BooleanSupplier stopRequested) {
    meters.abandonedAttemptsRecorded(deliveries.expire());
  }
}
