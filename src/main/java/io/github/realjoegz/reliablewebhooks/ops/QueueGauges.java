package io.github.realjoegz.reliablewebhooks.ops;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Reads the state of the queue for its gauges. Every replica runs it, once when it starts and then
 * 15 seconds after the end of each run. A run is one statement, which counts the pending deliveries
 * by what a claim would do with them. A run that fails leaves the gauges as the run before it left
 * them.
 */
@Component
public class QueueGauges extends PeriodicJob {

  private static final String NAME = "queue-gauges";
  private static final Duration INTERVAL = Duration.ofSeconds(15);

  private final QueueRepository queue;
  private final QueueMeters meters;

  @Autowired
  QueueGauges(QueueRepository queue, QueueMeters meters) {
    super(NAME, INTERVAL);
    this.queue = queue;
    this.meters = meters;
  }

  QueueGauges(QueueRepository queue, QueueMeters meters, Wait betweenRuns) {
    super(NAME, INTERVAL, betweenRuns);
    this.queue = queue;
    this.meters = meters;
  }

  @Override
  void run(BooleanSupplier stopRequested) {
    meters.show(queue.read());
  }
}
