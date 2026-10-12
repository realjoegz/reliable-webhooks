package io.github.realjoegz.reliablewebhooks.ops;

import io.github.realjoegz.reliablewebhooks.ops.QueueRepository.Queue;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import org.springframework.stereotype.Component;

/**
 * The gauges of the queue. They show what the last run of {@link QueueGauges} read, so a value is
 * as old as that run, and older when the runs since then have failed. A replica that has read
 * nothing yet shows no number.
 *
 * <p>None of them has a label that names a tenant or an endpoint: they count the whole queue.
 */
@Component
public class QueueMeters {

  private volatile Queue lastRead;

  // The Prometheus registry renders the names as webhooks_deliveries,
  // webhooks_oldest_due_age_seconds and webhooks_circuits_open.
  QueueMeters(MeterRegistry meters) {
    deliveries(meters, "due", Queue::due);
    deliveries(meters, "scheduled", Queue::scheduled);
    deliveries(meters, "leased", Queue::leased);
    deliveries(meters, "held", Queue::held);
    TimeGauge.builder(
            "webhooks.oldest.due.age",
            this,
            TimeUnit.NANOSECONDS,
            shown(queue -> queue.oldestDueAge().toNanos()))
        .register(meters);
    Gauge.builder("webhooks.circuits.open", this, shown(Queue::circuitsOpen)).register(meters);
  }

  void show(Queue read) {
    lastRead = read;
  }

  private void deliveries(MeterRegistry meters, String state, ToDoubleFunction<Queue> count) {
    Gauge.builder("webhooks.deliveries", this, shown(count)).tag("state", state).register(meters);
  }

  private static ToDoubleFunction<QueueMeters> shown(ToDoubleFunction<Queue> value) {
    return meters -> {
      Queue read = meters.lastRead;
      return read == null ? Double.NaN : value.applyAsDouble(read);
    };
  }
}
