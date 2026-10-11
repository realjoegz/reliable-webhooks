package io.github.realjoegz.reliablewebhooks.delivery;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * The counters of the delivery worker. None of them has a label that names a tenant, an endpoint, a
 * delivery or a host.
 */
@Component
class DeliveryMeters {

  private final Map<AttemptReason, Counter> attempts = new EnumMap<>(AttemptReason.class);
  private final Counter releasedForBusyLookup;
  private final Counter finalizeLostLease;
  private final Counter finalizeErrors;
  private final Counter claimErrors;

  // The Prometheus registry renders the names with underscores and the suffix _total, as
  // webhooks_delivery_attempts_total and so on.
  DeliveryMeters(MeterRegistry meters) {
    for (AttemptReason reason : AttemptReason.values()) {
      attempts.put(
          reason,
          Counter.builder("webhooks.delivery.attempts")
              .tag("outcome", reason.outcome())
              .tag("reason", reason.label())
              .register(meters));
    }
    this.releasedForBusyLookup =
        Counter.builder("webhooks.delivery.released").tag("reason", "dns_busy").register(meters);
    this.finalizeLostLease = meters.counter("webhooks.finalize.lost.lease");
    this.finalizeErrors = meters.counter("webhooks.finalize.errors");
    this.claimErrors = meters.counter("webhooks.claim.errors");
  }

  /** An attempt row was written. */
  void attemptRecorded(AttemptReason reason) {
    attempts.get(reason).increment();
  }

  void releasedForBusyLookup() {
    releasedForBusyLookup.increment();
  }

  void finalizeLostLease() {
    finalizeLostLease.increment();
  }

  void finalizeError() {
    finalizeErrors.increment();
  }

  void claimError() {
    claimErrors.increment();
  }
}
