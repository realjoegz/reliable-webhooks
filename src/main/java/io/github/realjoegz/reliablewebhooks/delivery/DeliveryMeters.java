package io.github.realjoegz.reliablewebhooks.delivery;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * The meters of the delivery worker, and of the jobs that end a delivery without an attempt. None
 * of them has a label that names a tenant, an endpoint, a delivery or a host.
 */
@Component
public class DeliveryMeters {

  // With the attempt deadline that ships, a request ends after 15 s at the latest.
  private static final Duration[] ATTEMPT_BUCKETS = {
    Duration.ofMillis(50),
    Duration.ofMillis(100),
    Duration.ofMillis(250),
    Duration.ofMillis(500),
    Duration.ofSeconds(1),
    Duration.ofMillis(2500),
    Duration.ofSeconds(5),
    Duration.ofSeconds(10),
    Duration.ofSeconds(15)
  };

  // A delivery that waits for a retry or behind an open circuit succeeds hours after it was
  // created, and expires after 72 of them.
  private static final Duration[] END_TO_END_BUCKETS = {
    Duration.ofSeconds(1),
    Duration.ofSeconds(5),
    Duration.ofSeconds(15),
    Duration.ofMinutes(1),
    Duration.ofMinutes(5),
    Duration.ofMinutes(30),
    Duration.ofHours(2),
    Duration.ofHours(6),
    Duration.ofHours(24),
    Duration.ofHours(72)
  };

  private final Map<AttemptReason, Counter> attempts = new EnumMap<>(AttemptReason.class);
  private final Timer successfulAttemptTime;
  private final Timer failedAttemptTime;
  private final Counter releasedForLostGate;
  private final Counter releasedForBusyLookup;
  private final Counter succeeded;
  private final Counter exhausted;
  private final Counter expired;
  private final Counter cancelled;
  private final Timer endToEnd;
  private final Timer claimTime;
  private final DistributionSummary claimRows;
  private final Counter finalizeLostLease;
  private final Counter finalizeErrors;
  private final Counter claimErrors;

  // The Prometheus registry renders the names with underscores, a counter with the suffix _total
  // and a timer with the suffix _seconds, as webhooks_delivery_attempts_total,
  // webhooks_delivery_attempt_seconds and so on.
  public DeliveryMeters(MeterRegistry meters) {
    for (AttemptReason reason : AttemptReason.values()) {
      attempts.put(
          reason,
          Counter.builder("webhooks.delivery.attempts")
              .tag("outcome", reason.outcome())
              .tag("reason", reason.label())
              .register(meters));
    }
    // An attempt of unknown outcome was never recorded by its holder, so it has no duration.
    this.successfulAttemptTime = attemptTime(meters, "success");
    this.failedAttemptTime = attemptTime(meters, "failure");
    this.releasedForLostGate =
        Counter.builder("webhooks.delivery.released").tag("reason", "gate_lost").register(meters);
    this.releasedForBusyLookup =
        Counter.builder("webhooks.delivery.released").tag("reason", "dns_busy").register(meters);
    this.succeeded = terminal(meters, "succeeded", "delivered");
    this.exhausted = terminal(meters, "dead", "exhausted");
    this.expired = terminal(meters, "dead", "expired");
    this.cancelled = terminal(meters, "cancelled", "endpoint_deleted");
    this.endToEnd =
        Timer.builder("webhooks.delivery.end.to.end")
            .serviceLevelObjectives(END_TO_END_BUCKETS)
            .register(meters);
    this.claimTime = meters.timer("webhooks.claim");
    this.claimRows = meters.summary("webhooks.claim.rows");
    this.finalizeLostLease = meters.counter("webhooks.finalize.lost.lease");
    this.finalizeErrors = meters.counter("webhooks.finalize.errors");
    this.claimErrors = meters.counter("webhooks.claim.errors");
  }

  /** An attempt row was written for a request that took this long. Only a 2xx is a success. */
  void attemptRecorded(AttemptReason reason, Duration took) {
    attempts.get(reason).increment();
    (reason == AttemptReason.HTTP_2XX ? successfulAttemptTime : failedAttemptTime).record(took);
  }

  /**
   * Attempt rows were written for leases that ran out with nothing recorded. A claim writes them
   * for the deliveries that it takes over, and the expiry job for the deliveries that it
   * dead-letters.
   */
  public void abandonedAttemptsRecorded(int rows) {
    attempts.get(AttemptReason.LEASE_EXPIRED).increment(rows);
  }

  void releasedForLostGate() {
    releasedForLostGate.increment();
  }

  void releasedForBusyLookup() {
    releasedForBusyLookup.increment();
  }

  /** A delivery got its 2xx, this long after it was created. */
  void succeeded(Duration sinceCreation) {
    succeeded.increment();
    endToEnd.record(sinceCreation);
  }

  /** A delivery was dead-lettered because it had no attempt left. */
  void exhausted() {
    exhausted.increment();
  }

  /** The expiry job dead-lettered deliveries that were pending when their 72 hours were over. */
  public void expired(int deliveries) {
    expired.increment(deliveries);
  }

  /** The cancel job cancelled pending deliveries of deleted endpoints. */
  public void cancelled(int deliveries) {
    cancelled.increment(deliveries);
  }

  /** The database answered a claim with this many rows after this long. */
  void claimed(int rows, Duration took) {
    claimTime.record(took);
    claimRows.record(rows);
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

  private static Timer attemptTime(MeterRegistry meters, String outcome) {
    return Timer.builder("webhooks.delivery.attempt")
        .tag("outcome", outcome)
        .serviceLevelObjectives(ATTEMPT_BUCKETS)
        .register(meters);
  }

  private static Counter terminal(MeterRegistry meters, String state, String reason) {
    return Counter.builder("webhooks.delivery.terminal")
        .tag("state", state)
        .tag("reason", reason)
        .register(meters);
  }
}
