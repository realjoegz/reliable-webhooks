package io.github.realjoegz.reliablewebhooks.delivery;

import io.github.realjoegz.reliablewebhooks.DatabaseFailures;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Delivery;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Attempt;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Loaded;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.NextState;
import io.github.realjoegz.reliablewebhooks.endpoint.SecretCipher;
import io.github.realjoegz.reliablewebhooks.http.Signer;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Failed;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.LookupBusy;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Outcome;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Response;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Runs one attempt of a delivery that a claim has leased: load, sign and send, then record the
 * result and the next state in one transaction. Everything it writes carries the lease token, so an
 * attempt whose lease another replica has taken over records nothing.
 *
 * <p>An attempt that ends without a record, because the replica stopped, the start cutoff passed or
 * the database failed, leaves the delivery leased. It becomes due again when the lease runs out,
 * and the claim that takes it then logs the attempt as {@code unknown}.
 */
@Component
class DeliveryWorker {

  private static final Logger log = LoggerFactory.getLogger(DeliveryWorker.class);

  private static final Duration BUSY_LOOKUP_DELAY = Duration.ofSeconds(2);

  /** An attempt, why it ended as it did, and what its response asked the next one to wait. */
  private record Result(Attempt attempt, AttemptReason reason, String retryAfter) {}

  private final DeliveryRepository deliveries;
  private final WebhookHttpClient http;
  private final SecretCipher secrets;
  private final DeliveryMeters meters;
  private final FinalizeRetry finalizeRetry;
  private final DeliveryLog fields;
  private final Duration startCutoff;
  private final int maxAttempts;
  private final String replica;
  private final LongSupplier nanoTime;

  @Autowired
  DeliveryWorker(
      DeliveryRepository deliveries,
      WebhookHttpClient http,
      SecretCipher secrets,
      DeliveryMeters meters,
      WebhooksProperties properties) {
    this(
        deliveries,
        http,
        secrets,
        meters,
        properties.delivery(),
        properties.replicaId(),
        System::nanoTime,
        Thread::sleep);
  }

  DeliveryWorker(
      DeliveryRepository deliveries,
      WebhookHttpClient http,
      SecretCipher secrets,
      DeliveryMeters meters,
      Delivery settings,
      String replica,
      LongSupplier nanoTime,
      FinalizeRetry.Pause pause) {
    this.deliveries = deliveries;
    this.http = http;
    this.secrets = secrets;
    this.meters = meters;
    this.fields = new DeliveryLog(replica);
    this.finalizeRetry =
        new FinalizeRetry(meters, fields, settings.finalizeCutoff(), nanoTime, pause);
    this.startCutoff = settings.startCutoff();
    this.maxAttempts = settings.maxAttempts();
    this.replica = replica;
    this.nanoTime = nanoTime;
  }

  /**
   * {@code claimStartedNanos} is the monotonic time just before the claim was sent. The lease began
   * after that, so a cutoff measured from it ends before the lease does, whatever the wall clocks
   * of the replica and the database say.
   */
  void attempt(Claimed delivery, long claimStartedNanos) {
    try {
      if (delivery.attempts() > maxAttempts) {
        deadLetterWithoutSending(delivery, claimStartedNanos);
      } else {
        loadSendAndRecord(delivery, claimStartedNanos);
      }
    } catch (RuntimeException failure) {
      // A database that restarts fails the load of every attempt in flight. That passes, and the
      // lease brings each delivery back. Any other failure is a fault.
      LoggingEventBuilder line =
          DatabaseFailures.isTransient(failure) ? log.atWarn() : log.atError();
      fields.about(delivery, line).setCause(failure).log("attempt_left_for_reclaim");
    }
  }

  // The attempts are counted by the claim, so a delivery whose every claim ended without a record
  // still runs out of them. This claim is one more than it may have, and nothing is sent for it.
  private void deadLetterWithoutSending(Claimed delivery, long claimStartedNanos) {
    if (finalizeRetry.run(
        delivery, claimStartedNanos, () -> deliveries.deadLetterExhausted(delivery))) {
      fields.about(delivery, log.atInfo()).log("dead_lettered_without_attempt");
    }
  }

  private void loadSendAndRecord(Claimed delivery, long claimStartedNanos) {
    Optional<Loaded> loaded = deliveries.load(delivery);
    // No row: the delivery was reclaimed or cancelled, or its endpoint was deleted.
    if (loaded.isEmpty()) {
      return;
    }
    // A request that started now could still be running when the lease ends.
    if (nanoTime.getAsLong() - claimStartedNanos >= startCutoff.toNanos()) {
      fields.about(delivery, log.atWarn()).log("start_cutoff_passed");
      return;
    }

    long sendStarted = nanoTime.getAsLong();
    Outcome outcome = send(delivery, loaded.get());
    Duration took = Duration.ofNanos(nanoTime.getAsLong() - sendStarted);

    switch (outcome) {
      case LookupBusy busy -> {
        // This replica had no room for the lookup, which says nothing about the endpoint.
        deliveries.release(delivery, BUSY_LOOKUP_DELAY);
        meters.releasedForBusyLookup();
      }
      case Response response -> record(delivery, claimStartedNanos, answered(response, took));
      case Failed failed -> record(delivery, claimStartedNanos, unanswered(failed, took));
    }
  }

  private Outcome send(Claimed delivery, Loaded loaded) {
    Map<String, String> headers =
        Signer.headers(
            delivery.id(), loaded.signedAt(), loaded.body(), secrets.decrypt(loaded.secret()));
    return http.send(delivery.tenantId(), URI.create(loaded.url()), headers, loaded.body());
  }

  // The status line decides. Only 2xx is a success: a redirect is not followed, and no status is
  // taken as final, because a subscriber that answers 404 for ten minutes must not lose events.
  private static Result answered(Response response, Duration took) {
    AttemptReason reason = AttemptReason.ofStatus(response.status());
    Attempt attempt =
        new Attempt(reason.outcome(), response.status(), null, response.remoteAddress(), took);
    return new Result(attempt, reason, response.retryAfter());
  }

  private static Result unanswered(Failed failed, Duration took) {
    AttemptReason reason = AttemptReason.ofNoResponse(failed.reason());
    // The attempt row names a reason as the meter does, except that a host that does not resolve
    // and a host with a denied address get one name: the attempt log must not tell its tenant
    // which of the two it is. The meter keeps them apart for the operator.
    String error =
        reason == AttemptReason.DNS || reason == AttemptReason.BLOCKED
            ? "destination_not_allowed"
            : reason.label();
    Attempt attempt = new Attempt(reason.outcome(), null, error, failed.remoteAddress(), took);
    return new Result(attempt, reason, null);
  }

  private void record(Claimed delivery, long claimStartedNanos, Result result) {
    NextState next = nextState(delivery, result);
    boolean recorded =
        finalizeRetry.run(
            delivery,
            claimStartedNanos,
            () -> deliveries.recordAttempt(delivery, next, result.attempt(), replica));
    if (recorded) {
      meters.attemptRecorded(result.reason());
      fields
          .about(delivery, log.atInfo())
          .addKeyValue("outcome", result.attempt().outcome())
          .addKeyValue("reason", result.reason().label())
          .addKeyValue("state", next.state())
          .log("delivery_attempt");
    }
  }

  private NextState nextState(Claimed delivery, Result result) {
    if (result.reason() == AttemptReason.HTTP_2XX) {
      return NextState.succeeded();
    }
    if (delivery.attempts() >= maxAttempts) {
      return NextState.exhausted();
    }
    Duration ownDelay =
        RetrySchedule.delay(delivery.attempts(), ThreadLocalRandom.current().nextDouble());
    return NextState.retryIn(RetrySchedule.honouring(result.retryAfter(), ownDelay));
  }
}
