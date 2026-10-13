package io.github.realjoegz.reliablewebhooks.delivery;

import io.github.realjoegz.reliablewebhooks.DatabaseFailures;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Delivery;
import io.github.realjoegz.reliablewebhooks.delivery.CircuitBreaker.Verdict;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Attempt;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Loaded;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.NextState;
import io.github.realjoegz.reliablewebhooks.endpoint.SecretCipher;
import io.github.realjoegz.reliablewebhooks.http.Signer;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.AttemptSpan;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Failed;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.LookupBusy;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Response;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient.Traced;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.time.Duration;
import java.util.List;
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
 * result, the next state and what the result says about the endpoint in one transaction. Everything
 * it writes to the delivery carries the lease token, so an attempt whose lease another replica has
 * taken over records nothing.
 *
 * <p>A delivery that was claimed behind an open circuit is sent only if it wins the gate, as the
 * one probe of its endpoint. Otherwise it is given back at once and has used no attempt.
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
  private final CircuitBreaker circuits;
  private final WebhookHttpClient http;
  private final SecretCipher cipher;
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
      CircuitBreaker circuits,
      WebhookHttpClient http,
      SecretCipher cipher,
      DeliveryMeters meters,
      WebhooksProperties properties) {
    this(
        deliveries,
        circuits,
        http,
        cipher,
        meters,
        properties.delivery(),
        properties.replicaId(),
        System::nanoTime,
        Thread::sleep);
  }

  DeliveryWorker(
      DeliveryRepository deliveries,
      CircuitBreaker circuits,
      WebhookHttpClient http,
      SecretCipher cipher,
      DeliveryMeters meters,
      Delivery settings,
      String replica,
      LongSupplier nanoTime,
      FinalizeRetry.Pause pause) {
    this.deliveries = deliveries;
    this.circuits = circuits;
    this.http = http;
    this.cipher = cipher;
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
      } else if (delivery.circuitOpen() && !circuits.admitProbe(delivery.endpointId())) {
        // Another delivery has the probe slot, or the circuit has closed since the claim.
        // Neither is a result of this delivery, which is due again at once: behind the circuit
        // if that is still open, and as an ordinary delivery if it is not.
        deliveries.release(delivery, Duration.ZERO);
        meters.releasedForLostGate();
      } else {
        loadSendAndRecord(delivery, claimStartedNanos);
      }
    } catch (RuntimeException failure) {
      // A database that restarts fails the gate and the load of every attempt in flight. That
      // failure is transient: the lease runs out and each delivery is claimed again. Any other
      // failure is a fault.
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
      meters.exhausted();
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
    Traced sent = send(delivery, loaded.get());
    Duration took = Duration.ofNanos(nanoTime.getAsLong() - sendStarted);

    // The span of the request has ended. Its context is current from here on, so the lines
    // about what came of the request carry its trace id and its span id.
    try (Scope ofTheAttempt = Span.wrap(sent.span()).makeCurrent()) {
      switch (sent.outcome()) {
        case LookupBusy busy -> {
          // This replica had no room for the lookup, which says nothing about the endpoint.
          deliveries.release(delivery, BUSY_LOOKUP_DELAY);
          meters.releasedForBusyLookup();
        }
        case Response response ->
            record(delivery, loaded.get(), claimStartedNanos, answered(response, took));
        case Failed failed ->
            record(delivery, loaded.get(), claimStartedNanos, unanswered(failed, took));
      }
    }
  }

  private Traced send(Claimed delivery, Loaded loaded) {
    List<byte[]> keys = loaded.secrets().stream().map(cipher::decrypt).toList();
    Map<String, String> headers =
        Signer.headers(delivery.id(), loaded.signedAt(), loaded.body(), keys);
    AttemptSpan span =
        new AttemptSpan(
            delivery.id(), delivery.attempts(), delivery.circuitOpen(), loaded.traceContext());
    return http.send(delivery.tenantId(), span, loaded.url(), headers, loaded.body());
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

  // The finalize of an attempt: one transaction records the result, and FinalizeRetry repeats it
  // over a transient failure of the database.
  private void record(Claimed delivery, Loaded loaded, long claimStartedNanos, Result result) {
    NextState next = nextState(delivery, result);
    Verdict verdict = verdict(delivery, result);
    boolean recorded =
        finalizeRetry.run(
            delivery,
            claimStartedNanos,
            () -> deliveries.recordAttempt(delivery, next, result.attempt(), verdict, replica));
    if (recorded) {
      Duration took = result.attempt().took();
      meters.attemptRecorded(result.reason(), took);
      if (next.equals(NextState.succeeded())) {
        // From the creation of the delivery to the end of the request that was answered: its age
        // at the load, on the database clock, and the time that the request took after it.
        meters.succeeded(loaded.age().plus(took));
      } else if (next.equals(NextState.exhausted())) {
        meters.exhausted();
      }
      // The line carries the host of the endpoint's URL and nothing else of it.
      fields
          .about(delivery, log.atInfo())
          .addKeyValue("outcome", result.attempt().outcome())
          .addKeyValue("reason", result.reason().label())
          .addKeyValue("state", next.state())
          .addKeyValue("host", loaded.url().getHost())
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

  // A delivery that was claimed behind an open circuit got this far by winning the gate, so its
  // attempt was the probe. A probe that fails does not count toward opening the circuit, which is
  // open. It moves the next probe further away.
  private static Verdict verdict(Claimed delivery, Result result) {
    if (result.reason() == AttemptReason.HTTP_2XX) {
      return Verdict.SUCCESS;
    }
    return delivery.circuitOpen() ? Verdict.PROBE_FAILURE : Verdict.FAILURE;
  }
}
