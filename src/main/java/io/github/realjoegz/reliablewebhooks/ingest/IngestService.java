package io.github.realjoegz.reliablewebhooks.ingest;

import io.github.realjoegz.reliablewebhooks.Traceparent;
import io.github.realjoegz.reliablewebhooks.ingest.EventRepository.KeyHolder;
import io.github.realjoegz.reliablewebhooks.ingest.EventRepository.NewEvent;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class IngestService {

  // One unit of the bucket pays for this much of a stored body.
  private static final int BYTES_PER_UNIT = 4096;

  public sealed interface Result permits Accepted, Refused, RateLimited {}

  /**
   * The event that holds the idempotency key. A replay answers a request whose event an earlier
   * request with the same key and body had already stored.
   */
  public record Accepted(UUID eventId, boolean replay) implements Result {}

  public enum Refused implements Result {
    /** The key belongs to an event that was created from a different body. */
    IDEMPOTENCY_KEY_REUSED
  }

  /** The bucket of the tenant did not hold what the event costs, which is this many units. */
  public record RateLimited(int cost) implements Result {}

  private final EventRepository events;
  private final TokenBucket bucket;

  IngestService(EventRepository events, TokenBucket bucket) {
    this.events = events;
    this.bucket = bucket;
  }

  public Result ingest(
      UUID tenantId,
      String idempotencyKey,
      EventRequest request,
      Optional<SpanContext> callerContext) {
    // The span of the request is the ingest span of the event. It is the root of a trace of its
    // own, because UntrustedTraceContextFilter hid the trace context that the caller sent from the
    // filter that started the span. What the caller sent becomes a link on it. Its own context is
    // stored with the event, where every attempt to deliver the event finds its parent.
    Span ingest = Span.current();
    callerContext.ifPresent(ingest::addLink);

    UUID id = UUID.randomUUID();
    // The clock of this replica. The body is rendered before any statement runs, and nothing is
    // scheduled from this value or compared with a database timestamp.
    byte[] body = request.render(id, Instant.now());

    Optional<KeyHolder> holder = events.findByIdempotencyKey(tenantId, idempotencyKey);
    if (holder.isPresent()) {
      return answerFrom(holder.get(), request);
    }

    List<UUID> matched = events.matchEndpoints(tenantId, request.type());
    // An event costs one unit for each endpoint that it goes to, and one where it goes to none,
    // plus one for each full 4 KiB of the body that is stored. The charge comes after the lookup,
    // so the repeat of an accepted event costs nothing. It comes before the transaction, which
    // creates deliveries for these endpoints only, so no delivery is created that was not paid.
    int cost = Math.max(1, matched.size()) + body.length / BYTES_PER_UNIT;
    if (!bucket.charge(tenantId, cost)) {
      return new RateLimited(cost);
    }
    NewEvent event =
        new NewEvent(
            id,
            tenantId,
            request.type(),
            body,
            idempotencyKey,
            request.hash(),
            Traceparent.of(ingest.getSpanContext()).orElse(null));
    if (events.create(event, matched)) {
      return new Accepted(id, false);
    }

    // A concurrent request with the same key committed first. Its event was not visible to the
    // lookup above and is visible to this one. Both requests have paid.
    KeyHolder winner =
        events
            .findByIdempotencyKey(tenantId, idempotencyKey)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No event holds the idempotency key that the insert conflicted with"));
    return answerFrom(winner, request);
  }

  private static Result answerFrom(KeyHolder holder, EventRequest request) {
    return Arrays.equals(holder.requestHash(), request.hash())
        ? new Accepted(holder.eventId(), true)
        : Refused.IDEMPOTENCY_KEY_REUSED;
  }
}
