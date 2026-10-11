package io.github.realjoegz.reliablewebhooks.ingest;

import io.github.realjoegz.reliablewebhooks.ingest.EventRepository.KeyHolder;
import io.github.realjoegz.reliablewebhooks.ingest.EventRepository.NewEvent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class IngestService {

  public sealed interface Result permits Accepted, Refused {}

  /**
   * The event that holds the idempotency key. A replay answers a request whose event an earlier
   * request with the same key and body had already stored.
   */
  public record Accepted(UUID eventId, boolean replay) implements Result {}

  public enum Refused implements Result {
    /** The key belongs to an event that was created from a different body. */
    IDEMPOTENCY_KEY_REUSED
  }

  private final EventRepository events;

  IngestService(EventRepository events) {
    this.events = events;
  }

  public Result ingest(UUID tenantId, String idempotencyKey, EventRequest request) {
    UUID id = UUID.randomUUID();
    // The clock of this replica. The body is rendered before any statement runs, and nothing is
    // scheduled from this value or compared with a database timestamp.
    byte[] body = request.render(id, Instant.now());

    Optional<KeyHolder> holder = events.findByIdempotencyKey(tenantId, idempotencyKey);
    if (holder.isPresent()) {
      return answerFrom(holder.get(), request);
    }

    List<UUID> matched = events.matchEndpoints(tenantId, request.type());
    NewEvent event =
        new NewEvent(id, tenantId, request.type(), body, idempotencyKey, request.hash());
    if (events.create(event, matched)) {
      return new Accepted(id, false);
    }

    // A concurrent request with the same key committed first. Its event was not visible to the
    // lookup above and is visible to this one.
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
