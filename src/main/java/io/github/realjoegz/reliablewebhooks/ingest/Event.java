package io.github.realjoegz.reliablewebhooks.ingest;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** An event as its tenant reads it, with one delivery for each endpoint the event went to. */
public record Event(UUID id, String type, Instant createdAt, List<Delivery> deliveries) {

  public record Delivery(UUID id, UUID endpointId, String state, int attempts) {}
}
