package io.github.realjoegz.reliablewebhooks.endpoint;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * An endpoint as its tenant reads it. The circuit is {@code closed}, {@code open} or {@code
 * half_open}. The signing secret is not part of it.
 */
public record Endpoint(
    UUID id, String url, List<String> eventTypes, String circuit, Instant createdAt) {

  /** Leaves out the URL, whose path and query can carry a credential of the subscriber. */
  @Override
  public String toString() {
    return "Endpoint[id=%s, eventTypes=%s, circuit=%s, createdAt=%s]"
        .formatted(id, eventTypes, circuit, createdAt);
  }
}
