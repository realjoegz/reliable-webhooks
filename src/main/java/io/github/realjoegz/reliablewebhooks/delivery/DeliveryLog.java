package io.github.realjoegz.reliablewebhooks.delivery;

import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import org.slf4j.spi.LoggingEventBuilder;

/**
 * The fields that a log line about a delivery carries: ids, the number of the attempt and the
 * replica. The URL of the endpoint, its secret, the signature and the body of the event are never
 * among them.
 */
final class DeliveryLog {

  private final String replica;

  DeliveryLog(String replica) {
    this.replica = replica;
  }

  LoggingEventBuilder about(Claimed delivery, LoggingEventBuilder line) {
    return line.addKeyValue("tenant_id", delivery.tenantId())
        .addKeyValue("event_id", delivery.eventId())
        .addKeyValue("delivery_id", delivery.id())
        .addKeyValue("endpoint_id", delivery.endpointId())
        .addKeyValue("attempt", delivery.attempts())
        .addKeyValue("replica", replica);
  }
}
