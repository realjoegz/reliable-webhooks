package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.ingest.Event;
import io.github.realjoegz.reliablewebhooks.ingest.EventRepository;
import io.github.realjoegz.reliablewebhooks.ingest.EventRequest;
import io.github.realjoegz.reliablewebhooks.ingest.IngestService;
import io.github.realjoegz.reliablewebhooks.ingest.IngestService.Accepted;
import io.github.realjoegz.reliablewebhooks.ingest.IngestService.RateLimited;
import io.github.realjoegz.reliablewebhooks.ingest.IngestService.Refused;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping(EventController.PATH)
public class EventController {

  static final String PATH = "/v1/events";
  static final String IDEMPOTENT_REPLAY = "Idempotent-Replay";

  private static final String IDEMPOTENCY_KEY = "Idempotency-Key";

  private static final Pattern PRINTABLE_ASCII_KEY = Pattern.compile("[ -~]{1,255}");

  record AcceptedEvent(UUID id) {}

  private final IngestService service;
  private final EventRepository events;

  EventController(IngestService service, EventRepository events) {
    this.service = service;
    this.events = events;
  }

  // The body arrives as bytes, because the idempotency check compares the request as it was sent.
  @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
  ResponseEntity<AcceptedEvent> ingest(
      @RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant,
      @RequestHeader(name = IDEMPOTENCY_KEY, required = false) String idempotencyKey,
      @RequestBody byte[] body) {
    if (idempotencyKey == null || !PRINTABLE_ASCII_KEY.matcher(idempotencyKey).matches()) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "Idempotency-Key is required and must have 1 to 255 printable ASCII characters");
    }
    EventRequest request =
        EventRequest.parse(body)
            .orElseThrow(
                () ->
                    new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "The body must be a JSON object in UTF-8 without a byte order mark,"
                            + " with a type of 1 to 128 characters of [a-z0-9_.-] and an object"
                            + " as data. It must keep to the limits on numbers, member names and"
                            + " nesting depth."));
    return switch (service.ingest(tenant.id(), idempotencyKey, request)) {
      case Accepted accepted -> {
        ResponseEntity.BodyBuilder response = ResponseEntity.accepted();
        if (accepted.replay()) {
          response.header(IDEMPOTENT_REPLAY, "true");
        }
        yield response.body(new AcceptedEvent(accepted.eventId()));
      }
      case Refused.IDEMPOTENCY_KEY_REUSED -> throw Problems.idempotencyKeyReused();
      case RateLimited limited -> throw Problems.rateLimited(tenant, limited.cost());
    };
  }

  @GetMapping("/{id}")
  Event read(@RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant, @PathVariable UUID id) {
    // The same answer for an id that does not exist and for one that belongs to another tenant.
    return events
        .find(tenant.id(), id)
        .orElseThrow(
            () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No event has this id"));
  }
}
