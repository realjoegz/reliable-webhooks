package io.github.realjoegz.reliablewebhooks.api;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.github.realjoegz.reliablewebhooks.delivery.PageCursor;
import io.github.realjoegz.reliablewebhooks.delivery.TenantDeliveryRepository;
import io.github.realjoegz.reliablewebhooks.delivery.TenantDeliveryRepository.Attempt;
import io.github.realjoegz.reliablewebhooks.delivery.TenantDeliveryRepository.Delivery;
import io.github.realjoegz.reliablewebhooks.delivery.TenantDeliveryRepository.Page;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1/deliveries")
public class DeliveryController {

  private static final int MAX_PAGE_SIZE = 100;
  private static final Set<String> STATES = Set.of("pending", "succeeded", "dead", "cancelled");

  /** A page of a list. {@code next} is the cursor of the page that follows, or null at the end. */
  record ListedDeliveries(List<Delivery> deliveries, String next) {}

  record DeliveryWithAttempts(
      @JsonUnwrapped Delivery delivery, int run, List<Attempt> attemptLog) {}

  private final TenantDeliveryRepository deliveries;

  DeliveryController(TenantDeliveryRepository deliveries) {
    this.deliveries = deliveries;
  }

  // Both filters are required, so that every list is one range of one index. A request is checked
  // for its form before any statement runs.
  @GetMapping
  ListedDeliveries list(
      @RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant,
      @RequestParam("endpoint_id") UUID endpointId,
      @RequestParam String state,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) String cursor) {
    if (!STATES.contains(state)) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "state must be pending, succeeded, dead or cancelled");
    }
    int size = limit == null ? MAX_PAGE_SIZE : limit;
    if (size < 1 || size > MAX_PAGE_SIZE) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "limit must be between 1 and %d".formatted(MAX_PAGE_SIZE));
    }
    PageCursor position = cursor == null ? null : position(cursor);
    if (!deliveries.endpointExists(tenant.id(), endpointId)) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No endpoint has this id");
    }
    Page page =
        position == null
            ? deliveries.firstPage(tenant.id(), endpointId, state, size)
            : deliveries.pageAfter(tenant.id(), endpointId, state, position, size);
    return new ListedDeliveries(
        page.deliveries(), page.next().map(PageCursor::encode).orElse(null));
  }

  @GetMapping("/{id}")
  DeliveryWithAttempts read(
      @RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant,
      @PathVariable UUID id,
      @RequestParam(required = false) Integer run) {
    // The same answer for an id that does not exist and for one that belongs to another tenant.
    Delivery delivery =
        deliveries
            .find(tenant.id(), id)
            .orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No delivery has this id"));
    int shown = run == null ? delivery.replays() : run;
    if (shown < 0 || shown > delivery.replays()) {
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND, "The delivery has no run %d".formatted(shown));
    }
    return new DeliveryWithAttempts(
        delivery, shown, deliveries.attemptsOfRun(tenant.id(), id, shown));
  }

  @PostMapping("/{id}/replay")
  ResponseEntity<Void> replay(
      @RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant, @PathVariable UUID id) {
    return switch (deliveries.replay(tenant.id(), id)) {
      case REPLAYED -> ResponseEntity.noContent().build();
      case NOT_FOUND ->
          throw new ResponseStatusException(
              HttpStatus.NOT_FOUND, "No delivery to an active endpoint has this id");
      case NOT_DEAD ->
          throw new ResponseStatusException(
              HttpStatus.CONFLICT, "Only a dead delivery can be replayed");
    };
  }

  private static PageCursor position(String cursor) {
    return PageCursor.parse(cursor)
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "cursor must be the next of an earlier page"));
  }
}
