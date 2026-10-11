package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.endpoint.Endpoint;
import io.github.realjoegz.reliablewebhooks.endpoint.EndpointRepository;
import io.github.realjoegz.reliablewebhooks.endpoint.EndpointService;
import io.github.realjoegz.reliablewebhooks.endpoint.EndpointService.Refused;
import io.github.realjoegz.reliablewebhooks.endpoint.EndpointService.Registered;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1/endpoints")
public class EndpointController {

  private static final int MAX_EVENT_TYPES = 20;
  private static final Pattern EVENT_TYPE = Pattern.compile("[a-z0-9_.-]{1,128}");
  private static final String ALL_EVENT_TYPES = "*";

  record CreateEndpoint(String url, List<String> eventTypes) {

    /** Leaves out the URL, whose path and query can carry a credential of the subscriber. */
    @Override
    public String toString() {
      return "CreateEndpoint[eventTypes=%s]".formatted(eventTypes);
    }
  }

  record CreatedEndpoint(UUID id, String secret) {

    /** Leaves out the secret, so that printing cannot leak it. */
    @Override
    public String toString() {
      return "CreatedEndpoint[id=%s]".formatted(id);
    }
  }

  record PatchEndpoint(List<String> eventTypes) {}

  private final EndpointService service;
  private final EndpointRepository endpoints;

  EndpointController(EndpointService service, EndpointRepository endpoints) {
    this.service = service;
    this.endpoints = endpoints;
  }

  @PostMapping
  ResponseEntity<CreatedEndpoint> create(
      @RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant, @RequestBody CreateEndpoint request) {
    if (request.url() == null) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "url is required");
    }
    requireEventTypes(request.eventTypes());
    return switch (service.register(tenant.id(), request.url(), request.eventTypes())) {
      case Registered registered ->
          ResponseEntity.status(HttpStatus.CREATED)
              .cacheControl(CacheControl.noStore())
              .body(new CreatedEndpoint(registered.id(), registered.secret().value()));
      case Refused.URL_NOT_ALLOWED -> throw Problems.endpointUrlNotAllowed();
      case Refused.LOOKUP_BUSY -> throw Problems.lookupBusy();
      case Refused.LIMIT_REACHED ->
          throw new ResponseStatusException(
              HttpStatus.CONFLICT, "The tenant has reached its endpoint limit");
    };
  }

  @GetMapping
  List<Endpoint> list(@RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant) {
    return endpoints.list(tenant.id());
  }

  @GetMapping("/{id}")
  Endpoint read(@RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant, @PathVariable UUID id) {
    return endpoints.find(tenant.id(), id).orElseThrow(EndpointController::notFound);
  }

  @PatchMapping("/{id}")
  ResponseEntity<Void> patch(
      @RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant,
      @PathVariable UUID id,
      @RequestBody PatchEndpoint request) {
    requireEventTypes(request.eventTypes());
    if (!endpoints.setEventTypes(tenant.id(), id, request.eventTypes())) {
      throw notFound();
    }
    return ResponseEntity.noContent().build();
  }

  private static void requireEventTypes(List<String> eventTypes) {
    if (eventTypes == null
        || eventTypes.isEmpty()
        || eventTypes.size() > MAX_EVENT_TYPES
        || !eventTypes.stream().allMatch(EndpointController::isEventTypeOrWildcard)) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "event_types must have 1 to %d entries, each '*' or 1 to 128 characters of [a-z0-9_.-]"
              .formatted(MAX_EVENT_TYPES));
    }
  }

  private static boolean isEventTypeOrWildcard(String entry) {
    return entry != null && (entry.equals(ALL_EVENT_TYPES) || EVENT_TYPE.matcher(entry).matches());
  }

  // The same answer for an id that does not exist and for one that belongs to another tenant.
  private static ResponseStatusException notFound() {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, "No endpoint has this id");
  }
}
