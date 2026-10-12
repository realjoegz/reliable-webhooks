package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.tenant.ApiKey;
import io.github.realjoegz.reliablewebhooks.tenant.TenantRepository;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/admin/tenants")
public class AdminController {

  private static final int MAX_NAME_LENGTH = 200;
  private static final int MAX_ENDPOINT_CAP = 50;
  private static final Set<String> STATUSES = Set.of("active", "suspended");

  // The statement of the bucket multiplies the rate by the seconds since the last charge, and the
  // database fails a product that a double cannot hold. A rate between these two stays far from
  // both ends of that range, however short or long that time is.
  private static final double MIN_RATE = 0.000001;
  private static final double MAX_RATE = 1_000_000;

  record CreateTenant(String name) {}

  record CreatedTenant(UUID id, String apiKey) {

    /** Leaves out the API key, so that printing cannot leak it. */
    @Override
    public String toString() {
      return "CreatedTenant[id=%s]".formatted(id);
    }
  }

  record RotatedApiKey(String apiKey) {

    /** Leaves out the API key, so that printing cannot leak it. */
    @Override
    public String toString() {
      return "RotatedApiKey[redacted]";
    }
  }

  /** A member that is absent or null leaves that setting of the tenant as it is. */
  record PatchTenant(Double ratePerSec, Integer burst, Integer maxEndpoints, String status) {}

  private final TenantRepository tenants;
  private final ApiKeyCache cache;

  AdminController(TenantRepository tenants, ApiKeyCache cache) {
    this.tenants = tenants;
    this.cache = cache;
  }

  @PostMapping
  ResponseEntity<CreatedTenant> create(@RequestBody CreateTenant request) {
    String name = request.name();
    if (name == null || name.isEmpty() || name.codePointCount(0, name.length()) > MAX_NAME_LENGTH) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "name must have 1 to %d characters".formatted(MAX_NAME_LENGTH));
    }
    ApiKey key = ApiKey.generate();
    UUID id = tenants.create(name, key.sha256());
    return ResponseEntity.status(HttpStatus.CREATED)
        .cacheControl(CacheControl.noStore())
        .body(new CreatedTenant(id, key.value()));
  }

  @PatchMapping("/{id}")
  ResponseEntity<Void> patch(@PathVariable UUID id, @RequestBody PatchTenant request) {
    requireValid(request);
    if (!tenants.patch(
        id, request.ratePerSec(), request.burst(), request.maxEndpoints(), request.status())) {
      throw noSuchTenant();
    }
    // The cached tenant carries the status, the rate and the burst. Other replicas keep theirs
    // until their own entry expires.
    cache.evict(id);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/{id}/api-key/rotate")
  ResponseEntity<RotatedApiKey> rotate(@PathVariable UUID id) {
    ApiKey key = ApiKey.generate();
    if (!tenants.replaceApiKey(id, key.sha256())) {
      throw noSuchTenant();
    }
    // Other replicas keep serving the old key until their own entry expires.
    cache.evict(id);
    return ResponseEntity.ok()
        .cacheControl(CacheControl.noStore())
        .body(new RotatedApiKey(key.value()));
  }

  // The limits of the schema and those on the rate, checked here so that a value outside them is
  // the fault of the request and not a failed statement.
  private static void requireValid(PatchTenant request) {
    Double rate = request.ratePerSec();
    Integer burst = request.burst();
    Integer cap = request.maxEndpoints();
    String status = request.status();
    if (rate == null && burst == null && cap == null && status == null) {
      throw badRequest("One of rate_per_sec, burst, max_endpoints and status is required");
    }
    if (rate != null && rate != 0 && !(rate >= MIN_RATE && rate <= MAX_RATE)) {
      throw badRequest("rate_per_sec must be 0, or between 0.000001 and 1000000");
    }
    if (burst != null && burst < 0) {
      throw badRequest("burst must be 0 or more");
    }
    if (cap != null && (cap < 0 || cap > MAX_ENDPOINT_CAP)) {
      throw badRequest("max_endpoints must be between 0 and %d".formatted(MAX_ENDPOINT_CAP));
    }
    if (status != null && !STATUSES.contains(status)) {
      throw badRequest("status must be active or suspended");
    }
  }

  private static ResponseStatusException badRequest(String detail) {
    return new ResponseStatusException(HttpStatus.BAD_REQUEST, detail);
  }

  private static ResponseStatusException noSuchTenant() {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, "No tenant has this id");
  }
}
