package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.tenant.ApiKey;
import io.github.realjoegz.reliablewebhooks.tenant.TenantRepository;
import java.util.UUID;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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

  @PostMapping("/{id}/api-key/rotate")
  ResponseEntity<RotatedApiKey> rotate(@PathVariable UUID id) {
    ApiKey key = ApiKey.generate();
    if (!tenants.replaceApiKey(id, key.sha256())) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No tenant has this id");
    }
    // Other replicas keep serving the old key until their own entry expires.
    cache.evict(id);
    return ResponseEntity.ok()
        .cacheControl(CacheControl.noStore())
        .body(new RotatedApiKey(key.value()));
  }
}
