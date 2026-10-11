package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RestController;

/**
 * A tenant route for tests of the request filters. Component scanning finds it on the test
 * classpath, so every replica that runs inside the test JVM serves it and the packaged service does
 * not.
 */
@RestController
class WhoAmIController {

  @GetMapping("/v1/whoami")
  Tenant whoAmI(@RequestAttribute(ApiKeyFilter.TENANT) Tenant tenant) {
    return tenant;
  }
}
