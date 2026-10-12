package io.github.realjoegz.reliablewebhooks.harness;

import io.github.realjoegz.reliablewebhooks.api.ApiKeyFilter;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Counts, for each tenant, the requests that a route of a replica inside the test JVM has been
 * given and has not finished. A request is counted once every filter of the service has let it
 * through and before the route reads its body. A test waits on the count to know that requests
 * which it keeps unfinished are inside the service, behind the filters.
 */
@Component
public final class OpenRequests implements HandlerInterceptor, WebMvcConfigurer {

  private final Map<UUID, Integer> open = new ConcurrentHashMap<>();

  /** The requests of the tenant that are with a route right now. */
  public int of(UUID tenantId) {
    return open.getOrDefault(tenantId, 0);
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(this);
  }

  @Override
  public boolean preHandle(
      HttpServletRequest request, HttpServletResponse response, Object handler) {
    if (request.getAttribute(ApiKeyFilter.TENANT) instanceof Tenant tenant) {
      open.merge(tenant.id(), 1, Integer::sum);
    }
    return true;
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception failure) {
    if (request.getAttribute(ApiKeyFilter.TENANT) instanceof Tenant tenant) {
      open.computeIfPresent(tenant.id(), (id, requests) -> requests == 1 ? null : requests - 1);
    }
  }
}
