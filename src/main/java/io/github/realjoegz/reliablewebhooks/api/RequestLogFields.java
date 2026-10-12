package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Names this replica, and the tenant of a tenant route, on every line that is logged while a route
 * handles a request. The trace id and the span id of the request are on those lines without it: the
 * tracing of Spring keeps them in the logging context for as long as the span of the request is
 * current.
 *
 * <p>Nothing of the request itself is among the fields: no header, no path and no body.
 */
@Component
class RequestLogFields implements HandlerInterceptor, WebMvcConfigurer {

  private static final String REPLICA = "replica";
  private static final String TENANT_ID = "tenant_id";

  private final String replica;

  RequestLogFields(WebhooksProperties properties) {
    this.replica = properties.replicaId();
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(this);
  }

  @Override
  public boolean preHandle(
      HttpServletRequest request, HttpServletResponse response, Object handler) {
    MDC.put(REPLICA, replica);
    // An admin route has no tenant.
    if (request.getAttribute(ApiKeyFilter.TENANT) instanceof Tenant tenant) {
      MDC.put(TENANT_ID, tenant.id().toString());
    }
    return true;
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception failure) {
    MDC.remove(TENANT_ID);
    MDC.remove(REPLICA);
  }
}
