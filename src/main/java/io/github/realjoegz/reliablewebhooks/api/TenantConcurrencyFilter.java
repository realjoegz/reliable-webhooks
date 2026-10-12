package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerExceptionResolver;

/**
 * Lets a tenant have 8 requests open on this replica and refuses every further one while they are.
 * A request holds its permit from here until it is over, which covers every statement that a route
 * runs for it. The routes of a tenant therefore hold at most 8 of the connections of the {@code
 * api} pool, and a request that is refused here runs no statement.
 *
 * <p>The filter comes after {@link ApiKeyFilter}, which names the tenant. The key lookup of a
 * request is therefore not behind a permit. A replica keeps its result for 30 seconds.
 *
 * <p>The count is kept in this replica and is gone with it, like the requests that it counts.
 */
@Component
@Order(ApiKeyFilter.ORDER + 1)
public class TenantConcurrencyFilter extends OncePerRequestFilter {

  private static final int PERMITS = 8;

  private final Map<UUID, Integer> openRequests = new HashMap<>();
  private final HandlerExceptionResolver problems;

  TenantConcurrencyFilter(
      @Qualifier("handlerExceptionResolver") HandlerExceptionResolver problems) {
    this.problems = problems;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    // Admin routes and the health endpoints have no tenant and take no permit.
    if (!(request.getAttribute(ApiKeyFilter.TENANT) instanceof Tenant tenant)) {
      chain.doFilter(request, response);
      return;
    }
    if (!enter(tenant.id())) {
      ErrorResponseException rejection = Problems.concurrencyLimit();
      if (problems.resolveException(request, response, null, rejection) == null) {
        throw rejection;
      }
      return;
    }
    try {
      chain.doFilter(request, response);
    } finally {
      leave(tenant.id());
    }
  }

  private synchronized boolean enter(UUID tenantId) {
    int open = openRequests.getOrDefault(tenantId, 0);
    if (open == PERMITS) {
      return false;
    }
    openRequests.put(tenantId, open + 1);
    return true;
  }

  private synchronized void leave(UUID tenantId) {
    openRequests.computeIfPresent(tenantId, (id, open) -> open == 1 ? null : open - 1);
  }
}
