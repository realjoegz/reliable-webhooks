package io.github.realjoegz.reliablewebhooks.api;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.tenant.ApiKey;
import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import io.github.realjoegz.reliablewebhooks.tenant.TenantRepository;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.PathContainer;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.util.ServletRequestPathUtils;

/**
 * Authenticates every request before it reaches a controller. Admin routes take the admin token,
 * the actuator endpoints are open, and every other path takes a tenant API key, so a new route is
 * protected without being listed here.
 */
@Component
public class ApiKeyFilter extends OncePerRequestFilter {

  /** Request attribute holding the authenticated {@link Tenant} on tenant routes. */
  public static final String TENANT = "webhooks.tenant";

  private static final String BEARER = "Bearer ";

  private final WebhooksProperties properties;
  private final TenantRepository tenants;
  private final ApiKeyCache cache;
  private final HandlerExceptionResolver problems;

  ApiKeyFilter(
      WebhooksProperties properties,
      TenantRepository tenants,
      ApiKeyCache cache,
      @Qualifier("handlerExceptionResolver") HandlerExceptionResolver problems) {
    this.properties = properties;
    this.tenants = tenants;
    this.cache = cache;
    this.problems = problems;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    try {
      authenticate(request);
    } catch (RuntimeException rejection) {
      if (problems.resolveException(request, response, null, rejection) == null) {
        throw rejection;
      }
      return;
    }
    chain.doFilter(request, response);
  }

  private void authenticate(HttpServletRequest request) {
    switch (firstPathSegment(request)) {
      case "actuator" -> {}
      case "admin" -> requireAdminToken(request);
      default -> request.setAttribute(TENANT, requireActiveTenant(request));
    }
  }

  // The segment is read the way Spring MVC matches it, decoded and without path parameters.
  // Reading the raw URI would let "/%61dmin/tenants" pass as a tenant route and then match the
  // admin controller.
  private static String firstPathSegment(HttpServletRequest request) {
    return ServletRequestPathUtils.parse(request).pathWithinApplication().elements().stream()
        .filter(PathContainer.PathSegment.class::isInstance)
        .map(PathContainer.PathSegment.class::cast)
        .map(PathContainer.PathSegment::valueToMatch)
        .findFirst()
        .orElse("");
  }

  private void requireAdminToken(HttpServletRequest request) {
    byte[] presented =
        bearerToken(request).orElseThrow(Problems::unauthorized).getBytes(StandardCharsets.UTF_8);
    byte[] expected = properties.adminToken().getBytes(StandardCharsets.UTF_8);
    // The presented value goes first: isEqual runs for the length of its first argument, so the
    // time it takes says nothing about the token.
    if (!MessageDigest.isEqual(presented, expected)) {
      throw Problems.unauthorized();
    }
  }

  private Tenant requireActiveTenant(HttpServletRequest request) {
    byte[] hash =
        bearerToken(request).flatMap(ApiKey::parse).orElseThrow(Problems::unauthorized).sha256();
    Tenant tenant =
        cache.get(hash, () -> tenants.findByApiKeyHash(hash)).orElseThrow(Problems::unauthorized);
    if (tenant.suspended()) {
      throw Problems.tenantSuspended();
    }
    return tenant;
  }

  private static Optional<String> bearerToken(HttpServletRequest request) {
    String header = request.getHeader(HttpHeaders.AUTHORIZATION);
    if (header == null || !header.regionMatches(true, 0, BEARER, 0, BEARER.length())) {
      return Optional.empty();
    }
    return Optional.of(header.substring(BEARER.length()));
  }
}
