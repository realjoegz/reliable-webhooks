package io.github.realjoegz.reliablewebhooks.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.realjoegz.reliablewebhooks.tenant.Tenant;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.servlet.ModelAndView;

/**
 * A request is open while the chain behind the filter runs. The tests open requests inside one
 * another: the chain of each sends the next one through the filter.
 */
class TenantConcurrencyFilterTest {

  private static final Tenant TENANT = new Tenant(UUID.randomUUID(), "active", 50, 100);

  private final List<Exception> refusals = new ArrayList<>();
  private final TenantConcurrencyFilter filter =
      new TenantConcurrencyFilter(
          (request, response, handler, refusal) -> {
            refusals.add(refusal);
            return new ModelAndView();
          });

  @Test
  void ninthRequestOfATenantIsRefusedWhileEightAreOpen() throws Exception {
    List<String> reached = new ArrayList<>();

    whileOpen(TENANT, 8, () -> send(TENANT, (request, response) -> reached.add("ninth")));

    assertThat(reached).isEmpty();
    assertThat(refusals).hasSize(1);
    assertThat(refusals.getFirst()).isInstanceOf(ErrorResponseException.class);
    assertThat(((ErrorResponseException) refusals.getFirst()).getBody().getType())
        .isEqualTo(URI.create("concurrency_limit"));
  }

  @Test
  void permitComesBackWhenItsRequestIsOver() throws Exception {
    whileOpen(TENANT, 8, () -> {});

    whileOpen(TENANT, 8, () -> {});

    assertThat(refusals).isEmpty();
  }

  @Test
  void permitComesBackWhenTheRouteFails() throws Exception {
    for (int i = 0; i < 8; i++) {
      assertThatThrownBy(
              () ->
                  send(
                      TENANT,
                      (request, response) -> {
                        throw new ServletException("route failed");
                      }))
          .isInstanceOf(ServletException.class);
    }

    whileOpen(TENANT, 8, () -> {});

    assertThat(refusals).isEmpty();
  }

  @Test
  void refusedRequestTakesNoPermitWithIt() throws Exception {
    whileOpen(TENANT, 8, () -> send(TENANT, (request, response) -> {}));
    assertThat(refusals).hasSize(1);

    whileOpen(TENANT, 8, () -> {});

    assertThat(refusals).hasSize(1);
  }

  @Test
  void tenantsDoNotShareTheirPermits() throws Exception {
    Tenant other = new Tenant(UUID.randomUUID(), "active", 50, 100);

    whileOpen(TENANT, 8, () -> whileOpen(other, 8, () -> {}));

    assertThat(refusals).isEmpty();
  }

  // An admin route and a health endpoint are requests without a tenant.
  @Test
  void requestWithoutATenantTakesNoPermit() throws Exception {
    whileOpen(null, 9, () -> {});

    assertThat(refusals).isEmpty();
  }

  private interface Inside {
    void run() throws IOException, ServletException;
  }

  /** Opens that many requests of the tenant, one inside the other, and runs the rest inside. */
  private void whileOpen(Tenant tenant, int requests, Inside inside)
      throws IOException, ServletException {
    if (requests == 0) {
      inside.run();
      return;
    }
    send(tenant, (request, response) -> whileOpen(tenant, requests - 1, inside));
  }

  private void send(Tenant tenant, FilterChain route) throws IOException, ServletException {
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/v1/events");
    if (tenant != null) {
      request.setAttribute(ApiKeyFilter.TENANT, tenant);
    }
    filter.doFilter(request, new MockHttpServletResponse(), route);
  }
}
