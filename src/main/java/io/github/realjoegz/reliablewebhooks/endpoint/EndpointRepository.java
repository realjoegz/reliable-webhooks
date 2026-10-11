package io.github.realjoegz.reliablewebhooks.endpoint;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
public class EndpointRepository {

  static final String LOCK_TENANT =
      "SELECT max_endpoints FROM tenant WHERE id = :t FOR NO KEY UPDATE";

  static final String COUNT_ACTIVE =
      "SELECT count(*) FROM endpoint WHERE tenant_id = :t AND status = 'active'";

  static final String INSERT_ENDPOINT =
      """
      INSERT INTO endpoint (tenant_id, url, event_types, secret)
      VALUES (:t, :url, :event_types, :secret) RETURNING id
      """;

  static final String INSERT_CIRCUIT = "INSERT INTO endpoint_circuit (endpoint_id) VALUES (:id)";

  // Only closed and open are stored. An open circuit whose probe time has passed reads as
  // half_open: the next claim may take one delivery to the endpoint as a probe.
  private static final String SELECT_ACTIVE =
      """
      SELECT e.id, e.url, e.event_types, e.created_at,
             CASE WHEN c.state = 'closed' THEN 'closed'
                  WHEN c.next_probe_at <= now() THEN 'half_open'
                  ELSE 'open'
             END AS circuit
      FROM endpoint e
      JOIN endpoint_circuit c ON c.endpoint_id = e.id
      WHERE e.tenant_id = :t AND e.status = 'active'
      """;

  static final String LIST = SELECT_ACTIVE + "ORDER BY e.created_at, e.id";

  static final String FIND = SELECT_ACTIVE + "AND e.id = :id";

  static final String SET_EVENT_TYPES =
      """
      UPDATE endpoint SET event_types = :event_types
      WHERE id = :id AND tenant_id = :t AND status = 'active'
      """;

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;

  EndpointRepository(
      @Qualifier("apiJdbc") JdbcClient jdbc,
      @Qualifier("apiTransactions") TransactionTemplate transactions) {
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  /**
   * Empty when the tenant already has as many active endpoints as it may have. The lock on the
   * tenant row makes concurrent creates of one tenant count one after the other, so the cap is
   * exact. The endpoint and its circuit commit together, so an endpoint never exists without a
   * circuit row.
   */
  Optional<UUID> create(
      UUID tenantId, String url, List<String> eventTypes, byte[] encryptedSecret) {
    return transactions.execute(
        transaction -> {
          int cap = jdbc.sql(LOCK_TENANT).param("t", tenantId).query(Integer.class).single();
          int active = jdbc.sql(COUNT_ACTIVE).param("t", tenantId).query(Integer.class).single();
          if (active >= cap) {
            return Optional.empty();
          }
          UUID id =
              jdbc.sql(INSERT_ENDPOINT)
                  .param("t", tenantId)
                  .param("url", url)
                  .param("event_types", eventTypes.toArray(String[]::new))
                  .param("secret", encryptedSecret)
                  .query(UUID.class)
                  .single();
          jdbc.sql(INSERT_CIRCUIT).param("id", id).update();
          return Optional.of(id);
        });
  }

  public List<Endpoint> list(UUID tenantId) {
    return jdbc.sql(LIST).param("t", tenantId).query(EndpointRepository::endpoint).list();
  }

  public Optional<Endpoint> find(UUID tenantId, UUID id) {
    return jdbc.sql(FIND)
        .param("t", tenantId)
        .param("id", id)
        .query(EndpointRepository::endpoint)
        .optional();
  }

  /** Returns false when the tenant has no active endpoint with this id. */
  public boolean setEventTypes(UUID tenantId, UUID id, List<String> eventTypes) {
    return jdbc.sql(SET_EVENT_TYPES)
            .param("event_types", eventTypes.toArray(String[]::new))
            .param("id", id)
            .param("t", tenantId)
            .update()
        == 1;
  }

  private static Endpoint endpoint(ResultSet row, int n) throws SQLException {
    return new Endpoint(
        row.getObject("id", UUID.class),
        row.getString("url"),
        List.of((String[]) row.getArray("event_types").getArray()),
        row.getString("circuit"),
        row.getObject("created_at", OffsetDateTime.class).toInstant());
  }
}
