package io.github.realjoegz.reliablewebhooks.ingest;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
public class EventRepository {

  static final String FIND_BY_IDEMPOTENCY_KEY =
      "SELECT id, request_hash FROM event WHERE tenant_id = :t AND idempotency_key = :k";

  // pgjdbc sends a string as varchar, and Postgres has no text[] @> varchar[]. Hence the cast.
  static final String MATCH_ENDPOINTS =
      """
      SELECT id FROM endpoint
      WHERE tenant_id = :t AND status = 'active'
        AND (event_types @> ARRAY['*'] OR event_types @> ARRAY[:type::text])
      """;

  // trace_context is for the context of the ingest span, and ingest starts no span.
  static final String INSERT_EVENT =
      """
      INSERT INTO event (id, tenant_id, type, body, idempotency_key, request_hash, trace_context)
      VALUES (:id, :t, :type, :body, :k, :hash, NULL)
      ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING id
      """;

  static final String INSERT_DELIVERIES =
      """
      INSERT INTO delivery (tenant_id, event_id, endpoint_id, next_attempt_at, expires_at)
      SELECT e.tenant_id, :event_id, e.id, now(), now() + interval '72 hours'
      FROM endpoint e
      WHERE e.id = ANY(:matched) AND e.tenant_id = :t AND e.status = 'active'
      FOR SHARE OF e
      """;

  static final String FIND = "SELECT type, created_at FROM event WHERE id = :id AND tenant_id = :t";

  // At most 50 rows: an event has one delivery for each endpoint that matched it, and a tenant
  // has at most 50 active endpoints.
  static final String LIST_DELIVERIES =
      """
      SELECT id, endpoint_id, state, attempts FROM delivery
      WHERE event_id = :id AND tenant_id = :t
      ORDER BY endpoint_id
      """;

  /** The event that holds an idempotency key, with the hash of the request that created it. */
  record KeyHolder(UUID eventId, byte[] requestHash) {}

  record NewEvent(
      UUID id,
      UUID tenantId,
      String type,
      byte[] body,
      String idempotencyKey,
      byte[] requestHash) {}

  private record Stored(String type, Instant createdAt) {}

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;

  EventRepository(
      @Qualifier("apiJdbc") JdbcClient jdbc,
      @Qualifier("apiTransactions") TransactionTemplate transactions) {
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  Optional<KeyHolder> findByIdempotencyKey(UUID tenantId, String idempotencyKey) {
    return jdbc.sql(FIND_BY_IDEMPOTENCY_KEY)
        .param("t", tenantId)
        .param("k", idempotencyKey)
        .query(
            (row, n) ->
                new KeyHolder(row.getObject("id", UUID.class), row.getBytes("request_hash")))
        .optional();
  }

  /** The active endpoints of the tenant that take events of this type. */
  List<UUID> matchEndpoints(UUID tenantId, String type) {
    return jdbc.sql(MATCH_ENDPOINTS)
        .param("t", tenantId)
        .param("type", type)
        .query(UUID.class)
        .list();
  }

  /**
   * Stores the event and one delivery for each matched endpoint that is still active, in one
   * transaction. Returns false, with nothing stored, when the tenant already has an event with this
   * idempotency key. An insert that meets the uncommitted event of a concurrent request with the
   * same key waits until that request has committed or rolled back.
   *
   * <p>The second insert share-locks each endpoint row and reads its status under that lock. An
   * endpoint whose deletion committed while the insert waited for the row gets no delivery.
   */
  boolean create(NewEvent event, List<UUID> matched) {
    return transactions.execute(
        transaction -> {
          boolean inserted =
              jdbc.sql(INSERT_EVENT)
                  .param("id", event.id())
                  .param("t", event.tenantId())
                  .param("type", event.type())
                  .param("body", event.body())
                  .param("k", event.idempotencyKey())
                  .param("hash", event.requestHash())
                  .query(UUID.class)
                  .optional()
                  .isPresent();
          if (inserted) {
            jdbc.sql(INSERT_DELIVERIES)
                .param("event_id", event.id())
                .param("matched", matched.toArray(UUID[]::new))
                .param("t", event.tenantId())
                .update();
          }
          return inserted;
        });
  }

  public Optional<Event> find(UUID tenantId, UUID id) {
    return jdbc.sql(FIND)
        .param("id", id)
        .param("t", tenantId)
        .query(
            (row, n) ->
                new Stored(
                    row.getString("type"),
                    row.getObject("created_at", OffsetDateTime.class).toInstant()))
        .optional()
        .map(
            stored -> new Event(id, stored.type(), stored.createdAt(), deliveriesOf(tenantId, id)));
  }

  private List<Event.Delivery> deliveriesOf(UUID tenantId, UUID eventId) {
    return jdbc.sql(LIST_DELIVERIES)
        .param("id", eventId)
        .param("t", tenantId)
        .query(
            (row, n) ->
                new Event.Delivery(
                    row.getObject("id", UUID.class),
                    row.getObject("endpoint_id", UUID.class),
                    row.getString("state"),
                    row.getInt("attempts")))
        .list();
  }
}
