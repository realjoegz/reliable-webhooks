package io.github.realjoegz.reliablewebhooks.delivery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The statements behind the delivery routes of a tenant. They run on the {@code api} pool, and each
 * of them carries the tenant scope.
 */
@Repository
public class TenantDeliveryRepository {

  // Any status: the deliveries of a deleted endpoint can still be listed.
  static final String ENDPOINT_EXISTS =
      "SELECT EXISTS (SELECT 1 FROM endpoint WHERE id = :e AND tenant_id = :t)";

  // The index delivery_by_endpoint holds the rows of a page side by side and in this order.
  private static final String SELECT_OF_ENDPOINT_IN_STATE =
      """
      SELECT id, event_id, endpoint_id, state, attempts, replays, dead_reason, created_at,
             completed_at
      FROM delivery
      WHERE endpoint_id = :e AND tenant_id = :t AND state = :state
      """;

  static final String FIRST_PAGE =
      SELECT_OF_ENDPOINT_IN_STATE + "ORDER BY created_at, id LIMIT :rows";

  static final String PAGE_AFTER =
      SELECT_OF_ENDPOINT_IN_STATE
          + """
            AND (created_at, id) > (:created_at, :id)
          ORDER BY created_at, id LIMIT :rows
          """;

  static final String FIND =
      """
      SELECT id, event_id, endpoint_id, state, attempts, replays, dead_reason, created_at,
             completed_at
      FROM delivery
      WHERE id = :id AND tenant_id = :t
      """;

  // The attempt rows have no tenant column, so the scope comes from their delivery.
  static final String LIST_ATTEMPTS_OF_RUN =
      """
      SELECT a.attempt_no, a.outcome, a.http_status, a.error, a.started_at, a.duration_ms
      FROM delivery_attempt a
      JOIN delivery d ON d.id = a.delivery_id
      WHERE a.delivery_id = :id AND d.tenant_id = :t AND a.run = :run
      ORDER BY a.attempt_no
      """;

  static final String LOCK_ENDPOINT_FOR_REPLAY =
      """
      SELECT e.id FROM endpoint e JOIN delivery d ON d.endpoint_id = e.id
      WHERE d.id = :id AND d.tenant_id = :t AND e.status = 'active' FOR SHARE OF e
      """;

  // What a replay writes to a delivery, for one delivery and for many. Each row gets a due time
  // of its own within the 60 seconds.
  static final String REARM_COLUMNS =
      """
      SET state = 'pending', attempts = 0, replays = replays + 1, dead_reason = NULL, completed_at = NULL,
          lease_token = NULL,
          next_attempt_at = now() + random() * interval '60 seconds',
          expires_at = now() + interval '72 hours'
      """;

  static final String REARM =
      "UPDATE delivery\n" + REARM_COLUMNS + "WHERE id = :id AND tenant_id = :t AND state = 'dead'";

  static final String ACTIVE_ENDPOINT_EXISTS =
      """
      SELECT EXISTS (SELECT 1 FROM endpoint
                     WHERE id = :e AND tenant_id = :t AND status = 'active')
      """;

  // The index delivery_by_endpoint ends the count at the limit.
  static final String COUNT_DEAD =
      """
      SELECT count(*) FROM (SELECT 1 FROM delivery
                            WHERE endpoint_id = :e AND tenant_id = :t AND state = 'dead'
                            LIMIT :rows) dead
      """;

  static final String LOCK_ENDPOINT_FOR_BULK_REPLAY =
      "SELECT id FROM endpoint WHERE id = :e AND tenant_id = :t AND status = 'active' FOR SHARE";

  // The rows are picked in a CTE, which Postgres runs once. A subquery of IN can be run again for
  // each row that it is compared with. It then passes over the rows that the statement has
  // re-armed already, and its LIMIT fills up with rows that nobody paid for.
  static final String REARM_OLDEST_DEAD =
      """
      WITH picked AS (
        SELECT id FROM delivery
        WHERE endpoint_id = :e AND tenant_id = :t AND state = 'dead'
        ORDER BY created_at, id LIMIT :k
        FOR UPDATE SKIP LOCKED
      )
      UPDATE delivery d
      """
          + REARM_COLUMNS
          + """
            FROM picked
            WHERE d.id = picked.id
            """;

  /**
   * A delivery as its tenant reads it. The attempts are those of its current run, and the replays
   * are the number of that run: a delivery that was never replayed is in run 0. A delivery that is
   * dead or cancelled has a reason, and every delivery that is not pending has a completion time.
   */
  public record Delivery(
      UUID id,
      UUID eventId,
      UUID endpointId,
      String state,
      int attempts,
      int replays,
      String deadReason,
      Instant createdAt,
      Instant completedAt) {}

  /**
   * One row of the attempt log as the tenant reads it. An attempt that got a response has its
   * status and no error, and one that got none has an error and no status. An attempt of unknown
   * outcome has no duration, and its start is the time at which it was logged. The address that a
   * request went to and the replica that sent it stay with the operator.
   */
  public record Attempt(
      int attemptNo,
      String outcome,
      Integer httpStatus,
      String error,
      Instant startedAt,
      Integer durationMs) {}

  /** One page of a delivery list. The cursor is empty when no delivery follows the page. */
  public record Page(List<Delivery> deliveries, Optional<PageCursor> next) {}

  public enum Replay {
    REPLAYED,
    /** The tenant has no delivery with this id, or the endpoint of the delivery was deleted. */
    NOT_FOUND,
    NOT_DEAD
  }

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;

  TenantDeliveryRepository(
      @Qualifier("apiJdbc") JdbcClient jdbc,
      @Qualifier("apiTransactions") TransactionTemplate transactions) {
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  /** Whether the tenant has an endpoint with this id, deleted or not. */
  public boolean endpointExists(UUID tenantId, UUID endpointId) {
    return jdbc.sql(ENDPOINT_EXISTS)
        .param("e", endpointId)
        .param("t", tenantId)
        .query(Boolean.class)
        .single();
  }

  public Page firstPage(UUID tenantId, UUID endpointId, String state, int size) {
    return page(
        jdbc.sql(FIRST_PAGE).param("e", endpointId).param("t", tenantId).param("state", state),
        size);
  }

  public Page pageAfter(
      UUID tenantId, UUID endpointId, String state, PageCursor position, int size) {
    return page(
        jdbc.sql(PAGE_AFTER)
            .param("e", endpointId)
            .param("t", tenantId)
            .param("state", state)
            .param("created_at", position.createdAt().atOffset(ZoneOffset.UTC))
            .param("id", position.id()),
        size);
  }

  public Optional<Delivery> find(UUID tenantId, UUID id) {
    return jdbc.sql(FIND)
        .param("id", id)
        .param("t", tenantId)
        .query(TenantDeliveryRepository::delivery)
        .optional();
  }

  public List<Attempt> attemptsOfRun(UUID tenantId, UUID id, int run) {
    return jdbc.sql(LIST_ATTEMPTS_OF_RUN)
        .param("id", id)
        .param("t", tenantId)
        .param("run", run)
        .query(
            (row, n) ->
                new Attempt(
                    row.getInt("attempt_no"),
                    row.getString("outcome"),
                    row.getObject("http_status", Integer.class),
                    row.getString("error"),
                    instant(row, "started_at"),
                    row.getObject("duration_ms", Integer.class)))
        .list();
  }

  /**
   * Makes a dead delivery pending again, in a new run with no attempt counted, a new 72 hours and a
   * due time within the next 60 seconds.
   *
   * <p>The first statement share-locks the row of the endpoint and reads its status under that
   * lock, which the deletion of an endpoint cannot hold at the same time. Once a deletion has
   * committed, no delivery for that endpoint is made pending. The second statement matches a dead
   * delivery only, so of two replays of one delivery the second finds it pending and changes
   * nothing.
   */
  public Replay replay(UUID tenantId, UUID id) {
    return transactions.execute(
        transaction -> {
          boolean endpointIsActive =
              jdbc.sql(LOCK_ENDPOINT_FOR_REPLAY)
                  .param("id", id)
                  .param("t", tenantId)
                  .query(UUID.class)
                  .optional()
                  .isPresent();
          if (!endpointIsActive) {
            return Replay.NOT_FOUND;
          }
          boolean rearmed = jdbc.sql(REARM).param("id", id).param("t", tenantId).update() == 1;
          return rearmed ? Replay.REPLAYED : Replay.NOT_DEAD;
        });
  }

  boolean activeEndpointExists(UUID tenantId, UUID endpointId) {
    return jdbc.sql(ACTIVE_ENDPOINT_EXISTS)
        .param("e", endpointId)
        .param("t", tenantId)
        .query(Boolean.class)
        .single();
  }

  /** The number of dead deliveries of the endpoint, or {@code atMost} when there are more. */
  int countDead(UUID tenantId, UUID endpointId, int atMost) {
    return jdbc.sql(COUNT_DEAD)
        .param("e", endpointId)
        .param("t", tenantId)
        .param("rows", atMost)
        .query(Integer.class)
        .single();
  }

  /**
   * Makes up to {@code rows} dead deliveries of the endpoint pending again, those that were created
   * first, each as the replay of one delivery does. Returns how many they were, or nothing when the
   * tenant has no active endpoint with this id.
   *
   * <p>The endpoint is share-locked as for one delivery, so no delivery is made pending once the
   * deletion of its endpoint has committed. A dead delivery that another transaction holds is
   * passed over and not waited for: it is being replayed already, or deleted.
   */
  OptionalInt replayOldestDead(UUID tenantId, UUID endpointId, int rows) {
    return transactions.execute(
        transaction -> {
          boolean endpointIsActive =
              jdbc.sql(LOCK_ENDPOINT_FOR_BULK_REPLAY)
                  .param("e", endpointId)
                  .param("t", tenantId)
                  .query(UUID.class)
                  .optional()
                  .isPresent();
          if (!endpointIsActive) {
            return OptionalInt.empty();
          }
          return OptionalInt.of(
              jdbc.sql(REARM_OLDEST_DEAD)
                  .param("e", endpointId)
                  .param("t", tenantId)
                  .param("k", rows)
                  .update());
        });
  }

  // The statement is asked for one row more than the page holds. A row beyond the page is not
  // returned, and says that another page follows.
  private static Page page(JdbcClient.StatementSpec statement, int size) {
    List<Delivery> rows =
        statement.param("rows", size + 1).query(TenantDeliveryRepository::delivery).list();
    if (rows.size() <= size) {
      return new Page(rows, Optional.empty());
    }
    List<Delivery> page = rows.subList(0, size);
    Delivery last = page.getLast();
    return new Page(page, Optional.of(new PageCursor(last.createdAt(), last.id())));
  }

  private static Delivery delivery(ResultSet row, int n) throws SQLException {
    return new Delivery(
        row.getObject("id", UUID.class),
        row.getObject("event_id", UUID.class),
        row.getObject("endpoint_id", UUID.class),
        row.getString("state"),
        row.getInt("attempts"),
        row.getInt("replays"),
        row.getString("dead_reason"),
        instant(row, "created_at"),
        instant(row, "completed_at"));
  }

  private static Instant instant(ResultSet row, String column) throws SQLException {
    OffsetDateTime value = row.getObject(column, OffsetDateTime.class);
    return value == null ? null : value.toInstant();
  }
}
