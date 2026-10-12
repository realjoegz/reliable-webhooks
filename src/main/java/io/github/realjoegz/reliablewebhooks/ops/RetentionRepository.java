package io.github.realjoegz.reliablewebhooks.ops;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The statements of the retention job. They run on the {@code worker} pool and carry no tenant
 * scope: the job serves every tenant.
 *
 * <p>Each takes up to 1,000 rows and returns how many they were. Each picks its rows in a CTE that
 * locks them and skips a row that another transaction holds, with the predicate on the locked row:
 * a row that changed before the lock was taken is judged as it is then. Each is one statement and
 * still a transaction, so a statement that outlives its replica cannot commit by itself.
 *
 * <p>The 30 days of retention are written as 720 hours. Postgres counts the days of an interval in
 * the time zone of the session, which is the zone of the replica: 30 days are 719 or 721 hours
 * across a change of the clocks, and two replicas in different zones would not agree on them.
 *
 * <p>The foreign keys from a delivery to its event and to its endpoint do not cascade. A statement
 * that tried to delete an event or an endpoint which still has a delivery would fail.
 */
@Repository
public class RetentionRepository {

  static final String DELETE_TERMINAL_DELIVERIES =
      """
      WITH picked AS (
        SELECT id FROM delivery
        WHERE state <> 'pending' AND completed_at < now() - interval '720 hours'
        ORDER BY completed_at LIMIT 1000
        FOR UPDATE SKIP LOCKED
      )
      DELETE FROM delivery USING picked WHERE delivery.id = picked.id
      """;

  static final String DELETE_EVENTS_WITHOUT_DELIVERIES =
      """
      WITH picked AS (
        SELECT ev.id FROM event ev
        WHERE ev.created_at < now() - interval '720 hours'
          AND NOT EXISTS (SELECT 1 FROM delivery d WHERE d.event_id = ev.id)
        ORDER BY ev.created_at LIMIT 1000
        FOR UPDATE OF ev SKIP LOCKED
      )
      DELETE FROM event USING picked WHERE event.id = picked.id
      """;

  static final String DELETE_DELETED_ENDPOINTS_WITHOUT_DELIVERIES =
      """
      WITH picked AS (
        SELECT e.id FROM endpoint e
        WHERE e.status = 'deleted'
          AND NOT EXISTS (SELECT 1 FROM delivery d WHERE d.endpoint_id = e.id)
        LIMIT 1000
        FOR UPDATE OF e SKIP LOCKED
      )
      DELETE FROM endpoint USING picked WHERE endpoint.id = picked.id
      """;

  static final String CLEAR_EXPIRED_PREVIOUS_SECRETS =
      """
      WITH picked AS (
        SELECT id FROM endpoint
        WHERE prev_secret_expires_at <= now()
        LIMIT 1000
        FOR UPDATE SKIP LOCKED
      )
      UPDATE endpoint SET prev_secret = NULL, prev_secret_expires_at = NULL
      FROM picked
      WHERE endpoint.id = picked.id
      """;

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;

  RetentionRepository(
      @Qualifier("workerJdbc") JdbcClient jdbc,
      @Qualifier("workerTransactions") TransactionTemplate transactions) {
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  /**
   * Deletes deliveries that reached a final state more than 30 days ago, those that reached it
   * first, with their attempt rows. A pending delivery is never among them, whatever its age: a
   * delivery that a replay has made pending again has no completion time.
   */
  int deleteTerminalDeliveries() {
    return run(DELETE_TERMINAL_DELIVERIES);
  }

  /**
   * Deletes events that are more than 30 days old and have no delivery. Deliveries are created with
   * their event and at no other time, so such an event never gets one.
   */
  int deleteEventsWithoutDeliveries() {
    return run(DELETE_EVENTS_WITHOUT_DELIVERIES);
  }

  /**
   * Deletes deleted endpoints that have no delivery, with their circuit rows. No delivery is
   * created for an endpoint once its deletion has committed, so such an endpoint never gets one.
   */
  int deleteDeletedEndpointsWithoutDeliveries() {
    return run(DELETE_DELETED_ENDPOINTS_WITHOUT_DELIVERIES);
  }

  int clearExpiredPreviousSecrets() {
    return run(CLEAR_EXPIRED_PREVIOUS_SECRETS);
  }

  private int run(String statement) {
    return transactions.execute(transaction -> jdbc.sql(statement).update());
  }
}
