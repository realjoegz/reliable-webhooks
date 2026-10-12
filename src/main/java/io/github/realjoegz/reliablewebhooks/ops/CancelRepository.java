package io.github.realjoegz.reliablewebhooks.ops;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The statement of the cancel job. It runs on the {@code worker} pool and carries no tenant scope:
 * the job serves every tenant.
 */
@Repository
public class CancelRepository {

  static final String CANCEL =
      """
      WITH picked AS (
        SELECT d.id FROM delivery d JOIN endpoint e ON e.id = d.endpoint_id
        WHERE e.status = 'deleted' AND d.state = 'pending'
        LIMIT 1000 FOR UPDATE OF d SKIP LOCKED
      )
      UPDATE delivery d
      SET state = 'cancelled', dead_reason = 'endpoint_deleted', lease_token = NULL, completed_at = now()
      FROM picked
      WHERE d.id = picked.id
      """;

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;

  CancelRepository(
      @Qualifier("workerJdbc") JdbcClient jdbc,
      @Qualifier("workerTransactions") TransactionTemplate transactions) {
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  /**
   * Cancels up to 1,000 pending deliveries of deleted endpoints and returns how many they were. A
   * row that another transaction holds is skipped. A delivery that is leased is cancelled like any
   * other and loses its lease token with that, so the result of its attempt is turned away.
   *
   * <p>It is one statement and still a transaction, so a statement that outlives its replica cannot
   * commit by itself.
   */
  int cancel() {
    return transactions.execute(transaction -> jdbc.sql(CANCEL).update());
  }
}
