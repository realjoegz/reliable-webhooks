package io.github.realjoegz.reliablewebhooks.ops;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The statement of the expiry job. It runs on the {@code worker} pool and carries no tenant scope:
 * the job serves every tenant.
 */
@Repository
public class ExpiryRepository {

  static final String EXPIRE =
      """
      WITH expired AS (
        UPDATE delivery d
        SET state = 'dead', dead_reason = 'expired', lease_token = NULL, completed_at = now()
        FROM (SELECT id, lease_token IS NOT NULL AS abandoned
              FROM delivery
              WHERE state = 'pending' AND expires_at <= now()
                AND NOT (lease_token IS NOT NULL AND next_attempt_at > now())     -- not leased
              ORDER BY expires_at LIMIT 1000
              FOR UPDATE SKIP LOCKED) x
        WHERE d.id = x.id
        RETURNING d.id, d.replays, d.attempts, x.abandoned
      )
      INSERT INTO delivery_attempt (delivery_id, run, attempt_no, outcome, error)
      SELECT id, replays, attempts, 'unknown', 'lease_expired' FROM expired WHERE abandoned
      """;

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;

  ExpiryRepository(
      @Qualifier("workerJdbc") JdbcClient jdbc,
      @Qualifier("workerTransactions") TransactionTemplate transactions) {
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  /**
   * Dead-letters up to 1,000 deliveries that are pending past their expiry and not leased, the
   * oldest first, and skips a row that another transaction holds. A delivery whose lease ran out
   * with nothing recorded gets the attempt row that its holder did not write. Returns the number of
   * those attempt rows.
   *
   * <p>It is one statement and still a transaction, so a statement that outlives its replica cannot
   * commit by itself.
   */
  int expire() {
    return transactions.execute(transaction -> jdbc.sql(EXPIRE).update());
  }
}
