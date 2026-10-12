package io.github.realjoegz.reliablewebhooks.ingest;

import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The rate limit of a tenant, which is its row in {@code rate_bucket}. The bucket fills at the rate
 * of the tenant up to its burst, and a charge takes units out of it. The state is in the database
 * and not in a replica, so a tenant has one limit however many replicas answer it, and a restart
 * does not fill the bucket.
 *
 * <p>The statement reads the rate and the burst from the tenant row, so a change of either counts
 * from the next charge on.
 */
@Repository
public class TokenBucket {

  static final String CHARGE =
      """
      UPDATE rate_bucket b
      SET tokens = least(t.burst, b.tokens + extract(epoch FROM now() - b.refilled_at) * t.rate_per_sec) - :cost,
          refilled_at = now()
      FROM tenant t
      WHERE t.id = b.tenant_id AND b.tenant_id = :t
        AND least(t.burst, b.tokens + extract(epoch FROM now() - b.refilled_at) * t.rate_per_sec) >= :cost
      RETURNING b.tokens
      """;

  private final JdbcClient jdbc;

  TokenBucket(@Qualifier("apiJdbc") JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Takes the units out of the bucket of the tenant and returns true, or returns false and changes
   * nothing when the bucket does not hold that many. Two charges of one tenant take the row one
   * after the other, and the second decides on what the first left.
   *
   * <p>The statement commits by itself, and its caller runs it outside its transaction. Inside one,
   * the lock on the bucket row would last until that transaction ends, and every request of the
   * tenant would wait for it, on every replica. The price is that a charge stays when what it paid
   * for fails afterwards.
   */
  public boolean charge(UUID tenantId, int cost) {
    return jdbc.sql(CHARGE)
        .param("t", tenantId)
        .param("cost", cost)
        .query(Double.class)
        .optional()
        .isPresent();
  }
}
