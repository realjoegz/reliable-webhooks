package io.github.realjoegz.reliablewebhooks.ops;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The statement of the queue gauges. It runs on the {@code worker} pool and carries no tenant
 * scope: the gauges count the deliveries of every tenant.
 */
@Repository
public class QueueRepository {

  static final String COUNT =
      """
      WITH pending AS (
        SELECT d.next_attempt_at,
               d.next_attempt_at <= now() AS due,
               d.lease_token IS NOT NULL  AS has_token,
               t.status = 'active'                                        -- as the claim decides
                 AND (c.state = 'closed' OR c.next_probe_at <= now()) AS claimable
        FROM delivery d
        JOIN endpoint e         ON e.id = d.endpoint_id
        JOIN tenant t           ON t.id = e.tenant_id
        JOIN endpoint_circuit c ON c.endpoint_id = e.id
        WHERE d.state = 'pending' AND e.status = 'active'
      )
      SELECT count(*) FILTER (WHERE due AND claimable)         AS due,
             count(*) FILTER (WHERE due AND NOT claimable)     AS held,
             count(*) FILTER (WHERE NOT due AND NOT has_token) AS scheduled,
             count(*) FILTER (WHERE NOT due AND has_token)     AS leased,
             coalesce(extract(epoch FROM now() - min(next_attempt_at) FILTER (WHERE due AND claimable)), 0)
               AS oldest_due_age,
             (SELECT count(*) FROM endpoint_circuit c JOIN endpoint e ON e.id = c.endpoint_id
              WHERE c.state = 'open' AND e.status = 'active') AS circuits_open
      FROM pending
      """;

  /**
   * The pending deliveries of active endpoints, by what a claim would do with them at the time of
   * the statement, and the open circuits of active endpoints.
   *
   * <p>A due delivery is one that a claim could take, and its age is how long it has been due. A
   * held delivery is due as well, and no claim takes it: the circuit of its endpoint is open with
   * no probe due, or its tenant is suspended. A leased delivery has a lease that still runs, and a
   * scheduled one waits for the time of its next attempt.
   */
  record Queue(
      long due, long held, long scheduled, long leased, Duration oldestDueAge, long circuitsOpen) {}

  private final JdbcClient jdbc;

  QueueRepository(@Qualifier("workerJdbc") JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  Queue read() {
    return jdbc.sql(COUNT)
        .query(
            (row, n) ->
                new Queue(
                    row.getLong("due"),
                    row.getLong("held"),
                    row.getLong("scheduled"),
                    row.getLong("leased"),
                    Duration.ofNanos(
                        row.getBigDecimal("oldest_due_age").movePointRight(9).longValue()),
                    row.getLong("circuits_open")))
        .single();
  }
}
