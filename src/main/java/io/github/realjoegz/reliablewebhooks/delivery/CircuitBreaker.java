package io.github.realjoegz.reliablewebhooks.delivery;

import java.time.Duration;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The circuit of an endpoint, which is its row in {@code endpoint_circuit}. The state is in the
 * database and not in a replica, so an open circuit holds the deliveries of its endpoint on every
 * replica and is still open after a restart. The claim reads it: an endpoint behind an open circuit
 * is left out until the time of its next probe has passed.
 *
 * <p>The statements run on the {@code worker} pool and carry no tenant scope, like those of the
 * delivery worker, which is their only caller.
 */
@Repository
public class CircuitBreaker {

  /**
   * How long the gate keeps a probe slot taken. The gate holds the same time as text, and the
   * startup invariants read it here: a probe has to be over before its slot is free again.
   */
  public static final Duration PROBE_SLOT = Duration.ofSeconds(60);

  static final String GATE =
      """
      UPDATE endpoint_circuit SET next_probe_at = now() + interval '60 seconds'
      WHERE endpoint_id = :e AND state = 'open' AND next_probe_at <= now()
      """;

  static final String CLOSE_ON_SUCCESS =
      """
      UPDATE endpoint_circuit SET state = 'closed', consecutive_failures = 0, probe_failures = 0,
             opened_at = NULL, next_probe_at = NULL
      WHERE endpoint_id = :e AND (state = 'open' OR consecutive_failures > 0)
      """;

  static final String COUNT_FAILURE =
      """
      UPDATE endpoint_circuit
      SET consecutive_failures = consecutive_failures + 1,
          state         = CASE WHEN consecutive_failures + 1 >= 5 THEN 'open' ELSE 'closed' END,
          opened_at     = CASE WHEN consecutive_failures + 1 >= 5 THEN now() END,
          next_probe_at = CASE WHEN consecutive_failures + 1 >= 5 THEN now() + interval '30 seconds' END
      WHERE endpoint_id = :e AND state = 'closed'
      """;

  // The exponent is capped before the multiplication. Capping the product would be too late: an
  // outage long enough makes the uncapped product overflow the interval, and the finalize of
  // every later probe would fail.
  static final String BACK_OFF_PROBE =
      """
      UPDATE endpoint_circuit
      SET probe_failures = probe_failures + 1,
          next_probe_at  = now() + least(interval '5 minutes',
                                         interval '30 seconds' * power(2, least(probe_failures + 1, 4)))
      WHERE endpoint_id = :e AND state = 'open'
      """;

  /** What a recorded attempt says about its endpoint. Each is one statement of the finalize. */
  enum Verdict {

    /**
     * Closes the circuit and clears its counts. For an endpoint that is healthy, no row changes.
     */
    SUCCESS(CLOSE_ON_SUCCESS),

    /**
     * The failure of a delivery that was claimed with the circuit closed. The fifth in a row opens
     * the circuit, with the first probe 30 seconds later. If the circuit has opened since the
     * claim, no row changes: the attempt was in flight when the circuit opened, and it neither
     * counts nor moves the probe.
     */
    FAILURE(COUNT_FAILURE),

    /**
     * The failure of a probe. It moves the next probe further away: 60 seconds after the first
     * failed probe, twice as long after each one that follows, and 5 minutes at most.
     */
    PROBE_FAILURE(BACK_OFF_PROBE);

    private final String statement;

    Verdict(String statement) {
      this.statement = statement;
    }
  }

  private final JdbcClient jdbc;

  CircuitBreaker(@Qualifier("workerJdbc") JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Takes the probe slot of an open circuit whose probe is due, by moving the time of the next
   * probe 60 seconds ahead. Returns true to the one caller that moved it. A caller that comes while
   * another holds the row waits for it, finds the time moved and changes nothing.
   *
   * <p>The slot is a time and not a state, so nothing has to give it back. If the prober dies, the
   * time passes and the next claim may probe again.
   */
  boolean admitProbe(UUID endpointId) {
    return jdbc.sql(GATE).param("e", endpointId).update() == 1;
  }

  /**
   * Runs the statement of the verdict. The caller is the finalize of the attempt, inside its
   * transaction, so the circuit changes only together with the attempt row that it counts.
   */
  void record(UUID endpointId, Verdict verdict) {
    jdbc.sql(verdict.statement).param("e", endpointId).update();
  }
}
