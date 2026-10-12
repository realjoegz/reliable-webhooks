package io.github.realjoegz.reliablewebhooks.delivery;

import io.github.realjoegz.reliablewebhooks.delivery.CircuitBreaker.Verdict;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.postgresql.util.PGInterval;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The statements of the delivery worker. They run on the {@code worker} pool and carry no tenant
 * scope: a worker serves every tenant, and what it may write is decided by the lease token.
 */
@Repository
public class DeliveryRepository {

  static final String CLAIM =
      """
      WITH eligible AS MATERIALIZED (            -- 1. endpoints this replica may serve right now
        SELECT e.id AS endpoint_id, e.tenant_id, c.state AS circuit
        FROM endpoint e
        JOIN tenant t           ON t.id = e.tenant_id
        JOIN endpoint_circuit c ON c.endpoint_id = e.id
        WHERE e.status = 'active'
          AND t.status = 'active'                                -- suspended tenants are held
          AND (c.state = 'closed' OR c.next_probe_at <= now())   -- open circuits are held
          AND e.id        <> ALL (:full_endpoints)               -- bulkheads: skip what this
          AND e.tenant_id <> ALL (:full_tenants)                 -- replica could not run anyway
      ), head AS (                               -- 2. the oldest lockable due row of each one
        SELECT g.endpoint_id, g.tenant_id, g.circuit, h.id, h.next_attempt_at, h.abandoned
        FROM eligible g
        CROSS JOIN LATERAL (
          SELECT d.id, d.next_attempt_at, d.lease_token IS NOT NULL AS abandoned
          FROM delivery d
          WHERE d.endpoint_id = g.endpoint_id
            AND d.state = 'pending'
            AND d.next_attempt_at <= now()
          ORDER BY d.next_attempt_at
          LIMIT 1
          FOR UPDATE OF d SKIP LOCKED
        ) h
      ), ranked AS (                             -- 3. tenant fairness
        SELECT *, row_number() OVER (PARTITION BY tenant_id ORDER BY next_attempt_at) AS nth_for_tenant
        FROM head
      ), picked AS (
        SELECT * FROM ranked
        WHERE nth_for_tenant <= 4
        ORDER BY nth_for_tenant, next_attempt_at                 -- every tenant's first row before any second
        LIMIT :free
      )
      UPDATE delivery d                          -- 4. lease
      SET attempts = d.attempts + 1,
          lease_token = gen_random_uuid(),
          next_attempt_at = now() + :lease                       -- the lease
      FROM picked p
      WHERE d.id = p.id
      RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.attempts, d.replays,
                d.lease_token, p.abandoned, p.circuit
      """;

  static final String INSERT_ABANDONED_ATTEMPT =
      """
      INSERT INTO delivery_attempt (delivery_id, run, attempt_no, outcome, error)
      VALUES (:id, :run, :attempt_no, 'unknown', 'lease_expired')
      """;

  static final String RELEASE =
      """
      UPDATE delivery SET lease_token = NULL, attempts = attempts - 1, next_attempt_at = now() + :delay
      WHERE id = :id AND lease_token = :token
      """;

  static final String LOAD =
      """
      SELECT e.url, e.secret,
             CASE WHEN e.prev_secret_expires_at > now() THEN e.prev_secret END AS prev_secret,
             ev.body, ev.trace_context,
             extract(epoch FROM now())::bigint AS signed_at        -- the signature timestamp
      FROM delivery d
      JOIN endpoint e ON e.id = d.endpoint_id
      JOIN event ev   ON ev.id = d.event_id
      WHERE d.id = :id AND d.lease_token = :token AND e.status = 'active'
      """;

  static final String FENCE_AND_CHANGE_STATE =
      """
      UPDATE delivery
      SET state = :state, lease_token = NULL, dead_reason = :dead_reason,
          next_attempt_at = CASE WHEN :state = 'pending' THEN now() + :delay ELSE next_attempt_at END,
          completed_at    = CASE WHEN :state = 'pending' THEN NULL ELSE now() END
      WHERE id = :id AND lease_token = :token
      """;

  // The attempt began as long before this transaction as the replica measured it to take, so
  // started_at plus duration_ms is the time at which the attempt was recorded.
  static final String INSERT_ATTEMPT =
      """
      INSERT INTO delivery_attempt (delivery_id, run, attempt_no, probe, outcome, http_status, error,
                                    remote_addr, started_at, duration_ms, replica)
      VALUES (:id, :run, :attempt_no, :probe, :outcome, :http_status, :error, :remote_addr::inet,
              now() - :duration_ms * interval '1 millisecond', :duration_ms, :replica)
      """;

  /**
   * A delivery that a claim has leased. The attempts include the one that the claim began. An
   * abandoned delivery had been leased before, and its holder recorded nothing.
   *
   * <p>A claim takes a delivery whose endpoint has an open circuit only when a probe of that
   * endpoint is due. Such a delivery may be sent only by the worker that wins the gate, and its
   * attempt is then the probe.
   */
  record Claimed(
      UUID id,
      UUID tenantId,
      UUID eventId,
      UUID endpointId,
      int attempts,
      int replays,
      UUID leaseToken,
      boolean abandoned,
      boolean circuitOpen) {}

  /** What the request of an attempt is made of. The timestamp is the database clock in seconds. */
  record Loaded(String url, byte[] secret, byte[] body, long signedAt) {

    /** Leaves out the URL, the secret and the body, none of which may reach a log. */
    @Override
    public String toString() {
      return "Loaded[signedAt=%d]".formatted(signedAt);
    }
  }

  /** The state that an attempt leaves its delivery in. */
  record NextState(String state, String deadReason, Duration delay) {

    static NextState succeeded() {
      return new NextState("succeeded", null, Duration.ZERO);
    }

    static NextState retryIn(Duration delay) {
      return new NextState("pending", null, delay);
    }

    static NextState exhausted() {
      return new NextState("dead", "exhausted", Duration.ZERO);
    }
  }

  /** An attempt as its row describes it. Status and error are null where there is none. */
  record Attempt(
      String outcome, Integer httpStatus, String error, InetAddress remoteAddress, Duration took) {}

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;
  private final CircuitBreaker circuits;

  DeliveryRepository(
      @Qualifier("workerJdbc") JdbcClient jdbc,
      @Qualifier("workerTransactions") TransactionTemplate transactions,
      CircuitBreaker circuits) {
    this.jdbc = jdbc;
    this.transactions = transactions;
    this.circuits = circuits;
  }

  /**
   * Leases up to {@code free} due deliveries, at most one for an endpoint and four for a tenant,
   * and none for an endpoint or a tenant in the two lists. A row that another transaction holds is
   * skipped, not waited for. An abandoned delivery gets the attempt row that its last holder did
   * not write, in the same transaction.
   */
  List<Claimed> claim(UUID[] fullEndpoints, UUID[] fullTenants, int free, Duration lease) {
    return transactions.execute(
        transaction -> {
          List<Claimed> claimed =
              jdbc.sql(CLAIM)
                  .param("full_endpoints", fullEndpoints)
                  .param("full_tenants", fullTenants)
                  .param("free", free)
                  .param("lease", interval(lease))
                  .query(
                      (row, n) ->
                          new Claimed(
                              row.getObject("id", UUID.class),
                              row.getObject("tenant_id", UUID.class),
                              row.getObject("event_id", UUID.class),
                              row.getObject("endpoint_id", UUID.class),
                              row.getInt("attempts"),
                              row.getInt("replays"),
                              row.getObject("lease_token", UUID.class),
                              row.getBoolean("abandoned"),
                              "open".equals(row.getString("circuit"))))
                  .list();
          for (Claimed delivery : claimed) {
            if (delivery.abandoned()) {
              jdbc.sql(INSERT_ABANDONED_ATTEMPT)
                  .param("id", delivery.id())
                  .param("run", delivery.replays())
                  .param("attempt_no", delivery.attempts() - 1)
                  .update();
            }
          }
          return claimed;
        });
  }

  /**
   * Gives a leased delivery back as if it had not been claimed: the attempt is not counted and no
   * attempt row is written. Changes nothing unless the token is still the lease of the delivery.
   */
  void release(Claimed delivery, Duration delay) {
    jdbc.sql(RELEASE)
        .param("delay", interval(delay))
        .param("id", delivery.id())
        .param("token", delivery.leaseToken())
        .update();
  }

  /**
   * Empty when the token is no longer the lease of the delivery, or its endpoint has been deleted.
   */
  Optional<Loaded> load(Claimed delivery) {
    return jdbc.sql(LOAD)
        .param("id", delivery.id())
        .param("token", delivery.leaseToken())
        .query(
            (row, n) ->
                new Loaded(
                    row.getString("url"),
                    row.getBytes("secret"),
                    row.getBytes("body"),
                    row.getLong("signed_at")))
        .optional();
  }

  /**
   * Records the attempt, moves the delivery to its next state and tells the circuit of its endpoint
   * what the attempt says about it, in one transaction. Returns false, with nothing changed, when
   * the token is no longer the lease of the delivery: another replica has reclaimed it, or this
   * result has been recorded before.
   */
  boolean recordAttempt(
      Claimed delivery, NextState next, Attempt attempt, Verdict verdict, String replica) {
    return transactions.execute(
        transaction -> {
          if (!fenceAndChangeState(delivery, next)) {
            transaction.setRollbackOnly();
            return false;
          }
          jdbc.sql(INSERT_ATTEMPT)
              .param("id", delivery.id())
              .param("run", delivery.replays())
              .param("attempt_no", delivery.attempts())
              .param("probe", delivery.circuitOpen())
              .param("outcome", attempt.outcome())
              .param("http_status", attempt.httpStatus())
              .param("error", attempt.error())
              .param("remote_addr", literal(attempt.remoteAddress()))
              .param("duration_ms", attempt.took().toMillis())
              .param("replica", replica)
              .update();
          circuits.record(delivery.endpointId(), verdict);
          return true;
        });
  }

  /**
   * Moves a delivery that was claimed more often than it may be attempted to {@code dead}, without
   * an attempt row, because no attempt was made. Returns false when the lease is lost.
   *
   * <p>It is one statement and still a transaction, like every finalize: a statement that outlives
   * its replica then cannot commit by itself.
   */
  boolean deadLetterExhausted(Claimed delivery) {
    return transactions.execute(
        transaction -> fenceAndChangeState(delivery, NextState.exhausted()));
  }

  private boolean fenceAndChangeState(Claimed delivery, NextState next) {
    return jdbc.sql(FENCE_AND_CHANGE_STATE)
            .param("state", next.state())
            .param("dead_reason", next.deadReason())
            .param("delay", interval(next.delay()))
            .param("id", delivery.id())
            .param("token", delivery.leaseToken())
            .update()
        == 1;
  }

  private static PGInterval interval(Duration duration) {
    return new PGInterval(0, 0, 0, 0, 0, duration.toNanos() / 1e9);
  }

  // The inet type takes no zone, which the text of a scoped IPv6 address carries.
  private static String literal(InetAddress address) {
    if (address == null) {
      return null;
    }
    try {
      return InetAddress.getByAddress(address.getAddress()).getHostAddress();
    } catch (UnknownHostException e) {
      throw new IllegalStateException(e);
    }
  }
}
