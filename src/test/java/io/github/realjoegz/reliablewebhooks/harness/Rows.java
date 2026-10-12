package io.github.realjoegz.reliablewebhooks.harness;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Reads the rows that a delivery leaves behind, the circuit row of an endpoint and the bucket row
 * of a tenant, and ages them, as well as an event and the secret from before a rotation. Ageing is
 * how a test lets time pass: it moves the time columns of a row into the past, and the service and
 * the database clock run unchanged.
 */
public final class Rows {

  /**
   * A delivery row. Whether it has a lease token says nothing about whether the lease has ended.
   */
  public record Delivery(
      String state,
      int attempts,
      int replays,
      boolean hasLeaseToken,
      String deadReason,
      boolean completed) {}

  /** A row of the attempt log. */
  public record Attempt(
      int run,
      int attemptNo,
      boolean probe,
      String outcome,
      Integer httpStatus,
      String error,
      String remoteAddr,
      String replica) {

    /**
     * An attempt of run 0 that the replica inside the test JVM recorded with the status of a
     * response. Only the receiver answers that replica, so the address is the loopback address.
     * Only a 2xx status is a success.
     */
    public static Attempt answered(int attemptNo, int status) {
      String outcome = status >= 200 && status < 300 ? "success" : "failure";
      return new Attempt(
          0,
          attemptNo,
          false,
          outcome,
          status,
          null,
          "127.0.0.1",
          ReplicaSettings.IN_PROCESS_REPLICA);
    }

    /**
     * An attempt of run 0 that the replica inside the test JVM recorded as failed without a
     * response. The address is null when the attempt ended before it had a connection.
     */
    public static Attempt unanswered(int attemptNo, String error, String remoteAddr) {
      return new Attempt(
          0,
          attemptNo,
          false,
          "failure",
          null,
          error,
          remoteAddr,
          ReplicaSettings.IN_PROCESS_REPLICA);
    }
  }

  /** A circuit row. Whether it has a probe time says nothing about whether that time has passed. */
  public record Circuit(
      String state,
      int consecutiveFailures,
      int probeFailures,
      boolean hasOpenedAt,
      boolean hasProbeTime) {

    /** The circuit of an endpoint that is created, and of one whose last attempt succeeded. */
    public static final Circuit CLOSED = new Circuit("closed", 0, 0, false, false);
  }

  private Rows() {}

  /** Moves the due time or lease end, the expiry and the completion of a delivery into the past. */
  public static void ageDelivery(UUID deliveryId, Duration by) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE delivery
            SET next_attempt_at = next_attempt_at - make_interval(secs => :seconds),
                expires_at      = expires_at      - make_interval(secs => :seconds),
                completed_at    = completed_at    - make_interval(secs => :seconds)
            WHERE id = :id
            """)
        .param("seconds", by.toNanos() / 1e9)
        .param("id", deliveryId)
        .update();
  }

  /**
   * Moves the due time of a delivery into the future, so that no claim takes it. Ageing it by as
   * much makes it due again.
   */
  public static void postponeDelivery(UUID deliveryId, Duration by) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE delivery SET next_attempt_at = next_attempt_at + make_interval(secs => :seconds)
            WHERE id = :id
            """)
        .param("seconds", by.toNanos() / 1e9)
        .param("id", deliveryId)
        .update();
  }

  /** The delivery of an event that went to one endpoint. */
  public static UUID deliveryOf(UUID eventId) {
    return TestDatabase.jdbc()
        .sql("SELECT id FROM delivery WHERE event_id = :id")
        .param("id", eventId)
        .query(UUID.class)
        .single();
  }

  public static Delivery delivery(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT state, attempts, replays, dead_reason,
                   lease_token IS NOT NULL AS has_lease_token,
                   completed_at IS NOT NULL AS completed
            FROM delivery WHERE id = :id
            """)
        .param("id", deliveryId)
        .query(
            (row, n) ->
                new Delivery(
                    row.getString("state"),
                    row.getInt("attempts"),
                    row.getInt("replays"),
                    row.getBoolean("has_lease_token"),
                    row.getString("dead_reason"),
                    row.getBoolean("completed")))
        .single();
  }

  /** The delivery rows and their attempt rows, each as the text of all of its columns. */
  public static List<String> everyColumnOf(List<UUID> deliveryIds) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT line FROM (
              SELECT 'delivery ' || d::text AS line FROM delivery d WHERE d.id = ANY(:ids)
              UNION ALL
              SELECT 'attempt ' || a::text FROM delivery_attempt a WHERE a.delivery_id = ANY(:ids)
            ) lines
            ORDER BY line
            """)
        .param("ids", deliveryIds.toArray(UUID[]::new))
        .query(String.class)
        .list();
  }

  /**
   * The wait that the finalize of an attempt gave its delivery: from the time at which the attempt
   * was recorded to the time at which the delivery is due next. An attempt row holds when the
   * attempt began and how long it took, and the two add up to when it was recorded.
   */
  public static Duration waitAfter(UUID deliveryId, int attemptNo) {
    BigDecimal seconds =
        TestDatabase.jdbc()
            .sql(
                """
                SELECT extract(epoch FROM d.next_attempt_at - recorded.at)
                FROM delivery d
                JOIN delivery_attempt a ON a.delivery_id = d.id
                CROSS JOIN LATERAL
                  (SELECT a.started_at + a.duration_ms * interval '1 millisecond' AS at) recorded
                WHERE d.id = :id AND a.run = d.replays AND a.attempt_no = :attempt_no
                """)
            .param("id", deliveryId)
            .param("attempt_no", attemptNo)
            .query(BigDecimal.class)
            .single();
    return Duration.ofNanos(seconds.movePointRight(9).longValueExact());
  }

  public static Circuit circuit(UUID endpointId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT state, consecutive_failures, probe_failures,
                   opened_at IS NOT NULL AS has_opened_at,
                   next_probe_at IS NOT NULL AS has_probe_time
            FROM endpoint_circuit WHERE endpoint_id = :id
            """)
        .param("id", endpointId)
        .query(
            (row, n) ->
                new Circuit(
                    row.getString("state"),
                    row.getInt("consecutive_failures"),
                    row.getInt("probe_failures"),
                    row.getBoolean("has_opened_at"),
                    row.getBoolean("has_probe_time")))
        .single();
  }

  /** Moves the time of the next probe of an endpoint into the past. */
  public static void ageCircuit(UUID endpointId, Duration by) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE endpoint_circuit SET next_probe_at = next_probe_at - make_interval(secs => :seconds)
            WHERE endpoint_id = :id
            """)
        .param("seconds", by.toNanos() / 1e9)
        .param("id", endpointId)
        .update();
  }

  /**
   * How far the finalize of an attempt put the next probe of its endpoint ahead: from the time at
   * which the attempt was recorded to the probe time of the circuit. It is the wait that this
   * finalize wrote only as long as nothing has moved the probe time since.
   */
  public static Duration probeWaitAfter(UUID deliveryId, int attemptNo) {
    BigDecimal seconds =
        TestDatabase.jdbc()
            .sql(
                """
                SELECT extract(epoch FROM c.next_probe_at - recorded.at)
                FROM delivery d
                JOIN endpoint_circuit c ON c.endpoint_id = d.endpoint_id
                JOIN delivery_attempt a ON a.delivery_id = d.id
                CROSS JOIN LATERAL
                  (SELECT a.started_at + a.duration_ms * interval '1 millisecond' AS at) recorded
                WHERE d.id = :id AND a.run = d.replays AND a.attempt_no = :attempt_no
                """)
            .param("id", deliveryId)
            .param("attempt_no", attemptNo)
            .query(BigDecimal.class)
            .single();
    return Duration.ofNanos(seconds.movePointRight(9).longValueExact());
  }

  /**
   * The tokens in the bucket row of a tenant. The row holds what its last charge left. What has
   * refilled since then is worked out by the next charge and is not in the row.
   */
  public static double bucketTokens(UUID tenantId) {
    return TestDatabase.jdbc()
        .sql("SELECT tokens FROM rate_bucket WHERE tenant_id = :t")
        .param("t", tenantId)
        .query(Double.class)
        .single();
  }

  /**
   * Moves the last refill of the bucket of a tenant into the past, so that the next charge finds
   * the bucket refilled for that much longer.
   */
  public static void ageBucket(UUID tenantId, Duration by) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE rate_bucket SET refilled_at = refilled_at - make_interval(secs => :seconds)
            WHERE tenant_id = :t
            """)
        .param("seconds", by.toNanos() / 1e9)
        .param("t", tenantId)
        .update();
  }

  /** Moves the creation of an event into the past. Its deliveries keep their times. */
  public static void ageEvent(UUID eventId, Duration by) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE event SET created_at = created_at - make_interval(secs => :seconds)
            WHERE id = :id
            """)
        .param("seconds", by.toNanos() / 1e9)
        .param("id", eventId)
        .update();
  }

  /**
   * Moves the time until which the secret from before a rotation counts into the past. An endpoint
   * whose secret was not rotated has no such time and stays as it is.
   */
  public static void agePreviousSecret(UUID endpointId, Duration by) {
    TestDatabase.jdbc()
        .sql(
            """
            UPDATE endpoint
            SET prev_secret_expires_at = prev_secret_expires_at - make_interval(secs => :seconds)
            WHERE id = :id
            """)
        .param("seconds", by.toNanos() / 1e9)
        .param("id", endpointId)
        .update();
  }

  /** The attempt log of a delivery, in the order of its runs and attempts. */
  public static List<Attempt> attempts(UUID deliveryId) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT run, attempt_no, probe, outcome, http_status, error, replica,
                   host(remote_addr) AS remote_addr
            FROM delivery_attempt WHERE delivery_id = :id
            ORDER BY run, attempt_no
            """)
        .param("id", deliveryId)
        .query(
            (row, n) ->
                new Attempt(
                    row.getInt("run"),
                    row.getInt("attempt_no"),
                    row.getBoolean("probe"),
                    row.getString("outcome"),
                    row.getObject("http_status", Integer.class),
                    row.getString("error"),
                    row.getString("remote_addr"),
                    row.getString("replica")))
        .list();
  }
}
