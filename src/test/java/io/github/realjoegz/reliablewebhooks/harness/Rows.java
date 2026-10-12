package io.github.realjoegz.reliablewebhooks.harness;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Reads the rows that a delivery leaves behind, and ages them. Ageing is how a test lets time pass:
 * it moves the time columns of a row into the past, and the service and the database clock run
 * unchanged.
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
