package io.github.realjoegz.reliablewebhooks.harness;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

/**
 * Waits for what a replica is doing in the database, as {@code pg_stat_activity} shows it, and ends
 * the backends that it has seen waiting. A replica is known by the {@code application_name} of its
 * connections.
 */
public final class PgWait {

  // Longer than the 10 s after which the server ends a session that idles in a transaction.
  private static final Duration SERVER_SIDE_END = Duration.ofSeconds(30);

  // The backends of a replica that wait for a lock in a statement that contains a fragment.
  private static final String FROM_BLOCKED_BACKENDS =
      """
      FROM pg_stat_activity
      WHERE application_name = :replica
        AND wait_event_type = 'Lock'
        AND strpos(query, :fragment) > 0
      """;

  private PgWait() {}

  /**
   * Returns once a backend of the replica is waiting for a lock in a statement that contains the
   * fragment. The statement has started by then. It goes on when the lock is released, and it fails
   * when the lock timeout of its pool ends the wait, so the caller has that long to act.
   */
  public static void untilBlocked(String sqlFragment, String replica) {
    await("a backend of %s blocked in: %s".formatted(replica, sqlFragment))
        .pollInterval(Duration.ofMillis(10))
        .until(() -> backendsBlockedIn(sqlFragment, replica) > 0);
  }

  /**
   * Ends the backends that {@link #untilBlocked} waits for, which is what a database that shuts
   * down does to a session, and returns how many they were. The statement of such a backend fails,
   * and its connection is closed.
   */
  public static int endBlocked(String sqlFragment, String replica) {
    return TestDatabase.jdbc()
        .sql("SELECT pg_terminate_backend(pid) " + FROM_BLOCKED_BACKENDS)
        .param("replica", replica)
        .param("fragment", sqlFragment)
        .query(Boolean.class)
        .list()
        .size();
  }

  /**
   * Returns once the replica has no backend left. That is where a replica that was killed ends up:
   * its connections are closed, and a backend that was in a statement leaves when the statement is
   * over and it finds nobody to answer. What its transaction did is rolled back by then.
   */
  public static void untilGone(String replica) {
    await("no backend of %s is left".formatted(replica))
        .atMost(SERVER_SIDE_END)
        .until(() -> backends(replica) == 0);
  }

  /**
   * Returns once no backend of the replica is in a transaction. The backends of a replica whose
   * connections went silent do not leave, because nothing closes them. A transaction that such a
   * replica left open ends only when the server ends its session.
   */
  public static void untilNoTransaction(String replica) {
    await("no backend of %s is in a transaction".formatted(replica))
        .atMost(SERVER_SIDE_END)
        .until(() -> backendsInATransaction(replica) == 0);
  }

  private static int backendsBlockedIn(String sqlFragment, String replica) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) " + FROM_BLOCKED_BACKENDS)
        .param("replica", replica)
        .param("fragment", sqlFragment)
        .query(Integer.class)
        .single();
  }

  private static int backends(String replica) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM pg_stat_activity WHERE application_name = :replica")
        .param("replica", replica)
        .query(Integer.class)
        .single();
  }

  private static int backendsInATransaction(String replica) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM pg_stat_activity
            WHERE application_name = :replica AND xact_start IS NOT NULL
            """)
        .param("replica", replica)
        .query(Integer.class)
        .single();
  }
}
