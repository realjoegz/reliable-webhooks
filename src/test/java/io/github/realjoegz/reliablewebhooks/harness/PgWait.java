package io.github.realjoegz.reliablewebhooks.harness;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

/**
 * Waits for what a replica is doing in the database, as {@code pg_stat_activity} shows it. A
 * replica is known by the {@code application_name} of its connections.
 */
public final class PgWait {

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

  private static int backendsBlockedIn(String sqlFragment, String replica) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM pg_stat_activity
            WHERE application_name = :replica
              AND wait_event_type = 'Lock'
              AND strpos(query, :fragment) > 0
            """)
        .param("replica", replica)
        .param("fragment", sqlFragment)
        .query(Integer.class)
        .single();
  }
}
