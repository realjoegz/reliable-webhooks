package io.github.realjoegz.reliablewebhooks;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Set;
import org.springframework.transaction.TransactionSystemException;

/**
 * Tells a transient failure of a database call from a permanent one. A transient failure says
 * nothing against the statement: the database was busy, was restarting or could not be reached, and
 * the same call can succeed a moment later. The worker tries a finalize again after one, and the
 * API asks its caller to come back. Any other failure is a fault in the service or in the data, and
 * trying again would not change it.
 */
public final class DatabaseFailures {

  private static final String CONNECTION_EXCEPTION_CLASS = "08";
  private static final Set<String> TRANSIENT_STATES =
      Set.of(
          "57P01", // admin shutdown
          "57P02", // crash shutdown
          "57P03", // cannot connect now
          "40P01", // deadlock detected
          "55P03", // lock timeout
          "57014"); // statement timeout

  private DatabaseFailures() {}

  public static boolean isTransient(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      // HikariCP reports a borrow wait that ran out as a transient connection failure.
      if (cause instanceof SQLTransientConnectionException) {
        return true;
      }
      if (cause instanceof SQLException sql && isTransient(sql.getSQLState())) {
        return true;
      }
      if (cause instanceof TransactionSystemException rollbackFailed
          && followsATransientFailure(rollbackFailed)) {
        return true;
      }
    }
    return false;
  }

  // A statement or a commit that fails because its connection is gone is followed by a rollback
  // that fails as well: the pool has closed the connection by then, and reports that without a
  // state. Spring throws the failure of the rollback in both cases.
  //
  // After a statement it keeps the failure of the statement beside that of the rollback, and not
  // as its cause. The failure of the statement decides.
  //
  // After a commit it only logs the failure of the commit, so nothing is beside the failure of
  // the rollback. The pool closes a connection that is in use only after a failure that broke it,
  // so a rollback that found its connection closed is transient.
  private static boolean followsATransientFailure(TransactionSystemException rollbackFailed) {
    Throwable ofTheStatement = rollbackFailed.getApplicationException();
    if (ofTheStatement != null) {
      return isTransient(ofTheStatement);
    }
    return rollbackFailed.getCause() instanceof SQLException closed && closed.getSQLState() == null;
  }

  // Class 08 is how the driver reports a connection that failed and a socket read that timed out.
  private static boolean isTransient(String sqlState) {
    return sqlState != null
        && (sqlState.startsWith(CONNECTION_EXCEPTION_CLASS) || TRANSIENT_STATES.contains(sqlState));
  }
}
