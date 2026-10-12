package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.transaction.TransactionSystemException;

class FinalizeRetryTest {

  private static final Duration CUTOFF = Duration.ofSeconds(55);
  private static final Duration PAUSE = Duration.ofMillis(200);
  private static final Claimed DELIVERY =
      new Claimed(
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
          1,
          0,
          UUID.randomUUID(),
          false);

  // System.nanoTime may be anywhere in the long range, so this clock starts just before it wraps.
  // The only thing that moves it is the pause of the retry.
  private final AtomicLong nanoTime = new AtomicLong(Long.MAX_VALUE - 5);
  private final long claimStarted = nanoTime.get();
  private final MeterRegistry registry = new SimpleMeterRegistry();
  private final FinalizeRetry retry =
      new FinalizeRetry(
          new DeliveryMeters(registry),
          new DeliveryLog("replica-1"),
          CUTOFF,
          nanoTime::get,
          pause -> nanoTime.addAndGet(pause.toNanos()));

  /** Fails as scripted, try by try, and then gives its answer. Knows when each try began. */
  private final class Finalize implements BooleanSupplier {

    private final List<RuntimeException> failures = new ArrayList<>();
    private final List<Duration> triesAt = new ArrayList<>();
    private final boolean answer;
    private RuntimeException failureOfEveryTry;

    private Finalize(boolean answer) {
      this.answer = answer;
    }

    @Override
    public boolean getAsBoolean() {
      triesAt.add(Duration.ofNanos(nanoTime.get() - claimStarted));
      if (failureOfEveryTry != null) {
        throw failureOfEveryTry;
      }
      if (!failures.isEmpty()) {
        throw failures.removeFirst();
      }
      return answer;
    }
  }

  @Test
  void recordedFinalizeIsNotRunAgain() {
    Finalize finalize = new Finalize(true);

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isTrue();

    assertThat(finalize.triesAt).hasSize(1);
    assertThat(lostLeases()).isZero();
    assertThat(errors()).isZero();
  }

  @Test
  void lostLeaseIsCountedAndNotRunAgain() {
    Finalize finalize = new Finalize(false);

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isFalse();

    assertThat(finalize.triesAt).hasSize(1);
    assertThat(lostLeases()).isEqualTo(1);
    assertThat(errors()).isZero();
  }

  // Connection failures and socket timeouts (class 08), a database that is shutting down or
  // starting up, a deadlock, a lock timeout and a statement timeout.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "08000", "08001", "08003", "08006", "57P01", "57P02", "57P03", "40P01", "55P03", "57014"
      })
  void failureThatCanPassIsTriedAgain(String sqlState) {
    Finalize finalize = new Finalize(true);
    finalize.failures.add(databaseFailure(sqlState));
    finalize.failures.add(databaseFailure(sqlState));

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isTrue();

    assertThat(finalize.triesAt).containsExactly(Duration.ZERO, PAUSE, PAUSE.multipliedBy(2));
    assertThat(errors()).isZero();
  }

  @Test
  void borrowWaitThatRanOutIsTriedAgain() {
    Finalize finalize = new Finalize(true);
    finalize.failures.add(
        new IllegalStateException(
            "Could not open a transaction",
            new SQLTransientConnectionException("worker - Connection is not available")));

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isTrue();

    assertThat(finalize.triesAt).hasSize(2);
  }

  // The first statement of the finalize ran on a connection that the database had closed, and
  // the rollback failed on it too. Spring throws the failure of the rollback, which has no state,
  // and keeps the failure of the statement beside it. That one decides.
  @Test
  void failureBehindARollbackThatFailedIsTriedAgain() {
    TransactionSystemException rollbackFailed =
        new TransactionSystemException(
            "JDBC rollback failed", new SQLException("Connection is closed"));
    rollbackFailed.initApplicationException(databaseFailure("57P01"));
    Finalize finalize = new Finalize(true);
    finalize.failures.add(rollbackFailed);

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isTrue();

    assertThat(finalize.triesAt).hasSize(2);
    assertThat(errors()).isZero();
  }

  // A constraint, a value that does not fit its column, a statement that is wrong, and a server
  // that is out of connections, which the design does not count among what passes.
  @ParameterizedTest
  @ValueSource(strings = {"23505", "23503", "22001", "42P01", "53300"})
  void failureThatCannotPassIsCountedAndNotTriedAgain(String sqlState) {
    Finalize finalize = new Finalize(true);
    finalize.failures.add(databaseFailure(sqlState));

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isFalse();

    assertThat(finalize.triesAt).hasSize(1);
    assertThat(errors()).isEqualTo(1);
    assertThat(lostLeases()).isZero();
  }

  @Test
  void failureThatIsNoDatabaseErrorIsCountedAndNotTriedAgain() {
    Finalize finalize = new Finalize(true);
    finalize.failures.add(new IllegalStateException("a fault of the service"));

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isFalse();

    assertThat(finalize.triesAt).hasSize(1);
    assertThat(errors()).isEqualTo(1);
  }

  // With a try every 200 ms, the last one before 55 s begins at 54.8 s.
  @Test
  void failureThatDoesNotPassIsTriedUntilTheCutoffAndThenGivenUp() {
    Finalize finalize = new Finalize(true);
    finalize.failureOfEveryTry = databaseFailure("08006");

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isFalse();

    assertThat(finalize.triesAt).hasSize(275);
    assertThat(finalize.triesAt.getLast()).isEqualTo(CUTOFF.minus(PAUSE));
    assertThat(errors()).isZero();
    assertThat(lostLeases()).isZero();
  }

  // The claim began 54.9 s before the first try. The pause after it ends past the cutoff.
  @Test
  void cutoffCountsFromTheStartOfTheClaim() {
    Finalize finalize = new Finalize(true);
    finalize.failureOfEveryTry = databaseFailure("08006");
    long claimedLongAgo = claimStarted - CUTOFF.minusMillis(100).toNanos();

    assertThat(retry.run(DELIVERY, claimedLongAgo, finalize)).isFalse();

    assertThat(finalize.triesAt).hasSize(1);
  }

  // The first try committed and its answer was lost with the connection. The second try finds
  // the lease token gone, which reads as a lost lease although the result is recorded.
  @Test
  void finalizeThatCommittedBeforeItsAnswerWasLostChangesNothingWhenTriedAgain() {
    Finalize finalize = new Finalize(false);
    finalize.failures.add(databaseFailure("08006"));

    assertThat(retry.run(DELIVERY, claimStarted, finalize)).isFalse();

    assertThat(finalize.triesAt).hasSize(2);
    assertThat(lostLeases()).isEqualTo(1);
  }

  // The way Spring reports it: its own exception, with the one of the driver as the cause.
  private static RuntimeException databaseFailure(String sqlState) {
    return new IllegalStateException(
        "A statement failed", new SQLException("reported by the database", sqlState));
  }

  private double lostLeases() {
    return registry.get("webhooks.finalize.lost.lease").counter().count();
  }

  private double errors() {
    return registry.get("webhooks.finalize.errors").counter().count();
  }
}
