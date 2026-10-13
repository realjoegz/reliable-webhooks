package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.realjoegz.reliablewebhooks.delivery.DeliveryMeters;
import io.github.realjoegz.reliablewebhooks.ops.ExpiryRepository.Expired;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ExpiryJobTest {

  private static final Duration SIXTY_SECONDS = Duration.ofSeconds(60);
  private static final Duration TIME_LIMIT = Duration.ofSeconds(10);
  private static final int FULL_BATCH = 1_000;

  private final MeterRegistry registry = new SimpleMeterRegistry();
  private final Statements statements = new Statements();
  private final Waits waits = new Waits();
  private final ExpiryJob job = new ExpiryJob(statements, new DeliveryMeters(registry), waits);

  /**
   * Counts the statements, and answers each with the deliveries and attempt rows or the failure
   * that a test set for it. A statement that a test gave an end stays in progress until that end
   * gets a permit.
   */
  private static final class Statements extends ExpiryRepository {

    private final AtomicInteger count = new AtomicInteger();
    private final Queue<Expired> resultsOfTheNextStatements = new ConcurrentLinkedQueue<>();
    private volatile Expired resultOfEveryLaterStatement = new Expired(0, 0);
    private volatile RuntimeException failureOfTheNextStatement;
    private volatile Semaphore endOfTheNextStatement;

    private Statements() {
      super(null, null);
    }

    @Override
    Expired expire() {
      count.incrementAndGet();
      Semaphore end = endOfTheNextStatement;
      if (end != null) {
        endOfTheNextStatement = null;
        end.acquireUninterruptibly();
      }
      RuntimeException failure = failureOfTheNextStatement;
      if (failure != null) {
        failureOfTheNextStatement = null;
        throw failure;
      }
      Expired result = resultsOfTheNextStatements.poll();
      return result == null ? resultOfEveryLaterStatement : result;
    }
  }

  @AfterEach
  void stopTheJob() {
    if (job.isRunning()) {
      job.stop();
    }
  }

  @Test
  void runThatExpiresLessThanABatchIsOneStatement() {
    statements.resultsOfTheNextStatements.add(new Expired(FULL_BATCH - 1, 0));

    job.run();

    assertThat(statements.count).hasValue(1);
  }

  // A statement that dead-lettered a full batch may have left deliveries behind, so the run goes
  // on. The third statement takes one delivery less than a batch, and the run ends with it.
  @Test
  void runGoesOnForAsLongAsAStatementExpiresAFullBatch() {
    statements.resultsOfTheNextStatements.addAll(
        List.of(
            new Expired(FULL_BATCH, 0),
            new Expired(FULL_BATCH, 0),
            new Expired(FULL_BATCH - 1, 0)));

    job.run();

    assertThat(statements.count).hasValue(3);
  }

  // Every statement of this test takes a full batch, so the run would never end by itself. The
  // stop comes while the first statement is in progress and returns at once. Once the test lets
  // that statement end, the job says that it has ended, and no second statement was run.
  @Test
  void stopEndsARunAfterTheStatementThatIsInProgress() {
    statements.resultOfEveryLaterStatement = new Expired(FULL_BATCH, 0);
    Semaphore endOfTheStatement = new Semaphore(0);
    statements.endOfTheNextStatement = endOfTheStatement;
    job.start();
    await().until(() -> statements.count.get() == 1);
    AtomicBoolean endReported = new AtomicBoolean();

    assertTimeoutPreemptively(TIME_LIMIT, () -> job.stop(() -> endReported.set(true)));

    assertThat(endReported).isFalse();

    endOfTheStatement.release();

    await().untilTrue(endReported);
    assertThat(statements.count).hasValue(1);
  }

  // The first run comes with the start and not after a wait. Every later run follows a wait of
  // sixty seconds, which begins when the run before it has ended. Each of these runs finds
  // nothing, so it is one statement.
  @Test
  void jobRunsWhenItStartsAndThenSixtySecondsAfterEachRun() {
    job.start();
    await().until(() -> waits.asked.size() == 1);
    assertThat(statements.count).hasValue(1);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);
    assertThat(statements.count).hasValue(2);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 3);
    assertThat(statements.count).hasValue(3);
    assertThat(waits.asked).containsOnly(SIXTY_SECONDS);
  }

  // A run whose statement failed is followed by the usual wait and then by the next run.
  @Test
  void runThatFailedIsFollowedByTheNextRun() {
    statements.failureOfTheNextStatement = new IllegalStateException("the database did not answer");
    job.start();
    await().until(() -> waits.asked.size() == 1);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);

    assertThat(statements.count).hasValue(2);
    assertThat(waits.asked).containsOnly(SIXTY_SECONDS);
  }

  // The wait of this test does not end by time, so the stop that returns has ended it.
  @Test
  void stopEndsTheWaitAndNoRunFollows() {
    job.start();
    await().until(() -> waits.asked.size() == 1);

    job.stop();

    assertThat(job.isRunning()).isFalse();
    assertThat(statements.count).hasValue(1);
    assertThat(waits.asked).hasSize(1);
  }

  // Spring calls this stop, on the thread that stops the worker as well. The run of the test ends
  // only when the test lets it, which is after the stop has returned: a stop that waited for the
  // run would wait for ever, and the time limit would fail it. The job says that it has ended
  // only once the run is over, and no run follows.
  @Test
  void stopDuringARunReturnsAtOnceAndTheEndIsReportedAfterTheRun() {
    Semaphore endOfTheRun = new Semaphore(0);
    statements.endOfTheNextStatement = endOfTheRun;
    job.start();
    await().until(() -> statements.count.get() == 1);
    AtomicBoolean endReported = new AtomicBoolean();

    assertTimeoutPreemptively(TIME_LIMIT, () -> job.stop(() -> endReported.set(true)));

    assertThat(job.isRunning()).isFalse();
    assertThat(endReported).isFalse();

    endOfTheRun.release();

    await().untilTrue(endReported);
    assertThat(statements.count).hasValue(1);
  }

  // A caller of this stop waits for the loop to end. The stop comes during a run, and its thread
  // is seen waiting before the test lets the run end. It returns after that, and no run follows.
  @Test
  void stopThatIsWaitedForReturnsAfterTheRunThatIsInProgress() {
    Semaphore endOfTheRun = new Semaphore(0);
    statements.endOfTheNextStatement = endOfTheRun;
    job.start();
    await().until(() -> statements.count.get() == 1);

    Thread stopping = Thread.ofPlatform().daemon().start(job::stop);
    await().until(() -> stopping.getState() == Thread.State.WAITING);

    endOfTheRun.release();

    await().until(() -> stopping.getState() == Thread.State.TERMINATED);
    assertThat(statements.count).hasValue(1);
  }

  // Spring may stop a lifecycle and start it again. The second start runs at once, like the
  // first, and then waits.
  @Test
  void jobThatWasStoppedRunsAgainOnceItIsStarted() {
    job.start();
    await().until(() -> waits.asked.size() == 1);
    job.stop();

    job.start();
    await().until(() -> waits.asked.size() == 2);

    assertThat(job.isRunning()).isTrue();
    assertThat(statements.count).hasValue(2);
  }

  // The run is two statements. The first dead-letters a full batch, of which two deliveries were
  // abandoned by their holders and got their attempt rows from it. The second dead-letters five,
  // three of them abandoned. Both are counted.
  @Test
  void runCountsTheDeliveriesThatItExpiredAndTheAttemptRowsThatItWrote() {
    statements.resultsOfTheNextStatements.addAll(
        List.of(new Expired(FULL_BATCH, 2), new Expired(5, 3)));

    job.run();

    assertThat(
            registry
                .get("webhooks.delivery.terminal")
                .tag("state", "dead")
                .tag("reason", "expired")
                .counter()
                .count())
        .isEqualTo(FULL_BATCH + 5);
    assertThat(
            registry
                .get("webhooks.delivery.attempts")
                .tag("outcome", "unknown")
                .tag("reason", "lease_expired")
                .counter()
                .count())
        .isEqualTo(5);
  }
}
