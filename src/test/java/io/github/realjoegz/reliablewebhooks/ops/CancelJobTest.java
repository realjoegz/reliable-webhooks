package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CancelJobTest {

  private static final Duration THIRTY_SECONDS = Duration.ofSeconds(30);
  private static final Duration TIME_LIMIT = Duration.ofSeconds(10);
  private static final int FULL_BATCH = 1_000;

  private final Statements statements = new Statements();
  private final Waits waits = new Waits();
  private final CancelJob job = new CancelJob(statements, waits);

  /**
   * Counts the statements, and answers each with the number of rows that a test set for it. A
   * statement that a test gave an end stays in progress until that end gets a permit.
   */
  private static final class Statements extends CancelRepository {

    private final AtomicInteger count = new AtomicInteger();
    private final Queue<Integer> rowsOfTheNextStatements = new ConcurrentLinkedQueue<>();
    private volatile int rowsOfEveryLaterStatement;
    private volatile Semaphore endOfTheNextStatement;

    private Statements() {
      super(null, null);
    }

    @Override
    int cancel() {
      count.incrementAndGet();
      Semaphore end = endOfTheNextStatement;
      if (end != null) {
        endOfTheNextStatement = null;
        end.acquireUninterruptibly();
      }
      Integer rows = rowsOfTheNextStatements.poll();
      return rows == null ? rowsOfEveryLaterStatement : rows;
    }
  }

  @AfterEach
  void stopTheJob() {
    if (job.isRunning()) {
      job.stop();
    }
  }

  @Test
  void runThatCancelsLessThanABatchIsOneStatement() {
    statements.rowsOfTheNextStatements.add(FULL_BATCH - 1);

    job.run();

    assertThat(statements.count).hasValue(1);
  }

  // A statement that cancelled a full batch may have left deliveries behind, so the run goes on.
  // The third statement cancels one delivery less than a batch, and the run ends with it.
  @Test
  void runGoesOnForAsLongAsAStatementCancelsAFullBatch() {
    statements.rowsOfTheNextStatements.addAll(List.of(FULL_BATCH, FULL_BATCH, FULL_BATCH - 1));

    job.run();

    assertThat(statements.count).hasValue(3);
  }

  @Test
  void jobRunsWhenItStartsAndThenThirtySecondsAfterEachRun() {
    job.start();
    await().until(() -> waits.asked.size() == 1);
    assertThat(statements.count).hasValue(1);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);

    assertThat(statements.count).hasValue(2);
    assertThat(waits.asked).containsOnly(THIRTY_SECONDS);
  }

  // Every statement of this test cancels a full batch, so the run would never end by itself. The
  // stop comes while the first statement is in progress and returns at once. Once the test lets
  // that statement end, the job says that it has ended, and no second statement was run.
  @Test
  void stopEndsARunAfterTheStatementThatIsInProgress() {
    statements.rowsOfEveryLaterStatement = FULL_BATCH;
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
}
