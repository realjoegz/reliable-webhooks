package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RetentionJobTest {

  private static final Duration AN_HOUR = Duration.ofHours(1);
  private static final Duration TIME_LIMIT = Duration.ofSeconds(10);
  private static final int FULL_BATCH = 1_000;
  private static final String DELIVERIES = "deliveries";
  private static final String EVENTS = "events";
  private static final String ENDPOINTS = "endpoints";
  private static final String SECRETS = "secrets";

  private final Statements statements = new Statements();
  private final Waits waits = new Waits();
  private final RetentionJob job = new RetentionJob(statements, waits);

  /**
   * Keeps the statements in the order in which they ran, each by the rows that it is for. A
   * statement takes a full batch as often as a test set for its rows, and one row less after that.
   * A statement that a test gave an end stays in progress until that end gets a permit.
   */
  private static final class Statements extends RetentionRepository {

    private final List<String> ran = new CopyOnWriteArrayList<>();
    private final Map<String, Integer> fullBatches = new ConcurrentHashMap<>();
    private volatile Semaphore endOfTheNextStatement;

    private Statements() {
      super(null, null);
    }

    @Override
    int deleteTerminalDeliveries() {
      return rowsOfAStatementFor(DELIVERIES);
    }

    @Override
    int deleteEventsWithoutDeliveries() {
      return rowsOfAStatementFor(EVENTS);
    }

    @Override
    int deleteDeletedEndpointsWithoutDeliveries() {
      return rowsOfAStatementFor(ENDPOINTS);
    }

    @Override
    int clearExpiredPreviousSecrets() {
      return rowsOfAStatementFor(SECRETS);
    }

    private int rowsOfAStatementFor(String rows) {
      ran.add(rows);
      Semaphore end = endOfTheNextStatement;
      if (end != null) {
        endOfTheNextStatement = null;
        end.acquireUninterruptibly();
      }
      int left = fullBatches.merge(rows, -1, Integer::sum);
      return left >= 0 ? FULL_BATCH : FULL_BATCH - 1;
    }
  }

  @AfterEach
  void stopTheJob() {
    if (job.isRunning()) {
      job.stop();
    }
  }

  // Deliveries go first: an event or a deleted endpoint can go only once it has none left.
  @Test
  void runDeletesDeliveriesThenEventsThenEndpointsAndClearsSecretsLast() {
    job.run();

    assertThat(statements.ran).containsExactly(DELIVERIES, EVENTS, ENDPOINTS, SECRETS);
  }

  // A step is over when its statement takes less than a batch, and only then does the next begin.
  @Test
  void stepGoesOnForAsLongAsItsStatementTakesAFullBatch() {
    statements.fullBatches.put(DELIVERIES, 2);
    statements.fullBatches.put(ENDPOINTS, 1);

    job.run();

    assertThat(statements.ran)
        .containsExactly(DELIVERIES, DELIVERIES, DELIVERIES, EVENTS, ENDPOINTS, ENDPOINTS, SECRETS);
  }

  @Test
  void jobRunsWhenItStartsAndThenAnHourAfterEachRun() {
    job.start();
    await().until(() -> waits.asked.size() == 1);
    assertThat(statements.ran).hasSize(4);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);

    assertThat(statements.ran).hasSize(8);
    assertThat(waits.asked).containsOnly(AN_HOUR);
  }

  // The statement for the deliveries takes a full batch every time, so the first step would never
  // end by itself. The stop comes while its first statement is in progress and returns at once.
  // Once the test lets that statement end, the job says that it has ended. No second statement
  // was run, for the deliveries or for a later step.
  @Test
  void stopEndsARunAfterTheStatementThatIsInProgress() {
    statements.fullBatches.put(DELIVERIES, Integer.MAX_VALUE);
    Semaphore endOfTheStatement = new Semaphore(0);
    statements.endOfTheNextStatement = endOfTheStatement;
    job.start();
    await().until(() -> statements.ran.size() == 1);
    AtomicBoolean endReported = new AtomicBoolean();

    assertTimeoutPreemptively(TIME_LIMIT, () -> job.stop(() -> endReported.set(true)));

    assertThat(endReported).isFalse();

    endOfTheStatement.release();

    await().untilTrue(endReported);
    assertThat(statements.ran).containsExactly(DELIVERIES);
  }
}
