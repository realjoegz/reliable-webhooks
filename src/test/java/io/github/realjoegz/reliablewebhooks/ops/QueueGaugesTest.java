package io.github.realjoegz.reliablewebhooks.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.ops.QueueRepository.Queue;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class QueueGaugesTest {

  private static final Duration FIFTEEN_SECONDS = Duration.ofSeconds(15);

  private final MeterRegistry registry = new SimpleMeterRegistry();
  private final Reads reads = new Reads();
  private final Waits waits = new Waits();
  private final QueueGauges gauges = new QueueGauges(reads, new QueueMeters(registry), waits);

  /** Counts the runs, and answers each with the queue or the failure that a test set. */
  private static final class Reads extends QueueRepository {

    private final AtomicInteger count = new AtomicInteger();
    private volatile Queue queue = new Queue(0, 0, 0, 0, Duration.ZERO, 0);
    private volatile RuntimeException failure;

    private Reads() {
      super(null);
    }

    @Override
    Queue read() {
      count.incrementAndGet();
      if (failure != null) {
        throw failure;
      }
      return queue;
    }
  }

  @AfterEach
  void stopTheJob() {
    if (gauges.isRunning()) {
      gauges.stop();
    }
  }

  // The first run comes with the start and not after a wait. Every later run follows a wait of
  // fifteen seconds, which begins when the run before it has ended.
  @Test
  void queueIsReadWhenTheJobStartsAndThenFifteenSecondsAfterEachRun() {
    gauges.start();
    await().until(() -> waits.asked.size() == 1);
    assertThat(reads.count).hasValue(1);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);

    assertThat(reads.count).hasValue(2);
    assertThat(waits.asked).containsOnly(FIFTEEN_SECONDS);
  }

  // A zero would say that nothing is due, which a replica that has not read the queue does not
  // know.
  @Test
  void gaugesShowNoNumberBeforeTheFirstRun() {
    assertThat(deliveries("due")).isNaN();
    assertThat(deliveries("scheduled")).isNaN();
    assertThat(deliveries("leased")).isNaN();
    assertThat(deliveries("held")).isNaN();
    assertThat(oldestDueAgeSeconds()).isNaN();
    assertThat(circuitsOpen()).isNaN();
  }

  @Test
  void runShowsWhatItRead() {
    reads.queue = new Queue(5, 2, 3, 4, Duration.ofSeconds(90), 1);

    gauges.run();

    assertThat(deliveries("due")).isEqualTo(5);
    assertThat(deliveries("held")).isEqualTo(2);
    assertThat(deliveries("scheduled")).isEqualTo(3);
    assertThat(deliveries("leased")).isEqualTo(4);
    assertThat(oldestDueAgeSeconds()).isEqualTo(90);
    assertThat(circuitsOpen()).isEqualTo(1);
  }

  // The loop of a job logs the failure of a run and goes on. The gauges then show what the last
  // run that was answered read, which is older than a reader expects and still the latest there
  // is.
  @Test
  void runThatFailsLeavesTheGaugesAsTheRunBeforeItLeftThem() {
    reads.queue = new Queue(5, 2, 3, 4, Duration.ofSeconds(90), 1);
    gauges.run();
    reads.failure = new IllegalStateException("the database did not answer");

    assertThatThrownBy(gauges::run).isSameAs(reads.failure);

    assertThat(deliveries("due")).isEqualTo(5);
    assertThat(oldestDueAgeSeconds()).isEqualTo(90);
  }

  private double deliveries(String state) {
    return registry.get("webhooks.deliveries").tag("state", state).gauge().value();
  }

  private double oldestDueAgeSeconds() {
    return registry.get("webhooks.oldest.due.age").timeGauge().value(TimeUnit.SECONDS);
  }

  private double circuitsOpen() {
    return registry.get("webhooks.circuits.open").gauge().value();
  }
}
