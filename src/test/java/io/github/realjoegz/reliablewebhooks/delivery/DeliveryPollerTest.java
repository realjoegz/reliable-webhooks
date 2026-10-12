package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Delivery;
import io.github.realjoegz.reliablewebhooks.delivery.DeliveryRepository.Claimed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DeliveryPollerTest {

  private static final int MAX_IN_FLIGHT = 64;
  private static final Duration LEASE = Duration.ofSeconds(60);
  private static final Delivery SETTINGS =
      new Delivery(
          Duration.ofSeconds(15),
          LEASE,
          Duration.ofSeconds(35),
          Duration.ofSeconds(55),
          12,
          MAX_IN_FLIGHT);
  private static final WebhooksProperties PROPERTIES =
      new WebhooksProperties(null, null, "replica-1", null, SETTINGS, null);
  private static final Duration FAR_LONGER_THAN_A_TEST_WAITS = Duration.ofMinutes(1);
  private static final Duration SEVENTY_SECONDS = Duration.ofSeconds(70);

  /** What a claim was called with. */
  private record Asked(Set<UUID> fullEndpoints, Set<UUID> fullTenants, int free, Duration lease) {}

  private final MeterRegistry registry = new SimpleMeterRegistry();
  private final DeliveryMeters meters = new DeliveryMeters(registry);
  private final Deliveries deliveries = new Deliveries();
  private final Attempts attempts = new Attempts();
  private final Waits waits = new Waits();
  private final AtomicLong heartbeatNanos = new AtomicLong();
  private final PollerHeartbeat heartbeat = new PollerHeartbeat(heartbeatNanos::get);
  private final DeliveryPoller poller =
      new DeliveryPoller(deliveries, attempts, meters, heartbeat, PROPERTIES, waits);
  private final Thread loop = Thread.ofPlatform().unstarted(poller::run);

  /**
   * Hands out the deliveries that a test made due, as many as a claim has room for, and keeps what
   * every claim was called with and when it was called. Whom a claim leaves out is the business of
   * the statement, so this one leaves out nobody.
   */
  private static final class Deliveries extends DeliveryRepository {

    private final Queue<Claimed> due = new ConcurrentLinkedQueue<>();
    private final List<Asked> claims = new CopyOnWriteArrayList<>();
    private final Map<Claimed, Long> calledAtNanos = new ConcurrentHashMap<>();
    private volatile RuntimeException failureOfTheNextClaim;

    private Deliveries() {
      super(null, null, null);
    }

    @Override
    List<Claimed> claim(UUID[] fullEndpoints, UUID[] fullTenants, int free, Duration lease) {
      long calledAt = System.nanoTime();
      claims.add(new Asked(Set.of(fullEndpoints), Set.of(fullTenants), free, lease));
      RuntimeException failure = failureOfTheNextClaim;
      if (failure != null) {
        failureOfTheNextClaim = null;
        throw failure;
      }
      List<Claimed> claimed = new ArrayList<>();
      while (claimed.size() < free && !due.isEmpty()) {
        Claimed delivery = due.remove();
        calledAtNanos.put(delivery, calledAt);
        claimed.add(delivery);
      }
      return claimed;
    }
  }

  /**
   * Runs no attempt. Each one stays in flight until the test lets it finish, and keeps the time
   * that it was given as the start of its claim.
   */
  private final class Attempts extends DeliveryWorker {

    private final Map<Claimed, CountDownLatch> inFlight = new ConcurrentHashMap<>();
    private final Map<Claimed, Long> claimStartedNanos = new ConcurrentHashMap<>();

    private Attempts() {
      super(null, null, null, null, meters, SETTINGS, "replica-1", System::nanoTime, Thread::sleep);
    }

    @Override
    void attempt(Claimed delivery, long claimStartedNanos) {
      CountDownLatch finished = new CountDownLatch(1);
      this.claimStartedNanos.put(delivery, claimStartedNanos);
      inFlight.put(delivery, finished);
      try {
        finished.await();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      } finally {
        inFlight.remove(delivery);
      }
    }

    private void finish(Claimed delivery) {
      inFlight.get(delivery).countDown();
    }

    private void finishAll() {
      inFlight.values().forEach(CountDownLatch::countDown);
    }
  }

  /**
   * Keeps how long the loop wanted to wait, and waits far longer than that. A wait therefore ends
   * when an attempt has finished, when the poller is stopped or when the test says that its time is
   * up, and not because the time that the loop asked for has passed.
   */
  private static final class Waits implements DeliveryPoller.Wait {

    private final List<Duration> asked = new CopyOnWriteArrayList<>();
    private volatile Semaphore permits;

    @Override
    public void forPermit(Semaphore permits, Duration atMost) throws InterruptedException {
      this.permits = permits;
      asked.add(atMost);
      permits.tryAcquire(FAR_LONGER_THAN_A_TEST_WAITS.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void timeIsUp() {
      permits.release();
    }
  }

  @AfterEach
  void endLoopAndAttempts() throws InterruptedException {
    poller.stop();
    attempts.finishAll();
    loop.join();
  }

  // Seventy deliveries are due. The first claim asks for all the room there is and gets it
  // filled. The loop then makes no claim at all, until an attempt finishes and there is room for
  // one row.
  @Test
  void claimAsksForAsManyRowsAsThereIsRoomForAndNoneWhenThereIsNoRoom() {
    List<Claimed> queued = makeDue(70);
    loop.start();

    await().until(() -> attempts.inFlight.size() == MAX_IN_FLIGHT);
    assertThat(deliveries.claims)
        .containsExactly(new Asked(Set.of(), Set.of(), MAX_IN_FLIGHT, LEASE));
    assertThat(deliveries.due).hasSize(6);

    attempts.finish(queued.getFirst());

    await().until(() -> deliveries.due.size() == 5);
    await().until(() -> attempts.inFlight.size() == MAX_IN_FLIGHT);
    assertThat(deliveries.claims).hasSize(2);
    assertThat(deliveries.claims.getLast().free()).isEqualTo(1);
  }

  // Four attempts of one endpoint are in flight, and thirteen of its tenant. The claims that
  // follow name both. When an attempt of the endpoint has finished, the endpoint has room, and
  // so has the tenant with its twelve.
  @Test
  void claimNamesTheEndpointsAndTenantsThatAreFull() {
    UUID tenant = UUID.randomUUID();
    UUID endpoint = UUID.randomUUID();
    List<Claimed> ofTheEndpoint = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      ofTheEndpoint.add(makeDue(tenant, endpoint, false));
    }
    for (int i = 0; i < 9; i++) {
      makeDue(tenant, UUID.randomUUID(), false);
    }
    loop.start();

    await().until(() -> attempts.inFlight.size() == 13);
    await()
        .untilAsserted(
            () -> {
              Asked latest = deliveries.claims.getLast();
              assertThat(latest.fullEndpoints()).containsExactly(endpoint);
              assertThat(latest.fullTenants()).containsExactly(tenant);
              assertThat(latest.free()).isEqualTo(MAX_IN_FLIGHT - 13);
            });

    attempts.finish(ofTheEndpoint.getFirst());

    await()
        .untilAsserted(
            () -> {
              Asked latest = deliveries.claims.getLast();
              assertThat(latest.fullEndpoints()).isEmpty();
              assertThat(latest.fullTenants()).isEmpty();
              assertThat(latest.free()).isEqualTo(MAX_IN_FLIGHT - 12);
            });
  }

  // The worker counts its cutoffs from the time that an attempt is given. The poller reads that
  // time before it makes the claim, so it is earlier than the lease that the claim takes, however
  // long the claim runs. Both times are read from the same clock.
  @Test
  void attemptIsGivenATimeFromBeforeItsClaimWasMade() {
    Claimed delivery = makeDue(1).getFirst();
    loop.start();

    await().until(() -> attempts.inFlight.size() == 1);

    assertThat(attempts.claimStartedNanos.get(delivery) - deliveries.calledAtNanos.get(delivery))
        .isLessThanOrEqualTo(0);
  }

  // Nothing is due, so every claim comes back empty and is followed by one wait. The waits grow,
  // and the claim that brings a row starts them over.
  @Test
  void emptyClaimIsFollowedByAWaitThatDoublesAndAClaimWithARowStartsItOver() {
    loop.start();
    await().until(() -> waits.asked.size() == 1);
    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);
    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 3);
    assertThat(deliveries.claims).hasSize(3);

    makeDue(1);
    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 4);

    assertThat(waits.asked)
        .containsExactly(
            Duration.ofMillis(50),
            Duration.ofMillis(100),
            Duration.ofMillis(200),
            Duration.ofMillis(50));
    assertThat(deliveries.claims).hasSize(5);
  }

  // The loop waits after the empty claim that followed the one with the row. The wait of this
  // test does not end by time, so the third claim shows that the finished attempt ended it.
  @Test
  void finishedAttemptEndsTheWait() {
    Claimed delivery = makeDue(1).getFirst();
    loop.start();
    await().until(() -> attempts.inFlight.size() == 1);
    await().until(() -> waits.asked.size() == 1);
    assertThat(deliveries.claims).hasSize(2);

    attempts.finish(delivery);

    await().until(() -> deliveries.claims.size() == 3);
  }

  // A claim that failed is followed by the wait of an empty one, and the loop goes on.
  @Test
  void failedClaimIsCountedAndTheLoopClaimsAgain() {
    deliveries.failureOfTheNextClaim = new IllegalStateException("the database did not answer");
    makeDue(1);
    loop.start();

    await().until(() -> waits.asked.size() == 1);
    assertThat(attempts.inFlight).isEmpty();
    waits.timeIsUp();
    await().until(() -> attempts.inFlight.size() == 1);

    assertThat(registry.get("webhooks.claim.errors").counter().count()).isEqualTo(1);
    assertThat(waits.asked.getFirst()).isEqualTo(Duration.ofMillis(50));
  }

  // The clock of the heartbeat stands still unless the test moves it, so a heartbeat of age zero
  // has been beaten since the test last moved the clock. The loop beats before it waits, and the
  // wait of this test does not end by time, so no beat comes while the test moves the clock.
  @Test
  void everyIterationBeatsTheHeartbeat() {
    loop.start();
    await().until(() -> waits.asked.size() == 1);
    heartbeatNanos.addAndGet(SEVENTY_SECONDS.toNanos());
    assertThat(heartbeat.sinceLastIteration()).isEqualTo(SEVENTY_SECONDS);

    waits.timeIsUp();
    await().until(() -> waits.asked.size() == 2);

    assertThat(deliveries.claims).hasSize(2);
    assertThat(heartbeat.sinceLastIteration()).isZero();
  }

  // A database that is down fails every claim. The loop still loops, and its heartbeat says so.
  @Test
  void iterationWhoseClaimFailedBeatsTheHeartbeatToo() {
    heartbeatNanos.addAndGet(SEVENTY_SECONDS.toNanos());
    deliveries.failureOfTheNextClaim = new IllegalStateException("the database did not answer");
    loop.start();

    await().until(() -> waits.asked.size() == 1);

    assertThat(registry.get("webhooks.claim.errors").counter().count()).isEqualTo(1);
    assertThat(heartbeat.sinceLastIteration()).isZero();
  }

  // The claim wrote the attempt row for the holder that recorded nothing. The poller counts it.
  @Test
  void abandonedDeliveryIsCountedAsAnAttemptOfUnknownOutcome() {
    makeDue(UUID.randomUUID(), UUID.randomUUID(), true);
    makeDue(UUID.randomUUID(), UUID.randomUUID(), false);
    loop.start();

    await().until(() -> attempts.inFlight.size() == 2);

    assertThat(
            registry
                .get("webhooks.delivery.attempts")
                .tag("outcome", "unknown")
                .tag("reason", "lease_expired")
                .counter()
                .count())
        .isEqualTo(1);
  }

  // A delivery that becomes due after the stop stays where it is. The two attempts that were in
  // flight go on, and the poller is idle only when they have finished.
  @Test
  void stopEndsTheClaimingAndLeavesTheAttemptsInFlightToFinish() throws InterruptedException {
    makeDue(2);
    loop.start();
    await().until(() -> attempts.inFlight.size() == 2);

    poller.stop();
    loop.join();
    makeDue(1);

    assertThat(poller.awaitIdle(Duration.ofMillis(50))).isFalse();
    assertThat(attempts.inFlight).hasSize(2);

    attempts.finishAll();

    assertThat(poller.awaitIdle(Duration.ofSeconds(10))).isTrue();
    assertThat(deliveries.due).hasSize(1);
  }

  private List<Claimed> makeDue(int count) {
    List<Claimed> queued = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      queued.add(makeDue(UUID.randomUUID(), UUID.randomUUID(), false));
    }
    return queued;
  }

  private Claimed makeDue(UUID tenantId, UUID endpointId, boolean abandoned) {
    Claimed delivery =
        new Claimed(
            UUID.randomUUID(),
            tenantId,
            UUID.randomUUID(),
            endpointId,
            abandoned ? 2 : 1,
            0,
            UUID.randomUUID(),
            abandoned,
            false);
    deliveries.due.add(delivery);
    return delivery;
  }
}
