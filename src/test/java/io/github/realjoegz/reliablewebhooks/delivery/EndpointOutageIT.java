package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.PgWait;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaProcess;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaSettings;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestDatabase;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import io.github.realjoegz.reliablewebhooks.ops.ExpiryJob;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * An endpoint that is down, from its fifth failure to its recovery or to the end of its deliveries.
 * The breaker runs with the settings that ship, which are part of its statements: five failures,
 * thirty seconds to the first probe, sixty seconds for a probe slot and five minutes between probes
 * at most. Those times pass in the rows. The test moves the probe time of the circuit and the due
 * time of a delivery into the past.
 *
 * <p>Every tenant has a second endpoint, which takes the canaries. That nothing was sent to the
 * endpoint that is down is asserted only once a canary, accepted after the deliveries behind the
 * circuit were due, has been delivered: a claim ran then and left them where they were.
 */
@UnsafeLocalReplica
class EndpointOutageIT {

  private static final String FOR_THE_ENDPOINT =
      """
      {"type":"outage.sent","data":{}}""";
  private static final String CANARY =
      """
      {"type":"canary.sent","data":{}}""";
  private static final String ENDPOINT = "/endpoint";
  private static final String CANARY_ENDPOINT = "/canary";

  private static final int FAILURES_THAT_OPEN = 5;
  private static final int MAX_ATTEMPTS = 12;
  private static final Duration FIRST_PROBE_AFTER = Duration.ofSeconds(30);
  private static final Duration PROBE_SLOT = Duration.ofSeconds(60);
  private static final Duration LONGEST_PROBE_WAIT = Duration.ofMinutes(5);

  // How far each failed probe of an outage moves the next one, from the first probe on.
  private static final List<Duration> WAITS_AFTER_FAILED_PROBES =
      List.of(
          Duration.ofSeconds(60),
          Duration.ofSeconds(120),
          Duration.ofSeconds(240),
          LONGEST_PROBE_WAIT,
          LONGEST_PROBE_WAIT,
          LONGEST_PROBE_WAIT,
          LONGEST_PROBE_WAIT);

  private static final Duration BUDGET = Duration.ofHours(72);
  private static final Duration SPREAD_OF_A_REPLAY = Duration.ofSeconds(60);
  private static final Duration PAST_THE_LEASE = Duration.ofSeconds(61);
  private static final Duration AN_HOUR = Duration.ofHours(1);
  private static final int MOST_IN_FLIGHT_FOR_AN_ENDPOINT = 4;

  private static final String KILLED = "replica-that-is-killed";
  private static final String SECOND_CLAIMER = "second-claimer-at-the-gate";

  // A piece of the gate, as the database shows it.
  private static final String GATE = "UPDATE endpoint_circuit SET next_probe_at";

  private static final Rows.Circuit OPEN_WITHOUT_A_FAILED_PROBE =
      new Rows.Circuit("open", FAILURES_THAT_OPEN, 0, true, true);
  private static final Rows.Delivery NEVER_CLAIMED =
      new Rows.Delivery("pending", 0, 0, false, null, false);

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;
  @Autowired PollerFleet fleet;
  @Autowired ExpiryJob expiry;
  @Autowired MeterRegistry meters;

  // Four deliveries fail, and the circuit is still closed. They are moved an hour ahead, so that
  // none of them comes due for its retry in what follows. The receiver then holds the next four
  // requests, which is as many as a replica has open to one endpoint, while two more deliveries
  // are due and stay unclaimed because the endpoint is full. The four are answered with a 500
  // together. One of them is the fifth failure in a row and opens the circuit. The other three
  // were in flight when it opened: they are recorded as the failures they were, and the circuit
  // does not count them. That makes the eight requests that one replica can send to an endpoint
  // that is down before it sends no more.
  //
  // Then every delivery of the endpoint is made due. The rows are read, a canary is delivered,
  // and the rows are read again: the delivery rows and the circuit row are what they were, column
  // for column, and no ninth request has arrived.
  @Test
  void circuitOpensAndHolds() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold inFlight = FaultReceiver.hold(FaultReceiver.status(500))) {
      receiver.answer(ENDPOINT, FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "outage-opens");
      UUID endpoint = registerWithCanary(tenant, receiver);
      worker.start();

      List<UUID> failedBeforeItOpened = deliveriesThatFailedOnce(tenant, FAILURES_THAT_OPEN - 1);
      assertThat(Rows.circuit(endpoint))
          .isEqualTo(new Rows.Circuit("closed", FAILURES_THAT_OPEN - 1, 0, false, false));
      failedBeforeItOpened.forEach(delivery -> Rows.postponeDelivery(delivery, AN_HOUR));

      receiver.answer(ENDPOINT, inFlight);
      List<UUID> accepted = new ArrayList<>();
      for (int i = 0; i < MOST_IN_FLIGHT_FOR_AN_ENDPOINT + 2; i++) {
        accepted.add(accept(tenant));
      }
      await().until(() -> inFlight.waiting() == MOST_IN_FLIGHT_FOR_AN_ENDPOINT);
      inFlight.release();
      await().until(() -> attempted(accepted).size() == MOST_IN_FLIGHT_FOR_AN_ENDPOINT);

      assertThat(Rows.circuit(endpoint)).isEqualTo(OPEN_WITHOUT_A_FAILED_PROBE);
      List<UUID> inFlightWhenItOpened = attempted(accepted);
      assertThat(inFlightWhenItOpened)
          .allSatisfy(
              delivery ->
                  assertThat(Rows.attempts(delivery))
                      .containsExactly(Rows.Attempt.answered(1, 500)));
      // The probe time is thirty seconds after the failure that opened the circuit, to the
      // microsecond, and that failure is one of the four.
      assertThat(inFlightWhenItOpened)
          .extracting(delivery -> Rows.probeWaitAfter(delivery, 1))
          .contains(FIRST_PROBE_AFTER);

      List<UUID> neverSent = new ArrayList<>(accepted);
      neverSent.removeAll(inFlightWhenItOpened);
      failedBeforeItOpened.forEach(delivery -> Rows.ageDelivery(delivery, AN_HOUR));
      failedBeforeItOpened.forEach(EndpointOutageIT::makeDueAfterItsFirstFailure);
      inFlightWhenItOpened.forEach(EndpointOutageIT::makeDueAfterItsFirstFailure);
      assertThat(pendingAndDue(endpoint)).isEqualTo(failedBeforeItOpened.size() + accepted.size());
      List<String> held = everyColumnOf(endpoint);

      deliverCanary(tenant);

      assertThat(everyColumnOf(endpoint)).isEqualTo(held);
      assertThat(receiver.requests(ENDPOINT))
          .hasSize(FAILURES_THAT_OPEN + MOST_IN_FLIGHT_FOR_AN_ENDPOINT - 1);
      assertThat(neverSent)
          .hasSize(2)
          .allSatisfy(
              delivery -> {
                assertThat(Rows.delivery(delivery)).isEqualTo(NEVER_CLAIMED);
                assertThat(Rows.attempts(delivery)).isEmpty();
              });
    }
  }

  // The gate by itself first, on two connections that the test holds, with no worker running. The
  // first has taken the probe slot and has not committed. It reads the probe time that it wrote,
  // which lies sixty seconds after the clock of its transaction, to the microsecond. The second
  // runs the gate and is seen waiting for the circuit row. When the first commits, the second
  // finds the probe time moved and changes nothing.
  //
  // Then four pollers, each a replica to the database, and eight deliveries behind the circuit.
  // For each probe interval the test makes the probe due while no delivery is, takes the circuit
  // row and only then makes the eight due. The claim does not need that row, so the pollers claim
  // all eight, which takes two of them at least, because one has no more than four in flight for
  // an endpoint. All eight are seen waiting at the gate at the same time. The test lets go of the
  // row: one request arrives, and the seven others are given back without an attempt.
  //
  // A gate that waits is ended by the lock timeout of the worker pool, so the eight have five
  // seconds to arrive. A run that took longer would fail and not pass.
  @Test
  void oneProbeAdmittedAcrossConcurrentClaimers() throws Exception {
    int intervals = 3;
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer(ENDPOINT, FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "outage-one-probe");
      UUID endpoint = registerWithCanary(tenant, receiver);
      worker.start();
      List<UUID> held = new ArrayList<>(deliveriesThatFailedOnce(tenant, FAILURES_THAT_OPEN));
      worker.stop();
      Rows.ageCircuit(endpoint, FIRST_PROBE_AFTER);

      try (ExecutorService claimer = Executors.newVirtualThreadPerTaskExecutor();
          Connection first = TestDatabase.connection();
          Connection second = TestDatabase.connection()) {
        nameSession(second, SECOND_CLAIMER);

        assertThat(gate(first, endpoint)).isEqualTo(1);
        assertThat(probeWaitSeenBy(first, endpoint)).isEqualTo(PROBE_SLOT);
        Future<Integer> gateOfTheSecond = claimer.submit(() -> gate(second, endpoint));
        PgWait.untilBlocked(GATE, SECOND_CLAIMER);
        first.commit();

        assertThat(gateOfTheSecond.get()).isZero();
        second.commit();
      }
      assertThat(Rows.circuit(endpoint)).isEqualTo(OPEN_WITHOUT_A_FAILED_PROBE);
      assertThat(probeLiesAhead(endpoint, "50 seconds", "60 seconds")).isTrue();

      for (int i = 0; i < 3; i++) {
        held.add(accept(tenant));
      }
      double gatesLostBefore = gatesLost();
      try (PollerFleet.Running replicas =
          fleet.start("replica-1", "replica-2", "replica-3", "replica-4")) {
        for (int interval = 1; interval <= intervals; interval++) {
          int probes = interval;
          held.forEach(delivery -> Rows.postponeDelivery(delivery, AN_HOUR));
          Rows.ageCircuit(endpoint, LONGEST_PROBE_WAIT);
          try (Connection holder = TestDatabase.connection();
              PreparedStatement lock =
                  holder.prepareStatement(
                      "SELECT 1 FROM endpoint_circuit WHERE endpoint_id = ? FOR UPDATE")) {
            lock.setObject(1, endpoint);
            lock.execute();
            held.forEach(delivery -> Rows.ageDelivery(delivery, AN_HOUR.multipliedBy(2)));
            PgWait.untilBlocked(GATE, ReplicaSettings.IN_PROCESS_REPLICA, held.size());
            holder.rollback();
          }
          await()
              .untilAsserted(
                  () -> {
                    assertThat(Rows.circuit(endpoint).probeFailures()).isEqualTo(probes);
                    assertThat(leased(endpoint)).isZero();
                  });
          assertThat(receiver.requests(ENDPOINT)).hasSize(FAILURES_THAT_OPEN + probes);
        }
        deliverCanary(tenant);
      }

      assertThat(receiver.requests(ENDPOINT)).hasSize(FAILURES_THAT_OPEN + intervals);
      assertThat(probed(endpoint)).hasSize(intervals);
      assertThat(deliveriesWhoseAttemptsAreNotTheirAttemptRows(endpoint)).isZero();
      assertThat(gatesLost() - gatesLostBefore).isEqualTo(intervals * (held.size() - 1));
    }
  }

  // An outage that has lasted for a thousand failed probes. The next probe fails as well, and its
  // finalize moves the probe time five minutes ahead, to the microsecond, as it does from the
  // fourth failed probe on.
  //
  // The control is the wait that the count would give if nothing capped it. The database refuses
  // to compute it. A statement that capped the wait only after computing it would fail in the
  // same way, the probe would never be recorded, and the test would wait for its row in vain.
  //
  // The five deliveries that opened the circuit are moved an hour ahead, so the delivery that is
  // accepted afterwards is the only one that is due when the probe time comes.
  @Test
  void probeBackoffIsCappedAfterManyFailures() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer(ENDPOINT, FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "outage-long");
      UUID endpoint = registerWithCanary(tenant, receiver);
      worker.start();
      deliveriesThatFailedOnce(tenant, FAILURES_THAT_OPEN)
          .forEach(delivery -> Rows.postponeDelivery(delivery, AN_HOUR));
      UUID held = accept(tenant);
      writeFailedProbes(endpoint, 1000);
      assertThatThrownBy(
              () ->
                  TestDatabase.jdbc()
                      .sql("SELECT (interval '30 seconds' * power(2, 1001))::text")
                      .query(String.class)
                      .single())
          .isInstanceOf(DataAccessException.class)
          .hasMessageContaining("interval out of range");

      Rows.ageCircuit(endpoint, FIRST_PROBE_AFTER);
      await().untilAsserted(() -> assertThat(Rows.attempts(held)).hasSize(1));

      assertThat(Rows.attempts(held)).containsExactly(failedProbe(1));
      assertThat(Rows.circuit(endpoint))
          .isEqualTo(new Rows.Circuit("open", FAILURES_THAT_OPEN, 1001, true, true));
      assertThat(Rows.probeWaitAfter(held, 1)).isEqualTo(LONGEST_PROBE_WAIT);
    }
  }

  // Ten deliveries are due behind the open circuit when the endpoint answers again: five that
  // failed and five that were never sent. The test writes into the circuit row that three probes
  // have failed, so that the success has a count to clear. The probe is answered with a 200 at
  // once, and the receiver holds every request after it.
  //
  // It comes to hold four. By then the probe has closed the circuit, which has neither count
  // left, and is the only delivery that has succeeded, and the endpoint is full with five
  // deliveries still waiting. A canary for the other endpoint of the tenant is delivered
  // meanwhile. When the receiver lets go, the rest follows. The endpoint never had more than four
  // requests open.
  @Test
  void recoveryDrainsHeldDeliveries() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold draining = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer(ENDPOINT, FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "outage-recovery");
      UUID endpoint = registerWithCanary(tenant, receiver);
      worker.start();
      List<UUID> failed = deliveriesThatFailedOnce(tenant, FAILURES_THAT_OPEN);
      List<UUID> held = new ArrayList<>(failed);
      for (int i = 0; i < 5; i++) {
        held.add(accept(tenant));
      }
      failed.forEach(EndpointOutageIT::makeDueAfterItsFirstFailure);
      assertThat(pendingAndDue(endpoint)).isEqualTo(held.size());
      writeFailedProbes(endpoint, 3);
      assertThat(Rows.circuit(endpoint))
          .isEqualTo(new Rows.Circuit("open", FAILURES_THAT_OPEN, 3, true, true));

      receiver.answerOnce(ENDPOINT, FaultReceiver.status(200));
      receiver.answer(ENDPOINT, draining);
      Rows.ageCircuit(endpoint, FIRST_PROBE_AFTER);
      await().until(() -> draining.waiting() == MOST_IN_FLIGHT_FOR_AN_ENDPOINT);

      assertThat(Rows.circuit(endpoint)).isEqualTo(Rows.Circuit.CLOSED);
      List<UUID> probed = probed(endpoint);
      assertThat(probed).hasSize(1);
      UUID probe = probed.getFirst();
      assertThat(succeeded(held)).containsExactly(probe);
      assertThat(Rows.attempts(probe).getLast())
          .satisfies(
              last -> {
                assertThat(last.probe()).isTrue();
                assertThat(last.outcome()).isEqualTo("success");
              });
      deliverCanary(tenant);
      assertThat(draining.waiting()).isEqualTo(MOST_IN_FLIGHT_FOR_AN_ENDPOINT);

      draining.release();
      await().untilAsserted(() -> assertThat(succeeded(held)).hasSameSizeAs(held));

      assertThat(receiver.peakConcurrency(ENDPOINT)).isEqualTo(MOST_IN_FLIGHT_FOR_AN_ENDPOINT);
      assertThat(receiver.requests(ENDPOINT)).hasSize(FAILURES_THAT_OPEN + held.size());
      assertThat(receiver.requests(ENDPOINT).get(FAILURES_THAT_OPEN).header("webhook-id"))
          .isEqualTo(probe.toString());
      assertThat(probed(endpoint)).containsExactly(probe);
      assertThat(deliveriesWhoseAttemptsAreNotTheirAttemptRows(endpoint)).isZero();
      assertThat(Rows.circuit(endpoint)).isEqualTo(Rows.Circuit.CLOSED);
    }
  }

  // A replica that runs as a process of its own is killed in the middle of a probe. The delivery
  // has failed five times, which opened the circuit, and its sixth attempt is the probe. The
  // receiver holds that request, so the replica has taken the probe slot and has the request open
  // when the SIGKILL comes.
  //
  // What is left is a time and no state. The probe time lies sixty seconds after the gate, and
  // the delivery is under the lease of the dead replica. The second replica is the one inside the
  // test JVM: its worker is started, and the lease is aged past its end. The delivery is due then
  // and is not claimed, because the slot is still taken: a canary is delivered, and nothing else
  // has changed.
  //
  // The probe time is aged by the sixty seconds of the slot. The second replica claims the
  // delivery, logs the attempt of the dead one as unknown and probes. The endpoint answers 200
  // now, which closes the circuit.
  //
  // The slot reopens by itself sixty seconds after the gate, so the test has that long from the
  // kill to the canary, and it starts no process in that time. A run that took longer would fail
  // and not pass: the second replica would have probed before the test asked.
  @Test
  @Tag("process")
  void probeSlotReopensAfterProberKilled() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold probe = FaultReceiver.hold(FaultReceiver.status(500))) {
      receiver.answer(ENDPOINT, FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "outage-prober-killed");
      UUID endpoint = registerWithCanary(tenant, receiver);
      UUID delivery = deliveryThatFailedFiveTimes(tenant);
      receiver.answer(ENDPOINT, probe);

      try (ReplicaProcess killed = ReplicaProcess.start(KILLED)) {
        makeDueForItsProbe(delivery, endpoint);
        await().until(() -> probe.waiting() == 1);
        assertThat(probeLiesAhead(endpoint, "45 seconds", "60 seconds")).isTrue();

        killed.kill();
      }
      PgWait.untilGone(KILLED);

      Rows.Delivery leasedForTheProbe = new Rows.Delivery("pending", 6, 0, true, null, false);
      assertThat(Rows.delivery(delivery)).isEqualTo(leasedForTheProbe);
      assertThat(Rows.attempts(delivery)).isEqualTo(fiveFailures());
      assertThat(Rows.circuit(endpoint)).isEqualTo(OPEN_WITHOUT_A_FAILED_PROBE);

      worker.start();
      Rows.ageDelivery(delivery, PAST_THE_LEASE);
      deliverCanary(tenant);
      assertThat(Rows.delivery(delivery)).isEqualTo(leasedForTheProbe);
      assertThat(Rows.attempts(delivery)).isEqualTo(fiveFailures());
      assertThat(receiver.requests(ENDPOINT)).hasSize(FAILURES_THAT_OPEN + 1);

      receiver.answer(ENDPOINT, FaultReceiver.status(200));
      Rows.ageCircuit(endpoint, PROBE_SLOT);
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 7, 0, false, null, true));
      List<Rows.Attempt> log = new ArrayList<>(fiveFailures());
      log.add(new Rows.Attempt(0, 6, false, "unknown", null, "lease_expired", null, null));
      log.add(
          new Rows.Attempt(
              0, 7, true, "success", 200, null, "127.0.0.1", ReplicaSettings.IN_PROCESS_REPLICA));
      assertThat(Rows.attempts(delivery)).isEqualTo(log);
      assertThat(Rows.circuit(endpoint)).isEqualTo(Rows.Circuit.CLOSED);
      assertThat(receiver.requests(ENDPOINT))
          .hasSize(FAILURES_THAT_OPEN + 2)
          .allSatisfy(
              request -> assertThat(request.header("webhook-id")).isEqualTo(delivery.toString()));
    }
  }

  // One delivery for an endpoint that answers 500 to every request. Nothing is shortened: after
  // each attempt the test reads the wait that the finalize gave the delivery and the wait that it
  // gave the circuit. Only then does it age the delivery by the longest wait of its window and
  // the circuit by the wait that it read.
  //
  // The fifth failure opens the circuit. Each attempt after it is a probe, and a probe uses an
  // attempt like any other request: the delivery waits in the window of its own schedule after
  // each, and the seventh probe is its twelfth attempt and its last.
  @Test
  void hardDownEndpointWithDefaultBreakerSettings() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer(ENDPOINT, FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "outage-for-good");
      UUID endpoint = registerWithCanary(tenant, receiver);
      worker.start();

      UUID delivery = accept(tenant);
      List<Rows.Attempt> log = new ArrayList<>();
      for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        int recorded = attempt;
        await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(recorded));
        int failedProbes = attempt - FAILURES_THAT_OPEN;
        log.add(failedProbes > 0 ? failedProbe(attempt) : Rows.Attempt.answered(attempt, 500));

        if (attempt < FAILURES_THAT_OPEN) {
          assertThat(Rows.circuit(endpoint))
              .isEqualTo(new Rows.Circuit("closed", attempt, 0, false, false));
        } else {
          assertThat(Rows.circuit(endpoint))
              .isEqualTo(new Rows.Circuit("open", FAILURES_THAT_OPEN, failedProbes, true, true));
          assertThat(Rows.probeWaitAfter(delivery, attempt))
              .as("wait for the next probe after attempt %d", attempt)
              .isEqualTo(probeWaitAfter(failedProbes));
        }
        if (attempt < MAX_ATTEMPTS) {
          assertThat(Rows.delivery(delivery))
              .isEqualTo(new Rows.Delivery("pending", attempt, 0, false, null, false));
          assertThat(Rows.waitAfter(delivery, attempt))
              .as("wait after attempt %d", attempt)
              .isBetween(RetrySchedule.delay(attempt, 0), RetrySchedule.delay(attempt, 1));
          Rows.ageDelivery(delivery, RetrySchedule.delay(attempt, 1));
          if (attempt >= FAILURES_THAT_OPEN) {
            Rows.ageCircuit(endpoint, probeWaitAfter(failedProbes));
          }
        }
      }

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("dead", MAX_ATTEMPTS, 0, false, "exhausted", true));
      assertThat(Rows.attempts(delivery)).isEqualTo(log);
      assertThat(log).filteredOn(Rows.Attempt::probe).hasSize(MAX_ATTEMPTS - FAILURES_THAT_OPEN);
      assertThat(receiver.requests(ENDPOINT))
          .hasSize(MAX_ATTEMPTS)
          .allSatisfy(
              request -> assertThat(request.header("webhook-id")).isEqualTo(delivery.toString()));
    }
  }

  // Eight deliveries behind an open circuit: five that failed once and three that were accepted
  // after it opened. Two probe intervals pass, and each probe goes to another delivery, because
  // the one that has just probed waits for its retry. An interval is over when its probe is
  // recorded and no delivery is under a lease: one that was claimed beside the probe and lost the
  // gate has been given back by then.
  //
  // Then seventy-two hours pass in all eight rows. They are due and past their budget, and a
  // canary shows that a claim leaves them where they are. The expiry run dead-letters all eight,
  // each with the attempts that it used. That is none for a delivery that was never sent.
  @Test
  void heldDeliveriesExpireAfterBudget() throws Exception {
    int probes = 2;
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer(ENDPOINT, FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "outage-past-the-budget");
      UUID endpoint = registerWithCanary(tenant, receiver);
      worker.start();
      List<UUID> backlog = new ArrayList<>(deliveriesThatFailedOnce(tenant, FAILURES_THAT_OPEN));
      for (int i = 0; i < 3; i++) {
        backlog.add(accept(tenant));
      }
      for (int probe = 1; probe <= probes; probe++) {
        int failedProbes = probe;
        Rows.ageCircuit(endpoint, LONGEST_PROBE_WAIT);
        await()
            .untilAsserted(
                () -> {
                  assertThat(Rows.circuit(endpoint).probeFailures()).isEqualTo(failedProbes);
                  assertThat(leased(endpoint)).isZero();
                });
      }
      assertThat(probed(endpoint)).hasSize(probes).doesNotHaveDuplicates();

      backlog.forEach(delivery -> Rows.ageDelivery(delivery, BUDGET));
      assertThat(pendingAndDue(endpoint)).isEqualTo(backlog.size());
      List<String> held = everyColumnOf(endpoint);
      deliverCanary(tenant);
      assertThat(everyColumnOf(endpoint)).isEqualTo(held);

      expiry.run();

      assertThat(backlog)
          .allSatisfy(
              delivery -> {
                Rows.Delivery expired = Rows.delivery(delivery);
                assertThat(expired)
                    .isEqualTo(
                        new Rows.Delivery("dead", expired.attempts(), 0, false, "expired", true));
                assertThat(Rows.attempts(delivery))
                    .hasSize(expired.attempts())
                    .allSatisfy(attempt -> assertThat(attempt.outcome()).isEqualTo("failure"));
              });
      assertThat(backlog)
          .extracting(delivery -> Rows.delivery(delivery).attempts())
          .contains(0)
          .satisfies(used -> assertThat(sum(used)).isEqualTo(FAILURES_THAT_OPEN + probes));
      assertThat(receiver.requests(ENDPOINT)).hasSize(FAILURES_THAT_OPEN + probes);
    }
  }

  // The delivery has failed five times, and a replica is killed in the middle of its sixth
  // attempt, which is the probe. Nothing else is running, so the delivery stays as the kill left
  // it: under a lease, with six attempts counted and five recorded.
  //
  // Seventy-two hours later the expiry run dead-letters it and writes the row of the attempt
  // that nobody recorded. The run also clears the lease of the dead replica: the row that is read
  // straight after it has no lease token. The delivery is then replayed, and its new run starts
  // at attempt 1 with no row before it.
  @Test
  @Tag("process")
  void expiredAfterHolderKilledReplaysCleanly() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold probe = FaultReceiver.hold(FaultReceiver.status(500))) {
      receiver.answer(ENDPOINT, FaultReceiver.status(500));
      TestTenant tenant = TestTenant.create(http, "outage-holder-killed");
      UUID endpoint = registerWithCanary(tenant, receiver);
      UUID delivery = deliveryThatFailedFiveTimes(tenant);
      receiver.answer(ENDPOINT, probe);

      try (ReplicaProcess killed = ReplicaProcess.start(KILLED)) {
        makeDueForItsProbe(delivery, endpoint);
        await().until(() -> probe.waiting() == 1);

        killed.kill();
      }
      PgWait.untilGone(KILLED);
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 6, 0, true, null, false));

      Rows.ageDelivery(delivery, BUDGET);
      expiry.run();

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("dead", 6, 0, false, "expired", true));
      List<Rows.Attempt> firstRun = new ArrayList<>(fiveFailures());
      firstRun.add(new Rows.Attempt(0, 6, false, "unknown", null, "lease_expired", null, null));
      assertThat(Rows.attempts(delivery)).isEqualTo(firstRun);

      receiver.answer(ENDPOINT, FaultReceiver.status(200));
      tenant.replay(http, delivery).expectStatus().isNoContent();
      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("pending", 0, 1, false, null, false));
      Rows.ageDelivery(delivery, SPREAD_OF_A_REPLAY);
      Rows.ageCircuit(endpoint, PROBE_SLOT);
      worker.start();
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivery).state()).isEqualTo("succeeded"));

      assertThat(Rows.delivery(delivery))
          .isEqualTo(new Rows.Delivery("succeeded", 1, 1, false, null, true));
      List<Rows.Attempt> bothRuns = new ArrayList<>(firstRun);
      bothRuns.add(
          new Rows.Attempt(
              1, 1, true, "success", 200, null, "127.0.0.1", ReplicaSettings.IN_PROCESS_REPLICA));
      assertThat(Rows.attempts(delivery)).isEqualTo(bothRuns);
      assertThat(Rows.circuit(endpoint)).isEqualTo(Rows.Circuit.CLOSED);
      assertThat(receiver.requests(ENDPOINT))
          .hasSize(FAILURES_THAT_OPEN + 2)
          .allSatisfy(
              request -> assertThat(request.header("webhook-id")).isEqualTo(delivery.toString()));
    }
  }

  // The endpoint of the test takes the events of one type, and a second endpoint of the tenant
  // takes the canaries.
  private UUID registerWithCanary(TestTenant tenant, FaultReceiver receiver) {
    receiver.answer(CANARY_ENDPOINT, FaultReceiver.status(200));
    tenant.register(http, receiver.url(CANARY_ENDPOINT), "canary.sent");
    return tenant.register(http, receiver.url(ENDPOINT), "outage.sent");
  }

  private UUID accept(TestTenant tenant) {
    return Rows.deliveryOf(tenant.accept(http, FOR_THE_ENDPOINT));
  }

  // The witness that claims ran: an event for the other endpoint, accepted now, is delivered.
  private void deliverCanary(TestTenant tenant) {
    UUID canary = Rows.deliveryOf(tenant.accept(http, CANARY));
    await().untilAsserted(() -> assertThat(Rows.delivery(canary).state()).isEqualTo("succeeded"));
  }

  /**
   * Sends that many events to the endpoint and returns their deliveries once the first attempt of
   * each has failed. Five of them open the circuit. A delivery that has failed comes due for its
   * retry five seconds later at the earliest, so the events have that long to be sent and
   * attempted. In a run that took longer a retry would be one of the five failures. The circuit
   * would open with a delivery still unsent, and the wait for its attempt would time out.
   */
  private List<UUID> deliveriesThatFailedOnce(TestTenant tenant, int count) {
    List<UUID> deliveries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      deliveries.add(accept(tenant));
    }
    await().until(() -> attempted(deliveries).size() == count);
    return deliveries;
  }

  /**
   * One delivery that failed five times, with the worker of the replica inside the test JVM, which
   * is stopped again afterwards. The circuit is open, and the delivery waits for its sixth attempt.
   */
  private UUID deliveryThatFailedFiveTimes(TestTenant tenant) {
    worker.start();
    UUID delivery = accept(tenant);
    for (int attempt = 1; attempt <= FAILURES_THAT_OPEN; attempt++) {
      int recorded = attempt;
      await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(recorded));
      if (attempt < FAILURES_THAT_OPEN) {
        Rows.ageDelivery(delivery, RetrySchedule.delay(attempt, 1));
      }
    }
    worker.stop();
    return delivery;
  }

  private static List<Rows.Attempt> fiveFailures() {
    List<Rows.Attempt> failures = new ArrayList<>();
    for (int attempt = 1; attempt <= FAILURES_THAT_OPEN; attempt++) {
      failures.add(Rows.Attempt.answered(attempt, 500));
    }
    return failures;
  }

  private static Rows.Attempt failedProbe(int attemptNo) {
    return new Rows.Attempt(
        0, attemptNo, true, "failure", 500, null, "127.0.0.1", ReplicaSettings.IN_PROCESS_REPLICA);
  }

  // How far the circuit puts its next probe ahead once it has opened and that many probes have
  // failed since.
  private static Duration probeWaitAfter(int failedProbes) {
    return failedProbes == 0 ? FIRST_PROBE_AFTER : WAITS_AFTER_FAILED_PROBES.get(failedProbes - 1);
  }

  // Writes into the circuit row that so many probes of the outage have failed.
  private static void writeFailedProbes(UUID endpoint, int count) {
    TestDatabase.jdbc()
        .sql("UPDATE endpoint_circuit SET probe_failures = :count WHERE endpoint_id = :id")
        .param("count", count)
        .param("id", endpoint)
        .update();
  }

  private static void makeDueAfterItsFirstFailure(UUID delivery) {
    Rows.ageDelivery(delivery, RetrySchedule.delay(1, 1));
  }

  // After its fifth failure the delivery waits for its sixth attempt and the circuit for its
  // first probe. Both waits are over after this.
  private static void makeDueForItsProbe(UUID delivery, UUID endpoint) {
    Rows.ageDelivery(delivery, RetrySchedule.delay(FAILURES_THAT_OPEN, 1));
    Rows.ageCircuit(endpoint, FIRST_PROBE_AFTER);
  }

  /** The deliveries that have an attempt row. */
  private static List<UUID> attempted(List<UUID> deliveries) {
    return deliveries.stream().filter(delivery -> !Rows.attempts(delivery).isEmpty()).toList();
  }

  private static List<UUID> succeeded(List<UUID> deliveries) {
    return deliveries.stream()
        .filter(delivery -> Rows.delivery(delivery).state().equals("succeeded"))
        .toList();
  }

  private static int sum(List<? extends Integer> numbers) {
    return numbers.stream().mapToInt(Integer::intValue).sum();
  }

  private double gatesLost() {
    return meters.get("webhooks.delivery.released").tag("reason", "gate_lost").counter().count();
  }

  private static int gate(Connection connection, UUID endpoint) {
    return JdbcClient.create(new SingleConnectionDataSource(connection, true))
        .sql(CircuitBreaker.GATE)
        .param("e", endpoint)
        .update();
  }

  // How far the probe time lies ahead of the clock of the transaction that the connection is in.
  // That clock does not move inside a transaction, so for the one that ran the gate this is the
  // time that its gate added.
  private static Duration probeWaitSeenBy(Connection connection, UUID endpoint) {
    BigDecimal seconds =
        JdbcClient.create(new SingleConnectionDataSource(connection, true))
            .sql(
                """
                SELECT extract(epoch FROM next_probe_at - now())
                FROM endpoint_circuit WHERE endpoint_id = :id
                """)
            .param("id", endpoint)
            .query(BigDecimal.class)
            .single();
    return Duration.ofNanos(seconds.movePointRight(9).longValueExact());
  }

  // PgWait knows a session by this name. It is committed, so that the transaction of the session
  // begins with the statement that follows.
  private static void nameSession(Connection connection, String name) throws SQLException {
    try (Statement settings = connection.createStatement()) {
      settings.execute("SET application_name = '%s'".formatted(name));
    }
    connection.commit();
  }

  // Whether the probe time lies more than the one interval and at most the other ahead of the
  // database clock. The gap between the two is the time that the test has to ask.
  private static boolean probeLiesAhead(UUID endpoint, String moreThan, String atMost) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT next_probe_at > now() + :more_than::interval
               AND next_probe_at <= now() + :at_most::interval
            FROM endpoint_circuit WHERE endpoint_id = :id
            """)
        .param("more_than", moreThan)
        .param("at_most", atMost)
        .param("id", endpoint)
        .query(Boolean.class)
        .single();
  }

  /** The deliveries of the endpoint that a claim would take if the circuit let it. */
  private static int pendingAndDue(UUID endpoint) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM delivery
            WHERE endpoint_id = :id AND state = 'pending' AND next_attempt_at <= now()
            """)
        .param("id", endpoint)
        .query(Integer.class)
        .single();
  }

  private static int leased(UUID endpoint) {
    return TestDatabase.jdbc()
        .sql("SELECT count(*) FROM delivery WHERE endpoint_id = :id AND lease_token IS NOT NULL")
        .param("id", endpoint)
        .query(Integer.class)
        .single();
  }

  /** The deliveries of the endpoint that have probed, one entry for each probe, in their order. */
  private static List<UUID> probed(UUID endpoint) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT a.delivery_id
            FROM delivery_attempt a JOIN delivery d ON d.id = a.delivery_id
            WHERE d.endpoint_id = :id AND a.probe
            ORDER BY a.id
            """)
        .param("id", endpoint)
        .query(UUID.class)
        .list();
  }

  // A delivery that was claimed and given back has as many attempts as it has attempt rows. One
  // that lost an attempt on the way has more.
  private static int deliveriesWhoseAttemptsAreNotTheirAttemptRows(UUID endpoint) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT count(*) FROM delivery d
            WHERE d.endpoint_id = :id
              AND d.attempts <> (SELECT count(*) FROM delivery_attempt a WHERE a.delivery_id = d.id)
            """)
        .param("id", endpoint)
        .query(Integer.class)
        .single();
  }

  /**
   * The circuit row of the endpoint and its delivery rows, each as the text of all of its columns.
   */
  private static List<String> everyColumnOf(UUID endpoint) {
    return TestDatabase.jdbc()
        .sql(
            """
            SELECT line FROM (
              SELECT 'circuit ' || c::text AS line FROM endpoint_circuit c WHERE c.endpoint_id = :id
              UNION ALL
              SELECT 'delivery ' || d::text FROM delivery d WHERE d.endpoint_id = :id
            ) lines
            ORDER BY line
            """)
        .param("id", endpoint)
        .query(String.class)
        .list();
  }
}
