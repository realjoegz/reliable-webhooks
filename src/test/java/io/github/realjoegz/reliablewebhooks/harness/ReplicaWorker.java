package io.github.realjoegz.reliablewebhooks.harness;

import io.github.realjoegz.reliablewebhooks.ops.WorkerLifecycle;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * The delivery worker of a replica that runs inside the test JVM. Such a replica does not start its
 * worker by itself, because the replicas of a build share one database: a worker that ran all the
 * time would take the deliveries of tests that are about something else, and those of a test that
 * needs to claim them itself. A test that wants deliveries sent starts the worker.
 *
 * <p>Every start gets a worker lifecycle and a poller of its own from the application context,
 * wired as the application wires them, and runs the same start and stop that Spring runs.
 */
@Component
public final class ReplicaWorker {

  private final ObjectProvider<WorkerLifecycle> lifecycles;
  private WorkerLifecycle running;

  ReplicaWorker(ObjectProvider<WorkerLifecycle> lifecycles) {
    this.lifecycles = lifecycles;
  }

  public synchronized void start() {
    if (running != null) {
      throw new IllegalStateException("The worker of this replica is running already");
    }
    running = lifecycles.getObject();
    running.start();
  }

  /**
   * Stops claiming and waits for the attempts in flight, as a shutdown of the replica does. Does
   * nothing when the worker is not running.
   */
  public synchronized void stop() {
    if (running != null) {
      running.stop();
      running = null;
    }
  }
}
