package io.github.realjoegz.reliablewebhooks.ops;

import io.github.realjoegz.reliablewebhooks.delivery.DeliveryPoller;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Starts the poller with the application and drains the worker when the application stops. It keeps
 * the default phase, which is the last to start and the first to stop: the worker stops while the
 * web server still answers and the connection pools are still open.
 *
 * <p>Stopping takes two steps. The poller stops claiming, so nothing new starts. Then the attempts
 * that are in flight finish and record their results. Nothing has to be released, because nothing
 * is claimed that has not started.
 */
@Component
public class WorkerLifecycle implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(WorkerLifecycle.class);

  // The attempt deadline of 15 s bounds an attempt, and its finalize needs some time after that.
  private static final Duration DRAIN_WAIT = Duration.ofSeconds(20);

  private final DeliveryPoller poller;
  private volatile Thread thread;

  WorkerLifecycle(DeliveryPoller poller) {
    this.poller = poller;
  }

  @Override
  public void start() {
    // A lifecycle that was stopped can be started again, and its poller is the same one.
    poller.clearStop();
    thread = Thread.ofPlatform().daemon().name("delivery-poller").start(poller::run);
  }

  @Override
  public void stop() {
    poller.stop();
    try {
      thread.join();
      // Written only once the loop has ended, so it is true that nothing is claimed after it.
      log.info("poller_stopped");
      if (!poller.awaitIdle(DRAIN_WAIT)) {
        log.warn("drain_timed_out");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
    thread = null;
  }

  @Override
  public boolean isRunning() {
    return thread != null;
  }
}
