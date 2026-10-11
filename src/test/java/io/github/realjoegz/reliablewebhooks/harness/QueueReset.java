package io.github.realjoegz.reliablewebhooks.harness;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Keeps the deliveries of one test away from the worker of the next. Before a test, the pending
 * deliveries that earlier tests left are deleted: they belong to no test any more, and a worker
 * that a test starts would send them. After a test, the worker of its replica is stopped if the
 * test left it running.
 */
final class QueueReset implements BeforeEachCallback, AfterEachCallback {

  @Override
  public void beforeEach(ExtensionContext context) {
    TestDatabase.jdbc().sql("DELETE FROM delivery WHERE state = 'pending'").update();
  }

  @Override
  public void afterEach(ExtensionContext context) {
    SpringExtension.getApplicationContext(context).getBean(ReplicaWorker.class).stop();
  }
}
