package io.github.realjoegz.reliablewebhooks.ops;

import java.time.Duration;

/** What a session of one pool reports about itself. A zero timeout means the limit is off. */
public record SessionSettings(
    String defaultTransactionIsolation,
    String synchronousCommit,
    Duration lockTimeout,
    Duration statementTimeout,
    Duration idleInTransactionTimeout) {}
