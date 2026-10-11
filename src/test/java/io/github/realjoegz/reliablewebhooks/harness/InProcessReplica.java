package io.github.realjoegz.reliablewebhooks.harness;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

/**
 * Runs a test class against a replica inside the test JVM, listening on a random port and connected
 * to {@link TestDatabase}. Every class with this annotation and no other configuration gets the
 * same replica: one application context, and so one pair of connection pools.
 *
 * <p>The replica runs with the shipped configuration, so it connects to public addresses only. Its
 * delivery worker runs only while a test has started it through {@link ReplicaWorker}, and every
 * test begins with an empty queue.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureRestTestClient
@ContextConfiguration(initializers = ReplicaSettings.class)
@ExtendWith(QueueReset.class)
public @interface InProcessReplica {}
