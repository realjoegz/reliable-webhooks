package io.github.realjoegz.reliablewebhooks.harness;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

/**
 * Runs a test class against a replica inside the test JVM, listening on a random port and connected
 * to {@link TestDatabase}. Every class with this annotation gets the same replica: one annotation
 * means one application context, and so one pair of connection pools per build.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureRestTestClient
@ContextConfiguration(initializers = ReplicaSettings.class)
public @interface InProcessReplica {}
