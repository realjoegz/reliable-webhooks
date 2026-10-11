package io.github.realjoegz.reliablewebhooks.harness;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.test.context.ActiveProfiles;

/**
 * An {@link InProcessReplica} that can deliver to {@link FaultReceiver}. It runs with the {@code
 * unsafe-local} profile and the allowance in {@code application-unsafe-local.yaml}: plain {@code
 * http}, and the loopback address that the receiver listens on. Nothing else differs from the
 * shipped configuration. The classes with this annotation share one application context, apart from
 * the one that the classes with {@link InProcessReplica} alone share.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@InProcessReplica
@ActiveProfiles("unsafe-local")
public @interface UnsafeLocalReplica {}
