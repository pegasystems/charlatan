package org.apache.zookeeper.impl.common;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation allows for implementation classes to be autodetected through classpath scanning.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Service {
}
