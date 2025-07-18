/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.waiter;

import com.oracle.bmc.waiter.DelayStrategy;
import com.oracle.bmc.waiter.WaiterConfiguration;

public class ResettingExponentialBackoffStrategy implements DelayStrategy {
    private final long resetThresholdInSeconds;

    final private long  jitterInMillis;
    private int maxAttemptsPossible;

    public ResettingExponentialBackoffStrategy(long resetThresholdInSeconds) {
        this(resetThresholdInSeconds, 0);
    }

    public ResettingExponentialBackoffStrategy(long resetThresholdInSeconds, long jitterInMillis) {
        this.resetThresholdInSeconds = resetThresholdInSeconds;
        this.maxAttemptsPossible = (int) (Math.log(resetThresholdInSeconds) / Math.log(2)) + 1;
        this.jitterInMillis = jitterInMillis < 0 ? 0 : jitterInMillis;
    }

    @Override
    public long nextDelay(WaiterConfiguration.WaitContext context) {
        long delay = (long) Math.pow(2, (context.getAttemptsMade() % maxAttemptsPossible));
        delay *= 1000;
        if (delay <= 0) {
            return resetThresholdInSeconds * 1000;
        }
        long jitterValue = 0L;
        if (jitterInMillis > 0) {
            double random = Math.random();
            jitterValue = Math.round(random) % (jitterInMillis * 2) - jitterInMillis;
        }

        delay = Math.min(delay, resetThresholdInSeconds * 1000);
        return delay + jitterValue >= 0 ?  delay + jitterValue : delay - jitterValue;
    }
}
