/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.waiter;

import com.oracle.bmc.waiter.DelayStrategy;
import com.oracle.bmc.waiter.WaiterConfiguration;

public class ResettingExponentialBackoffStrategy implements DelayStrategy {
    private final long resetThresholdInSeconds;
    private int maxAttemptsPossible;

    public ResettingExponentialBackoffStrategy(long resetThresholdInSeconds) {
        this.resetThresholdInSeconds = resetThresholdInSeconds;
        this.maxAttemptsPossible = (int) (Math.log(resetThresholdInSeconds) / Math.log(2)) + 1;
    }

    @Override
    public long nextDelay(WaiterConfiguration.WaitContext context) {
        long delay = (long) Math.pow(2, (context.getAttemptsMade() % maxAttemptsPossible));
        delay *= 1000;
        if (delay <= 0) {
            return resetThresholdInSeconds * 1000;
        }
        return Math.min(delay, resetThresholdInSeconds * 1000);
    }
}
