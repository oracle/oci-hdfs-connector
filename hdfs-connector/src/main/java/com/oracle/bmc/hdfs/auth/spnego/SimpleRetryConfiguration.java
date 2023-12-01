package com.oracle.bmc.hdfs.auth.spnego;

import java.io.IOException;
import java.util.Set;

public class SimpleRetryConfiguration {

    private final int maxRetryCount;
    private final Set<Class<? extends IOException>> whitelistExceptions;

    public SimpleRetryConfiguration(int maxRetryCount, Set<Class<? extends IOException>> whitelistExceptions) {
        this.maxRetryCount = maxRetryCount;
        this.whitelistExceptions = whitelistExceptions;
    }

    public boolean shouldRetryOnException(IOException exception, int currentExecutionCount) {
        if (currentExecutionCount >= maxRetryCount) {
            return false;
        }

        return whitelistExceptions.contains(exception.getClass());
    }
}