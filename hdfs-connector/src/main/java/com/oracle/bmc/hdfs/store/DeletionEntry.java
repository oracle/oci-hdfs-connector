/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import org.apache.hadoop.fs.Path;

/**
 * Entry in the deletion queue for tracking per-object retry state in batch delete operations.
 */
public class DeletionEntry {
    private final Path path;
    private int retryAttempt;
    private int retry429Count;
    private int retry503Count;

    public DeletionEntry(Path path) {
        this.path = path;
        this.retryAttempt = 0;
        this.retry429Count = 0;
        this.retry503Count = 0;
    }

    /**
     * Records a retry attempt with the status code that triggered it.
     */
    public void recordRetry(int statusCode) {
        this.retryAttempt++;
        if (statusCode == 429) {
            retry429Count++;
        }
        if (statusCode == 503) {
            retry503Count++;
        }
    }

    /**
     * Checks if this object should be retried based on max retry limit.
     */
    public boolean shouldRetry(int maxRetries) {
        return retryAttempt < maxRetries;
    }

    /**
     * Gets the file path for this deletion entry.
     */
    public Path getPath() {
        return path;
    }

    /**
     * Gets the current retry attempt count.
     */
    public int getRetryAttempt() {
        return retryAttempt;
    }


    public int getRetry429Count() {
        return retry429Count;
    }

    public int getRetry503Count() {
        return retry503Count;
    }

    public String pathToObject() {
        // strip leading '/', everything else is the object name
        return path.toUri().getPath().substring(1);
    }
}
