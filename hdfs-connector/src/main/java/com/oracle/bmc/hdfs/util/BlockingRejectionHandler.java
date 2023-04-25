/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.util;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public class BlockingRejectionHandler implements RejectedExecutionHandler {
    private final long timeout;

    public BlockingRejectionHandler(long timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        } else {
            this.timeout = timeout;
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        String.format(
                                "Initializing %s with timeout: %d seconds",
                                this.getClass().getSimpleName(),
                                this.timeout));
            }
        }
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (executor.isShutdown()) {
            throw new RejectedExecutionException("The executor has already been shutdown");
        } else {
            BlockingQueue<Runnable> executorQueue = executor.getQueue();
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            String.format(
                                    "Attempting to block and queue task, timeout: %d seconds",
                                    this.timeout));
                }

                if (!executorQueue.offer(r, this.timeout, TimeUnit.SECONDS)) {
                    throw new RejectedExecutionException(
                            String.format(
                                    "Timed-out enqueue of blocking queue, duration %d seconds",
                                    this.timeout));
                }

                LOG.debug("BlockingHandler successfully queued task");
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt(); // we must reset our interrupted status
                throw new RejectedExecutionException("Interrupted while waiting to enqueue task");
            }
        }
    }
}
