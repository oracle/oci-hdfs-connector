package com.oracle.bmc.hdfs.util;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public class BlockingRejectionHandler implements RejectedExecutionHandler {
    private final long timeout; // DEFAULT TO ONE HOUR

    public BlockingRejectionHandler() {
        // default is one hour
        this.timeout = 3600;
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Initializing %s with default timeout: %d (sec)", this.getClass().getSimpleName(), this.timeout));
        }
    }

    public BlockingRejectionHandler(long timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive and greater then zero");
        } else {
            this.timeout = timeout;
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Initializing %s with timeout: %d (sec)", this.getClass().getSimpleName(), this.timeout));
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
                    LOG.debug(String.format("Attempting to block and queue task, timeout: %d (sec)", this.timeout));
                }

                if (!executorQueue.offer(r, this.timeout, TimeUnit.SECONDS)) {
                    throw new RejectedExecutionException(
                            String.format("Timed-out enqueue of blocking queue, duration %d (sec)", this.timeout));
                }

                LOG.debug("BlockingHandler successfully queued task");
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt(); // we must reset our interrupted status
                throw new RejectedExecutionException("Interrupted while waiting to enqueue task");
            }
        }
    }
}
