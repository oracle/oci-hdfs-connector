package com.oracle.bmc.hdfs.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A simple ExecutorService that executes tasks on the calling thread.
 *
 * This class is useful when you need to execute a task synchronously on the current thread, but still want to use the
 * ExecutorService interface for consistency with other parts of your code that use thread pools.
 */
public class DirectExecutorService extends AbstractExecutorService {
    private volatile boolean shutdown;

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }

    @Override
    public void execute(Runnable command) {
        command.run();
    }
}