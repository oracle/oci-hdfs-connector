package com.oracle.bmc.hdfs.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.Progressable;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProgressReporterService {
    private final ScheduledExecutorService scheduledExecutorService;

    public ProgressReporterService(final int corePoolSize) {
        scheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize);
    }

    public ProgressHandle register(final Progressable progressable, final long intervalInSeconds) {
        if (progressable == null) {
            LOG.info("Cannot report progress since Progressable is null");
            return new ProgressHandle(null);
        }

        return new ProgressHandle(scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                LOG.info("Reporting progress");
                progressable.progress();
            }
        }, 0, intervalInSeconds, TimeUnit.SECONDS));
    }

    public void unregister(final ProgressHandle progressHandle) {
        if (progressHandle.scheduledFuture == null) {
            return;
        }

        try {
            final boolean isCancelled = progressHandle.scheduledFuture.cancel(true);
            if (!isCancelled) {
                LOG.error("Could not cancel the scheduled future");
            }
            progressHandle.scheduledFuture.get();
        } catch (CancellationException ex) {
            // Ignore the cancellation exception as it was just cancelled above.
        } catch (InterruptedException | ExecutionException ex) {
            LOG.error("Exception encountered during unregistering progress reporter", ex);
        }
    }

    public static class ProgressHandle {
        private final ScheduledFuture<?> scheduledFuture;

        public ProgressHandle(ScheduledFuture<?> scheduledFuture) {
            this.scheduledFuture = scheduledFuture;
        }
    }
}
