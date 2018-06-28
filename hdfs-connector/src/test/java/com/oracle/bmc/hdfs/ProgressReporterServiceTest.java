package com.oracle.bmc.hdfs;

import com.oracle.bmc.hdfs.util.ProgressReporterService;
import org.apache.hadoop.util.Progressable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

public class ProgressReporterServiceTest {
    private static final int CORE_POOL_SIZE = 5;
    private static final ProgressReporterService PROGRESS_REPORTER_SERVICE =
            new ProgressReporterService(CORE_POOL_SIZE);
    private static final int NOTIFICATION_INTERVAL_IN_SECONDS = 2;
    private static final int RUN_DURATION_IN_MILLISECONDS = 5100;

    @Test
    public void testRegisterNullProgressable() {
        final ProgressReporterService.ProgressHandle progressHandle =
                PROGRESS_REPORTER_SERVICE.register(null, NOTIFICATION_INTERVAL_IN_SECONDS);
        Assert.assertNotNull(progressHandle);
    }

    @Test
    public void testUnregisterNullScheduledFuture() {
        final ProgressReporterService.ProgressHandle progressHandle =
                new ProgressReporterService.ProgressHandle(null);
        PROGRESS_REPORTER_SERVICE.unregister(progressHandle);
    }

    @Test
    public void testPeriodicCallbacks() throws InterruptedException {
        final Progressable progressable = Mockito.mock(Progressable.class);

        final ProgressReporterService.ProgressHandle progressHandle =
                PROGRESS_REPORTER_SERVICE.register(progressable, NOTIFICATION_INTERVAL_IN_SECONDS);
        Thread.sleep(RUN_DURATION_IN_MILLISECONDS);
        PROGRESS_REPORTER_SERVICE.unregister(progressHandle);

        final int expectedNumberOfProgressUpdates =
                1 + RUN_DURATION_IN_MILLISECONDS / (NOTIFICATION_INTERVAL_IN_SECONDS * 1000);
        Mockito.verify(progressable, Mockito.times(expectedNumberOfProgressUpdates)).progress();
    }

    @Test
    public void testFailingScheduledFuture() throws InterruptedException, ExecutionException {
        final ScheduledFuture<?> scheduledFuture = Mockito.mock(ScheduledFuture.class);
        Mockito.when(scheduledFuture.cancel(Mockito.anyBoolean())).thenReturn(true);
        Mockito.when(scheduledFuture.get()).thenThrow(new InterruptedException());

        PROGRESS_REPORTER_SERVICE.unregister(new ProgressReporterService.ProgressHandle(scheduledFuture));
    }

    @Test
    public void testMultipleProgressReporters() throws InterruptedException {
        final int numberOfProgressReporters = 90000;
        final Progressable progressable = Mockito.mock(Progressable.class);

        final List<ProgressReporterService.ProgressHandle> progressHandles = new ArrayList<>();
        for (int i = 0; i < numberOfProgressReporters; i++) {
            progressHandles.add(PROGRESS_REPORTER_SERVICE.register(progressable, NOTIFICATION_INTERVAL_IN_SECONDS));
        }
        Thread.sleep(RUN_DURATION_IN_MILLISECONDS);
        for (int i = 0; i < numberOfProgressReporters; i++) {
            PROGRESS_REPORTER_SERVICE.unregister(progressHandles.get(i));
        }

        final int expectedNumberOfProgressUpdates =
                numberOfProgressReporters * (1 + RUN_DURATION_IN_MILLISECONDS / (NOTIFICATION_INTERVAL_IN_SECONDS * 1000));

        Mockito.verify(progressable, Mockito.times(expectedNumberOfProgressUpdates)).progress();
    }

    @Test
    public void testNoNotificationsAfterUnregister() throws InterruptedException {
        final Progressable progressable = Mockito.mock(Progressable.class);

        final ProgressReporterService.ProgressHandle progressHandle =
                PROGRESS_REPORTER_SERVICE.register(progressable, NOTIFICATION_INTERVAL_IN_SECONDS);
        Thread.sleep(50);
        PROGRESS_REPORTER_SERVICE.unregister(progressHandle);

        Thread.sleep(RUN_DURATION_IN_MILLISECONDS);

        Mockito.verify(progressable, Mockito.times(1)).progress();
    }
}
