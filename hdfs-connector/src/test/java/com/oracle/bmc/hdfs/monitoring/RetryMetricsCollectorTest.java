package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.retrier.RetryCondition;
import com.oracle.bmc.retrier.RetryConfiguration;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
public class RetryMetricsCollectorTest {

    @Test
    public void countsAttemptsAndStatusCodesViaRetryConfigCtor() {
        RetryConfiguration cfg = RetryConfiguration.builder().build();

        RetryMetricsCollector s = new RetryMetricsCollector(cfg, "PUT") {
            @Override protected void recordException(BmcException ex, boolean shouldBeRetried) {
                super.recordException(ex, shouldBeRetried);
            }
        };

        s.recordException(new BmcException(503, "svc", "msg", null), true);
        s.recordException(new BmcException(429, "rl", "msg", null), true);
        s.recordException(new BmcException(400, "bad", "msg", null), false);

        assertEquals(2, s.getAttemptCount());
        assertEquals(1, s.getRetry503Count());
        assertEquals(1, s.getRetry429Count());
        assertTrue(s.getRetriedStatusCodes().contains(503));
        assertTrue(s.getRetriedStatusCodes().contains(429));
        assertFalse(s.getRetriedStatusCodes().contains(400));
        assertNotNull(s.getRetryConfiguration());
        s.close();
    }

    @Test
    public void countsAttemptsConstructorWithOperation() {
        RetryMetricsCollector s = new RetryMetricsCollector("READ", 60L, 10L) {
            @Override protected void recordException(BmcException ex, boolean shouldBeRetried) {
                super.recordException(ex, shouldBeRetried);
            }
        };
        s.recordException(new BmcException(500, "e", "m", null), true);
        assertEquals(1, s.getAttemptCount());
        s.close();
    }


    @Test
    public void delegatesAndIntercepts() {
        RetryCondition delegate = mock(RetryCondition.class);
        when(delegate.shouldBeRetried(any(BmcException.class))).thenReturn(true);

        final boolean[] intercepted = {false};
        DelegatingRetryCondition cond = new DelegatingRetryCondition(delegate) {
            @Override public void intercept(BmcException ex, boolean shouldBeRetried) {
                intercepted[0] = shouldBeRetried;
                assertEquals(429, ex.getStatusCode());
            }
        };

        BmcException ex = new BmcException(429, "TooMany", "msg", null);
        boolean res = cond.shouldBeRetried(ex);

        assertTrue(res);
        assertTrue(intercepted[0]);
        verify(delegate, times(1)).shouldBeRetried(ex);
    }

    @Test
    public void testRetryMetricsCollectorCountsRetriesCorrectly() {
        RetryMetricsCollector collector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.WRITE);

        BmcException ex503 = new BmcException(503, "Service Unavailable", null, null);
        BmcException ex429 = new BmcException(429, "Too Many Requests", null, null);
        BmcException ex404 = new BmcException(404, "Not Found", null, null);

        collector.recordException(ex503, true);
        collector.recordException(ex429, true);
        collector.recordException(ex404, true);
        collector.recordException(ex404, false); // Should NOT increment counters (no retry)

        assertEquals(3, collector.getAttemptCount());
        assertEquals(1, collector.getRetry503Count());
        assertEquals(1, collector.getRetry429Count());

        List<Integer> retriedStatusCodes = collector.getRetriedStatusCodes();
        assertEquals(3, retriedStatusCodes.size());
        assertTrue(retriedStatusCodes.contains(503));
        assertTrue(retriedStatusCodes.contains(429));
        assertTrue(retriedStatusCodes.contains(404));
    }

    @Test
    public void testCloseLogsSummaryWithoutException() {
        RetryMetricsCollector collector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.READ);
        // Should not throw
        collector.close();
    }

    @Test
    public void testMultipleCloseCalls() {
        RetryMetricsCollector collector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.WRITE);
        collector.close();
        collector.close();
        assertEquals(0, collector.getAttemptCount());
    }

    @Test
    public void testNoRetriesRecorded() {
        RetryMetricsCollector collector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.DELETE);
        assertEquals(0, collector.getAttemptCount());
        assertEquals(0, collector.getRetry503Count());
        assertEquals(0, collector.getRetry429Count());
        assertTrue(collector.getRetriedStatusCodes().isEmpty());
    }

    @Test
    public void testRetryOnlySpecificStatusCodesCounted() {
        RetryMetricsCollector collector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.HEAD);

        BmcException ex503 = new BmcException(503, "Service Unavailable", null, null);
        BmcException ex429 = new BmcException(429, "Too Many Requests", null, null);
        BmcException ex502 = new BmcException(502, "Bad Gateway", null, null);

        collector.recordException(ex503, true);
        collector.recordException(ex429, true);
        collector.recordException(ex502, true);

        assertEquals(3, collector.getAttemptCount());
        assertEquals(1, collector.getRetry503Count());
        assertEquals(1, collector.getRetry429Count());

        // 502 should not increment retry503Count or retry429Count
        assertEquals(3, collector.getRetriedStatusCodes().size());
    }
}