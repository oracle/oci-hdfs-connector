package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.retrier.RetryConfiguration;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class RetryMetricsCollectorTest {

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