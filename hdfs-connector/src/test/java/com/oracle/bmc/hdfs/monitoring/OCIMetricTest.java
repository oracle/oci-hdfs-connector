package com.oracle.bmc.hdfs.monitoring;

import org.junit.Test;
import static org.junit.Assert.*;


public class OCIMetricTest {

    @Test
    public void buildsAndReportsFields() {
        OCIMetric ok = new OCIMetric(12.3, "READ:key", null, "bucketA", 2, 1, 0);
        assertEquals("READ:key", ok.getKey());
        assertEquals(12.3, ok.getOverallTime(), 0.0001);
        assertEquals("bucketA", ok.getBucketName());
        assertEquals(2, ok.getRetryAttempts());
        assertEquals(1, ok.getRetry503Count());
        assertEquals(0, ok.getRetry429Count());
        assertFalse(ok.isError());
        assertTrue(ok.getRecordedTime() > 0L);
        assertEquals(0, ok.getErrorStatusCode());
        assertNotNull(ok.toString());

        Exception e = new RuntimeException("boom");
        OCIMetric err = new OCIMetric(1.0, "WRITE:key", e, "bucketB", 0, 0, 1, true);
        assertTrue(err.isError());
        assertTrue(err.getIsCoalesced());
        err.getErrorStatusCode(); // must not throw
        assertNotNull(err.toString());
    }

    @Test
    public void holdsThroughputAndBytes() {
        OCIMetricWithThroughput m = new OCIMetricWithThroughput(
                "READ", 10.0, 200.0, null, 1_000_000d, "bkt", 3, 0, 0);
        assertEquals("READ", m.getKey());
        assertEquals(10.0, m.getOverallTime(), 0.0001);
        assertEquals(200.0, m.getThroughput(), 0.0001);
        assertEquals(1_000_000d, m.getBytesTransferred(), 0.0001);
        assertEquals("bkt", m.getBucketName());
        assertNotNull(m.toString());
    }

    @Test
    public void exposesTtfb() {
        OCIMetricWithFBLatency m = new OCIMetricWithFBLatency(
                "WRITE", 7.5, 0.85, 150.0, null, 512_000d, "bkt", 1, 0, 1);
        assertEquals("WRITE", m.getKey());
        assertEquals(7.5, m.getOverallTime(), 0.0001);
        assertEquals(0.85, m.getTtfb(), 0.0001);
        assertEquals(150.0, m.getThroughput(), 0.0001);
        assertEquals(512_000d, m.getBytesTransferred(), 0.0001);
        assertEquals("bkt", m.getBucketName());
        assertNotNull(m.toString());
    }

    @Test
    public void constantsPresent() {
        assertNotNull(OCIMetricKeys.READ);
        assertNotNull(OCIMetricKeys.WRITE);
        assertNotNull(OCIMetricKeys.HEAD);
        assertNotNull(OCIMetricKeys.LIST);
        assertNotNull(OCIMetricKeys.RENAME);
        assertNotNull(OCIMetricKeys.DELETE);
        assertNotNull(OCIMetricKeys.WRITE_PART);
        assertNotNull(OCIMetricKeys.WRITE_CREATE_MULTIPART);
        assertNotNull(OCIMetricKeys.WRITE_COMMIT_MULTIPART);
        assertNotNull(OCIMetricKeys.WRITE_ABORT_MULTIPART);
    }
}
