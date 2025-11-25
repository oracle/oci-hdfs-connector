package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Enriched tests for OCIMonitorPluginHandler.
 * Uses PowerMockRunner only for classloader consistency across the monitoring package.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({
        "javax.net.ssl.*",
        "javax.management.*",
        "org.mockito.*",
        "org.slf4j.*",
        "org.apache.*"
})
public class OCIMonitorPluginHandlerTest {

    /** Tiny polling helper to wait for async fan-out without flakiness. */
    private static void waitUntil(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long until = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < until) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(10);
        }
    }
    private interface BooleanSupplier { boolean getAsBoolean(); }

    /**
     * Build a handler with N SpyPlugins attached and enabled.
     * Returns the handler; fills 'spiesOut' with the SpyPlugin instances for assertions.
     */
    private static OCIMonitorPluginHandler newHandlerWithSpies(
            String bucketName, int pluginCount, List<SpyPlugin> spiesOut) {

        OCIMonitorPluginHandler handler = new OCIMonitorPluginHandler();
        handler.setEnabled(true);
        handler.setBucketName(bucketName);
        handler.setMaxBacklogBeforeDrop(10_000); // high to avoid drops in tests

        List<OCIMonitorConsumerPlugin> pluginList = new ArrayList<OCIMonitorConsumerPlugin>(pluginCount);
        List<SpyPlugin> spies = new ArrayList<SpyPlugin>(pluginCount);
        for (int i = 0; i < pluginCount; i++) {
            SpyPlugin sp = new SpyPlugin(mock(BmcPropertyAccessor.class), bucketName, "group", "ns");
            spies.add(sp);
            pluginList.add(sp); // upcast to OCIMonitorConsumerPlugin
        }

        handler.setListOfPlugins(pluginList);
        handler.init();

        spiesOut.clear();
        spiesOut.addAll(spies);
        return handler;
    }

    @Test
    public void initAndRecordStatsAllOverloadsFanOutToMultiplePlugins() throws Exception {
        List<SpyPlugin> spies = new ArrayList<SpyPlugin>();
        OCIMonitorPluginHandler handler = newHandlerWithSpies("bucketA", 2, spies);
        assertEquals(2, spies.size());
        final SpyPlugin p1 = spies.get(0);
        final SpyPlugin p2 = spies.get(1);

        // Overload 1: plain metric
        handler.recordStats("READ:keyA", 10.0, null, 2, 1, 0);
        // Overload 2: throughput metric
        handler.recordStats("WRITE:keyB", 15.0, 120.0, null, 2048.0, 3, 1, 1);
        // Overload 3: TTFB + throughput metric
        handler.recordStats("LIST:keyC", 5.0, 0.5, 300.0, null, 4096.0, 0, 0, 0);

        waitUntil(new BooleanSupplier() {
            public boolean getAsBoolean() { return p1.accepted.size() >= 3 && p2.accepted.size() >= 3; }
        }, TimeUnit.SECONDS.toMillis(2));

        assertTrue("p1 should have >=3 metrics", p1.accepted.size() >= 3);
        assertTrue("p2 should have >=3 metrics", p2.accepted.size() >= 3);

        boolean sawRead = false, sawWrite = false, sawList = false;
        for (OCIMetric m : p1.accepted) {
            assertEquals("bucketA", m.getBucketName());
            if (m.getKey().startsWith("READ:"))  sawRead = true;
            if (m.getKey().startsWith("WRITE:")) sawWrite = true;
            if (m.getKey().startsWith("LIST:"))  sawList = true;
        }
        assertTrue(sawRead && sawWrite && sawList);
    }

    @Test
    public void disabledDoesNotEmitUntilEnabled() throws Exception {
        SpyPlugin plugin = new SpyPlugin(mock(BmcPropertyAccessor.class), "bucketA", "group", "ns");
        OCIMonitorPluginHandler handler = new OCIMonitorPluginHandler();
        handler.setEnabled(false); // start disabled
        handler.setBucketName("bucketA");
        handler.setListOfPlugins(Collections.<OCIMonitorConsumerPlugin>singletonList(plugin));
        handler.setMaxBacklogBeforeDrop(10_000);
        handler.init();

        handler.recordStats("READ:a", 1.0, null, 0, 0, 0);
        handler.recordStats("WRITE:b", 2.0, 128.0, null, 256.0, 0, 0, 0);

        Thread.sleep(100);
        assertEquals(0, plugin.accepted.size());

        handler.setEnabled(true);
        handler.recordStats("LIST:c", 3.0, 0.1, null, 10.0, 0, 0, 0);

        waitUntil(new BooleanSupplier() {
            public boolean getAsBoolean() { return plugin.accepted.size() >= 1; }
        }, TimeUnit.SECONDS.toMillis(2));

        assertTrue("expected at least 1 metric after enabling", plugin.accepted.size() >= 1);
    }

    @Test
    public void bucketNameChangeIsAppliedToSubsequentMetrics() throws Exception {
        SpyPlugin plugin = new SpyPlugin(mock(BmcPropertyAccessor.class), "bucketA", "group", "ns");
        OCIMonitorPluginHandler handler = new OCIMonitorPluginHandler();
        handler.setEnabled(true);
        handler.setBucketName("bucketA");
        handler.setListOfPlugins(Collections.<OCIMonitorConsumerPlugin>singletonList(plugin));
        handler.setMaxBacklogBeforeDrop(10_000);
        handler.init();

        handler.recordStats("HEAD:old", 0.5, null, 0, 0, 0);

        handler.setBucketName("bucketB");
        handler.recordStats("DELETE:new", 0.6, null, 0, 0, 0);

        waitUntil(new BooleanSupplier() {
            public boolean getAsBoolean() { return plugin.accepted.size() >= 2; }
        }, TimeUnit.SECONDS.toMillis(2));

        boolean sawA = false, sawB = false;
        for (OCIMetric m : plugin.accepted) {
            if ("bucketA".equals(m.getBucketName())) sawA = true;
            if ("bucketB".equals(m.getBucketName())) sawB = true;
        }
        assertTrue("should see metrics for both buckets A and B", sawA && sawB);
    }

    @Test
    public void errorMetricsAndRetryCountersArePropagated() throws Exception {
        SpyPlugin plugin = new SpyPlugin(mock(BmcPropertyAccessor.class), "bucketZ", "group", "ns");
        OCIMonitorPluginHandler handler = new OCIMonitorPluginHandler();
        handler.setEnabled(true);
        handler.setBucketName("bucketZ");
        handler.setListOfPlugins(Collections.<OCIMonitorConsumerPlugin>singletonList(plugin));
        handler.setMaxBacklogBeforeDrop(10_000);
        handler.init();

        Exception err = new RuntimeException("boom");
        handler.recordStats("WRITE:err503", 2.0, err, 4, 2, 0); // 503-like counters
        handler.recordStats("READ:err429", 1.0, err, 3, 0, 2);  // 429-like counters

        waitUntil(new BooleanSupplier() {
            public boolean getAsBoolean() { return plugin.accepted.size() >= 2; }
        }, TimeUnit.SECONDS.toMillis(2));

        boolean saw503Counters = false, saw429Counters = false;
        for (OCIMetric m : plugin.accepted) {
            assertTrue(m.isError());
            if (m.getKey().startsWith("WRITE:err503")) {
                assertEquals(4, m.getRetryAttempts());
                assertEquals(2, m.getRetry503Count());
                assertEquals(0, m.getRetry429Count());
                saw503Counters = true;
            }
            if (m.getKey().startsWith("READ:err429")) {
                assertEquals(3, m.getRetryAttempts());
                assertEquals(0, m.getRetry503Count());
                assertEquals(2, m.getRetry429Count());
                saw429Counters = true;
            }
        }
        assertTrue(saw503Counters && saw429Counters);
    }

    @Test
    public void singlePluginReceivesAllThreeMetricFlavors() throws Exception {
        SpyPlugin plugin = new SpyPlugin(mock(BmcPropertyAccessor.class), "bucketC", "group", "ns");
        OCIMonitorPluginHandler handler = new OCIMonitorPluginHandler();
        handler.setEnabled(true);
        handler.setBucketName("bucketC");
        handler.setListOfPlugins(Collections.<OCIMonitorConsumerPlugin>singletonList(plugin));
        handler.setMaxBacklogBeforeDrop(10_000);
        handler.init();

        handler.recordStats("READ:plain", 2.5, null, 1, 0, 0);                        // plain
        handler.recordStats("WRITE:tput", 3.5, 256.0, null, 8192.0, 0, 0, 0);         // throughput
        handler.recordStats("LIST:ttfb", 1.2, 0.2, 128.0, null, 1024.0, 0, 0, 0);     // ttfb + throughput

        waitUntil(new BooleanSupplier() {
            public boolean getAsBoolean() { return plugin.accepted.size() >= 3; }
        }, TimeUnit.SECONDS.toMillis(2));

        boolean hasPlain = false, hasTput = false, hasTtfb = false;
        for (OCIMetric m : plugin.accepted) {
            if (m instanceof OCIMetricWithFBLatency) {
                hasTtfb = true;
            } else if (m instanceof OCIMetricWithThroughput) {
                hasTput = true;
            } else {
                hasPlain = true;
            }
        }
        assertTrue("expected plain metric",  hasPlain);
        assertTrue("expected throughput metric", hasTput);
        assertTrue("expected ttfb metric",  hasTtfb);
    }
}
