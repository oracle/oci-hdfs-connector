package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.monitoring.MonitoringClient;
import com.oracle.bmc.monitoring.requests.PostMetricDataRequest;
import com.oracle.bmc.monitoring.responses.PostMetricDataResponse;
import com.oracle.bmc.monitoring.model.MetricDataDetails;
import com.oracle.bmc.monitoring.model.PostMetricDataDetails;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ MonitoringClient.class })
@SuppressStaticInitializationFor({ "com.oracle.bmc.monitoring.MonitoringClient" })
@PowerMockIgnore({
        "javax.net.ssl.*", "javax.management.*",
        "org.slf4j.*", "org.apache.log4j.*", "org.apache.commons.logging.*",
        "org.mockito.*"
})
public class OCIMonitorPluginTest {

    private static void disableBackground(OCIMonitorPlugin plugin) throws Exception {
        java.lang.reflect.Field f = OCIMonitorPlugin.class.getDeclaredField("handOffES");
        f.setAccessible(true);
        java.util.concurrent.ScheduledExecutorService es =
                (java.util.concurrent.ScheduledExecutorService) f.get(plugin);
        if (es != null) es.shutdownNow();
    }

    private OCIMonitorPlugin newPlugin(MonitoringClient mc, boolean bucketLevelStats) throws Exception {
        OCIMonitorPlugin p = new OCIMonitorPlugin(
                "https://telemetry-ingestion.unit-test",
                "ocid1.compartment.oc1..unit",
                "cluster-xyz", "rg-hdfs", "oci_hdfs_connector",
                bucketLevelStats,
                1_000_000,                 // effectively never fires in tests
                1000L,
                mc, "bucketA",
                PowerMockito.mock(BmcPropertyAccessor.class),
                "ossNamespace"
        );
        disableBackground(p); // belt & suspenders
        return p;
    }

    private static void runOnce(OCIMonitorPlugin plugin) throws Exception {
        java.lang.reflect.Method m = OCIMonitorPlugin.class.getDeclaredMethod("doOneRun");
        m.setAccessible(true);
        m.invoke(plugin);
    }

    @Test
    public void doOneRunEnqueuesAndPostsAllMetricTypes() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);
        verify(mc, times(1)).setEndpoint("https://telemetry-ingestion.unit-test");

        // enqueue 3 → expect 3 posts (implementation posts per metric)
        plugin.accept(new OCIMetric(2.0, "READ:foo", null, "bucketA", 0, 0, 0));
        plugin.accept(new OCIMetricWithThroughput("WRITE:bar", 3.0, 128.0, null, 4096.0, "bucketA", 1, 0, 0));
        plugin.accept(new OCIMetricWithFBLatency("LIST:baz", 1.0, 0.2, 256.0, null, 2048.0, "bucketA", 0, 0, 0));

        runOnce(plugin);

        ArgumentCaptor<PostMetricDataRequest> cap = ArgumentCaptor.forClass(PostMetricDataRequest.class);
        verify(mc, times(3)).postMetricData(cap.capture());
        assertEquals(3, cap.getAllValues().size());

        plugin.shutdown();
    }

    @Test
    public void doOneRunEmptyQueueNoPost() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        runOnce(plugin);

        verify(mc, Mockito.times(0)).postMetricData(any(PostMetricDataRequest.class));
        plugin.shutdown();
    }

    @Test
    public void doOneRunHandlesPostExceptionAndContinues() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenThrow(new RuntimeException("boom"));

        OCIMonitorPlugin plugin = newPlugin(mc, false);
        plugin.accept(new OCIMetric(1.0, "DELETE:qux", null, "bucketA", 0, 0, 0));

        runOnce(plugin);

        verify(mc, times(1)).postMetricData(any(PostMetricDataRequest.class));
        plugin.shutdown();
    }

    @Test
    public void doOneRunProcessesRetryStatusCodes503And429() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        // enqueue 3 (two errors + one success) → expect 3 posts
        BmcException ex503 = new BmcException(503, "ServiceUnavailable", "msg", null);
        BmcException ex429 = new BmcException(429, "TooManyRequests", "msg", null);
        plugin.accept(new OCIMetric(0.50, "READ:err503", ex503, "bucketA", 2, 1, 0, false));
        plugin.accept(new OCIMetric(0.75, "WRITE:err429", ex429, "bucketA", 3, 0, 2, false));
        plugin.accept(new OCIMetric(1.25, "LIST:ok", null, "bucketA", 0, 0, 0, false));

        runOnce(plugin);

        verify(mc, times(3)).postMetricData(any(PostMetricDataRequest.class));
        plugin.shutdown();
    }

    @Test
    public void doOneRunProcessesCoalescedAndNonCoalescedMetrics() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        // enqueue 3 (two coalesced errors + one success) → expect 3 posts
        BmcException ex503 = new BmcException(503, "ServiceUnavailable", "msg", null);
        BmcException ex429 = new BmcException(429, "TooManyRequests", "msg", null);
        plugin.accept(new OCIMetric(0.40, "HEAD:coal503", ex503, "bucketA", 4, 2, 0, true));
        plugin.accept(new OCIMetric(0.30, "RENAME:coal429", ex429, "bucketA", 5, 0, 3, true));
        plugin.accept(new OCIMetric(0.10, "DELETE:single", null, "bucketA", 0, 0, 0, false));

        runOnce(plugin);

        verify(mc, times(3)).postMetricData(any(PostMetricDataRequest.class));
        plugin.shutdown();
    }

    @Test
    public void doOneRunProcessesMixedErrorAndThroughputVariants() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, false);

        // enqueue 3 (FB-latency error + throughput success + plain success) → expect 3 posts
        BmcException ex429 = new BmcException(429, "TooManyRequests", "msg", null);
        plugin.accept(new OCIMetricWithFBLatency("READ:mix429", 0.90, 0.15, 256.0, ex429, 1024.0, "bucketA", 1, 0, 1));
        plugin.accept(new OCIMetricWithThroughput("WRITE:okTput", 0.70, 512.0, null, 8192.0, "bucketA", 0, 0, 0));
        plugin.accept(new OCIMetric(0.20, "LIST:plain", null, "bucketA", 0, 0, 0));

        runOnce(plugin);

        verify(mc, times(3)).postMetricData(any(PostMetricDataRequest.class));
        plugin.shutdown();
    }

    private Optional<Double> findMetricValue(List<PostMetricDataRequest> requests, String metricNameSuffix) {
        for (PostMetricDataRequest req : requests) {
            PostMetricDataDetails details = req.getPostMetricDataDetails();
            if (details != null && details.getMetricData() != null) {
                for (MetricDataDetails md : details.getMetricData()) {
                    if (md.getName() != null && md.getName().endsWith(metricNameSuffix)) {
                        if (md.getDatapoints() != null && !md.getDatapoints().isEmpty()) {
                            return Optional.of(md.getDatapoints().get(0).getValue());
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    @Test
    public void testWeightedThroughput_SingleOperation() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        // Single operation: 10,000 bytes in 1000ms
        double bytesTransferred = 10000.0;
        double overallTimeMs = 1000.0;
        double individualThroughput = bytesTransferred / (overallTimeMs / 1000.0);

        plugin.accept(new OCIMetricWithThroughput(
                "WRITE", overallTimeMs, individualThroughput, null, bytesTransferred, "bucketA", 0, 0, 0));

        runOnce(plugin);

        ArgumentCaptor<PostMetricDataRequest> captor = ArgumentCaptor.forClass(PostMetricDataRequest.class);
        verify(mc, times(1)).postMetricData(captor.capture());

        Optional<Double> throughputValue = findMetricValue(captor.getAllValues(), "_THROUGHPUT");
        assertTrue("THROUGHPUT metric should be present", throughputValue.isPresent());

        // Expected: 10,000 bytes / 1 second = 10,000 bytes/sec
        double expectedThroughput = 10000.0;
        assertEquals("Single operation throughput should be bytes/time",
                expectedThroughput, throughputValue.get(), 0.01);

        plugin.shutdown();
    }

    @Test
    public void testWeightedThroughput_SameTimeDifferentBytes() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        // Operation A: 100,000 bytes in 1000ms
        double bytesA = 100000.0;
        double timeA = 1000.0;
        double throughputA = bytesA / (timeA / 1000.0);

        // Operation B: 10,000 bytes in 1000ms
        double bytesB = 10000.0;
        double timeB = 1000.0;
        double throughputB = bytesB / (timeB / 1000.0);

        plugin.accept(new OCIMetricWithThroughput("WRITE", timeA, throughputA, null, bytesA, "bucketA", 0, 0, 0));
        plugin.accept(new OCIMetricWithThroughput("WRITE", timeB, throughputB, null, bytesB, "bucketA", 0, 0, 0));

        runOnce(plugin);

        ArgumentCaptor<PostMetricDataRequest> captor = ArgumentCaptor.forClass(PostMetricDataRequest.class);
        verify(mc, times(1)).postMetricData(captor.capture());

        Optional<Double> throughputValue = findMetricValue(captor.getAllValues(), "_THROUGHPUT");
        assertTrue("THROUGHPUT metric should be present", throughputValue.isPresent());

        double expectedThroughput = (bytesA + bytesB) / ((timeA + timeB) / 1000.0);
        assertEquals("Throughput should be total bytes / total time",
                expectedThroughput, throughputValue.get(), 0.01);

        plugin.shutdown();
    }


    @Test
    public void testWeightedThroughput_DifferentTimes() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        double bytesA = 1000000.0;
        double timeA = 10000.0;
        double throughputA = bytesA / (timeA / 1000.0);

        double bytesB = 10000.0;
        double timeB = 100000.0;
        double throughputB = bytesB / (timeB / 1000.0);

        plugin.accept(new OCIMetricWithThroughput("WRITE", timeA, throughputA, null, bytesA, "bucketA", 0, 0, 0));
        plugin.accept(new OCIMetricWithThroughput("WRITE", timeB, throughputB, null, bytesB, "bucketA", 0, 0, 0));

        runOnce(plugin);

        ArgumentCaptor<PostMetricDataRequest> captor = ArgumentCaptor.forClass(PostMetricDataRequest.class);
        verify(mc, times(1)).postMetricData(captor.capture());

        Optional<Double> throughputValue = findMetricValue(captor.getAllValues(), "_THROUGHPUT");
        assertTrue("THROUGHPUT metric should be present", throughputValue.isPresent());

        double simpleAverage = (throughputA + throughputB) / 2;
        double weightedAverage = (bytesA + bytesB) / ((timeA + timeB) / 1000.0);

        assertEquals("Throughput MUST be weighted average (totalBytes/totalTime)",
                weightedAverage, throughputValue.get(), 0.01);

        assertNotEquals("Throughput must NOT be simple average",
                simpleAverage, throughputValue.get(), 1000.0);

        plugin.shutdown();
    }

    @Test
    public void testWeightedThroughput_ReadWithTTFB() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        // READ operation A: 500,000 bytes in 5000ms, TTFB = 100ms
        double bytesA = 500000.0;
        double timeA = 5000.0;
        double ttfbA = 100.0;
        double throughputA = bytesA / (timeA / 1000.0);

        // READ operation B: 100,000 bytes in 20000ms, TTFB = 500ms
        double bytesB = 100000.0;
        double timeB = 20000.0;
        double ttfbB = 500.0;
        double throughputB = bytesB / (timeB / 1000.0);

        plugin.accept(new OCIMetricWithFBLatency("READ", timeA, ttfbA, throughputA, null, bytesA, "bucketA", 0, 0, 0));
        plugin.accept(new OCIMetricWithFBLatency("READ", timeB, ttfbB, throughputB, null, bytesB, "bucketA", 0, 0, 0));

        runOnce(plugin);

        ArgumentCaptor<PostMetricDataRequest> captor = ArgumentCaptor.forClass(PostMetricDataRequest.class);
        verify(mc, times(1)).postMetricData(captor.capture());

        Optional<Double> throughputValue = findMetricValue(captor.getAllValues(), "_THROUGHPUT");
        assertTrue("THROUGHPUT metric should be present", throughputValue.isPresent());

        double expectedThroughput = (bytesA + bytesB) / ((timeA + timeB) / 1000.0);
        assertEquals("READ throughput should also use weighted average",
                expectedThroughput, throughputValue.get(), 0.01);

        plugin.shutdown();
    }

    @Test
    public void testWeightedThroughput_ExcludesErrors() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        double bytesSuccess = 50000.0;
        double timeSuccess = 5000.0;
        double throughputSuccess = bytesSuccess / (timeSuccess / 1000.0);

        BmcException error = new BmcException(500, "InternalError", "msg", null);
        double bytesError = 1000000.0;
        double timeError = 1000.0;
        double throughputError = bytesError / (timeError / 1000.0);

        plugin.accept(new OCIMetricWithThroughput("WRITE", timeSuccess, throughputSuccess, null, bytesSuccess, "bucketA", 0, 0, 0));
        plugin.accept(new OCIMetricWithThroughput("WRITE", timeError, throughputError, error, bytesError, "bucketA", 0, 0, 0));

        runOnce(plugin);

        ArgumentCaptor<PostMetricDataRequest> captor = ArgumentCaptor.forClass(PostMetricDataRequest.class);
        verify(mc, times(2)).postMetricData(captor.capture());  // 2 posts: success group + error group

        Optional<Double> throughputValue = findMetricValue(captor.getAllValues(), "_THROUGHPUT");
        assertTrue("THROUGHPUT metric should be present", throughputValue.isPresent());

        double expectedThroughput = bytesSuccess / (timeSuccess / 1000.0);
        assertEquals("Throughput should only include successful operations",
                expectedThroughput, throughputValue.get(), 0.01);

        plugin.shutdown();
    }

    @Test
    public void testTotalBytesMetric_IsSum() throws Exception {
        MonitoringClient mc = PowerMockito.mock(MonitoringClient.class);
        PowerMockito.doNothing().when(mc).setEndpoint(anyString());
        PowerMockito.when(mc.postMetricData(any(PostMetricDataRequest.class)))
                .thenReturn(PostMetricDataResponse.builder().opcRequestId("ok").build());

        OCIMonitorPlugin plugin = newPlugin(mc, true);

        double bytesA = 100000.0;
        double bytesB = 50000.0;
        double bytesC = 25000.0;

        plugin.accept(new OCIMetricWithThroughput("WRITE", 1000.0, 100000.0, null, bytesA, "bucketA", 0, 0, 0));
        plugin.accept(new OCIMetricWithThroughput("WRITE", 1000.0, 50000.0, null, bytesB, "bucketA", 0, 0, 0));
        plugin.accept(new OCIMetricWithThroughput("WRITE", 1000.0, 25000.0, null, bytesC, "bucketA", 0, 0, 0));

        runOnce(plugin);

        ArgumentCaptor<PostMetricDataRequest> captor = ArgumentCaptor.forClass(PostMetricDataRequest.class);
        verify(mc, times(1)).postMetricData(captor.capture());

        Optional<Double> bytesValue = findMetricValue(captor.getAllValues(), "_BYTES");
        assertTrue("BYTES metric should be present", bytesValue.isPresent());

        double expectedTotalBytes = bytesA + bytesB + bytesC;
        assertEquals("BYTES metric should be total (sum), not average",
                expectedTotalBytes, bytesValue.get(), 0.01);

        plugin.shutdown();
    }
}
