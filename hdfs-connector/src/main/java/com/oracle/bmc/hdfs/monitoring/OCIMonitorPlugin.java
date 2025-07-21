package com.oracle.bmc.hdfs.monitoring;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import com.oracle.bmc.monitoring.MonitoringClient;
import com.oracle.bmc.monitoring.model.Datapoint;
import com.oracle.bmc.monitoring.model.MetricDataDetails;
import com.oracle.bmc.monitoring.model.PostMetricDataDetails;
import com.oracle.bmc.monitoring.requests.PostMetricDataRequest;
import com.oracle.bmc.monitoring.responses.PostMetricDataResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OCIMonitorPlugin extends OCIMonitorConsumerPlugin {
    /**
     * Thread-safe queue used to store incoming metrics.
     * Written by accept() (main thread) and drained by doOneRun() (background emitter thread).
     */
    private final Queue<OCIMetric> metricsCache = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService handOffES;
    private final MonitoringClient monClient;
    private final String namespaceName;
    private final String rgName;
    private final String uniqueID;
    private final String compartmentID;
    private final boolean bucketLevelStatsEnabled;
    private final long maxBacklogBeforeDrop;

    private String bucketName = null;

    private boolean awaitingShutdown = false;

    public OCIMonitorPlugin(String telemetryIngestionEndpoint, String monCompartmentOCID, String monGroupingClusterID,
                            String monRGName, String monNamespacename, boolean bucketLevelStatsEnabled,
                            int emitThreadIntervalSeconds, long maxBacklogBeforeDrop, MonitoringClient mc,
                            String bucketName, BmcPropertyAccessor propertyAccessor, String ossNamespaceName) {
        super(propertyAccessor, bucketName, monGroupingClusterID, ossNamespaceName);
        this.compartmentID = monCompartmentOCID;
        this.uniqueID = monGroupingClusterID;
        this.rgName = monRGName;
        this.namespaceName = monNamespacename;
        this.bucketLevelStatsEnabled = bucketLevelStatsEnabled;
        this.maxBacklogBeforeDrop = maxBacklogBeforeDrop;
        this.monClient = mc;
        this.bucketName = bucketName;

        if (monClient != null) {
            monClient.setEndpoint(telemetryIngestionEndpoint);
            handOffES = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("bmcs-oci-monitor-thread")
                    .build());
            handOffES.scheduleAtFixedRate(new HandoffHandler(), 0, emitThreadIntervalSeconds, TimeUnit.SECONDS);
        } else {
            handOffES = null;
        }
    }

    private void doOneRun() {
        int size = metricsCache.size();

        if (size < 1) {
            return;
        }

        Map<String, List<OCIMetric>> bunchedMetrics = new HashMap<>();

        for (int i = 0; i < size; i++) {
            OCIMetric m = metricsCache.poll();

            String bunchKey = null;

            // Group the errors together so that we also emit the errorCode as a dimension.
            if (m.isError()) {
                bunchKey = m.getKey() + "_" + bucketName + "_" + m.getErrorStatusCode();
            } else {
                bunchKey = m.getKey() + "_" + bucketName;
            }

            List<OCIMetric> metricsList = bunchedMetrics.get(bunchKey);
            if (metricsList == null) {
                metricsList = new ArrayList<>();
                bunchedMetrics.put(bunchKey, metricsList);
            }
            metricsList.add(m);
        }

        for (String key : bunchedMetrics.keySet()) {

            long lastRecordedTime = 0l;
            int totalCount = 0;
            int totalCoalescedCount = 0;
            int successCount = 0;
            double averageOverallTime = 0.0d;
            int errorCount = 0;
            double averageTTFB = 0.0d;
            double averageThroughput = 0.0d;
            double totalBytesTransferred = 0.0d;
            int retryAttemptCount = 0;
            int retry503Count = 0;
            int retry429Count = 0;
            int maxRetryCount = 0;

            List<OCIMetric> metricsList = bunchedMetrics.get(key);
            String actualKey = null;

            int bunchedErrorStatusCode = 0;

            for (OCIMetric om : metricsList) {
                totalCount++;
                actualKey = om.getKey();
                int retryCount = om.getRetryAttempts();
                maxRetryCount = Math.max(maxRetryCount, retryCount);

                if (!om.isError()) {
                    successCount++;
                    averageOverallTime += om.getOverallTime();

                    if (om instanceof OCIMetricWithFBLatency) {

                        double ttfb = ((OCIMetricWithFBLatency) om).getTtfb();
                        if (ttfb > 0) {
                            averageTTFB += ttfb;
                        }

                        averageThroughput += ((OCIMetricWithFBLatency) om).getThroughput();
                        totalBytesTransferred += ((OCIMetricWithFBLatency) om).getBytesTransferred();
                    } else if (om instanceof OCIMetricWithThroughput) {
                        averageThroughput += ((OCIMetricWithThroughput) om).getThroughput();
                        totalBytesTransferred += ((OCIMetricWithThroughput) om).getBytesTransferred();
                    }

                    if (retryCount > 0) {
                        retryAttemptCount += retryCount;
                    }

                    retry503Count += om.getRetry503Count();
                    retry429Count += om.getRetry429Count();

                } else {
                    bunchedErrorStatusCode = om.getErrorStatusCode();
                    errorCount++;
                }
                if (om.getIsCoalesced()) {
                    totalCoalescedCount++;
                }
                lastRecordedTime = om.getRecordedTime();
            }

            LOG.debug("Emitting metric group: key={}, totalCount={}, retryAttempts={}, avgRetry={}, maxRetry={}, errorStatusCode={}",
                    actualKey, totalCount, retryAttemptCount,
                    retryAttemptCount > 0 ? String.format("%.2f", (double) retryAttemptCount / totalCount) : "0",
                    maxRetryCount,
                    bunchedErrorStatusCode > 0 ? bunchedErrorStatusCode : "none");

            if (successCount > 0) {
                averageOverallTime = averageOverallTime / successCount;

                if (averageTTFB > 0) {
                    averageTTFB = averageTTFB / successCount;
                }

                if (averageThroughput > 0) {
                    averageThroughput = averageThroughput / successCount;
                }
            }

            List<MetricDataDetails> mdList = new ArrayList<>();
            if (totalCount > 0) {
                mdList.add(getMetricDataDetails(actualKey + "_COUNT", totalCount, lastRecordedTime));
                if (actualKey.equals("HEAD")) {
                    mdList.add(getMetricDataDetails(actualKey + "_COALESCED_COUNT", totalCoalescedCount, lastRecordedTime));
                }
                mdList.add(getMetricDataDetails(actualKey + "_OVERALL_LATENCY",
                        averageOverallTime, lastRecordedTime));

                if (errorCount > 0) {
                    mdList.add(getMetricDataDetails(actualKey + "_ERROR_COUNT", errorCount, lastRecordedTime,
                            bunchedErrorStatusCode));
                }

                if (averageTTFB > 0) {
                    mdList.add(getMetricDataDetails(actualKey + "_TTFB", averageTTFB, lastRecordedTime));
                }

                if (averageThroughput > 0) {
                    mdList.add(getMetricDataDetails(actualKey + "_THROUGHPUT", averageThroughput, lastRecordedTime));
                }

                if (totalBytesTransferred > 0) {
                    mdList.add(getMetricDataDetails(actualKey + "_BYTES", totalBytesTransferred, lastRecordedTime));
                }

                // A metric group is defined by a unique combination of:
                // - OCIMetricKey (e.g., HEAD, WRITE, DELETE, etc.)
                // - target bucket
                // - and (if applicable) error status code
                //
                // All metrics in the same group are aggregated together during one emit cycle,
                // which runs every N seconds (configurable via OCI_MON_EMIT_THREAD_POLL_INTERVAL_SECONDS, default: 2s).


                // Total number of retry attempts across all requests (both successful and failed) in this metric group.
                if (retryAttemptCount > 0) {
                    mdList.add(getMetricDataDetails(actualKey + "_RETRY_ATTEMPT_COUNT", retryAttemptCount, lastRecordedTime));
                }

                // Total number of times a 429 response triggered a retry in this metric group.
                if (retry429Count > 0) {
                    mdList.add(getMetricDataDetails(actualKey + "_RETRY_429_COUNT", retry429Count, lastRecordedTime));
                }

                // Total number of times a 503 response triggered a retry in this metric group.
                if (retry503Count > 0) {
                    mdList.add(getMetricDataDetails(actualKey + "_RETRY_503_COUNT", retry503Count, lastRecordedTime));
                }

                // Average retry attempts per request (total retry attempts ÷ total requests in this metric group).
                if (retryAttemptCount > 0) {
                    double averageRetries = (double) retryAttemptCount / totalCount;
                    mdList.add(getMetricDataDetails(actualKey + "_AVERAGE_RETRY_COUNT", averageRetries, lastRecordedTime));
                }

                // Highest retry count observed for a single request in this metric group.
                if (maxRetryCount > 0) {
                    mdList.add(getMetricDataDetails(actualKey + "_MAX_RETRY_COUNT", maxRetryCount, lastRecordedTime));
                }

                PostMetricDataDetails postMetricDataDetails = PostMetricDataDetails.builder()
                        .metricData(mdList).build();

                PostMetricDataRequest postMetricDataRequest = PostMetricDataRequest.builder()
                        .postMetricDataDetails(postMetricDataDetails)
                        .opcRequestId(UUID.randomUUID().toString())
                        .build();

                try {
                    PostMetricDataResponse response = monClient.postMetricData(postMetricDataRequest);
                    int statusCode = response.get__httpStatusCode__();
                    if (statusCode == 429 || statusCode == 500) {
                        LOG.warn("Received status code {} while emitting metrics", statusCode);
                    }
                } catch (Exception e) {
                    LOG.error("Unable to emit metrics: {}", e.toString());
                    if (LOG.isDebugEnabled()) {
                        LOG.error("Metrics emit failed! ", e);
                    }
                }
            }
        }
    }

    private MetricDataDetails getMetricDataDetails(String key, double val, long lastRecordedTime,
                                                   int bunchedErrorStatusCode) {
        return MetricDataDetails.builder()
                .namespace(namespaceName)
                .resourceGroup(rgName)
                .compartmentId(compartmentID)
                .name(key)
                .dimensions(new HashMap<java.lang.String, java.lang.String>() {
                    {
                        put("ID", uniqueID);
                        if (bucketLevelStatsEnabled) {
                            put("BUCKET", bucketName);
                        }

                        if (bunchedErrorStatusCode != 0) {
                            put("ERRCODE", String.valueOf(bunchedErrorStatusCode));
                        }
                    }
                }).datapoints(new ArrayList<>(Arrays.asList(Datapoint.builder()
                        .timestamp(new Date(lastRecordedTime))
                        .value(val)
                        .build()))).build();
    }

    private MetricDataDetails getMetricDataDetails(String key, double val, long lastRecordedTime) {
        return getMetricDataDetails(key, val, lastRecordedTime, 0);
    }

    @Override
    public void accept(OCIMetric ociMetric) {
        if (metricsCache.size() < maxBacklogBeforeDrop && !awaitingShutdown) {
            metricsCache.add(ociMetric);
            // Per request logging
            LOG.debug("Accepted OCIMetric: key={}, isError={}, retries={}, 503Retries={}, 429Retries={}, statusCode={}, time={}, latency={}ms",
                        ociMetric.getKey(),
                        ociMetric.isError(),
                        ociMetric.getRetryAttempts(),
                        ociMetric.getRetry503Count(),
                        ociMetric.getRetry429Count(),
                        ociMetric.getErrorStatusCode(),
                        new Date(ociMetric.getRecordedTime()),
                        ociMetric.getOverallTime()
                );
        } else {
            // Metrics are dropped when backlog exceeds limit.
            // Drop Condition: R × T ≥ maxBacklog
            //   R = metrics generated per second
            //   T = emit thread interval (BmcProperties.OCI_MON_EMIT_THREAD_POLL_INTERVAL_SECONDS)
            //   maxBacklog = backlog limit (BmcProperties.OCI_MON_MAX_BACKLOG_BEFORE_DROP)
            // If drops occur frequently, consider tuning T or maxBacklog to match workload characteristics.

            LOG.debug("Dropping metric: key={}, isError={}, retryAttempts={}, statusCode={}, time={}, cacheSize={}, shutdown={}",
                    ociMetric.getKey(),
                    ociMetric.isError(),
                    ociMetric.getRetryAttempts(),
                    ociMetric.getErrorStatusCode(),
                    new Date(ociMetric.getRecordedTime()),
                    metricsCache.size(),
                    awaitingShutdown);
        }
    }

    @Override
    public void shutdown() {
        try {
            awaitingShutdown = true;
            handOffES.shutdown();

            while (!handOffES.isTerminated()) {
                handOffES.awaitTermination(500, TimeUnit.MILLISECONDS);
            }

            // Do another run of emitting metrics right after this emit thread shutdown to clear off
            // remaining backlog.
            doOneRun();

        } catch (Exception e) {
            LOG.error("Error encountered in shutdown hook of OCIMonitor", e);
        }
    }

    private class HandoffHandler implements Runnable {
        public void run() {
            doOneRun();
        }
    }
}
