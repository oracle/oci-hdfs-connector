package com.oracle.bmc.hdfs.monitoring;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OCIMonitorPluginHandler {

    private ThreadPoolExecutor fixedSizeES;

    private List<OCIMonitorConsumerPlugin> listOfPlugins;

    private String bucketName;

    private boolean enabled;

    private int maxBacklogBeforeDrop;

    private boolean awaitingShutdown = false;

    public void setListOfPlugins(List<OCIMonitorConsumerPlugin> listOfPlugins) {
        this.listOfPlugins = listOfPlugins;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setMaxBacklogBeforeDrop(int maxBacklogBeforeDrop) {
        this.maxBacklogBeforeDrop = maxBacklogBeforeDrop;
    }

    public void setAwaitingShutdown(boolean awaitingShutdown) {
        this.awaitingShutdown = awaitingShutdown;
    }

    public void recordStats(String key, double elapsedTime, Exception e) {

        if (!enabled) {
            return;
        }

        OCIMetric metric = new OCIMetric(elapsedTime, key, e, bucketName);
        handoff(metric);
    }

    public void recordStats(String key, double elapsedTime, double throughput, Exception e, double bytesTransferred) {

        if (!enabled) {
            return;
        }

        OCIMetricWithThroughput metric = new OCIMetricWithThroughput(key, elapsedTime, throughput, e,
                bytesTransferred, bucketName);
        handoff(metric);
    }

    public void recordStats(String key, double elapsedTime, double ttfb, double throughput, Exception e,
                            double bytesTransferred) {

        if (!enabled) {
            return;
        }

        OCIMetricWithFBLatency metric = new OCIMetricWithFBLatency(key, elapsedTime, ttfb, throughput, e,
                bytesTransferred, bucketName);
        handoff(metric);
    }

    private void handoff(OCIMetric metric) {
        if (!awaitingShutdown) {
            fixedSizeES.submit(() -> {
                for (OCIMonitorConsumerPlugin plugin : listOfPlugins) {
                    try {
                        plugin.accept(metric);
                    } catch (Exception e) {
                        LOG.error("Plugin {} resulted in an exception during accept metrics.", plugin, e);
                    }
                }
            });
        } else {
            LOG.warn("Awaiting shutdown hence not accepting any more metrics.");
        }
    }

    public void init() {

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(maxBacklogBeforeDrop);
        fixedSizeES = new ThreadPoolExecutor(1, 1, 0l, TimeUnit.SECONDS, queue,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("OCI-Mon-Plugin-Framework-Thread")
                        .build());

        Runtime.getRuntime().addShutdownHook(new Thread(
            () -> {
                try {
                    awaitingShutdown = true;

                    fixedSizeES.shutdown();
                    while (!fixedSizeES.isTerminated()) {
                        fixedSizeES.awaitTermination(500, TimeUnit.MILLISECONDS);
                    }

                    for (OCIMonitorConsumerPlugin plugin : listOfPlugins) {
                        plugin.shutdown();
                    }
                } catch (Exception e) {
                    LOG.error("Error encountered in shutdown hook of OCIMonitor", e);
                }
            }));
    }

    public ThreadPoolExecutor getExecutorService() {
        return fixedSizeES;
    }

    public List<OCIMonitorConsumerPlugin> getListOfPlugins() {
        return listOfPlugins;
    }
}
