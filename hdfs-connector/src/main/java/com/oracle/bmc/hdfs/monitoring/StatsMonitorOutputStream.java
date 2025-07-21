package com.oracle.bmc.hdfs.monitoring;

import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import com.oracle.bmc.hdfs.store.BmcMultipartOutputStream;
import com.oracle.bmc.hdfs.store.BmcOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsMonitorOutputStream extends OutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(StatsMonitorOutputStream.class);
    private long totalBytesWritten;
    private long totalElapsedTime;
    private OutputStream destinationStream;
    private final RetryMetricsCollector retryMetricsCollector;
    private boolean metricEmitted = false;
    private OCIMonitorPluginHandler ociMonitorPluginHandler;

    private Stopwatch sw = Stopwatch.createUnstarted();

    public StatsMonitorOutputStream(OutputStream destinationStream, OCIMonitorPluginHandler ociMonitorPluginHandler,
            RetryMetricsCollector retryMetricsCollector) {
        this.destinationStream = destinationStream;
        this.ociMonitorPluginHandler = ociMonitorPluginHandler;
        this.retryMetricsCollector = retryMetricsCollector;
    }

    private void triggerStopWatch() {
        if (!sw.isRunning()) {
            sw.start();
        }
    }

    private void recordStats(int count) {
        if (count > 0) {
            totalBytesWritten += count;
        }
    }

    @Override
    public void write(int b) throws IOException {
        triggerStopWatch();
        try {
            destinationStream.write(b);
            recordStats(1);
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    @Override
    public void write(byte b[]) throws IOException {
        triggerStopWatch();
        try {
            destinationStream.write(b);
            recordStats(b.length);
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        triggerStopWatch();
        try {
            destinationStream.write(b, off, len);
            recordStats(len);
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    @Override
    public void flush() throws IOException {
        triggerStopWatch();
        try {
            destinationStream.flush();
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        triggerStopWatch();
        try {
            destinationStream.close();
            if (sw.isRunning()) {
                sw.stop();
            }
            emitMetric(null);
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    private void emitMetric(Exception e) {
        if (ociMonitorPluginHandler.isEnabled() && !metricEmitted) {
            totalElapsedTime = sw.elapsed(TimeUnit.MILLISECONDS);

            double throughput = 0.0d;
            if (totalBytesWritten > 0 && totalElapsedTime > 0) {
                throughput = totalBytesWritten / (totalElapsedTime / 1000.0);
            }

            int retryAttempts = retryMetricsCollector != null ? retryMetricsCollector.getAttemptCount() : 0;
            boolean isMultipart = false;
            boolean isNewFlow = false;

            if (destinationStream instanceof BmcOutputStream) {
                BmcOutputStream bos = (BmcOutputStream) destinationStream;
                isMultipart = bos.isMultipart();
                isNewFlow = bos.isNewFlow();
            }

            int retry503Count = retryMetricsCollector != null ? retryMetricsCollector.getRetry503Count() : 0;
            int retry429Count = retryMetricsCollector != null ? retryMetricsCollector.getRetry429Count() : 0;

            if (!isMultipart) {
                ociMonitorPluginHandler.recordStats(
                        OCIMetricKeys.WRITE, totalElapsedTime, throughput, e, totalBytesWritten, retryAttempts, retry503Count, retry429Count);
                LOG.debug("Recorded base WRITE metric (non-multipart)");
            }

            // Case 1: New flow + multipart => emit all detailed write phases
            if (isNewFlow && isMultipart) {
                LOG.debug("Case 1: New flow + multipart => emit all detailed write phases");
                ociMonitorPluginHandler.recordStats(
                        OCIMetricKeys.WRITE_PART, totalElapsedTime, throughput, e, totalBytesWritten, retryAttempts, retry503Count, retry429Count);
                ociMonitorPluginHandler.recordStats(
                        OCIMetricKeys.WRITE_CREATE_MULTIPART, totalElapsedTime, throughput, e, totalBytesWritten, retryAttempts, retry503Count, retry429Count);
                ociMonitorPluginHandler.recordStats(
                        OCIMetricKeys.WRITE_COMMIT_MULTIPART, totalElapsedTime, throughput, e, totalBytesWritten, retryAttempts, retry503Count, retry429Count);
                ociMonitorPluginHandler.recordStats(
                        OCIMetricKeys.WRITE_ABORT_MULTIPART, totalElapsedTime, throughput, e, totalBytesWritten, retryAttempts, retry503Count, retry429Count);
            }

            // Case 2: Old flow + multipart => emit WRITE_PART only
            else if (!isNewFlow && isMultipart) {
                LOG.debug("Case 2: Old flow + multipart => emit WRITE_PART only");
                ociMonitorPluginHandler.recordStats(
                        OCIMetricKeys.WRITE_PART, totalElapsedTime, throughput, e, totalBytesWritten, retryAttempts, retry503Count, retry429Count);
            }

            retryMetricsCollector.close();
            metricEmitted = true;
        }
    }
}
