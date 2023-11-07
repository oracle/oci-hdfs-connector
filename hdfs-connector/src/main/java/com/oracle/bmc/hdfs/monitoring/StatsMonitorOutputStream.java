package com.oracle.bmc.hdfs.monitoring;

import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

public class StatsMonitorOutputStream extends OutputStream {
    private long totalBytesWritten;
    private long totalElapsedTime;
    private OutputStream destinationStream;
    private boolean metricEmitted = false;
    private OCIMonitorPluginHandler ociMonitorPluginHandler;

    private Stopwatch sw = Stopwatch.createUnstarted();

    public StatsMonitorOutputStream(OutputStream destinationStream, OCIMonitorPluginHandler ociMonitorPluginHandler) {
        this.destinationStream = destinationStream;
        this.ociMonitorPluginHandler = ociMonitorPluginHandler;
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

            ociMonitorPluginHandler.recordStats(OCIMetricKeys.WRITE, totalElapsedTime, throughput, e, totalBytesWritten);
            metricEmitted = true;
        }
    }
}
