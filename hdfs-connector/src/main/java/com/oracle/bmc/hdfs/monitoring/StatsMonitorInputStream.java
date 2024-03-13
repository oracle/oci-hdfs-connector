package com.oracle.bmc.hdfs.monitoring;

import com.google.common.base.Stopwatch;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class StatsMonitorInputStream extends FSInputStream {

    private long totalBytesRead;
    private long ttfb;
    private long totalElapsedTime;
    private FSInputStream sourceStream;
    private boolean firstReadFinished = false;
    private boolean metricEmitted = false;
    private OCIMonitorPluginHandler ociMonitorPluginHandler;

    private Stopwatch sw = Stopwatch.createUnstarted();

    /**
     * To capture kind of accurate TTFB, keep the buffer size to 1MB for the first read.
     */
    private static final int FIRST_BYTE_LATENCY_BUFFER_SIZE = 1 * 1024 * 1024;

    public StatsMonitorInputStream(FSInputStream sourceStream, OCIMonitorPluginHandler ociMonitorPluginHandler) {
        this.sourceStream = sourceStream;
        this.ociMonitorPluginHandler = ociMonitorPluginHandler;
    }

    public int available() throws IOException {
        return sourceStream.available();
    }

    @Override
    public int read() throws IOException {
        triggerStopWatch();
        try {
            int b = sourceStream.read();
            recordStats(1);
            return b;
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    private void triggerStopWatch() {
        if (!sw.isRunning()) {
            sw.start();
        }
    }

    private void recordStats(int count) {
        if (count != -1) {
            if (!firstReadFinished && count > 0) {
                firstReadFinished = true;
                ttfb = sw.elapsed(TimeUnit.MILLISECONDS);
            }
            totalBytesRead += count;
        }
    }

    @Override
    public int read(byte b[]) throws IOException {
        triggerStopWatch();
        try {

            int count;
            if (!firstReadFinished && b.length > FIRST_BYTE_LATENCY_BUFFER_SIZE) {
                byte[] firstByteBuffer = new byte[FIRST_BYTE_LATENCY_BUFFER_SIZE];
                count = sourceStream.read(firstByteBuffer);
                if (count != -1) {
                    System.arraycopy(firstByteBuffer, 0, b, 0, count);
                }
            } else {
                count = sourceStream.read(b);
            }
            recordStats(count);
            return count;
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    public int read(byte b[], int off, int len) throws IOException {
        triggerStopWatch();
        try {
            int count;
            if (!firstReadFinished && len > FIRST_BYTE_LATENCY_BUFFER_SIZE) {
                byte[] firstByteBuffer = new byte[FIRST_BYTE_LATENCY_BUFFER_SIZE];
                count = sourceStream.read(firstByteBuffer);
                if (count != -1) {
                    System.arraycopy(firstByteBuffer, 0, b, off, count);
                }
            } else {
                count = sourceStream.read(b, off, len);
            }
            recordStats(count);
            return count;
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    private void emitMetric(Exception e) {
        if (ociMonitorPluginHandler.isEnabled() && !metricEmitted) {

            totalElapsedTime = sw.elapsed(TimeUnit.MILLISECONDS);

            double throughput = 0.0d;
            if (totalBytesRead > 0 && totalElapsedTime > 0) {
                throughput = totalBytesRead / (totalElapsedTime / 1000.0);
            }

            ociMonitorPluginHandler.recordStats(OCIMetricKeys.READ, totalElapsedTime, ttfb, throughput,
                    e, totalBytesRead);
            metricEmitted = true;
        }
    }

    public void close() throws IOException {
        triggerStopWatch();
        try {
            sourceStream.close();
            if (sw.isRunning()) {
                sw.stop();
            }
            emitMetric(null);
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    @Override
    public void seek(long l) throws IOException {
        triggerStopWatch();
        try {
            sourceStream.seek(l);
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    @Override
    public long getPos() throws IOException {
        triggerStopWatch();
        try {
            return sourceStream.getPos();
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        triggerStopWatch();
        try {
            return sourceStream.seekToNewSource(l);
        } catch (Exception e) {
            emitMetric(e);
            throw e;
        }
    }
}
