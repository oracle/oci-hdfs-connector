package com.oracle.bmc.hdfs.monitoring;

public class OCIMetricWithFBLatency extends OCIMetricWithThroughput {
    /**
     * The time to first byte when a read operation was performed in milliseconds.
     */
    private final double ttfb;
    public OCIMetricWithFBLatency(String key, double overallTime, double ttfb, double throughput, Exception e,
                                  double bytesTransferred, String bucketName) {
        super(key, overallTime, throughput, e, bytesTransferred, bucketName);
        this.ttfb = ttfb;
    }

    public double getTtfb() {
        return ttfb;
    }

    @Override
    public String toString() {
        return super.toString() + " OCIMetricWithFBLatency{" +
                "ttfb=" + ttfb +
                '}';
    }
}
