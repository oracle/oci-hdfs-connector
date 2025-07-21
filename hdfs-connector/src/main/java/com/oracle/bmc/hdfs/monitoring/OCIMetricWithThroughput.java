package com.oracle.bmc.hdfs.monitoring;

public class OCIMetricWithThroughput extends OCIMetric {
    /**
     * The throughput that was recorded for the operation in bytes/second
     */
    private final double throughput;
    /**
     * The total count of bytes that were transferred in or out.
     */
    private final double bytesTransferred;

    public OCIMetricWithThroughput(String key, double overallTime, double throughput, Exception e,
                                   double bytesTransferred, String bucketName, int retryAttempts,
                                   int retry503Count, int retry429Count) {
        super(overallTime, key, e, bucketName, retryAttempts, retry503Count, retry429Count);
        this.throughput = throughput;
        this.bytesTransferred = bytesTransferred;
    }

    public double getThroughput() {
        return throughput;
    }

    public double getBytesTransferred() { return bytesTransferred; }

    @Override
    public String toString() {
        return super.toString() + " OCIMetricWithThroughput{" +
                ", throughput=" + throughput +
                ", bytesTransferred=" + bytesTransferred +
                '}';
    }
}
