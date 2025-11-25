package com.oracle.bmc.hdfs.monitoring;

public class OCIMetricWithBatchCounts extends OCIMetric {
    /**
     * Number of objects successfully deleted in this batch.
     */
    private final int deletedCount;

    /**
     * Number of objects that failed to delete in this batch.
     */
    private final int failedCount;

    public OCIMetricWithBatchCounts(
            String key,
            double overallTime,
            Exception e,
            int deletedCount,
            int failedCount,
            String bucketName,
            int retryAttempts,
            int retry503Count,
            int retry429Count) {
        super(overallTime, key, e, bucketName, retryAttempts, retry503Count, retry429Count);
        this.deletedCount = deletedCount;
        this.failedCount = failedCount;
    }

    public int getDeletedCount() {
        return deletedCount;
    }

    public int getFailedCount() {
        return failedCount;
    }

    @Override
    public String toString() {
        return super.toString() + " OCIMetricWithBatchCounts{" +
                "deletedCount=" + deletedCount +
                ", failedCount=" + failedCount +
                '}';
    }
}
