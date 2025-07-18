package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.model.BmcException;

public class OCIMetric {
    /**
     * The time in milliseconds (epoch) when the metric was recorded.
     */
    private final long recordedTime;
    /**
     * The overall time taken by the operation to complete/error-out in milliseconds.
     */
    private final double overallTime;
    /**
     * The operation key. This will be one of {"LIST", "HEAD", "WRITE", "READ", "DELETE", "RENAME"}
     */
    private final String key;
    /**
     * The boolean error indicates whether the operation errored out.
     */
    private final boolean error;
    /**
     * If the operation errored out, then the errorStatusCode is populated with the status code of the
     * error when communicating with OCI.
     */
    private final int errorStatusCode;
    /*
     * If this operation is coalesced.
     */
    private final boolean isCoalesced;
    /**
     * The target OCI bucket where the operation was attempted to.
     */
    private final String bucketName;
    /**
     * The number of retry attempts made for the operation.
     * This is non-zero if the operation was retried due to a transient failure.
     */
    private final int retryAttempts;

    /**
     * The number of retry attempts specifically for 503 status code.
     */
    private final int retry503Count;

    /**
     * The number of retry attempts specifically for 429 status code.
     */
    private final int retry429Count;


    public OCIMetric(double overallTime, String key, Exception e, String bucketName, int retryAttempts,
                     int retry503Count, int retry429Count) {
        this(overallTime, key, e, bucketName, retryAttempts, retry503Count, retry429Count, false);
    }

    public OCIMetric(double overallTime, String key, Exception e, String bucketName, int retryAttempts,
            int retry503Count, int retry429Count, boolean isCoalesced) {
        this.recordedTime = System.currentTimeMillis();
        this.overallTime = overallTime;
        this.key = key;
        this.bucketName = bucketName;
        this.retryAttempts = retryAttempts;
        this.retry503Count = retry503Count;
        this.retry429Count = retry429Count;

        int errStatusCode = 0;
        if (e != null) {
            this.error = true;
            if (e instanceof BmcException) {
                errStatusCode = ((BmcException) e).getStatusCode();
            }
        } else {
            this.error = false;
        }
        this.errorStatusCode = errStatusCode;
        this.isCoalesced = isCoalesced;
    }
    public String getKey() {
        return key;
    }

    public double getOverallTime() {
        return overallTime;
    }

    public boolean isError() {
        return error;
    }

    public long getRecordedTime() {
        return recordedTime;
    }

    public String getBucketName() { return bucketName; }

    public int getErrorStatusCode() { return errorStatusCode; }

    public int getRetryAttempts() { return retryAttempts; }

    public int getRetry503Count() { return retry503Count; }

    public int getRetry429Count() { return retry429Count;}
    public boolean getIsCoalesced() { return isCoalesced; }

    @Override
    public String toString() {
        return "OCIMetric{" +
                "recordedTime=" + recordedTime +
                ", overallTime=" + overallTime +
                ", key='" + key + '\'' +
                ", error=" + error +
                ", bucketName=" + bucketName +
                ", errorStatusCode=" + errorStatusCode +
                ", retryAttempts=" + retryAttempts +
                ", retry503Count=" + retry503Count +
                ", retry429Count=" + retry429Count +
                ", isCoalesced=" + isCoalesced +
                '}';
    }
}
