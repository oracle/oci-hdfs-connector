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
    /**
     * The target OCI bucket where the operation was attempted to.
     */
    private final String bucketName;

    public OCIMetric(double overallTime, String key, Exception e, String bucketName) {
        this.recordedTime = System.currentTimeMillis();
        this.overallTime = overallTime;
        this.key = key;
        this.bucketName = bucketName;

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

    @Override
    public String toString() {
        return "OCIMetric{" +
                "recordedTime=" + recordedTime +
                ", overallTime=" + overallTime +
                ", key='" + key + '\'' +
                ", error=" + error +
                ", bucketName=" + bucketName +
                ", errorStatusCode=" + errorStatusCode +
                '}';
    }
}
