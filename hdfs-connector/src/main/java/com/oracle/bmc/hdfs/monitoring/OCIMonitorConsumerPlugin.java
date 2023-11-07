package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;

/**
 * This class has to be extended by any plugin, that wants to consume the metrics emitted by OCI HDFS connector.
 */
public abstract class OCIMonitorConsumerPlugin {

    /**
     * This provides a handle to the properties defined for the hdfs connector.
     */
    protected BmcPropertyAccessor propertyAccessor;
    /**
     * This is the object storage bucket for which the metrics are being emitted for.
     */
    protected String bucketName;
    /**
     * This is the unique identifier defined for grouping monitoring together.
     */
    protected String monitoringGroupingID;
    /**
     * This is the namespace of the bucket in OSS.
     */
    protected String namespaceName;

    public OCIMonitorConsumerPlugin(BmcPropertyAccessor propertyAccessor, String bucketName,
                                    String monitoringGroupingID, String namespaceName) {
        this.propertyAccessor = propertyAccessor;
        this.bucketName = bucketName;
        this.monitoringGroupingID = monitoringGroupingID;
        this.namespaceName = namespaceName;
    }

    /**
     * This method will be called on plugins by the OCI monitoring framework, whenever it wants to emit out a metric.
     * This method should finish as quickly as possible, so the consumer of this should ideally handover the
     * ocimetric and stage it elsewhere for processing, instead of trying to deal with it in the accept call itself.
     * @param ociMetric The metric that is being emitted by the OCI HDFS connector
     */
    public abstract void accept(OCIMetric ociMetric);

    /**
     * This shutdown method will be called on the implementing plugins, whenever the JVM is shutting down.
     * It could be used to cleanup, finish pending tasks before exit.
     */
    public abstract void shutdown();
}
