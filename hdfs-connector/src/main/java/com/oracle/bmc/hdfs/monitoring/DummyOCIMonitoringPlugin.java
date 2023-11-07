package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DummyOCIMonitoringPlugin extends OCIMonitorConsumerPlugin {

    private static DummyOCIMonitoringPlugin INSTANCE;

    private List<OCIMetric> ociMetricList = new ArrayList<>();

    public static DummyOCIMonitoringPlugin getInstance() {
        return INSTANCE;
    }

    public DummyOCIMonitoringPlugin(BmcPropertyAccessor propertyAccessor, String bucketName,
                                    String monitoringGroupingID, String namespaceName) {
        super(propertyAccessor, bucketName, monitoringGroupingID, namespaceName);
        LOG.debug("DummyOCIMonitoringPlugin constructor {} {} {} {}",propertyAccessor, bucketName,
                monitoringGroupingID, namespaceName);
    }

    @Override
    public void accept(OCIMetric ociMetric) {
        INSTANCE = this;

        try {

            // Hold on to 100 metric objects for verification in tests and drop them.
            if (ociMetricList.size() > 100) {
                ociMetricList = new ArrayList<>();
            }
            ociMetricList.add(ociMetric);

            LOG.debug("Accepting {} inside accept method of DummyOCIMonitoringPlugin.", ociMetric);
        } catch (Exception e) {
            // Ignore this exception
        }
    }

    public List<OCIMetric> getMetrics() {
        return ociMetricList;
    }

    @Override
    public void shutdown() {
        LOG.debug("Inside shutdown method of DummyOCIMonitoringPlugin.");
    }
}
