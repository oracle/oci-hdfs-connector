package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import java.util.concurrent.ConcurrentLinkedQueue;

class SpyPlugin extends OCIMonitorConsumerPlugin {
    final ConcurrentLinkedQueue<OCIMetric> accepted = new ConcurrentLinkedQueue<>();
    volatile boolean shutdownCalled = false;

    SpyPlugin(BmcPropertyAccessor accessor, String bucket, String group, String ns) {
        super(accessor, bucket, group, ns);
    }
    @Override public void accept(OCIMetric metric) { accepted.add(metric); }
    @Override public void shutdown() { shutdownCalled = true; }
}
