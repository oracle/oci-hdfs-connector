/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.oracle.bmc.hdfs.monitoring.OCIMetricKeys;
import com.oracle.bmc.hdfs.monitoring.OCIMonitorPlugin;
import com.oracle.bmc.hdfs.monitoring.OCIMonitorPluginHandler;
import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import com.oracle.bmc.objectstorage.ObjectStorage;

import com.oracle.bmc.objectstorage.requests.RenameObjectRequest;
import com.oracle.bmc.objectstorage.responses.RenameObjectResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Callable that performs a rename as a sequence of copy+delete steps.
 */
@RequiredArgsConstructor
@Slf4j
public class RenameOperation implements Callable<String> {
    private final ObjectStorage objectStorage;
    private final RenameObjectRequest renameRequest;

    private final OCIMonitorPluginHandler ociMonitorPluginHandler;

    private final BmcPropertyAccessor propertyAccessor;

    /**
     * Delete will not happen if the copy fails. Returns the entity tag of the newly copied renamed object.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public String call() throws Exception {
        LOG.debug(
                "Renaming object from {} to {}.",
                this.renameRequest.getRenameObjectDetails().getSourceName(),
                this.renameRequest.getRenameObjectDetails().getNewName());

        Stopwatch sw = Stopwatch.createStarted();
        RetryMetricsCollector collector = new RetryMetricsCollector(OCIMetricKeys.RENAME, propertyAccessor);
        try {
            RenameObjectRequest requestWithRetry =
                    RenameObjectRequest.builder()
                            .copy(this.renameRequest)
                            .retryConfiguration(collector.getRetryConfiguration())
                            .build();

            RenameObjectResponse renameResponse = this.objectStorage.renameObject(requestWithRetry);

            sw.stop();
            recordRenameStats(sw.elapsed(TimeUnit.MILLISECONDS), null, collector.getAttemptCount(),
                    collector.getRetry503Count(), collector.getRetry429Count());

            return renameResponse.getETag();
        } catch (Exception e) {
            sw.stop();
            recordRenameStats(sw.elapsed(TimeUnit.MILLISECONDS), e, collector.getAttemptCount()
                    , collector.getRetry503Count(), collector.getRetry429Count());
            throw e;
        } finally {
            collector.close();
        }
    }

    private void recordRenameStats(long overallTime, Exception e, int attempts, int retry503Count, int retry429Count) {
        if (ociMonitorPluginHandler.isEnabled()) {
            ociMonitorPluginHandler.recordStats(OCIMetricKeys.RENAME, overallTime, e, attempts, retry503Count, retry429Count);
        }
    }
}
