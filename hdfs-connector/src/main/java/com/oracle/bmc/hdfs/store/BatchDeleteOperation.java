/**
 * Copyright (c) 2016, 2025, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.oracle.bmc.hdfs.monitoring.OCIMetricKeys;
import com.oracle.bmc.hdfs.monitoring.OCIMonitorPluginHandler;
import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.BatchDeleteObjectIdentifier;
import com.oracle.bmc.objectstorage.model.BatchDeleteObjectsDetails;
import com.oracle.bmc.objectstorage.model.BatchDeleteObjectsResult;
import com.oracle.bmc.objectstorage.requests.BatchDeleteObjectsRequest;
import com.oracle.bmc.objectstorage.responses.BatchDeleteObjectsResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Callable that performs batch delete of multiple objects using Batch Delete API.
 */
@RequiredArgsConstructor
@Slf4j
public class BatchDeleteOperation implements Callable<BatchDeleteObjectsResult> {
    private final ObjectStorage objectStorage;
    private final RequestBuilder requestBuilder;
    private final List<Path> paths;
    private final FileSystem.Statistics statistics;
    private final OCIMonitorPluginHandler ociMonitorPluginHandler;
    private final BmcPropertyAccessor propertyAccessor;

    @Override
    public BatchDeleteObjectsResult call() throws Exception {
        LOG.debug("Batch deleting {} objects", paths.size());

        // Paths to object identifiers
        final List<BatchDeleteObjectIdentifier> identifiers = new ArrayList<>(paths.size());
        for (Path path : paths) {
            final String objectName = pathToObject(path);
            identifiers.add(BatchDeleteObjectIdentifier.builder().objectName(objectName).build());
        }

        Stopwatch sw = Stopwatch.createStarted();
        RetryMetricsCollector collector =
                new RetryMetricsCollector(OCIMetricKeys.DELETE, propertyAccessor);

        try {
            final BatchDeleteObjectsDetails details =
                    BatchDeleteObjectsDetails.builder().objects(identifiers).build();
            final BatchDeleteObjectsRequest request = requestBuilder.batchDeleteObjects(details);

            final BatchDeleteObjectsRequest requestWithRetry =
                    BatchDeleteObjectsRequest.builder()
                            .copy(request)
                            .retryConfiguration(collector.getRetryConfiguration())
                            .build();

            final BatchDeleteObjectsResponse response =
                    objectStorage.batchDeleteObjects(requestWithRetry);
            final BatchDeleteObjectsResult result = response.getBatchDeleteObjectsResult();

            sw.stop();

            final int deletedCount =
                    result.getDeleted() != null
                            ? result.getDeleted().size()
                            : 0;

            if (deletedCount > 0) {
                statistics.incrementWriteOps(deletedCount);
            }

            final int failedCount =
                    result.getFailed() != null
                            ? result.getFailed().size()
                            : 0;

            recordBatchDeleteStats(
                    sw.elapsed(TimeUnit.MILLISECONDS),
                    null,
                    collector.getAttemptCount(),
                    collector.getRetry503Count(),
                    collector.getRetry429Count(),
                    deletedCount,
                    failedCount);

            LOG.info("Batch delete completed: {} deleted, {} failed",
                    deletedCount,
                    failedCount);

            return result;

        } catch (Exception e) {
            sw.stop();
            recordBatchDeleteStats(
                    sw.elapsed(TimeUnit.MILLISECONDS),
                    e,
                    collector.getAttemptCount(),
                    collector.getRetry503Count(),
                    collector.getRetry429Count(),
                    0,
                    paths.size());
            throw new IOException("Failed to execute batch delete for " + paths.size() + " files", e);
        } finally {
            collector.close();
        }
    }

    /**
     * Converts a Path to an object name.
     */
    private String pathToObject(final Path path) {
        String pathString = path.toUri().getPath();
        if (pathString.startsWith("/")) {
            pathString = pathString.substring(1);
        }
        return pathString;
    }

    private void recordBatchDeleteStats(
            long overallTime, Exception e, int attempts, int retry503Count, int retry429Count,
            int deletedCount, int failedCount) {
        if (ociMonitorPluginHandler.isEnabled()) {
            ociMonitorPluginHandler.recordStats(
                    OCIMetricKeys.BATCH_DELETE, overallTime, e,
                    deletedCount, failedCount,
                    attempts, retry503Count, retry429Count);
        }
    }
}
