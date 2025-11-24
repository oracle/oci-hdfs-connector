/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.oracle.bmc.hdfs.monitoring.OCIMetricKeys;
import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import com.oracle.bmc.objectstorage.model.BatchDeleteObjectsDetails;
import com.oracle.bmc.objectstorage.model.RenameObjectDetails;
import com.oracle.bmc.objectstorage.requests.*;
import com.oracle.bmc.objectstorage.transfer.ProgressReporter;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;

import com.oracle.bmc.retrier.RetryConfiguration;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StopWatch;

/**
 * Simple helper class to build request objects for various Object Store calls. Removes the redundancy of specifying
 * namespace/bucket, as well as some optional field checking. Optional args are denoted by {@link Nullable}, everything
 * else is assumed to be non-null.
 * <p>
 * Note, all APIs are meant to be used for one-off calls and should not be reused (otherwise they will have the same
 * client request ID).
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class RequestBuilder {
    // TODO: these fields should ideally come from the SDK.
    private static final String SIZE_AND_TIME_FILDS = "size,timeCreated,timeModified";

    private final String namespace;
    private final String bucket;

    ListObjectsRequest listObjects(
            @Nullable final String prefix,
            @Nullable final String nextToken,
            @Nullable final String delimiter,
            @Nullable final Integer limit) {
        final ListObjectsRequest.Builder builder =
                ListObjectsRequest.builder().namespaceName(this.namespace).bucketName(this.bucket);
        if ((prefix != null) && !prefix.isEmpty()) {
            builder.prefix(prefix);
        }
        if (nextToken != null) {
            builder.start(nextToken);
        }
        if (delimiter != null) {
            builder.delimiter(delimiter);
        }
        if (limit != null) {
            builder.limit(limit);
        }
        return builder.fields(SIZE_AND_TIME_FILDS)
                .opcClientRequestId(createClientRequestId("listObjects"))
                .build();
    }

    GetObjectRequest getObject(final String objectName) {
        return this.getObjectBuilder(objectName).build();
    }

    GetObjectRequest.Builder getObjectBuilder(final String objectName) {
        return GetObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .opcClientRequestId(createClientRequestId("getObject"));
    }

    GetObjectRequest.Builder getObjectBuilder(final String objectName, final RetryMetricsCollector retryMetricsCollector) {
        return GetObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .retryConfiguration(retryMetricsCollector.getRetryConfiguration())
                .opcClientRequestId(createClientRequestId("getObject"));
    }

    HeadObjectRequest headObject(final String objectName) {
        return HeadObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .opcClientRequestId(createClientRequestId("headObject"))
                .build();
    }

    /**
     * Builds a {@link HeadObjectRequest} with a deliberately non-matching If-Match ETag header.
     * This technique enables a low-cost existence check for an object in OCI Object Storage.
     * When an object exists and the provided If-Match value does not match the actual ETag,
     * the server returns a 412 Precondition Failed response instead of a full object metadata response.
     * This bypasses KMS-based metadata decryption, making it significantly cheaper and faster than a GET or a normal HEAD call.
     */
    HeadObjectRequest headObjectWithNonMatchingIfMatch(final String objectName) {
        return HeadObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .ifMatch("non-matching-etag")
                .opcClientRequestId(createClientRequestId("headObject"))
                .build();
    }

    PutObjectRequest putObject(
            final String objectName, final InputStream input, final long contentLengthInBytes,  final RetryMetricsCollector retryMetricsCollector) {
        return PutObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .putObjectBody(input)
                .contentLength(contentLengthInBytes)
                .retryConfiguration(retryMetricsCollector.getRetryConfiguration())
                .opcClientRequestId(createClientRequestId("putObject"))
                .build();
    }

    PutObjectRequest putObject(
            final String objectName,
            final InputStream input,
            final long contentLengthInBytes,
            final String md5,
            final RetryMetricsCollector retryMetricsCollector) {
        return PutObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .putObjectBody(input)
                .contentLength(contentLengthInBytes)
                .contentMD5(md5)
                .opcClientRequestId(createClientRequestId("putObject"))
                .retryConfiguration(retryMetricsCollector.getRetryConfiguration())
                .build();
    }

    PutObjectRequest putObjectWithIfNoneMatch(
            final String objectName, final InputStream input, final long contentLengthInBytes,
            final RetryMetricsCollector retryMetricsCollector) {
        return PutObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .putObjectBody(input)
                .contentLength(contentLengthInBytes)
                .ifNoneMatch("*")
                .retryConfiguration(retryMetricsCollector.getRetryConfiguration())
                .opcClientRequestId(createClientRequestId("putObject"))
                .build();
    }


    RenameObjectRequest renameObject(final String sourceName, final String newName) {
        return RenameObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .renameObjectDetails(
                        RenameObjectDetails.builder()
                                .sourceName(sourceName)
                                .newName(newName)
                                .newObjIfNoneMatchETag(getIfNoneMatchHeader(false))
                                .build())
                .opcClientRequestId(createClientRequestId("renameObject"))
                .build();
    }

    UploadRequest uploadRequest(
            final String objectName,
            final InputStream input,
            final long contentLengthInBytes,
            final Progressable progressable,
            final boolean allowOverwrite,
            ExecutorService parallelUploadExecutor,
            final RetryMetricsCollector collector) {

        PutObjectRequest putObjectRequest =
                PutObjectRequest.builder()
                        .namespaceName(this.namespace)
                        .bucketName(this.bucket)
                        .objectName(objectName)
                        .opcClientRequestId(createClientRequestId("uploadRequest"))
                        .retryConfiguration(collector.getRetryConfiguration())
                        .build();

        UploadRequest.UploadRequestBuilder uploadRequestBuilder =
                UploadRequest.builder(input, contentLengthInBytes)
                        .parallelUploadExecutorService(parallelUploadExecutor)
                        .allowOverwrite(allowOverwrite);

        if (progressable != null) {
            uploadRequestBuilder.progressReporter(new HadoopProgressReporter(progressable));
        } else {
            LOG.info("No Progressable passed, not reporting progress.");
        }

        return uploadRequestBuilder.build(putObjectRequest);
    }

    DeleteObjectRequest deleteObject(final String objectName) {
        return DeleteObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .opcClientRequestId(createClientRequestId("deleteObject"))
                .build();
    }

    BatchDeleteObjectsRequest batchDeleteObjects(
            final BatchDeleteObjectsDetails details) {
        return BatchDeleteObjectsRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .batchDeleteObjectsDetails(details)
                .opcClientRequestId(createClientRequestId("batchDeleteObjects"))
                .build();
    }

    private static String createClientRequestId(final String operation) {
        final String uuid = UUID.randomUUID().toString();
        LOG.debug("Using request ID {} for {}", uuid, operation);
        return uuid;
    }

    private static String getIfNoneMatchHeader(boolean allowOverwrite) {
        return allowOverwrite ? null : "*";
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class HadoopProgressReporter implements ProgressReporter {
        private static final long NOTIFICATION_THRESHOLD_IN_SECONDS = 10;

        private final Progressable progressable;
        private final StopWatch stopWatch = new StopWatch();

        @Override
        public void onProgress(final long completed, final long total) {
            if (!stopWatch.isRunning()
                    || stopWatch.now(TimeUnit.SECONDS) >= NOTIFICATION_THRESHOLD_IN_SECONDS) {
                stopWatch.reset().start();
                LOG.info("Reporting progress to Application Master ({}/{})", completed, total);
                progressable.progress();
            }
        }
    }
}
