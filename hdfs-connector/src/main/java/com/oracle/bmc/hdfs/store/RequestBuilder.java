/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import com.oracle.bmc.objectstorage.model.RenameObjectDetails;
import com.oracle.bmc.objectstorage.requests.*;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
    private static final String SIZE_AND_TIME_FILDS = "size,timeCreated";

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

    HeadObjectRequest headObject(final String objectName) {
        return HeadObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .opcClientRequestId(createClientRequestId("headObject"))
                .build();
    }

    PutObjectRequest putObject(
            final String objectName, final InputStream input, final long contentLengthInBytes) {
        return PutObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .putObjectBody(input)
                .contentLength(contentLengthInBytes)
                .opcClientRequestId(createClientRequestId("putObject"))
                .build();
    }

    PutObjectRequest putObject(
            final String objectName,
            final InputStream input,
            final long contentLengthInBytes,
            final String md5) {
        return PutObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .putObjectBody(input)
                .contentLength(contentLengthInBytes)
                .contentMD5(md5)
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
                                .build())
                .opcClientRequestId(createClientRequestId("renameObject"))
                .build();
    }

    UploadRequest uploadRequest(
            final String objectName,
            final InputStream input,
            final long contentLengthInBytes,
            ExecutorService parallelUploadExecutor) {
        PutObjectRequest putObjectRequest =
                PutObjectRequest.builder()
                        .namespaceName(this.namespace)
                        .bucketName(this.bucket)
                        .objectName(objectName)
                        .opcClientRequestId(createClientRequestId("uploadRequest"))
                        .build();
        return UploadRequest.builder(input, contentLengthInBytes)
                .parallelUploadExecutorService(parallelUploadExecutor)
                .build(putObjectRequest);
    }

    DeleteObjectRequest deleteObject(final String objectName) {
        return DeleteObjectRequest.builder()
                .namespaceName(this.namespace)
                .bucketName(this.bucket)
                .objectName(objectName)
                .opcClientRequestId(createClientRequestId("deleteObject"))
                .build();
    }

    private static String createClientRequestId(final String operation) {
        final String uuid = UUID.randomUUID().toString();
        LOG.debug("Using request ID {} for {}", uuid, operation);
        return uuid;
    }
}
