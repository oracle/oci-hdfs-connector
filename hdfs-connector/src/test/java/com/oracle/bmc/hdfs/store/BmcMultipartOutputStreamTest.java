/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.monitoring.OCIMetricKeys;
import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import com.oracle.bmc.hdfs.util.BlockingRejectionHandler;
import com.oracle.bmc.hdfs.util.DirectExecutorService;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.CreateMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.MultipartUpload;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.*;
import com.oracle.bmc.objectstorage.responses.CommitMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.CreateMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.retrier.RetryConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BmcMultipartOutputStreamTest {
    @Mock private ObjectStorage objectStorage;

    @Mock private BmcPropertyAccessor mockPropAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Integer> mockIntegerAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Boolean> mockBooleanAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Long> mockLongAccessor;

    private static final Random randomGenerator = new Random();

    private static final int WRITE_COUNT = 1024;

    private static final int MAX_BUFFER_SIZE = 1024;

    @Before
    public void setUp() {
        // Setup mockIntegerAccessor
        when(mockIntegerAccessor.get(eq(BmcProperties.MULTIPART_NUM_UPLOAD_THREADS))).thenReturn(1);
        when(mockBooleanAccessor.get(eq(BmcProperties.MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED)))
                .thenReturn(true);
        when(
                        mockIntegerAccessor.get(
                                eq(BmcProperties.MULTIPART_IN_MEMORY_WRITE_TASK_TIMEOUT_SECONDS)))
                .thenReturn(900);
        when(mockBooleanAccessor.get(eq(BmcProperties.OBJECT_ALLOW_OVERWRITE))).thenReturn(true);
        when(mockLongAccessor.get(eq(BmcProperties.RETRY_TIMEOUT_IN_SECONDS))).thenReturn(30L);
        when(mockLongAccessor.get(eq(BmcProperties.RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS))).thenReturn(0L);

        when(mockPropAccessor.asInteger()).thenReturn(mockIntegerAccessor);
        when(mockPropAccessor.asBoolean()).thenReturn(mockBooleanAccessor);
        when(mockPropAccessor.asLong()).thenReturn(mockLongAccessor);
    }

    @Test
    public void normalWrites() throws IOException {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";
        CreateMultipartUploadDetails details =
                CreateMultipartUploadDetails.builder().object(objectName).build();
        CreateMultipartUploadRequest multipartUploadRequest =
                CreateMultipartUploadRequest.builder()
                        .bucketName(bucket)
                        .namespaceName(namespace)
                        .createMultipartUploadDetails(details)
                        .build();
        MultipartUploadRequest uploadRequest =
                MultipartUploadRequest.builder()
                        .objectStorage(objectStorage)
                        .multipartUploadRequest(multipartUploadRequest)
                        .allowOverwrite(true)
                        .build();
        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.WRITE);
        UploadConfiguration uploadConfiguration = UploadConfiguration.builder()
                .allowMultipartUploads(true)
                .lengthPerUploadPart(128)
                .build();
        BmcMultipartOutputStream bmos =
                new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE, createExecutorService(), 2, retryMetricsCollector, true, uploadConfiguration);

        String uploadId = "TestRequest";
        MultipartUpload upload =
                MultipartUpload.builder()
                        .uploadId(uploadId)
                        .bucket(bucket)
                        .namespace(namespace)
                        .object(objectName)
                        .storageTier(StorageTier.Standard)
                        .build();
        Mockito.when(objectStorage.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(
                        CreateMultipartUploadResponse.builder().multipartUpload(upload).build());

        Mockito.when(objectStorage.uploadPart(any(UploadPartRequest.class)))
                .thenReturn(UploadPartResponse.builder().eTag("etag").build());

        Mockito.when(objectStorage.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
                .thenReturn(CommitMultipartUploadResponse.builder().eTag("testingEtag").build());

        for (int parts = 0; parts < 2; ++parts) {
            // even splits
            bmos.write(generateRandomBytes(1024));
        }

        bmos.close();

        Mockito.verify(objectStorage, times(2)).uploadPart(any(UploadPartRequest.class));
        Mockito.verify(objectStorage, times(1))
                .createMultipartUpload(any(CreateMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(1))
                .commitMultipartUpload(any(CommitMultipartUploadRequest.class));
    }

    @Test
    public void normalWritesUnevenSplits() throws IOException {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";
        CreateMultipartUploadDetails details =
                CreateMultipartUploadDetails.builder().object(objectName).build();
        CreateMultipartUploadRequest multipartUploadRequest =
                CreateMultipartUploadRequest.builder()
                        .bucketName(bucket)
                        .namespaceName(namespace)
                        .createMultipartUploadDetails(details)
                        .build();
        MultipartUploadRequest uploadRequest =
                MultipartUploadRequest.builder()
                        .objectStorage(objectStorage)
                        .multipartUploadRequest(multipartUploadRequest)
                        .allowOverwrite(true)
                        .build();
        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.WRITE);
        UploadConfiguration uploadConfiguration = UploadConfiguration.builder()
                .allowMultipartUploads(true)
                .lengthPerUploadPart(128)
                .build();
        BmcMultipartOutputStream bmos =
                new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE, createExecutorService(), 2, retryMetricsCollector,true, uploadConfiguration);

        String uploadId = "TestRequest";
        MultipartUpload upload =
                MultipartUpload.builder()
                        .uploadId(uploadId)
                        .bucket(bucket)
                        .namespace(namespace)
                        .object(objectName)
                        .storageTier(StorageTier.Standard)
                        .build();
        Mockito.when(objectStorage.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(
                        CreateMultipartUploadResponse.builder().multipartUpload(upload).build());

        Mockito.when(objectStorage.uploadPart(any(UploadPartRequest.class)))
                .thenReturn(UploadPartResponse.builder().eTag("etag").build());

        Mockito.when(objectStorage.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
                .thenReturn(CommitMultipartUploadResponse.builder().eTag("testingEtag").build());

        // 3 parts * 1296 = 3888 / 1024 (max buffer) ~ 4 uploads
        for (int parts = 0; parts < 3; ++parts) {
            bmos.write(generateRandomBytes(1296));
        }

        bmos.close();

        // 3 parts * 1296 = 3888 / 1024 (max buffer) ~ 4 uploads
        Mockito.verify(objectStorage, times(4)).uploadPart(any(UploadPartRequest.class));
        Mockito.verify(objectStorage, times(1))
                .createMultipartUpload(any(CreateMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(1))
                .commitMultipartUpload(any(CommitMultipartUploadRequest.class));
    }

    @Test()
    public void failedPartWrite() {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";
        CreateMultipartUploadDetails details =
                CreateMultipartUploadDetails.builder().object(objectName).build();
        CreateMultipartUploadRequest multipartUploadRequest =
                CreateMultipartUploadRequest.builder()
                        .bucketName(bucket)
                        .namespaceName(namespace)
                        .createMultipartUploadDetails(details)
                        .build();
        MultipartUploadRequest uploadRequest =
                MultipartUploadRequest.builder()
                        .objectStorage(objectStorage)
                        .multipartUploadRequest(multipartUploadRequest)
                        .allowOverwrite(true)
                        .build();

        String uploadId = "TestRequest";
        MultipartUpload upload =
                MultipartUpload.builder()
                        .uploadId(uploadId)
                        .bucket(bucket)
                        .namespace(namespace)
                        .object(objectName)
                        .storageTier(StorageTier.Standard)
                        .build();
        Mockito.when(objectStorage.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(
                        CreateMultipartUploadResponse.builder().multipartUpload(upload).build());

        Mockito.when(objectStorage.uploadPart(any(UploadPartRequest.class)))
                .thenThrow(Exception.class);

        Mockito.when(objectStorage.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
                .thenReturn(CommitMultipartUploadResponse.builder().eTag("testingEtag").build());

        Exception exception = null;
        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.WRITE);
        UploadConfiguration uploadConfiguration = UploadConfiguration.builder()
                .allowMultipartUploads(true)
                .lengthPerUploadPart(128)
                .build();
        try (BmcMultipartOutputStream bmos =
                new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE, createExecutorService(), 2,retryMetricsCollector, true, uploadConfiguration)) {
            for (int parts = 0; parts < 1; ++parts) {
                bmos.write(generateRandomBytes(1024));
            }
        } catch (IOException ioe) {
            exception = ioe;
        }

        assert (exception != null);
        Mockito.verify(objectStorage, times(3)).uploadPart(any(UploadPartRequest.class));
        Mockito.verify(objectStorage, times(1))
                .createMultipartUpload(any(CreateMultipartUploadRequest.class));
        Mockito.verify(objectStorage, never())
                .commitMultipartUpload(any(CommitMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(1))
                .abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    public void failOnNullDetails() {
        String bucket = "test-bucket";
        String namespace = "testing";
        CreateMultipartUploadRequest multipartUploadRequest =
                CreateMultipartUploadRequest.builder()
                        .bucketName(bucket)
                        .namespaceName(namespace)
                        .build();
        Exception exception = null;
        try {
            MultipartUploadRequest.builder()
                    .objectStorage(objectStorage)
                    .multipartUploadRequest(multipartUploadRequest)
                    .allowOverwrite(true)
                    .build();
        } catch (NullPointerException npe) {
            exception = npe;
        }

        assert (exception != null);
        Mockito.verify(objectStorage, never())
                .createMultipartUpload(any(CreateMultipartUploadRequest.class));
    }

    @Test
    public void emptyFileWrite() throws IOException {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";

        CreateMultipartUploadDetails details =
                CreateMultipartUploadDetails.builder().object(objectName).build();
        CreateMultipartUploadRequest multipartUploadRequest =
                CreateMultipartUploadRequest.builder()
                        .bucketName(bucket)
                        .namespaceName(namespace)
                        .createMultipartUploadDetails(details)
                        .build();
        MultipartUploadRequest uploadRequest =
                MultipartUploadRequest.builder()
                        .objectStorage(objectStorage)
                        .multipartUploadRequest(multipartUploadRequest)
                        .allowOverwrite(true)
                        .build();

        PutObjectResponse putResponse = PutObjectResponse.builder().build();
        Mockito.when(objectStorage.putObject(any(PutObjectRequest.class))).thenReturn(putResponse);
        ExecutorService md5Executor = createExecutorService();

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.WRITE);
        UploadConfiguration uploadConfiguration = UploadConfiguration.builder()
                .allowMultipartUploads(true)
                .lengthPerUploadPart(128)
                .build();
        try (BmcMultipartOutputStream bmos =
                     new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE, md5Executor, 2, retryMetricsCollector, true, uploadConfiguration)) {
            bmos.write(new byte[0]);
            bmos.flush();
        } finally {
            md5Executor.shutdown();
        }

        Mockito.verify(objectStorage, times(1)).putObject(any(PutObjectRequest.class));
        Mockito.verify(objectStorage, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    }

    /**
     * Test retry on a two-part object upload
     */

    @Test()
    public void retryOnFailedPart() {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";
        CreateMultipartUploadDetails details =
                CreateMultipartUploadDetails.builder().object(objectName).build();
        CreateMultipartUploadRequest multipartUploadRequest =
                CreateMultipartUploadRequest.builder()
                        .bucketName(bucket)
                        .namespaceName(namespace)
                        .createMultipartUploadDetails(details)
                        .build();
        MultipartUploadRequest uploadRequest =
                MultipartUploadRequest.builder()
                        .objectStorage(objectStorage)
                        .multipartUploadRequest(multipartUploadRequest)
                        .allowOverwrite(true)
                        .build();

        String uploadId = "TestRequest";
        MultipartUpload upload =
                MultipartUpload.builder()
                        .uploadId(uploadId)
                        .bucket(bucket)
                        .namespace(namespace)
                        .object(objectName)
                        .storageTier(StorageTier.Standard)
                        .build();
        Mockito.when(objectStorage.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(
                        CreateMultipartUploadResponse.builder().multipartUpload(upload).build());

        Mockito.when(objectStorage.uploadPart(any(UploadPartRequest.class)))
                .thenThrow(new BmcException(true, "timeout", new SocketTimeoutException("Read Timeout"), "abcd1-xdfay"))
                .thenThrow(new BmcException(429, null, "Too many requests", null))
                .thenReturn(UploadPartResponse.builder().eTag("etag").build())
                .thenThrow(new BmcException(true, "timeout", new SocketTimeoutException("Read Timeout"), "abcd2-xdfay"))
                .thenThrow(new BmcException(429, null, "Too many requests", null))
                .thenReturn(UploadPartResponse.builder().eTag("etag").build());

        Mockito.when(objectStorage.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
                .thenReturn(CommitMultipartUploadResponse.builder().eTag("testingEtag").build());

        Exception exception = null;
        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.WRITE);
        UploadConfiguration uploadConfiguration = UploadConfiguration.builder()
                .allowMultipartUploads(true)
                .lengthPerUploadPart(128)
                .build();
        try (BmcMultipartOutputStream bmos =
                     new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE, createExecutorService(), 2, retryMetricsCollector, true, uploadConfiguration)) {
            // two parts, first part 1024, second part 2 bytes
            for (int parts = 0; parts < 2; ++parts) {
                bmos.write(generateRandomBytes(512 + 1));
            }
        } catch (IOException ioe) {
            exception = ioe;
        }

        assert (exception == null);
        Mockito.verify(objectStorage, times(6)).uploadPart(any(UploadPartRequest.class));
        Mockito.verify(objectStorage, times(1))
                .createMultipartUpload(any(CreateMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(1))
                .commitMultipartUpload(any(CommitMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(0))
                .abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    /**
     * Test retry on small object upload, using plain PutObject operation
     */
    @Test
    public void retryOnSmallFileWrite() throws IOException {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";

        CreateMultipartUploadDetails details =
                CreateMultipartUploadDetails.builder().object(objectName).build();
        CreateMultipartUploadRequest multipartUploadRequest =
                CreateMultipartUploadRequest.builder()
                        .bucketName(bucket)
                        .namespaceName(namespace)
                        .createMultipartUploadDetails(details)
                        .build();
        MultipartUploadRequest uploadRequest =
                MultipartUploadRequest.builder()
                        .objectStorage(objectStorage)
                        .multipartUploadRequest(multipartUploadRequest)
                        .allowOverwrite(true)
                        .build();

        PutObjectResponse putResponse = PutObjectResponse.builder().build();
        Mockito.when(objectStorage.putObject(any(PutObjectRequest.class)))
                .thenThrow(new BmcException(true, "timeout", new SocketTimeoutException("Read Timeout"), "abcd1-xdfay"))
                .thenThrow(new BmcException(429, null, "Too many requests", null))
                .thenReturn(putResponse);
        ExecutorService md5Executor = createExecutorService();
        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION, OCIMetricKeys.WRITE);
        UploadConfiguration uploadConfiguration = UploadConfiguration.builder()
                .allowMultipartUploads(true)
                .lengthPerUploadPart(128)
                .build();
        try (BmcMultipartOutputStream bmos =
                     new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE, md5Executor, 2, retryMetricsCollector, true, uploadConfiguration)) {
            bmos.write(new byte[0]);
            bmos.flush();
        } finally {
            md5Executor.shutdown();
        }

        Mockito.verify(objectStorage, times(3)).putObject(any(PutObjectRequest.class));
        Mockito.verify(objectStorage, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    }

    private static byte[] generateRandomBytes(int num) {
        byte[] result = new byte[num];
        randomGenerator.nextBytes(result);
        return result;
    }

    private ExecutorService createExecutorService() {
        final int taskTimeout =
                mockPropAccessor
                        .asInteger()
                        .get(BmcProperties.MULTIPART_IN_MEMORY_WRITE_TASK_TIMEOUT_SECONDS);
        final int numThreadsForParallelPartUpload =
                mockPropAccessor.asInteger().get(BmcProperties.MULTIPART_NUM_UPLOAD_THREADS);
        final BlockingRejectionHandler rejectedExecutionHandler =
                new BlockingRejectionHandler(taskTimeout);
        final ExecutorService executorService;
        if (numThreadsForParallelPartUpload <= 1) {
            executorService = new DirectExecutorService();
        } else {
            executorService = new ThreadPoolExecutor(
                    numThreadsForParallelPartUpload,
                    numThreadsForParallelPartUpload,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(numThreadsForParallelPartUpload),
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("bmcs-hdfs-multipart-md5-%d")
                            .build(),
                    rejectedExecutionHandler);
        }
        return executorService;
    }
}
