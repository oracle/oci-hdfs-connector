package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.MultipartUpload;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.*;
import com.oracle.bmc.objectstorage.responses.CommitMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.CreateMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Random;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BmcMultipartOutputStreamTest {
    @Mock
    private ObjectStorage objectStorage;

    @Mock private Configuration mockConfiguration;
    @Mock private BmcPropertyAccessor mockPropAccessor;
    @Mock private BmcPropertyAccessor.Accessor<String> mockStringAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Integer> mockIntegerAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Boolean> mockBooleanAccessor;

    private static final Random randomGenerator = new Random();

    private static final int WRITE_COUNT = 1024;

    private static final int MAX_BUFFER_SIZE = 1024;

    @Before
    public void setUp() {
        // Setup mockIntegerAccessor
        when(mockIntegerAccessor.get(eq(BmcProperties.MULTIPART_IN_MEMORY_WRITE_MAX_INFLIGHT))).thenReturn(1);
        when(mockIntegerAccessor.get(eq(BmcProperties.MULTIPART_NUM_UPLOAD_THREADS))).thenReturn(1);
        when(mockBooleanAccessor.get(eq(BmcProperties.MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED))).thenReturn(true);

        when(mockPropAccessor.asString()).thenReturn(mockStringAccessor);
        when(mockPropAccessor.asInteger()).thenReturn(mockIntegerAccessor);
        when(mockPropAccessor.asBoolean()).thenReturn(mockBooleanAccessor);
    }

    @Test
    public void normalWrites() throws IOException, InterruptedException {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";
        MultipartUploadRequest uploadRequest = MultipartUploadRequest.builder()
                .setObjectStorage(objectStorage)
                .setNamespaceName(namespace)
                .setObjectName(objectName)
                .setBucketName(bucket)
                .setAllowOverwrite(true).build();
        BmcMultipartOutputStream bmos = new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE);

        String uploadId = "TestRequest";
        MultipartUpload upload = MultipartUpload.builder()
                .uploadId(uploadId)
                .bucket(bucket)
                .namespace(namespace)
                .object(objectName)
                .storageTier(StorageTier.Standard).build();
        Mockito.when(objectStorage.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().multipartUpload(upload).build());

        Mockito.when(objectStorage.uploadPart(any(UploadPartRequest.class))).thenReturn(UploadPartResponse.builder().eTag("etag").build());

        Mockito.when(objectStorage.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
                .thenReturn(CommitMultipartUploadResponse.builder().eTag("testingEtag").build());

        for (int parts = 0; parts < 2; ++parts) {
            // even splits
            bmos.write(generateRandomBytes(1024));
        }

        bmos.close();

        Mockito.verify(objectStorage, times(2)).uploadPart(any(UploadPartRequest.class));
        Mockito.verify(objectStorage, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(1)).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
    }

    @Test
    public void normalWritesUnevenSplits() throws IOException, InterruptedException {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";
        MultipartUploadRequest uploadRequest = MultipartUploadRequest.builder()
                .setObjectStorage(objectStorage)
                .setNamespaceName(namespace)
                .setObjectName(objectName)
                .setBucketName(bucket)
                .setAllowOverwrite(true).build();
        BmcMultipartOutputStream bmos = new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE);

        String uploadId = "TestRequest";
        MultipartUpload upload = MultipartUpload.builder()
                .uploadId(uploadId)
                .bucket(bucket)
                .namespace(namespace)
                .object(objectName)
                .storageTier(StorageTier.Standard).build();
        Mockito.when(objectStorage.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().multipartUpload(upload).build());

        Mockito.when(objectStorage.uploadPart(any(UploadPartRequest.class))).thenReturn(UploadPartResponse.builder().eTag("etag").build());

        Mockito.when(objectStorage.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
                .thenReturn(CommitMultipartUploadResponse.builder().eTag("testingEtag").build());

        // 3 parts * 1296 = 3888 / 1024 (max buffer) ~ 4 uploads
        for (int parts = 0; parts < 3; ++parts) {
            bmos.write(generateRandomBytes(1296));
        }

        bmos.close();

        // 3 parts * 1296 = 3888 / 1024 (max buffer) ~ 4 uploads
        Mockito.verify(objectStorage, times(4)).uploadPart(any(UploadPartRequest.class));
        Mockito.verify(objectStorage, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(1)).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
    }

    @Test()
    public void failedPartWrite() throws IOException, InterruptedException {
        String bucket = "test-bucket";
        String namespace = "testing";
        String objectName = "test-object.txt";
        MultipartUploadRequest uploadRequest = MultipartUploadRequest.builder()
                .setObjectStorage(objectStorage)
                .setNamespaceName(namespace)
                .setObjectName(objectName)
                .setBucketName(bucket)
                .setAllowOverwrite(true).build();

        String uploadId = "TestRequest";
        MultipartUpload upload = MultipartUpload.builder()
                .uploadId(uploadId)
                .bucket(bucket)
                .namespace(namespace)
                .object(objectName)
                .storageTier(StorageTier.Standard).build();
        Mockito.when(objectStorage.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().multipartUpload(upload).build());

        Mockito.when(objectStorage.uploadPart(any(UploadPartRequest.class))).thenThrow(Exception.class);

        Mockito.when(objectStorage.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
                .thenReturn(CommitMultipartUploadResponse.builder().eTag("testingEtag").build());

        Exception exception = null;
        try (BmcMultipartOutputStream bmos = new BmcMultipartOutputStream(mockPropAccessor, uploadRequest, MAX_BUFFER_SIZE)) {
            for (int parts = 0; parts < 1; ++parts) {
                bmos.write(generateRandomBytes(1024));
            }
        } catch (IOException ioe) {
            exception = ioe;
        }

        assert(exception != null);
        Mockito.verify(objectStorage, times(1)).uploadPart(any(UploadPartRequest.class));
        Mockito.verify(objectStorage, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        Mockito.verify(objectStorage, never()).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    private static byte[] generateRandomBytes(int num) {
        byte[] result = new byte[num];
        randomGenerator.nextBytes(result);
        return result;
    }
}
