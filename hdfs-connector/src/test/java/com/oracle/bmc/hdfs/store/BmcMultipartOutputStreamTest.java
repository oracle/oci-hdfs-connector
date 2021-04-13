package com.oracle.bmc.hdfs.store;

import com.google.common.base.Supplier;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.MultipartUpload;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.CommitMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.UploadPartRequest;
import com.oracle.bmc.objectstorage.responses.CommitMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.CreateMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.spy;
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
        when(mockIntegerAccessor.get(eq(BmcProperties.MULTIPART_IN_MEMORY_NUM_UPLOAD_THREADS))).thenReturn(1);
        when(mockBooleanAccessor.get(eq(BmcProperties.MULTIPART_IN_MEMORY_WRITE_BUFFER))).thenReturn(true);

        when(mockPropAccessor.asString()).thenReturn(mockStringAccessor);
        when(mockPropAccessor.asInteger()).thenReturn(mockIntegerAccessor);
        when(mockPropAccessor.asBoolean()).thenReturn(mockBooleanAccessor);
    }

    @Test
    public void normalReads() throws IOException, InterruptedException {
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
            bmos.write(generateRandomBytes(WRITE_COUNT));
        }

        bmos.close();

        Thread.sleep(5000);

        Mockito.verify(objectStorage, times(2)).uploadPart(any(UploadPartRequest.class));
        Mockito.verify(objectStorage, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        Mockito.verify(objectStorage, times(1)).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
    }

    private static byte[] generateRandomBytes(int num) {
        byte[] result = new byte[num];
        randomGenerator.nextBytes(result);
        return result;
    }
}
