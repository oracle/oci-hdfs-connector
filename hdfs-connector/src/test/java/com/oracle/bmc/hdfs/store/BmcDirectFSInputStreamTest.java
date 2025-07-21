/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.time.Duration;
import java.util.function.Supplier;

import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.monitoring.OCIMetricKeys;
import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.retrier.RetryConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BmcDirectFSInputStreamTest {
    @Mock private InputStream inputStream;

    @Mock private ObjectStorage objectStorage;

    @Mock private FileStatus fileStatus;

    private static final FileSystem.Statistics statistics = new FileSystem.Statistics("ocitest");
    private static final Random randomGenerator = new Random();

    @Mock private BmcPropertyAccessor mockPropAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Long> mockLongAccessor;

    @Before
    public void setUp() {
        when(mockLongAccessor.get(eq(BmcProperties.RETRY_TIMEOUT_IN_SECONDS))).thenReturn(30L);
        when(mockLongAccessor.get(eq(BmcProperties.RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS))).thenReturn(0L);
        when(mockPropAccessor.asLong()).thenReturn(mockLongAccessor);
    }

    private static final Supplier<GetObjectRequest.Builder> getObjectRequestBuilderSupplier =
            new Supplier<GetObjectRequest.Builder>() {
                @Override
                public GetObjectRequest.Builder get() {
                    return GetObjectRequest.builder();
                }
            };

    private static final int READ_COUNT = 100;

    @Test
    public void normalReads() throws IOException {
        when(inputStream.read()).thenReturn(generateRandomByte());
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(inputStream).build());

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcDirectFSInputStream directFSInputStream =
                new BmcDirectFSInputStream(
                        objectStorage, fileStatus, getObjectRequestBuilderSupplier, 0, statistics, retryMetricsCollector);

        for (int i = 0; i < READ_COUNT; i++) {
            directFSInputStream.read();
        }

        // ObjectStorage#read should be invoked exactly once
        verify(objectStorage, times(1)).getObject(any(GetObjectRequest.class));
        // InputStream#read should be invoked READ_COUNT number of times
        verify(inputStream, times(READ_COUNT)).read();
    }

    @Test
    public void readsWithFailure() throws IOException {
        List<Integer> failurePositions = new ArrayList<>();
        // Assume the first read does not result in a failure
        int lastFailurePosition = 1;
        while (true) {
            // Consecutive failures not expected during reads
            lastFailurePosition +=
                    (2 + randomGenerator.nextInt(READ_COUNT - lastFailurePosition - 1));
            if (lastFailurePosition >= (READ_COUNT - 1)) {
                break;
            }
            failurePositions.add(lastFailurePosition);
        }

        // Since the first read is not a failure, return a random byte when read
        Stubber stubber = doReturn(generateRandomByte());
        for (int i = 1, j = 0; i < READ_COUNT; i++) {
            if (j < failurePositions.size() && i == failurePositions.get(j)) {
                // Throw an IOException if this was a failure position
                stubber = stubber.doThrow(new IOException("test exception"));
                j++;
            } else {
                // Else return a random byte
                stubber = stubber.doReturn(generateRandomByte());
            }
        }

        stubber.when(inputStream).read();

        ArgumentCaptor<GetObjectRequest> argumentCaptor =
                ArgumentCaptor.forClass(GetObjectRequest.class);
        when(objectStorage.getObject(argumentCaptor.capture()))
                .thenAnswer(
                        new Answer<GetObjectResponse>() {
                            @Override
                            public GetObjectResponse answer(InvocationOnMock invocationOnMock) {
                                final GetObjectRequest getObjectRequest =
                                        invocationOnMock.getArgumentAt(0, GetObjectRequest.class);
                                final Range contentRange =
                                        getObjectRequest.getRange() == null
                                                ? new Range(0L, null)
                                                : getObjectRequest.getRange();
                                return GetObjectResponse.builder()
                                        .inputStream(inputStream)
                                        .contentRange(contentRange)
                                        .eTag("dummy etag")
                                        .contentLength(0L)
                                        .build();
                            }
                        });
        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        final BmcDirectFSInputStream directFSInputStream =
                new BmcDirectFSInputStream(
                        objectStorage, fileStatus, getObjectRequestBuilderSupplier, 1, statistics, retryMetricsCollector);

        for (int i = 0; i < READ_COUNT; i++) {
            directFSInputStream.read();
        }

        // ObjectStorage#read should be invoked 1 more than the count of items in the failurePositions list
        verify(objectStorage, times(1 + failurePositions.size()))
                .getObject(any(GetObjectRequest.class));
        // InputStream#read should be invoked again for all the failurePositions
        verify(inputStream, times(READ_COUNT + failurePositions.size())).read();

        assertEquals(1 + failurePositions.size(), argumentCaptor.getAllValues().size());
        for (int i = 0, counter = 0; i < argumentCaptor.getAllValues().size(); i++) {
            final Range range = argumentCaptor.getAllValues().get(i).getRange();
            if (i == 0) {
                assertEquals(null, range);
            } else {
                assertEquals(
                        failurePositions.get(i - 1) - counter++, range.getStartByte().intValue());
                assertEquals(null, range.getEndByte());
            }
        }
    }

    @Test
    public void testReadRetryOnceSingleByte() throws IOException {
        // Create a mock InputStream
        InputStream mockStreamFailed = Mockito.mock(InputStream.class);
        InputStream mockStreamSucceeded = Mockito.mock(InputStream.class);
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read();


        // Mock reading 2 bytes successfully
        when(mockStreamSucceeded.read())
                .thenReturn(32)
                .thenReturn(125);

        // Configure the mock to throw a TimeoutException on the first call and succeed on the second call
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamSucceeded).build());

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcDirectFSInputStream underTest =
                new BmcDirectFSInputStream(
                        objectStorage, fileStatus, getObjectRequestBuilderSupplier, 1, statistics, retryMetricsCollector);
        underTest.retryPolicy().withDelay(Duration.ofMillis(200));

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        assertEquals(32, underTest.read());
        // Second read, read from local buffer.
        assertEquals(125, underTest.read());
        // Verify that objectStorage.getObject was called twice
        verify(objectStorage, times(2)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testReadRetryOnceMultiByte() throws IOException {
        // Create mock InputStreams
        InputStream mockStreamFailed = Mockito.mock(InputStream.class);
        InputStream mockStreamSucceeded = Mockito.mock(InputStream.class);

        // Mock the InputStream read method to throw IOException on the first read and return data on the second
        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read(any(byte[].class), anyInt(), anyInt());

        byte[] buffer = new byte[]{32, 125};  // bytes to be read by the mockStreamSucceeded
        Mockito.when(mockStreamSucceeded.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
                    byte[] b = (byte[]) invocation.getArguments()[0];
                    int off = (int) invocation.getArguments()[1];
                    int len = (int) invocation.getArguments()[2];
                    System.arraycopy(buffer, 0, b, off, len);
                    return len;
                });

        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        // Configure the mock objectStorage to return the failed stream first, then the succeeded stream
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamSucceeded).build());

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcDirectFSInputStream underTest = new BmcDirectFSInputStream(
                objectStorage, fileStatus, requestBuilder, 1, statistics, retryMetricsCollector);
        underTest.retryPolicy().withDelay(Duration.ofMillis(200));

        // Byte array to read into
        byte[] byteArray = new byte[2];

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        int bytesRead = underTest.read(byteArray, 0, byteArray.length);
        assertEquals(2, bytesRead);
        assertArrayEquals(buffer, byteArray);

        // Verify that objectStorage.getObject was called twice
        verify(objectStorage, times(2)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testReadNoRetrySingleByte() throws IOException {
        // Create a mock InputStream
        InputStream mockStreamSucceeded = Mockito.mock(InputStream.class);
        InputStream mockDummyStream = Mockito.mock(InputStream.class);
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");


        // Mock reading 2 bytes successfully
        when(mockStreamSucceeded.read())
                .thenReturn(32)
                .thenReturn(125);

        //when(status.getLen()).thenReturn(30L);
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamSucceeded).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockDummyStream).build());

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcDirectFSInputStream underTest =
                new BmcDirectFSInputStream(
                        objectStorage, fileStatus, getObjectRequestBuilderSupplier, 1, statistics, retryMetricsCollector);

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        assertEquals(32, underTest.read());
        // Second read, read from local buffer.
        assertEquals(125, underTest.read());
        // Verify that objectStorage.getObject was called only once
        verify(objectStorage, times(1)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testReadNoRetryMultiByte() throws IOException {
        // Create mock InputStreams
        InputStream mockStreamSucceeded = Mockito.mock(InputStream.class);
        InputStream mockDummyStream = Mockito.mock(InputStream.class);

        byte[] buffer = new byte[]{32, 125};  // bytes to be read by the mockStreamSucceeded
        Mockito.when(mockStreamSucceeded.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
                    byte[] b = (byte[]) invocation.getArguments()[0];
                    int off = (int) invocation.getArguments()[1];
                    int len = (int) invocation.getArguments()[2];
                    System.arraycopy(buffer, 0, b, off, len);
                    return len;
                });

        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        // Configure the mock objectStorage to return the failed stream first, then the succeeded stream
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamSucceeded).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockDummyStream).build());

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcDirectFSInputStream underTest = new BmcDirectFSInputStream(
                objectStorage, fileStatus, requestBuilder, 1, statistics, retryMetricsCollector);
        underTest.retryPolicy().withDelay(Duration.ofMillis(200));

        // Byte array to read into
        byte[] byteArray = new byte[2];

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        int bytesRead = underTest.read(byteArray, 0, byteArray.length);
        assertEquals(2, bytesRead);
        assertArrayEquals(buffer, byteArray);

        // Verify that objectStorage.getObject was called twice
        verify(objectStorage, times(1)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testReadRetryFailed() throws IOException {
        // Create a mock InputStream
        InputStream mockStreamFailed = Mockito.mock(InputStream.class);
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read();
        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read(Mockito.any(byte[].class));
        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        // Configure the mock to throw a TimeoutException on the first call and succeed on the second call

        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build());

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcDirectFSInputStream underTest =
                new BmcDirectFSInputStream(
                        objectStorage, fileStatus, getObjectRequestBuilderSupplier, 3, statistics, retryMetricsCollector);
        underTest.retryPolicy().withDelay(Duration.ofMillis(200));

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        assertThrows(IOException.class, () -> underTest.read());
        verify(objectStorage, times(4)).getObject(any(GetObjectRequest.class));

        assertThrows(IOException.class, () -> underTest.read(new byte[2]));
        verify(objectStorage, times(8)).getObject(any(GetObjectRequest.class));

        assertThrows(IOException.class, () -> underTest.read(new byte[3], 0, 2));
        verify(objectStorage, times(12)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testReadRetryFailedThenRecover() throws IOException {
        // Create a mock InputStream
        InputStream mockStreamFailed = Mockito.mock(InputStream.class);
        InputStream mockStreamSucceeded = Mockito.mock(InputStream.class);
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read();

        when(mockStreamSucceeded.read())
                .thenReturn(32)
                .thenReturn(125);
        // Configure the mock to throw a TimeoutException on the first call and succeed on the second call

        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamSucceeded).build());

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcDirectFSInputStream underTest =
                new BmcDirectFSInputStream(
                        objectStorage, fileStatus, getObjectRequestBuilderSupplier, 3, statistics, retryMetricsCollector);
        underTest.retryPolicy().withDelay(Duration.ofMillis(200));

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        assertThrows(IOException.class, () -> underTest.read());
        verify(objectStorage, times(4)).getObject(any(GetObjectRequest.class));

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        assertEquals(32, underTest.read());
        // Second read, read from local buffer.
        assertEquals(125, underTest.read());

        verify(objectStorage, times(5)).getObject(any(GetObjectRequest.class));
    }

    private static int generateRandomByte() {
        return randomGenerator.nextInt(256);
    }
}
