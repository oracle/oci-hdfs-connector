/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.function.Supplier;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BmcReadAheadFSInputStreamTest {
    @Mock private ObjectStorage objectStorage;

    @Mock private FileStatus status;

    private FileSystem.Statistics statistics;

    private Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> parquetCache;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        String spec = "maximumSize=0";
        parquetCache = CacheBuilder.from(spec).build();

        statistics = new FileSystem.Statistics("oci");
    }

    @Test
    public void testRead() throws IOException {
        String contents = "1234567890ABCDEFGHIJKLMNOPQRST";
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        when(status.getLen()).thenReturn(30L);
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .then(
                        a -> {
                            GetObjectRequest o = (GetObjectRequest) a.getArguments()[0];
                            int startByte = o.getRange().getStartByte().intValue();
                            int endByte = o.getRange().getEndByte().intValue();
                            ByteArrayInputStream inputStream =
                                    new ByteArrayInputStream(
                                            contents.substring(startByte, endByte).getBytes());
                            return GetObjectResponse.builder().inputStream(inputStream).build();
                        });

        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 0, statistics, 10, parquetCache);

        int readIndex = 0;
        for (int i = 0; i < 30; ++i) {
            assertEquals(contents.getBytes()[readIndex++], underTest.read());
        }
        assertEquals(-1, underTest.read());
    }


    @Test
    public void testReadRetryOnceSingleByte() throws IOException {
        // Create a mock InputStream
        InputStream mockStreamFailed = Mockito.mock(InputStream.class);
        InputStream mockStreamSucceeded = Mockito.mock(InputStream.class);
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());


        // Mock reading 2 bytes successfully
        Answer<Integer> answer = invocation -> {
            byte[] buffer = (byte[]) invocation.getArguments()[0];
            int off = (int) invocation.getArguments()[1];
            buffer[off] = 32;
            if (off + 1 < buffer.length) {
                buffer[off + 1] = 125;
                return 2;
            }
            return 1;
        };

        doAnswer(answer).when(mockStreamSucceeded).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        // Configure the mock to throw a TimeoutException on the first call and succeed on the second call
        when(status.getLen()).thenReturn(30L);
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamSucceeded).build());

        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 3, statistics, 10, parquetCache);
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
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        // Mock the InputStream read method to throw IOException on the first read and return data on the second
        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        // Mock reading 2 bytes successfully
        Answer<Integer> answer = invocation -> {
            byte[] buffer = (byte[]) invocation.getArguments()[0];
            int off = (int) invocation.getArguments()[1];
            buffer[off] = 32;
            if (off + 1 < buffer.length) {
                buffer[off + 1] = 125;
                return 2;
            }
            return 1;
        };

        doAnswer(answer).when(mockStreamSucceeded).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        // Configure the mock objectStorage to return the failed stream first, then the succeeded stream
        when(status.getLen()).thenReturn(30L);
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamSucceeded).build());

        BmcReadAheadFSInputStream underTest = new BmcReadAheadFSInputStream(
                objectStorage, status, requestBuilder, 3, statistics, 10, parquetCache);
        underTest.retryPolicy().withDelay(Duration.ofMillis(200));

        // Byte array to read into
        byte[] byteArray = new byte[2];

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        int bytesRead = underTest.read(byteArray, 0, byteArray.length);
        assertEquals(2, bytesRead);
        assertArrayEquals(new byte[]{32, 125}, byteArray);

        // Verify that objectStorage.getObject was called twice
        verify(objectStorage, times(2)).getObject(any(GetObjectRequest.class));
    }


    @Test
    public void testReadNoRetry() throws IOException {
        // Create a mock InputStream
        InputStream mockStreamSucceeded = Mockito.mock(InputStream.class);
        InputStream mockDummyStream = Mockito.mock(InputStream.class);
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");


        // Mock reading 2 bytes successfully
        Answer<Integer> answer = invocation -> {
            byte[] buffer = (byte[]) invocation.getArguments()[0];
            buffer[0] = 32;
            buffer[1] = 125;
            return 2;
        };

        doAnswer(answer).when(mockStreamSucceeded).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        when(status.getLen()).thenReturn(30L);
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamSucceeded).build())
                .thenReturn(GetObjectResponse.builder().inputStream(mockDummyStream).build());

        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 2, statistics, 10, parquetCache);

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        assertEquals(32, underTest.read());
        // Second read, read from local buffer.
        assertEquals(125, underTest.read());
        // Verify that objectStorage.getObject was called only once
        verify(objectStorage, times(1)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testReadRetryFailed() throws IOException {
        // Create a mock InputStream
        InputStream mockStreamFailed = Mockito.mock(InputStream.class);
        Supplier<GetObjectRequest.Builder> requestBuilder = () -> GetObjectRequest.builder().objectName("testObject");

        Mockito.doThrow(new IOException("Read timed out"))
                .when(mockStreamFailed).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        // Configure the mock to throw a TimeoutException on the first call and succeed on the second call

        when(status.getLen()).thenReturn(30L);
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(mockStreamFailed).build());

        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 3, statistics, 10, parquetCache);
        underTest.retryPolicy().withDelay(Duration.ofMillis(200));

        // First read, initial ObjectStorage stream read would fail, retry triggered.
        assertThrows(IOException.class, () -> underTest.read());
        verify(objectStorage, times(4)).getObject(any(GetObjectRequest.class));

        assertThrows(IOException.class, () -> underTest.read(new byte[2]));
        verify(objectStorage, times(8)).getObject(any(GetObjectRequest.class));

        assertThrows(IOException.class, () -> underTest.read(new byte[3], 0, 2));
        verify(objectStorage, times(12)).getObject(any(GetObjectRequest.class));
    }
}
