/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.monitoring.OCIMetricKeys;
import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import org.apache.hadoop.fs.FSExceptionMessages;
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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BmcReadAheadFSInputStreamTest {
    @Mock private ObjectStorage objectStorage;

    @Mock private FileStatus status;

    private FileSystem.Statistics statistics;

    private Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> parquetCache;

    private boolean firstReadOptimizationForTTFBEnabled = false;

    // In the tests below parquet cache has been disabled in the spec in init() method.
    private boolean parquetCacheEnabled = false;

    @Mock private BmcPropertyAccessor mockPropAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Long> mockLongAccessor;
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(mockLongAccessor.get(Mockito.eq(BmcProperties.RETRY_TIMEOUT_IN_SECONDS))).thenReturn(30L);
        Mockito.when(mockLongAccessor.get(Mockito.eq(BmcProperties.RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS))).thenReturn(0L);
        Mockito.when(mockPropAccessor.asLong()).thenReturn(mockLongAccessor);
    }

    @Before
    public void init() {
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

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 0, statistics, 10, parquetCache, retryMetricsCollector,
                        firstReadOptimizationForTTFBEnabled, parquetCacheEnabled);

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

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 3, statistics, 10, parquetCache, retryMetricsCollector,
                        firstReadOptimizationForTTFBEnabled, parquetCacheEnabled);
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

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcReadAheadFSInputStream underTest = new BmcReadAheadFSInputStream(
                objectStorage, status, requestBuilder, 3, statistics, 10, parquetCache, retryMetricsCollector,
                firstReadOptimizationForTTFBEnabled, parquetCacheEnabled);
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

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 2, statistics, 10, parquetCache, retryMetricsCollector,
                        firstReadOptimizationForTTFBEnabled, parquetCacheEnabled);

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

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 3, statistics, 10, parquetCache, retryMetricsCollector,
                        firstReadOptimizationForTTFBEnabled, parquetCacheEnabled);
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
    public void testReadFully() throws IOException {
        String contents = "1234567890ABCDEFGHIJKLMNOPQRST";
        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");

        when(status.getLen()).thenReturn((long) contents.length());
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .then(
                        a -> {
                            GetObjectRequest o = (GetObjectRequest) a.getArguments()[0];
                            int startByte = o.getRange().getStartByte().intValue();
                            int endByte =
                                    o.getRange().getEndByte() == null
                                            ? contents.length()
                                            : o.getRange().getEndByte().intValue();
                            ByteArrayInputStream inputStream =
                                    new ByteArrayInputStream(
                                            contents.substring(startByte, endByte).getBytes());
                            return GetObjectResponse.builder().inputStream(inputStream).build();
                        });

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 0, statistics, 10, parquetCache, retryMetricsCollector,
                        false, parquetCacheEnabled);

        byte[] buffer = new byte[10];
        underTest.readFully(5, buffer, 0, 10);

        assertArrayEquals(contents.substring(5, 15).getBytes(), buffer);
        verify(objectStorage, times(1)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testReadFullyZeroLength() throws IOException {
        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");

        when(status.getLen()).thenReturn(30L);

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 0, statistics, 10, parquetCache, retryMetricsCollector, false, parquetCacheEnabled);

        underTest.readFully(5, new byte[0], 0, 0);

        verify(objectStorage, times(0)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testCloseClearsBuffer() throws IOException {
        String contents = "HELLO";
        when(status.getLen()).thenReturn((long) contents.length());
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            GetObjectRequest req = (GetObjectRequest) invocation.getArguments()[0];
                            int startByte = req.getRange().getStartByte().intValue();
                            int endByte = req.getRange().getEndByte().intValue();
                            return GetObjectResponse.builder()
                                    .inputStream(
                                            new ByteArrayInputStream(
                                                    contents.substring(startByte, endByte).getBytes()))
                                    .build();
                        });

        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ , mockPropAccessor);
        BmcReadAheadFSInputStream underTest =
                new BmcReadAheadFSInputStream(
                        objectStorage, status, requestBuilder, 0, statistics, 2, parquetCache, retryMetricsCollector,
                        false, parquetCacheEnabled);

        assertEquals((int) contents.charAt(0), underTest.read());

        underTest.close();

        IOException ioe = assertThrows(IOException.class, () -> underTest.read());
        assertEquals(FSExceptionMessages.STREAM_IS_CLOSED, ioe.getMessage());
    }

    @Test
    public void testEarlyReturnWhenAtEOF() throws IOException {
        long fileSize = 100L;
        when(status.getLen()).thenReturn(fileSize);

        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenThrow(new AssertionError("getObject() should not be called when at EOF"));

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ, mockPropAccessor);
        BmcReadAheadFSInputStream stream = new BmcReadAheadFSInputStream(
                objectStorage,
                status,
                requestBuilder,
                0,
                statistics,
                10,
                CacheBuilder.newBuilder().build(),
                retryMetricsCollector,
                false, false
        );

        // Simulate end-of-file by setting position manually
        stream.currentPosition = fileSize;

        int result = stream.read();
        assertEquals(-1, result);
        verify(objectStorage, times(0)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testFooterReadWithParquetCacheLoad() throws IOException {
        // Simulate a Parquet file layout: metadata + 8-byte footer
        byte[] metadata = "META".getBytes(StandardCharsets.UTF_8); // 4 bytes
        byte[] footer = new byte[] {4, 0, 0, 0, 'P', 'A', 'R', '1'}; // metadata length = 4
        byte[] contents = ByteBuffer.allocate(metadata.length + footer.length)
                .put(metadata)
                .put(footer)
                .array();

        // Expected file length = metadata + footer
        long fileSize = contents.length;
        int footerStart = contents.length - 8;

        when(status.getLen()).thenReturn(fileSize);
        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");

        List<Range> requestedRanges = new ArrayList<>();

        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenAnswer(invocation -> {
                    GetObjectRequest req = (GetObjectRequest) invocation.getArguments()[0];
                    Range r = req.getRange();
                    requestedRanges.add(r);
                    int start = r.getStartByte().intValue();
                    int end = r.getEndByte().intValue();
                    ByteArrayInputStream in = new ByteArrayInputStream(Arrays.copyOfRange(contents, start, end));
                    return GetObjectResponse.builder().inputStream(in).build();
                });

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ, mockPropAccessor);

        Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> parquetCache =
                CacheBuilder.newBuilder().build();

        BmcReadAheadFSInputStream stream = new BmcReadAheadFSInputStream(
                objectStorage,
                status,
                requestBuilder,
                0,
                statistics,
                16,
                parquetCache,
                retryMetricsCollector,
                false, true
        );

        byte[] footerBuf = new byte[8];
        stream.readFully(footerStart, footerBuf, 0, 8);
        assertArrayEquals(footer, footerBuf);

        assertEquals(2, requestedRanges.size());
        assertEquals(footerStart, requestedRanges.get(0).getStartByte().intValue());
        assertEquals(fileSize, requestedRanges.get(0).getEndByte().intValue());

        int metadataStart = footerStart - metadata.length;
        assertEquals(metadataStart, requestedRanges.get(1).getStartByte().intValue());
        assertEquals(footerStart, requestedRanges.get(1).getEndByte().intValue());

        assertEquals(1, parquetCache.size());
    }

    @Test
    public void testMetadataReadFromParquetCache() throws IOException {
        byte[] metadata = "META".getBytes(StandardCharsets.UTF_8);
        byte[] footer = new byte[] {4, 0, 0, 0, 'P', 'A', 'R', '1'};
        long fileSize = metadata.length + footer.length;
        long metadataStart = fileSize - footer.length - metadata.length;

        when(status.getLen()).thenReturn(fileSize);
        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");

        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenThrow(new AssertionError("getObject() should not be called for cached metadata"));

        BmcReadAheadFSInputStream.ParquetFooterInfo cached = new BmcReadAheadFSInputStream.ParquetFooterInfo();
        cached.footer = footer;
        cached.metadata = metadata;
        cached.metadataLen = metadata.length;
        cached.metadataStart = metadataStart;

        Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> parquetCache =
                CacheBuilder.newBuilder().build();
        parquetCache.put("testObject", cached);

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ, mockPropAccessor);

        BmcReadAheadFSInputStream stream = new BmcReadAheadFSInputStream(
                objectStorage,
                status,
                requestBuilder,
                0,
                statistics,
                16,
                parquetCache,
                retryMetricsCollector,
                false, true
        );


        byte[] buf = new byte[metadata.length];
        stream.readFully(metadataStart, buf, 0, buf.length);

        assertArrayEquals(metadata, buf);
        verify(objectStorage, times(0)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testParquetCacheMissForNonFooter() throws IOException {
        byte[] contents = new byte[100];
        for (int i = 0; i < contents.length; i++) {
            contents[i] = (byte) i;
        }

        when(status.getLen()).thenReturn((long) contents.length);

        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");

        List<Range> requestedRanges = new ArrayList<>();

        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenAnswer(invocation -> {
                    GetObjectRequest req = (GetObjectRequest) invocation.getArguments()[0];
                    Range r = req.getRange();
                    requestedRanges.add(r);
                    int start = r.getStartByte().intValue();
                    int end = r.getEndByte().intValue();
                    ByteArrayInputStream in = new ByteArrayInputStream(Arrays.copyOfRange(contents, start, end));
                    return GetObjectResponse.builder().inputStream(in).build();
                });

        Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> parquetCache =
                CacheBuilder.newBuilder().build();

        RetryMetricsCollector retryMetricsCollector =
                new RetryMetricsCollector(OCIMetricKeys.READ, mockPropAccessor);

        BmcReadAheadFSInputStream stream = new BmcReadAheadFSInputStream(
                objectStorage,
                status,
                requestBuilder,
                0,
                statistics,
                16,
                parquetCache,
                retryMetricsCollector,
                false, true
        );

        byte[] buf = new byte[10];
        stream.readFully(50, buf, 0, buf.length);
        assertArrayEquals(Arrays.copyOfRange(contents, 50, 60), buf);

        assertEquals(1, requestedRanges.size());
        Range r = requestedRanges.get(0);
        assertEquals(50, r.getStartByte().intValue());
        assertTrue(r.getEndByte().intValue() <= contents.length);
        assertNull(parquetCache.getIfPresent("testObject"));

    }

    @Test
    public void testParquetDisabledGenericRead() throws IOException {
        byte[] contents = new byte[100];
        for (int i = 0; i < contents.length; i++) {
            contents[i] = (byte) (i % 256);
        }

        long fileSize = contents.length;
        int readOffset = contents.length - 8;

        when(status.getLen()).thenReturn(fileSize);

        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");

        List<Range> requestedRanges = new ArrayList<>();

        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenAnswer(invocation -> {
                    GetObjectRequest req = (GetObjectRequest) invocation.getArguments()[0];
                    Range r = req.getRange();
                    requestedRanges.add(r);
                    int start = r.getStartByte().intValue();
                    int end = r.getEndByte().intValue();
                    return GetObjectResponse.builder()
                            .inputStream(new ByteArrayInputStream(Arrays.copyOfRange(contents, start, end)))
                            .build();
                });

        // Empty cache
        Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> parquetCache =
                CacheBuilder.newBuilder().build();

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ, mockPropAccessor);

        BmcReadAheadFSInputStream stream = new BmcReadAheadFSInputStream(
                objectStorage,
                status,
                requestBuilder,
                0,
                statistics,
                16,
                parquetCache,
                retryMetricsCollector,
                false, false
        );

        // Read the last 8 bytes
        byte[] buf = new byte[8];
        stream.readFully(readOffset, buf, 0, 8);

        assertArrayEquals(Arrays.copyOfRange(contents, readOffset, readOffset + 8), buf);

        assertEquals(1, requestedRanges.size());
        Range r = requestedRanges.get(0);
        assertEquals(readOffset, r.getStartByte().intValue());
        assertEquals(fileSize, r.getEndByte().intValue());
    }

    @Test
    public void testFooterCacheLoaderThrows() throws IOException, ExecutionException {
        long fileSize = 100L;
        long footerStart = fileSize - 8;

        when(status.getLen()).thenReturn(fileSize);

        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("testObject");

        Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> parquetCache = Mockito.mock(Cache.class);
        when(parquetCache.get(Mockito.eq("testObject"), Mockito.any()))
                .thenThrow(new ExecutionException(new RuntimeException("simulated failure")));

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ, mockPropAccessor);

        BmcReadAheadFSInputStream stream = new BmcReadAheadFSInputStream(
                objectStorage,
                status,
                requestBuilder,
                0,
                statistics,
                128 * 1024,
                parquetCache,
                retryMetricsCollector,
                false, true
        );

        byte[] buf = new byte[8];

        IOException ex = assertThrows(IOException.class, () -> {
            stream.readFully(footerStart, buf, 0, 8);
        });

        assertTrue(ex.getMessage().contains("Error getting file"));
        verify(objectStorage, times(0)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testToStringReturnsExpectedFormat() throws IOException {
        when(status.getLen()).thenReturn(100L);

        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("my-object");

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ, mockPropAccessor);

        BmcReadAheadFSInputStream stream = new BmcReadAheadFSInputStream(
                objectStorage,
                status,
                requestBuilder,
                0,
                statistics,
                16,
                CacheBuilder.newBuilder().build(),
                retryMetricsCollector,
                false, false
        );

        String str = stream.toString();
        assertEquals("ReadAhead Stream for my-object", str);

        // Ensure second call returns same string (cached path)
        assertSame(str, stream.toString());
    }

    @Test
    public void testParquetCacheRemovalListener() {
        // Create a removal listener that sets a flag when invoked
        AtomicBoolean removalCalled = new AtomicBoolean(false);

        RemovalListener<String, BmcReadAheadFSInputStream.ParquetFooterInfo> listener = rn -> {
            removalCalled.set(true);
            BmcReadAheadFSInputStream.getParquetCacheRemovalListener().onRemoval(rn);
        };

        // Create a cache with max size 1 to force eviction
        Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> cache =
                CacheBuilder.newBuilder()
                        .maximumSize(1)
                        .removalListener(listener)
                        .build();

        BmcReadAheadFSInputStream.ParquetFooterInfo info = new BmcReadAheadFSInputStream.ParquetFooterInfo();
        cache.put("first", info);
        cache.put("second", info);

        assertTrue("RemovalListener should have been called", removalCalled.get());
    }

    @Test
    public void testConfigureParquetCacheWithEviction() throws IOException, NoSuchFieldException, IllegalAccessException {
        String cacheSpec = "maximumSize=1";

        when(status.getLen()).thenReturn(100L);

        Supplier<GetObjectRequest.Builder> requestBuilder =
                () -> GetObjectRequest.builder().objectName("test-object");

        RetryMetricsCollector retryMetricsCollector = new RetryMetricsCollector(OCIMetricKeys.READ, mockPropAccessor);

        BmcReadAheadFSInputStream stream = new BmcReadAheadFSInputStream(
                objectStorage,
                status,
                requestBuilder,
                0,
                statistics,
                16,
                cacheSpec,
                retryMetricsCollector,
                false, true
        );

        Field field = BmcReadAheadFSInputStream.class.getDeclaredField("parquetCache");
        field.setAccessible(true);
        Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo> cache =
                (Cache<String, BmcReadAheadFSInputStream.ParquetFooterInfo>) field.get(stream);

        BmcReadAheadFSInputStream.ParquetFooterInfo info = new BmcReadAheadFSInputStream.ParquetFooterInfo();
        cache.put("first", info);
        cache.put("second", info);

        assertNull("First entry should be evicted", cache.getIfPresent("first"));
        assertNotNull("Second entry should exist", cache.getIfPresent("second"));
    }
}
