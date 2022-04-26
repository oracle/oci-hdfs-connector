/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.ws.rs.core.Response;

import com.google.common.cache.CacheBuilder;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.HeadObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.transfer.DownloadConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.powermock.api.mockito.PowerMockito.when;

@Slf4j
@PowerMockIgnore({"javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({CachingObjectStorage.class, ObjectStorageClient.class, CacheBuilder.class})
public class CachingObjectStorageTest {
    private static final String CONTENT = "The quick brown fox jumped over the lazy dog.";
    private static final String CONTENT2 = "Nighttime shows us where they are.";

    @Mock private ObjectStorageClient mockClient;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void tryToDeleteFileWhileReading() throws IOException {
        Random r = new Random();
        byte[] original = new byte[1024 * 1024];
        r.nextBytes(original);

        File tempFile = File.createTempFile(this.getClass().getName() + "-", ".tmp");
        Files.write(tempFile.toPath(), original, StandardOpenOption.CREATE);

        FileInputStream fis = new FileInputStream(tempFile);
        byte[] retrieved = new byte[original.length];
        int rerievedIndex = 0;
        int read = fis.read();

        boolean deletedWhileOpen = tempFile.delete();

        while (read != -1) {
            retrieved[rerievedIndex++] = (byte) read;
            read = fis.read();
        }

        fis.close();

        boolean existsAfter = tempFile.exists();
        boolean deletedAfter = tempFile.delete();

        assertArrayEquals(original, retrieved);

        // Linux: file is deleted immediately (but reading can continue), file doesn't exist anymore and
        // can't be deleted again after reading is donw
        boolean linuxBehavior = deletedWhileOpen && !existsAfter && !deletedAfter;

        // Windows: file cannot be deleted while it is read, which means it still exists after reading is done,
        // and can then be deleted
        boolean windowsBehavior = deletedWhileOpen && !existsAfter && !deletedAfter;

        assertTrue(linuxBehavior || windowsBehavior);
    }

    @Test
    public void testSingleGet() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = 44;
        int expireSeconds = 3;
        getExpireAfterWriteSeconds();

        when(mockClient.getObject(any()))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1")
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireSeconds))
                                .consistencyPolicy(new NoOpConsistencyPolicy())
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();

        GetObjectResponse response = cachingObjectStorageClient.getObject(request);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);
        assertEquals(CONTENT, sw.toString());

        // evicted right away, because of size
        Set<Path> evictedButNotDeleted = cachingObjectStorageClient.getEvictedButNotDeleted();
        assertFalse(evictedButNotDeleted.isEmpty());
        assertTrue(
                evictedButNotDeleted.contains(
                        ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                                .getCachedContentPath()));

        LOG.debug("Trying to force the cache to be cleared");
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(5000);
        System.gc();

        // same number of requests, since expired immediately
        verify(mockClient, times(1)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        cachingObjectStorageClient.close();

        assertTrue(cachingObjectStorageClient.getEvictedButNotDeleted().isEmpty());
    }

    @Test
    public void testTwoGetsCached() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        int expireSeconds = 3;
        getExpireAfterWriteSeconds();

        when(mockClient.getObject(any()))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1")
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .expireAfterWrite(Duration.ofSeconds(expireSeconds))
                                .consistencyPolicy(new NoOpConsistencyPolicy())
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();

        GetObjectResponse response = cachingObjectStorageClient.getObject(request);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);
        assertEquals(CONTENT, sw.toString());
        Path cachedContentPath =
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath();

        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request);
        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);
        assertEquals(CONTENT, sw2.toString());
        Path cachedContentPath2 =
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath();

        // not yet evicted
        assertTrue(cachingObjectStorageClient.getEvictedButNotDeleted().isEmpty());

        LOG.debug("Trying to force the cache to be cleared");
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(5000);
        System.gc();

        LOG.debug("Trying to force the cache to be cleared");
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(5000);
        System.gc();

        Set<Path> evictedButNotDeleted = cachingObjectStorageClient.getEvictedButNotDeleted();
        assertTrue(evictedButNotDeleted.isEmpty());
        assertFalse(cachedContentPath.toFile().exists());
        assertFalse(cachedContentPath2.toFile().exists());

        // same number of requests, since expired immediately
        verify(mockClient, times(1)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        cachingObjectStorageClient.close();
    }

    @Test
    public void testMultipleGets() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        int requestCount = 5;

        long maximumWeight = 44;
        int expireSeconds = getExpireAfterWriteSeconds();

        when(mockClient.getObject(any()))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1")
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireSeconds))
                                .consistencyPolicy(new NoOpConsistencyPolicy())
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();

        List<File> files = new ArrayList<>();
        for (int i = 0; i < requestCount; ++i) {
            GetObjectResponse response = cachingObjectStorageClient.getObject(request);

            StringWriter sw = new StringWriter();
            IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);
            assertEquals(CONTENT, sw.toString());

            File file =
                    ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                            .getCachedContentPath()
                            .toFile();
            files.add(file);

            LOG.debug("Trying to force the cache to be cleared");
            cachingObjectStorageClient.cleanUp();
            System.gc();
            Thread.sleep(500);
            System.gc();
        }

        // same number of requests, since expired immediately
        verify(mockClient, times(requestCount)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        cachingObjectStorageClient.cleanUp();

        System.gc();
        Thread.sleep(500);

        // the files have been evicted and cleared, so they should not still be there
        for (File f : files) {
            assertFalse(f.exists());
        }

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoGets_Cached() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length();
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        when(mockClient.getObject(any()))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1")
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .consistencyPolicy(new NoOpConsistencyPolicy())
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);

        // because the maximumWeight is so small, these are expired immediately from the cache

        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT, sw2.toString());

        // two requests, since data got cached
        verify(mockClient, times(1)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        // the file hasn't been evicted yet, so it should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // and they should be the same file
        assertEquals(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath(),
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoDistinctGets() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length() + CONTENT2.length();
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();
        GetObjectRequest request2 =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object2")
                        .build();

        Predicate<GetObjectRequest> lambda, lambda2;
        when(
                        mockClient.getObject(
                                where(
                                        lambda =
                                                r ->
                                                        request.getObjectName()
                                                                .equals(r.getObjectName()))))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1")
                                    .build();
                        });
        when(
                        mockClient.getObject(
                                where(
                                        lambda2 =
                                                r ->
                                                        request2.getObjectName()
                                                                .equals(r.getObjectName()))))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT2.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT2.getBytes()))
                                    .eTag("etag2")
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .consistencyPolicy(new NoOpConsistencyPolicy())
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        GetObjectResponse response = cachingObjectStorageClient.getObject(request);

        // because the maximumWeight is so small, these are expired immediately from the cache

        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request2);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT2, sw2.toString());

        // two requests, since distinct objects
        verify(mockClient, times(1)).getObject(where(lambda));
        verify(mockClient, times(1)).getObject(where(lambda2));
        verifyNoMoreInteractions(mockClient);

        // the files haven't been evicted yet, so they should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // but they should be the different files
        assertNotEquals(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath(),
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // now the files should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    // strong consistency

    @Test
    public void testTwoGets_StrongConsistency() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = 5;
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        when(mockClient.getObject(where(r -> r.getIfNoneMatch() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .__httpStatusCode__(200)
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);
        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // because the maximumWeight is so small, these are expired immediately from the cache
        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT, sw2.toString());

        // two requests, since expired immediately
        verify(mockClient, times(2)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // the files have been evicted, so they should not still be there
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoGets_Cached_StrongConsistency() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length();
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        when(mockClient.getObject(where(r -> r.getIfNoneMatch() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1a")
                                    .__httpStatusCode__(200)
                                    .build();
                        });
        when(mockClient.getObject(where(r -> r.getIfNoneMatch() != null)))
                .thenReturn(
                        GetObjectResponse.builder()
                                .eTag("etag1a")
                                .__httpStatusCode__(Response.Status.NOT_MODIFIED.getStatusCode())
                                .isNotModified(true)
                                .inputStream(null)
                                .build());

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);
        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT, sw2.toString());

        // two GET request, but the second with if-none-match
        verify(mockClient, times(2)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        // the file hasn't been evicted yet, so it should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // and they should be the same file
        assertEquals(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath(),
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // now it should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoDistinctGets_StrongConsistency() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length() + CONTENT2.length();
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();
        GetObjectRequest request2 =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object2")
                        .build();

        Predicate<GetObjectRequest> lambda1with, lambda1without, lambda2with, lambda2without;

        // first object, without if-none-match
        when(
                        mockClient.getObject(
                                where(
                                        lambda1without =
                                                r ->
                                                        request.getObjectName()
                                                                        .equals(r.getObjectName())
                                                                && r.getIfNoneMatch() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .eTag("etag1a")
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .__httpStatusCode__(200)
                                    .build();
                        });
        // first object, with if-none-match
        when(
                        mockClient.getObject(
                                where(
                                        lambda1with =
                                                r ->
                                                        request.getObjectName()
                                                                        .equals(r.getObjectName())
                                                                && r.getIfNoneMatch() != null)))
                .thenReturn(
                        GetObjectResponse.builder()
                                .eTag("etag1a")
                                .__httpStatusCode__(Response.Status.NOT_MODIFIED.getStatusCode())
                                .isNotModified(true)
                                .inputStream(null)
                                .build());

        // second object, without if-none-match
        when(
                        mockClient.getObject(
                                where(
                                        lambda2without =
                                                r ->
                                                        request2.getObjectName()
                                                                        .equals(r.getObjectName())
                                                                && r.getIfNoneMatch() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT2.length()))
                                    .eTag("etag2a")
                                    .inputStream(new ByteArrayInputStream(CONTENT2.getBytes()))
                                    .__httpStatusCode__(200)
                                    .build();
                        });
        // second object, with if-none-match
        when(
                        mockClient.getObject(
                                where(
                                        lambda2with =
                                                r ->
                                                        request2.getObjectName()
                                                                        .equals(r.getObjectName())
                                                                && r.getIfNoneMatch() != null)))
                .thenReturn(
                        GetObjectResponse.builder()
                                .eTag("etag2a")
                                .__httpStatusCode__(Response.Status.NOT_MODIFIED.getStatusCode())
                                .isNotModified(true)
                                .inputStream(null)
                                .build());

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        // first read of both
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);
        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request2);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT2, sw2.toString());

        // second read of both
        response = cachingObjectStorageClient.getObject(request);
        response2 = cachingObjectStorageClient.getObject(request2);

        sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT2, sw2.toString());

        // two times two GET requests, since distinct objects
        verify(mockClient, times(1)).getObject(where(lambda1without));
        verify(mockClient, times(1)).getObject(where(lambda1with));
        verify(mockClient, times(1)).getObject(where(lambda2without));
        verify(mockClient, times(1)).getObject(where(lambda2with));
        verifyNoMoreInteractions(mockClient);

        // the files haven't been evicted yet, so they should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // but they should be the different files
        assertNotEquals(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath(),
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // now the files should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoDistinctGets_StrongConsistency_ETagChanges() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = 2 * (CONTENT.length() + CONTENT2.length());
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();
        GetObjectRequest request2 =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object2")
                        .build();

        Predicate<GetObjectRequest> lambda, lambda2;
        when(
                        mockClient.getObject(
                                where(
                                        lambda =
                                                r ->
                                                        request.getObjectName()
                                                                .equals(r.getObjectName()))))
                .thenAnswer(
                        new Answer<GetObjectResponse>() {
                            int count = 0;

                            @Override
                            public GetObjectResponse answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                // create a new response, with a new ByteArrayInputStream, every time
                                return GetObjectResponse.builder()
                                        .contentLength(Long.valueOf(CONTENT.length()))
                                        .eTag((++count == 1) ? "etag1a" : "etag1b")
                                        .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                        .__httpStatusCode__(200)
                                        .build();
                            }
                        });
        when(
                        mockClient.getObject(
                                where(
                                        lambda2 =
                                                r ->
                                                        request2.getObjectName()
                                                                .equals(r.getObjectName()))))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT2.length()))
                                    .eTag("etag2a")
                                    .inputStream(new ByteArrayInputStream(CONTENT2.getBytes()))
                                    .__httpStatusCode__(200)
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .aggressiveCacheGarbageCollection(500)
                                .build());

        // first read of both
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);
        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request2);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT2, sw2.toString());

        // second read of both
        LOG.info("Second read of both");
        response =
                cachingObjectStorageClient.getObject(
                        request); // this will have a different etag and reload
        response2 = cachingObjectStorageClient.getObject(request2);

        sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT2, sw2.toString());

        // third read of both
        LOG.info("Third read of both");
        response = cachingObjectStorageClient.getObject(request);
        response2 = cachingObjectStorageClient.getObject(request2);

        sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT2, sw2.toString());

        // three GET requests each
        verify(mockClient, times(3)).getObject(where(lambda));
        verify(mockClient, times(3)).getObject(where(lambda2));
        verifyNoMoreInteractions(mockClient);

        // the files haven't been evicted yet, so they should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // but they should be the different files
        assertNotEquals(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath(),
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // now the files should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    // strong consistency, if-match

    @Test
    public void testTwoGets_StrongConsistency_ifMatch() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = 5;
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        when(mockClient.getObject(where(r -> r.getIfNoneMatch() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1")
                                    .__httpStatusCode__(200)
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .aggressiveCacheGarbageCollection(500)
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .ifMatch("etag1")
                        .build();
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);
        assertEquals(CONTENT, sw.toString());

        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // because the maximumWeight is so small, these are expired immediately from the cache

        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);
        assertEquals(CONTENT, sw2.toString());

        // two requests, since expired immediately
        verify(mockClient, times(2)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // the files have been evicted, so they should not still be there
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        // now it should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoGets_Cached_StrongConsistency_ifMatch() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length();
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        when(mockClient.getObject(where(r -> r.getIfMatch() != null && r.getRange() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1a")
                                    .__httpStatusCode__(200)
                                    .build();
                        });

        // second request is with "etag1a" and for a 1-byte range
        when(
                        mockClient.getObject(
                                where(
                                        r ->
                                                r.getIfMatch().equals("etag1a")
                                                        && r.getRange() != null
                                                        && Long.valueOf(0)
                                                                .equals(r.getRange().getStartByte())
                                                        && Long.valueOf(0)
                                                                .equals(
                                                                        r.getRange()
                                                                                .getEndByte()))))
                .thenReturn(
                        GetObjectResponse.builder()
                                .eTag("etag1a")
                                .__httpStatusCode__(200)
                                .inputStream(
                                        new ByteArrayInputStream(
                                                new byte[] {CONTENT.getBytes()[0]}))
                                .build());

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .ifMatch("etag1a")
                        .build();
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);
        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);

        assertEquals(CONTENT, sw.toString());
        assertEquals(CONTENT, sw2.toString());

        // two GET request, but the second with short range
        verify(mockClient, times(2)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        // the file hasn't been evicted yet, so it should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // and they should be the same file
        assertEquals(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath(),
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // now it should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoGets_Cached_StrongConsistency_ifMatch_changed() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length();
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        when(mockClient.getObject(where(r -> r.getIfMatch() != null && r.getRange() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1a")
                                    .__httpStatusCode__(200)
                                    .build();
                        });

        // second request is with "etag1a" and for a 1-byte range
        when(
                        mockClient.getObject(
                                where(
                                        r ->
                                                r.getIfMatch().equals("etag1a")
                                                        && r.getRange() != null
                                                        && Long.valueOf(0)
                                                                .equals(r.getRange().getStartByte())
                                                        && Long.valueOf(0)
                                                                .equals(
                                                                        r.getRange()
                                                                                .getEndByte()))))
                .thenThrow(
                        new BmcException(
                                Response.Status.PRECONDITION_FAILED.getStatusCode(),
                                Response.Status.PRECONDITION_FAILED.getReasonPhrase(),
                                "etag doesn't match if-match",
                                "request-id"));

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .ifMatch("etag1a")
                        .build();
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);

        try {
            cachingObjectStorageClient.getObject(request);
            fail("Should have failed");
        } catch (BmcException e) {
            assertEquals(412, e.getStatusCode());
        } catch (Exception e) {
            fail("Should have failed with 412");
        }

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        assertEquals(CONTENT, sw.toString());

        // two GET request, but the second with short range
        verify(mockClient, times(2)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        // the file hasn't been evicted yet, so it should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // now it should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoGets_Cached_StrongConsistency_ifMatch_different() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length();
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        when(mockClient.getObject(where(r -> r.getIfMatch() != null && r.getRange() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1a")
                                    .__httpStatusCode__(200)
                                    .build();
                        });

        // second request is with "etag1b"
        when(
                        mockClient.getObject(
                                where(
                                        r ->
                                                r.getIfMatch().equals("etag1b")
                                                        && r.getRange() == null)))
                .thenThrow(
                        new BmcException(
                                Response.Status.PRECONDITION_FAILED.getStatusCode(),
                                Response.Status.PRECONDITION_FAILED.getReasonPhrase(),
                                "etag doesn't match if-match",
                                "request-id"));

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .ifMatch("etag1a")
                        .build();
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);

        GetObjectRequest request2 =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .ifMatch("etag1b")
                        .build();

        try {
            cachingObjectStorageClient.getObject(request2);
            fail("Should have failed");
        } catch (BmcException e) {
            assertEquals(412, e.getStatusCode());
        } catch (Exception e) {
            fail("Should have failed with 412");
        }

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);

        assertEquals(CONTENT, sw.toString());

        // two GET request
        verify(mockClient, times(2)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        // the file hasn't been evicted yet, so it should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // now it should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    @Test
    public void testTwoGets_Cached_StrongConsistency_ifMatch_different_changed() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length() * 2;
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        when(
                        mockClient.getObject(
                                where(
                                        r ->
                                                r.getIfMatch().equals("etag1a")
                                                        && r.getRange() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                                    .eTag("etag1a")
                                    .__httpStatusCode__(200)
                                    .build();
                        });

        // second request is with "etag1b"
        when(
                        mockClient.getObject(
                                where(
                                        r ->
                                                r.getIfMatch().equals("etag1b")
                                                        && r.getRange() == null)))
                .thenAnswer(
                        i -> {
                            // create a new response, with a new ByteArrayInputStream, every time
                            return GetObjectResponse.builder()
                                    .contentLength(Long.valueOf(CONTENT2.length()))
                                    .inputStream(new ByteArrayInputStream(CONTENT2.getBytes()))
                                    .eTag("etag1b")
                                    .__httpStatusCode__(200)
                                    .build();
                        });

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .aggressiveCacheGarbageCollection(1000)
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .ifMatch("etag1a")
                        .build();
        GetObjectResponse response = cachingObjectStorageClient.getObject(request);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(response.getInputStream()), sw);
        assertEquals(CONTENT, sw.toString());

        GetObjectRequest request2 =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .ifMatch("etag1b")
                        .build();
        GetObjectResponse response2 = cachingObjectStorageClient.getObject(request2);

        StringWriter sw2 = new StringWriter();
        IOUtils.copy(new InputStreamReader(response2.getInputStream()), sw2);
        assertEquals(CONTENT2, sw2.toString());

        // two GET request
        verify(mockClient, times(2)).getObject(any());
        verifyNoMoreInteractions(mockClient);

        // the file hasn't been evicted yet, so it should still be there
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // clean-up doesn't do anything
        cachingObjectStorageClient.cleanUp();
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());
        assertTrue(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        // sleep
        TimeUnit.SECONDS.sleep(expireAfterWriteSeconds);
        cachingObjectStorageClient.cleanUp();
        System.gc();
        Thread.sleep(500);

        // now it should have been evicted
        assertFalse(
                ((CachingObjectStorage.CachedInputStream) response2.getInputStream())
                        .getCachedContentPath()
                        .toFile()
                        .exists());

        cachingObjectStorageClient.close();
    }

    private int getExpireAfterWriteSeconds() {
        if (isBeingDebugged()) {
            return Integer.MAX_VALUE;
        }
        return 3;
    }

    // other tests

    @Test
    public void testCacheKeys() {
        GetObjectRequest.Builder baseBuilder =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .ifMatch("ifMatch")
                        .ifNoneMatch("*")
                        .httpResponseContentType("application/json");

        CachingObjectStorage.GetObjectRequestCacheKey key1 =
                new CachingObjectStorage.GetObjectRequestCacheKey(baseBuilder.build());
        CachingObjectStorage.GetObjectRequestCacheKey key1a =
                new CachingObjectStorage.GetObjectRequestCacheKey(baseBuilder.build());
        CachingObjectStorage.GetObjectRequestCacheKey key1b =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.opcClientRequestId("requestId").build());
        CachingObjectStorage.GetObjectRequestCacheKey key1c =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.opcClientRequestId("requestId2").build());
        CachingObjectStorage.GetObjectRequestCacheKey key2 =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.objectName("object2").build());
        CachingObjectStorage.GetObjectRequestCacheKey key2a =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.objectName("object2").opcClientRequestId("requestId3").build());
        CachingObjectStorage.GetObjectRequestCacheKey key2b =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.objectName("object2").opcClientRequestId("requestId4").build());
        CachingObjectStorage.GetObjectRequestCacheKey key3 =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.versionId("1234").build());
        CachingObjectStorage.GetObjectRequestCacheKey key3a =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.versionId("1234").opcClientRequestId("requestId5").build());
        CachingObjectStorage.GetObjectRequestCacheKey key3b =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.versionId("1234").opcClientRequestId("requestId6").build());
        CachingObjectStorage.GetObjectRequestCacheKey key4 =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.ifMatch("ifMatch2").build());
        CachingObjectStorage.GetObjectRequestCacheKey key5 =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.range(new Range(1L, null)).build());
        CachingObjectStorage.GetObjectRequestCacheKey key5a =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.range(new Range(1L, null)).build());
        CachingObjectStorage.GetObjectRequestCacheKey key6 =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.range(new Range(1L, 123L)).build());
        CachingObjectStorage.GetObjectRequestCacheKey key6a =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.range(new Range(1L, 123L)).build());
        CachingObjectStorage.GetObjectRequestCacheKey key7 =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.range(new Range(null, 123L)).build());
        CachingObjectStorage.GetObjectRequestCacheKey key7a =
                new CachingObjectStorage.GetObjectRequestCacheKey(
                        baseBuilder.range(new Range(null, 123L)).build());

        // equals
        assertEquals(key1, key1a);
        assertEquals(key1, key1b);
        assertEquals(key1, key1c);
        assertEquals(key1a, key1b);
        assertEquals(key1a, key1c);
        assertEquals(key1b, key1c);

        assertEquals(key2, key2a);
        assertEquals(key2, key2b);
        assertEquals(key2a, key2b);

        assertEquals(key3, key3a);
        assertEquals(key3, key3b);
        assertEquals(key3a, key3b);

        assertEquals(key5, key5a);
        assertEquals(key6, key6a);
        assertEquals(key7, key7a);

        assertNotEquals(key1, key2);
        assertNotEquals(key1, key3);
        assertNotEquals(key1, key4);
        assertNotEquals(key1, key5);
        assertNotEquals(key1, key6);
        assertNotEquals(key1, key7);
        assertNotEquals(key2, key3);
        assertNotEquals(key2, key4);
        assertNotEquals(key2, key5);
        assertNotEquals(key2, key6);
        assertNotEquals(key2, key7);
        assertNotEquals(key3, key4);
        assertNotEquals(key3, key5);
        assertNotEquals(key3, key6);
        assertNotEquals(key3, key7);
        assertNotEquals(key4, key5);
        assertNotEquals(key4, key6);
        assertNotEquals(key4, key7);
        assertNotEquals(key5, key6);
        assertNotEquals(key5, key7);
        assertNotEquals(key6, key7);

        // hashCode
        assertEquals(key1.hashCode(), key1a.hashCode());
        assertEquals(key1.hashCode(), key1b.hashCode());
        assertEquals(key1.hashCode(), key1c.hashCode());
        assertEquals(key1a.hashCode(), key1b.hashCode());
        assertEquals(key1a.hashCode(), key1c.hashCode());
        assertEquals(key1b.hashCode(), key1.hashCode());

        assertEquals(key2.hashCode(), key2a.hashCode());
        assertEquals(key2.hashCode(), key2b.hashCode());
        assertEquals(key2a.hashCode(), key2b.hashCode());

        assertEquals(key3.hashCode(), key3a.hashCode());
        assertEquals(key3.hashCode(), key3b.hashCode());
        assertEquals(key3a.hashCode(), key3b.hashCode());

        assertEquals(key5.hashCode(), key5a.hashCode());
        assertEquals(key6.hashCode(), key6a.hashCode());
        assertEquals(key7.hashCode(), key7a.hashCode());

        assertNotEquals(key1.hashCode(), key2.hashCode());
        assertNotEquals(key1.hashCode(), key3.hashCode());
        assertNotEquals(key1.hashCode(), key4.hashCode());
        assertNotEquals(key1.hashCode(), key5.hashCode());
        assertNotEquals(key1.hashCode(), key6.hashCode());
        assertNotEquals(key1.hashCode(), key7.hashCode());
        assertNotEquals(key2.hashCode(), key3.hashCode());
        assertNotEquals(key2.hashCode(), key4.hashCode());
        assertNotEquals(key2.hashCode(), key5.hashCode());
        assertNotEquals(key2.hashCode(), key6.hashCode());
        assertNotEquals(key2.hashCode(), key7.hashCode());
        assertNotEquals(key3.hashCode(), key4.hashCode());
        assertNotEquals(key3.hashCode(), key5.hashCode());
        assertNotEquals(key3.hashCode(), key6.hashCode());
        assertNotEquals(key3.hashCode(), key7.hashCode());
        assertNotEquals(key4.hashCode(), key5.hashCode());
        assertNotEquals(key4.hashCode(), key6.hashCode());
        assertNotEquals(key4.hashCode(), key7.hashCode());
        assertNotEquals(key5.hashCode(), key6.hashCode());
        assertNotEquals(key5.hashCode(), key7.hashCode());
        assertNotEquals(key6.hashCode(), key7.hashCode());

        // toString
        assertEquals(key1.toString(), key1a.toString());
        assertEquals(key1.toString(), key1b.toString());
        assertEquals(key1.toString(), key1c.toString());
        assertEquals(key1a.toString(), key1b.toString());
        assertEquals(key1a.toString(), key1c.toString());
        assertEquals(key1b.toString(), key1.toString());

        assertEquals(key2.toString(), key2a.toString());
        assertEquals(key2.toString(), key2b.toString());
        assertEquals(key2a.toString(), key2b.toString());

        assertEquals(key3.toString(), key3a.toString());
        assertEquals(key3.toString(), key3b.toString());
        assertEquals(key3a.toString(), key3b.toString());

        assertNotEquals(key1.toString(), key2.toString());
        assertNotEquals(key1.toString(), key3.toString());
        assertNotEquals(key2.toString(), key3.toString());
    }

    @RequiredArgsConstructor
    private static class LambdaMatcher<T> extends ArgumentMatcher<T> {
        private final Predicate<T> predicate;

        @Override
        public boolean matches(Object o) {
            return o != null && predicate.test((T) o);
        }
    }

    private static <X> X where(Predicate<X> predicate) {
        return argThat(new LambdaMatcher<>(predicate));
    }

    @Test
    public void testTwoFailures() throws Exception {
        Path directory = Paths.get(System.getProperty("java.io.tmpdir") + "/test");

        long maximumWeight = 5;

        BmcException bmcException =
                new BmcException(404, "NotAuthorizedOrNotFound", "test", "test-requestId");
        when(mockClient.getObject(any())).thenThrow(bmcException);
        when(mockClient.headObject(any())).thenThrow(bmcException);

        int maxRetries = 1;
        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .consistencyPolicy(new NoOpConsistencyPolicy())
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(5))
                                .downloadConfiguration(
                                        DownloadConfiguration.builder()
                                                .maxRetries(maxRetries)
                                                .maxBackoff(Duration.ofMillis(300))
                                                .build())
                                .build());

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();
        try {
            cachingObjectStorageClient.getObject(request);
            fail("Should have thrown");
        } catch (BmcException bmce) {
            assertEquals(bmcException, bmce);
        }

        try {
            cachingObjectStorageClient.getObject(request);
            fail("Should have thrown");
        } catch (BmcException bmce) {
            assertEquals(bmcException, bmce);
        }

        // check other, non-cached methods

        HeadObjectRequest headRequest =
                HeadObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();
        try {
            cachingObjectStorageClient.headObject(headRequest);
            fail("Should have thrown");
        } catch (BmcException bmce) {
            assertEquals(bmcException, bmce);
        }

        try {
            cachingObjectStorageClient.headObject(headRequest);
            fail("Should have thrown");
        } catch (BmcException bmce) {
            assertEquals(bmcException, bmce);
        }

        // two requests, since failures aren't cached
        verify(mockClient, times(2)).getObject(any());
        verify(mockClient, times(2)).headObject(any());
        verifyNoMoreInteractions(mockClient);

        cachingObjectStorageClient.close();
    }

    public static boolean isBeingDebugged() {
        return java.lang.management.ManagementFactory.getRuntimeMXBean()
                .getInputArguments()
                .toString()
                .contains("jdwp");
    }

    //
    // Phantom reference tests
    //

    @Test
    public void testPhantomReferences() throws Exception {
        ReferenceQueue<CachingObjectStorage.PathHolder> referenceQueue = new ReferenceQueue<>();
        List<CachingObjectStorage.PathPhantomReference> references = new ArrayList<>();
        List<CachingObjectStorage.PathHolder> pathObjects = new ArrayList<>();

        for (int i = 0; i < 10; ++i) {
            CachingObjectStorage.PathHolder pathObject =
                    new CachingObjectStorage.PathHolder(Paths.get("/tmp/" + i + ".txt"));
            pathObjects.add(pathObject);
        }

        CachingObjectStorage.PathHolder path0 = pathObjects.get(0);

        for (int i = 0; i < 10; ++i) {
            references.add(
                    new CachingObjectStorage.PathPhantomReference(
                            pathObjects.get(i), referenceQueue, i));
        }

        for (int i = 0; i < 10; ++i) {
            PhantomReference<CachingObjectStorage.PathHolder> reference = references.get(i);
            System.out.println(reference);
            assertFalse(reference.isEnqueued());
            assertFalse(((CachingObjectStorage.PathPhantomReference) reference).isCleared());
        }

        pathObjects = null;
        System.gc();
        Thread.sleep(5000);
        System.gc();

        for (int i = 0; i < 10; ++i) {
            PhantomReference<CachingObjectStorage.PathHolder> reference = references.get(i);
            System.out.println(reference);
            if (i == 0) {
                assertFalse(reference.isEnqueued());
                assertFalse(((CachingObjectStorage.PathPhantomReference) reference).isCleared());
            } else {
                assertTrue(reference.isEnqueued());
                assertFalse(((CachingObjectStorage.PathPhantomReference) reference).isCleared());
            }
        }

        Reference<? extends CachingObjectStorage.PathHolder> referenceFromQueue;
        while ((referenceFromQueue = referenceQueue.poll()) != null) {
            referenceFromQueue.clear();
        }

        for (int i = 0; i < 10; ++i) {
            PhantomReference<CachingObjectStorage.PathHolder> reference = references.get(i);
            System.out.println(reference);
            if (i == 0) {
                assertFalse(reference.isEnqueued());
                assertFalse(((CachingObjectStorage.PathPhantomReference) reference).isCleared());
            } else {
                assertFalse(reference.isEnqueued());
                assertTrue(((CachingObjectStorage.PathPhantomReference) reference).isCleared());
            }
        }
    }

    @Test
    public void testPrepopulateCache() throws Exception {
        Path directory = Files.createTempDirectory(this.getClass().getSimpleName());
        directory.toFile().deleteOnExit();

        long maximumWeight = CONTENT.length();
        int expireAfterWriteSeconds = getExpireAfterWriteSeconds();

        CachingObjectStorage cachingObjectStorageClient =
                CachingObjectStorage.build(
                        CachingObjectStorage.newConfiguration()
                                .client(mockClient)
                                .cacheDirectory(directory)
                                .consistencyPolicy(new NoOpConsistencyPolicy())
                                .maximumWeight(maximumWeight)
                                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                                .build());

        GetObjectResponse response =
                GetObjectResponse.builder()
                        .contentLength(Long.valueOf(CONTENT.length()))
                        .inputStream(new ByteArrayInputStream(CONTENT.getBytes()))
                        .eTag("etag")
                        .__httpStatusCode__(200)
                        .build();

        GetObjectRequest request =
                GetObjectRequest.builder()
                        .namespaceName("namespace")
                        .bucketName("bucket")
                        .objectName("object")
                        .build();

        // Prepopulate the cache.
        cachingObjectStorageClient.prepopulateCache(request, response);

        // Retrieve the response from the cache to validate its content.
        GetObjectResponse cacheResponse = cachingObjectStorageClient.getObject(request);

        StringWriter sw = new StringWriter();
        IOUtils.copy(new InputStreamReader(cacheResponse.getInputStream()), sw);

        assertEquals(CONTENT, sw.toString());
        assertEquals(response.getContentLength(), cacheResponse.getContentLength());
        assertEquals(response.getETag(), cacheResponse.getETag());
        assertEquals(response.get__httpStatusCode__(), cacheResponse.get__httpStatusCode__());
    }
}
