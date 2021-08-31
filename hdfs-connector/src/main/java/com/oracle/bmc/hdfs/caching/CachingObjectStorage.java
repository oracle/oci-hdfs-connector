/**
 * Copyright (c) 2016, 2021, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.oracle.bmc.InternalSdk;
import com.oracle.bmc.hdfs.caching.internal.BufferedInputStreamMultiplexer;
import com.oracle.bmc.hdfs.caching.internal.GuavaCacheBuilder;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.transfer.DownloadConfiguration;
import com.oracle.bmc.objectstorage.transfer.DownloadManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

/**
 * A wrapper around {@link ObjectStorage} that caches the contents returned by
 * {@link ObjectStorage#getObject(GetObjectRequest)} on disk.
 *
 * Requests that result in exceptions are not cached and will be retried.
 *
 * Note that the streams returned by {@link GetObjectResponse#getInputStream()} are automatically closed once they
 * have been completely read. This is necessary to make clean-up as reliable as possible.
 *
 * Note also that, if a stream is not not closed, the cache file will remain on disk and will not be deleted.
 *
 * The cache supports "maximumWeight", and the weight is expressed in bytes. Note that the default {@link Cache}
 * that is used considers "maximumWeight" to be per cache section. The number of cache sections is controlled using
 * the "concurrencyLevel" setting. If you want to limit the overall size to X bytes, you should use
 * "maximumWeight=X,concurrencyLevel=1".
 *
 * Cache clean-up (i.e. timed evictions) may only take place when operations happen. If you need to explicitly
 * clean-up, you can call {@link CachingObjectStorage#cleanUp()}.
 */
public interface CachingObjectStorage extends ObjectStorage {
    // factory methods

    /**
     * Configuration. This way, we get a builder and defaults. Lombok isn't able to do that on an interface
     * with a static builder method.
     */
    @lombok.Builder
    @Slf4j
    class Configuration {
        private ObjectStorage client;

        /**
         * Default is to not download in parallel.
         */
        @lombok.Builder.Default private DownloadConfiguration downloadConfiguration = DownloadConfiguration
                .builder()
                .parallelDownloads(1)
                .build();

        private Path cacheDirectory;

        /**
         * Sets the initial capacity of the cache.
         */
        @lombok.Builder.Default private Integer initialCapacity = 16;

        /**
         * Whether to record statistics. This incurs additional overhead.
         */
        @lombok.Builder.Default private boolean recordStats = false;

        /**
         * If set, the maximum size, in number of cached items.
         *
         * Cannot be used together with maximumWeight.
         */
        @lombok.Builder.Default private Integer maximumSize = null;

        /**
         * If set, the maximum weight, in bytes per cache section.
         *
         * Cannot be used together with maximumSize.
         */
        @lombok.Builder.Default private Long maximumWeight = null;

        /**
         * Sets the number of cache sections.
         */
        @lombok.Builder.Default private Integer concurrencyLevel = 1;

        /**
         * Sets the duration following the last access of any kind after which an item should be evicted.
         */
        @lombok.Builder.Default private Duration expireAfterAccess = null;

        /**
         * Sets the duration following the last write of any kind after which an item should be evicted.
         */
        @lombok.Builder.Default private Duration expireAfterWrite = null;

        @lombok.Builder.Default private ConsistencyPolicy consistencyPolicy = new StrongConsistencyPolicy();

        /**
         * This executor provides the threads in requests will be made. This controls how many requests can be made
         * in parallel.
         *
         * By default, 50 requests can be made in parallel.
         */
        @lombok.Builder.Default private ExecutorService downloadExecutor = new java.util.concurrent.ThreadPoolExecutor(
                50,
                50,
                60L,
                TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(),
                new com.google.common.util.concurrent.ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(CachingObjectStorage.class.getSimpleName() + "-download-%d")
                        .build());

        /**
         * This executor provides the thread in which files will be deleted from the cache.
         */
        @lombok.Builder.Default private ExecutorService deletionExecutor = Executors.newSingleThreadExecutor(
                new com.google.common.util.concurrent.ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(CachingObjectStorage.class.getSimpleName() + "-deletion-%d")
                        .build());

        /**
         * Sets the predicate that decides if a request cannot be cached.
         */
        @lombok.Builder.Default private UncacheablePredicate uncacheablePredicate = new DefaultUncacheablePredicate();

        /**
         * Sets the row lock provider.
         */
        @lombok.Builder.Default private RowLockProvider rowLockProvider = new DefaultRowLockProvider(1024);

        /**
         * Sets additional operations to be run when garbage collection is possible.
         */
        @lombok.Builder.Default private Runnable cacheGarbageCollection = () -> {};

        /**
         * Builder for a caching configuration.
         */
        public static class ConfigurationBuilder {
            /**
             * Sets an aggressive garbage collection strategy that causes a system-wide garbage collection
             * and then sleeps for a certain amount of milliseconds.
             * @param sleepMillis milliseconds to sleep
             * @return this builder
             */
            public ConfigurationBuilder aggressiveCacheGarbageCollection(int sleepMillis) {
                return this.cacheGarbageCollection(() -> {
                    LOG.info("Aggressive garbage collection");
                    System.gc();
                    try {
                        TimeUnit.MILLISECONDS.sleep(sleepMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }
    }

    /**
     * Returns a new configuration builder.
     * @return configuration builder
     */
    static Configuration.ConfigurationBuilder newConfiguration() {
        return Configuration.builder();
    }

    /**
     * Returns a new {@link CachingObjectStorage} client.
     * @param config the configuration, which can be created with the {@link CachingObjectStorage#newConfiguration} method.
     * @return the new {@link CachingObjectStorage} client
     */
    static CachingObjectStorage build(Configuration config) {
        Objects.requireNonNull(config.client, "must provide ObjectStorage client to client to");
        Objects.requireNonNull(config.downloadConfiguration, "must provide ObjectdownloadConfiguration");
        Objects.requireNonNull(config.cacheDirectory, "must provide cache directory");
        Objects.requireNonNull(config.consistencyPolicy, "must provide consistency policy");
        Objects.requireNonNull(config.deletionExecutor, "must provide deletion executor");
        Objects.requireNonNull(config.uncacheablePredicate, "must provide uncacheable predicate");
        Objects.requireNonNull(config.downloadExecutor, "must provide download executor");
        Objects.requireNonNull(config.rowLockProvider, "must provide row lock provider");
        Objects.requireNonNull(config.cacheGarbageCollection, "must provide cacheGarbageCollection");
        return CachingObjectStorage.build(config.client,
                                          config.downloadConfiguration,
                                          config.cacheDirectory,
                                          config.downloadExecutor,
                                          config.uncacheablePredicate,
                                          config.consistencyPolicy,
                                          new GuavaCacheBuilder<GetObjectRequestCacheKey, GetObjectResponseCacheValue>()
                                                  .concurrencyLevel(config.concurrencyLevel)
                                                  .expireAfterAccess(config.expireAfterAccess)
                                                  .expireAfterWrite(config.expireAfterWrite)
                                                  .initialCapacity(config.initialCapacity)
                                                  .maximumSize(config.maximumSize)
                                                  .maximumWeight(config.maximumWeight)
                                                  .recordStats(config.recordStats),
                                          config.deletionExecutor,
                                          config.rowLockProvider,
                                          config.cacheGarbageCollection);
    }

    /**
     * Returns a new {@link CachingObjectStorage} client.
     * @param client the actual {@link ObjectStorage} that the cached client delegates to
     * @param downloadConfiguration the {@link DownloadConfiguration} that the cached client will use for downloading
     * @param cacheDirectory directory where items are cached
     * @param downloadExecutor executor for providing the threads in which objects will be downloaded
     * @param uncacheablePredicate predicate that decides if a request cannot be cached
     * @param consistencyPolicy controls eviction of conflicting items
     * @param cacheBuilder cache builder that will actually create the cache
     * @param deletionExecutor for creating a thread that deletes evicted files after they are not being read anymore
     * @param rowLockProvider provider for row locks
     * @param cacheGarbageCollection runnable to be executed when there is a cache update
     * @return the new {@link CachingObjectStorage} client
     */
    static CachingObjectStorage build(
            ObjectStorage client,
            DownloadConfiguration downloadConfiguration,
            Path cacheDirectory,
            ExecutorService downloadExecutor,
            UncacheablePredicate uncacheablePredicate,
            ConsistencyPolicy consistencyPolicy,
            CacheBuilder<GetObjectRequestCacheKey, GetObjectResponseCacheValue, ?> cacheBuilder,
            ExecutorService deletionExecutor,
            RowLockProvider rowLockProvider,
            Runnable cacheGarbageCollection) {
        return (CachingObjectStorage)
                Proxy.newProxyInstance(
                        CachingObjectStorage.class.getClassLoader(),
                        new Class[] {CachingObjectStorage.class},
                        new Handler(client, downloadConfiguration, cacheDirectory, downloadExecutor, uncacheablePredicate,
                                    consistencyPolicy, cacheBuilder, deletionExecutor, rowLockProvider,
                                    cacheGarbageCollection));
    }

    // new interface methods

    /**
     * Get the object, circumventing the cache.
     *
     * @param getObjectRequest request
     * @return response
     */
    GetObjectResponse getObjectUncached(GetObjectRequest getObjectRequest);

    /**
     * Performs cache clean-up, i.e. evictions. This evicts items from the cache to make sure the cache matches
     * expiration times and, to a lesser degree, size and weight constraints. Size and weight constraints may not
     * be matched immediately, since this method does not automatically delete cached files from disk if they are still
     * being read.
     *
     * The files cached on disk will only be cleaned up once all streams currently reading them have been closed.
     *
     * Calling this method is optional. This kind of clean-up is also performed whenever
     * {@link CachingObjectStorage#getObject(GetObjectRequest)} operations are performed, but in absence of such
     * activity, this method can be used to trigger clean-up on-demand.
     */
    void cleanUp();

    /**
     * Returns a read-only set of paths to files that have been evicted but not deleted yet, since they are still being
     * read.
     * @return read-only set of paths to files that have been evicted but not yet deleted
     */
    Set<Path> getEvictedButNotDeleted();

    /**
     * Return the statistics for the getObject cache.
     * @return getObject cache statistics
     */
    Cache.Statistics getCacheStatistics();

    /**
     * Populates the cache at the request with the given response.
     *
     * @param cacheRequest request entry to fill.
     * @param cacheResponse content to be put at the cache entry.
     */
    void prepopulateCache(GetObjectRequest cacheRequest, GetObjectResponse cacheResponse);

    // proxy class

    /**
     * Implemented as proxy/invocation handler, so we don't have to add more and more methods that just
     * client to the inner {@link ObjectStorage} instance.
     * This will continue to work, even as new methods are added to the interface.
     */
    @Slf4j
    class Handler implements InvocationHandler {
        private final ObjectStorage client;
        private final DownloadManager downloadManager;
        private final Cache<GetObjectRequestCacheKey, GetObjectResponseCacheValue> getObjectCache;
        private final Path cacheDirectory;

        private final ExecutorService downloadExecutor;

        private final UncacheablePredicate uncacheablePredicate;
        private final ConsistencyPolicy consistencyPolicy;
        private final RowLockProvider rowLockProvider;

        private volatile boolean isShutDown = false;
        private final DeletionRunnable deletionRunnable;
        private final Future<?> deletionFuture;

        private final ReferenceQueue<PathHolder> referenceQueue = new ReferenceQueue<>();
        private final Set<PathPhantomReference> phantomReferences = new LinkedHashSet<>();
        private final Set<Path> evictedButNotDeleted = new HashSet<>();
        private volatile long diskSpaceUsedInBytes = 0;
        private final Long maximumWeightInBytes;
        private final Runnable cacheGarbageCollection;

        /**
         * Create an {@link ObjectStorage} client that caches the contents returned by
         * {@link ObjectStorage#getObject(GetObjectRequest)} on disk.
         *
         * @param client       {@link ObjectStorage} client
         * @param downloadConfiguration {@link DownloadConfiguration} to use for downloading
         * @param cacheDirectory directory where the cache is stored
         * @param downloadExecutor executor for providing the threads in which objects will be downloaded to the cache
         * @param uncacheablePredicate predicate that decides if a request cannot be cached
         * @param consistencyPolicy controls eviction of conflicting items
         * @param cacheBuilder   cache builder to control size and eviction
         * @param deletionExecutor for creating a thread that deletes evicted files after they are not being read anymore
         * @param rowLockProvider provider for row locks
         * @param cacheGarbageCollection runnable to be executed when there is a cache update
         */
        public Handler(
                ObjectStorage client,
                DownloadConfiguration downloadConfiguration,
                Path cacheDirectory,
                ExecutorService downloadExecutor,
                UncacheablePredicate uncacheablePredicate,
                ConsistencyPolicy consistencyPolicy,
                CacheBuilder<GetObjectRequestCacheKey, GetObjectResponseCacheValue, ?> cacheBuilder,
                ExecutorService deletionExecutor,
                RowLockProvider rowLockProvider,
                Runnable cacheGarbageCollection) {
            this.client = client;
            this.downloadManager = new DownloadManager(client, downloadConfiguration);
            this.cacheDirectory = cacheDirectory;
            this.cacheDirectory.toFile().mkdirs();
            this.downloadExecutor = downloadExecutor;
            this.uncacheablePredicate = uncacheablePredicate;
            this.consistencyPolicy = consistencyPolicy;
            this.rowLockProvider = rowLockProvider;
            this.cacheGarbageCollection = cacheGarbageCollection;

            deletionRunnable = new DeletionRunnable(this);
            deletionFuture = deletionExecutor.submit(deletionRunnable);

            Cache.RemovalListener<GetObjectRequestCacheKey, GetObjectResponseCacheValue>
                    removalListener = new CachingObjectStorage.RemovalListener(this);

            Cache.Weigher<GetObjectRequestCacheKey, GetObjectResponseCacheValue>
                    weighByLength =
                            (key, value) -> {
                                Long contentLength = value.originalResponse.getContentLength();
                                if (contentLength == null) {
                                    if (value.cachedContent != null) {
                                        contentLength = value.cachedContent.getPath().toFile().length();
                                    } else {
                                        contentLength = 0L;
                                    }
                                }
                                if (contentLength > Integer.MAX_VALUE) {
                                    contentLength = Long.valueOf(Integer.MAX_VALUE);
                                }
                                LOG.debug("Weight for '{}' is '{}'", value.cachedContent, contentLength);
                                return contentLength.intValue();
                            };
            cacheBuilder = cacheBuilder.removalListener(removalListener);


            if (cacheBuilder instanceof CacheBuilderWithWeight) {
                Long maximumWeightInBytes =
                        ((CacheBuilderWithWeight<GetObjectRequestCacheKey, GetObjectResponseCacheValue, ?>) cacheBuilder)
                                .getMaximumWeight();
                this.maximumWeightInBytes = maximumWeightInBytes;
                if (maximumWeightInBytes != null) {
                    cacheBuilder = cacheBuilder.weigher(weighByLength);
                }
            } else {
                this.maximumWeightInBytes = null;
            }

            getObjectCache = cacheBuilder.build();
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // these are the methods for which we have special handling
            switch (method.getName()) {
                // the overridden method
                case "getObject":
                    return getObject((GetObjectRequest) args[0]);

                // the newly added methods, found in CachingObjectStorage, but not in ObjectStorage
                case "getObjectUncached":
                    {
                        return getObjectUncached((GetObjectRequest) args[0]);
                    }
                case "prepopulateCache":
                    {
                        prepopulateCache((GetObjectRequest) args[0], (GetObjectResponse) args[1]);
                        return null;
                    }
                case "cleanUp":
                    {
                        cleanUp();
                        return null;
                    }
                case "getEvictedButNotDeleted":
                    {
                        return getEvictedButNotDeleted();
                    }
                case "getCacheStatistics":
                    {
                        return getCacheStatistics();
                    }
                case "close":
                    {
                        close();
                        return null;
                    }
            }

            // no special handling, just client
            try {
                return method.invoke(client, args);
            } catch (InvocationTargetException ite) {
                throw ite.getCause();
            }
        }

        protected GetObjectResponse getObject(GetObjectRequest getObjectRequest) {
            if (uncacheablePredicate.test(getObjectRequest)) {
                // cannot be cached
                LOG.debug("Request is uncacheable: {}", getObjectRequest);
                return getObjectUncached(getObjectRequest);
            }

            GetObjectRequestCacheKey key = consistencyPolicy.constructKey(getObjectRequest);

            try (RowLock lock = rowLockProvider.lock(key)) {
                LOG.trace("Acquired lock on key '{}'", key);
                GetObjectResponseCacheValue value = getObjectCache.getIfPresent(key);
                GetObjectResponse getObjectResponse =
                        consistencyPolicy.initiateRequest(client, downloadManager, getObjectRequest, key, value);
                if (getObjectResponse == null) {
                    if (value == null) {
                        throw new IllegalStateException("No previous value in cache, but no new request initiated");
                    }
                    // 304, not changed
                    return value.getResponse();
                }

                // new or changed
                if (value != null) {
                    value = null; // null out to allow garbage collection
                    getObjectCache.invalidate(key); // will also invoke cacheGarbageCollection
                }
                if (maximumWeightInBytes != null && diskSpaceUsedInBytes >= maximumWeightInBytes) {
                    // this is possible if consumers read streams slowly or don't close them
                    LOG.warn("Not caching request, cache size '{}' already at maximum '{}' weight",
                             diskSpaceUsedInBytes, maximumWeightInBytes);
                    return getObjectResponse;
                } else {
                    value = load(key, getObjectResponse);
                    getObjectCache.put(key, value);

                    return value.getResponse();
                }
            } finally {
                LOG.trace("Released lock on key '{}'", key);
            }
        }

        /**
         * Populates the cache at the request with the given response.
         *
         * @param cacheRequest request entry to fill.
         * @param cacheResponse content to be put at the cache entry.
         */
        protected void prepopulateCache(GetObjectRequest cacheRequest, GetObjectResponse cacheResponse) {
            if(cacheRequest == null || cacheResponse == null) {
                LOG.debug("Cannot prepopulate cache with null values. Request: {}, Response: {}",
                        cacheRequest, cacheResponse);
                return;
            }

            if (uncacheablePredicate.test(cacheRequest)) {
                LOG.debug("Request is uncacheable: {}", cacheRequest);
                return;
            }

            GetObjectRequestCacheKey key = consistencyPolicy.constructKey(cacheRequest);

            try (RowLock lock = rowLockProvider.lock(key)) {
                LOG.trace("Acquired lock on key '{}'", key);
                GetObjectResponseCacheValue value = getObjectCache.getIfPresent(key);

                if (value != null) {
                    LOG.warn("Replacing cache value for request: {}.", cacheRequest);
                    value = null; // null out to allow garbage collection
                    getObjectCache.invalidate(key); // will also invoke cacheGarbageCollection
                }
                if (maximumWeightInBytes != null && diskSpaceUsedInBytes >= maximumWeightInBytes) {
                    // this is possible if consumers read streams slowly or don't close them
                    LOG.warn("Not caching request, cache size '{}' already at maximum '{}' weight",
                            diskSpaceUsedInBytes, maximumWeightInBytes);
                } else {
                    value = load(key, cacheResponse);
                    getObjectCache.put(key, value);
                }
            } finally {
                LOG.trace("Released lock on key '{}'", key);
            }
        }

        static GetObjectRequest.Builder updateRequestId(
                GetObjectRequest.Builder builder, GetObjectRequest getObjectRequest) {
            if (getObjectRequest.getOpcClientRequestId() != null) {
                String clientRequestId = getObjectRequest.getOpcClientRequestId() + "-" + System.currentTimeMillis();
                LOG.trace("Instead of client-provided opcClientRequestId '{}', using opcClientRequestId '{}'",
                          getObjectRequest.getOpcClientRequestId(),
                          clientRequestId);
                builder = builder.opcClientRequestId(clientRequestId);
            }
            return builder;
        }

        /**
         * Get the object, circumventing the cache.
         *
         * @param getObjectRequest request
         * @return response
         */
        protected GetObjectResponse getObjectUncached(GetObjectRequest getObjectRequest) {
            GetObjectResponse response = client.getObject(getObjectRequest);

            return response;
        }

        /**
         * Performs cache clean-up, i.e. evictions. This evicts items from the cache to make sure the cache matches
         * expiration times and, to a lesser degree, size and weight constraints. Size and weight constraints may not
         * be matched immediately, since this method does not automatically delete cached files from disk if they are still
         * being read.
         *
         * The files cached on disk will only be cleaned up once all streams currently reading them have been closed.
         *
         * Calling this method is optional. This kind of clean-up is also performed whenever
         * {@link CachingObjectStorage#getObject(GetObjectRequest)} operations are performed, but in absence of such
         * activity, this method can be used to trigger clean-up on-demand.
         */
        protected void cleanUp() {
            getObjectCache.cleanUp();
        }

        /**
         * Returns a read-only set of paths to files that have been evicted but not deleted yet, since they are still
         * being read.
         *
         * @return read-only set of paths to files that have been evicted but not yet deleted
         */
        protected Set<Path> getEvictedButNotDeleted() {
            synchronized(evictedButNotDeleted) {
                return Collections.unmodifiableSet(new HashSet<>(evictedButNotDeleted));
            }
        }

        /**
         * For debug purposes, returns a read-only set of phantom references for the cached items.
         * @return set of phantom references
         */
        @InternalSdk
        protected Set<? extends PhantomReference> getPhantomReferences() {
            synchronized (phantomReferences) {
                return Collections.unmodifiableSet(phantomReferences);
            }
        }

        /**
         * Return the statistics for the getObject cache.
         * @return getObject cache statistics
         */
        protected Cache.Statistics getCacheStatistics() {
            return getObjectCache.getStatistics();
        }

        protected void close() throws Exception {
            client.close();
            isShutDown = true;
            deletionFuture.cancel(true);
        }

        private GetObjectResponseCacheValue load(
                GetObjectRequestCacheKey key, GetObjectResponse getObjectResponse) {
            Path cachedContent = null;
            try {
                if (getObjectResponse.getInputStream() != null) {
                    cachedContent = Files.createTempFile(cacheDirectory, String.format("%08x-", hashCode()), ".tmp");
                    cachedContent.toFile().deleteOnExit();

                    LOG.debug(
                            "Retrieving contents for '{}' to store at '{}' (expecting {} bytes)",
                            key,
                            cachedContent,
                            getObjectResponse.getContentLength());
                }
                GetObjectResponseCacheValue cacheValue =
                        new GetObjectResponseCacheValue(
                                this, getObjectResponse, cachedContent);

                if (getObjectResponse.getInputStream() != null) {
                    LOG.debug(
                            "Retrieved contents for '{}' and stored them at '{}'",
                            key,
                            cachedContent);
                } else {
                    LOG.debug("Retrieved contents for '{}', was null response", key);
                }

                return cacheValue;
            } catch(IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

    }

    // helper classes

    /**
     * We unfortunately need a wrapper around {@link GetObjectRequest}, because we don't want everything to be
     * considered for equals and hashCode. We don't want getOpcClientRequestId.
     *
     * equals, hashCode, and toString are implemented using reflection, so these will working even if there are more
     * properties in GetObjectRequest.
     */
    @RequiredArgsConstructor
    class GetObjectRequestCacheKey {
        private static final Predicate<Method> gettersOnlyPredicate =
                m ->
                        m.getName().startsWith("get")
                                && m.getParameterCount() == 0
                                && Modifier.isPublic(m.getModifiers());
        private static final Predicate<Method> ignoreSomeGettersPredicate =
                m ->
                        !m.getName().equals("getOpcClientRequestId")
                                && !m.getName().equals("getBody$")
                                && !m.getName().equals("getInvocationCallback")
                                && !m.getName().equals("getRetryConfiguration")
                                && !m.getName().equals("getClass");

        private static final Map<String, Function<Object, Object>> specialEqualsHashCodeRegistry = new HashMap<>();
        {
            specialEqualsHashCodeRegistry.put("getRange", o -> o != null ? o.toString() : null);
        }

        @Getter private final GetObjectRequest request;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GetObjectRequestCacheKey that = (GetObjectRequestCacheKey) o;

            // use reflection, so this continues to work, even if more properties are added to GetObjectRequest
            // the stream finds any properties for which the two are NOT equal, so we need to negate
            return !Arrays.stream(GetObjectRequest.class.getDeclaredMethods())
                    // only consider public getter methods
                    .filter(gettersOnlyPredicate)
                    // ignore getOpcClientRequestId(), which doesn't matter and may change,
                    // getBody$(), which is the stream here and which we don't want to compare,
                    // and methods that don't get sent to the service, i.e. getInvocationCallback(),
                    // getRetryConfiguration(), and getClass()
                    .filter(ignoreSomeGettersPredicate)
                    // find any for which these are NOT equal and short circuit
                    .anyMatch(
                            m -> {
                                try {
                                    Object thisValue = m.invoke(request);
                                    Object thatValue = m.invoke(that.request);
                                    Function<Object, Object> specialEqualsHashCode =
                                            GetObjectRequestCacheKey.specialEqualsHashCodeRegistry.get(m.getName());
                                    if (specialEqualsHashCode != null) {
                                        thisValue = specialEqualsHashCode.apply(thisValue);
                                        thatValue = specialEqualsHashCode.apply(thatValue);
                                    }
                                    return !Objects.equals(thisValue, thatValue);
                                } catch (InvocationTargetException | IllegalAccessException e) {
                                    throw new RuntimeException(
                                            "Unexpected exception while comparing GetObjectRequest instances",
                                            e);
                                }
                            });
        }

        @Override
        public int hashCode() {
            List<Object> objects = new ArrayList<>();
            objects.addAll(
                    Arrays.stream(GetObjectRequest.class.getDeclaredMethods())
                            // only consider public getter methods
                            .filter(gettersOnlyPredicate)
                            // ignore getOpcClientRequestId(), which doesn't matter and may change,
                            // getBody$(), which is the stream here and which we don't want to compare,
                            // and methods that don't get sent to the service, i.e. getInvocationCallback(),
                            // getRetryConfiguration(), and getClass()
                            .filter(ignoreSomeGettersPredicate)
                            // find any for which these are NOT equal and short circuit
                            .map(
                                    m -> {
                                        try {
                                            Object thisValue = m.invoke(request);
                                            Function<Object, Object> specialEqualsHashCode =
                                                    GetObjectRequestCacheKey.specialEqualsHashCodeRegistry.get(m.getName());
                                            if (specialEqualsHashCode != null) {
                                                thisValue = specialEqualsHashCode.apply(thisValue);
                                            }
                                            return thisValue;
                                        } catch (InvocationTargetException
                                                | IllegalAccessException e) {
                                            throw new RuntimeException(
                                                    "Unexpected exception while computing GetObjectRequest hashCode",
                                                    e);
                                        }
                                    })
                            .collect(Collectors.toList()));
            return Objects.hash(objects.toArray());
        }

        @Override
        public String toString() {
            String s =
                    Arrays.stream(GetObjectRequest.class.getDeclaredMethods())
                            // only consider public getter methods
                            .filter(gettersOnlyPredicate)
                            // ignore getOpcClientRequestId(), which doesn't matter and may change,
                            // getBody$(), which is the stream here and which we don't want to compare,
                            // and methods that don't get sent to the service, i.e. getInvocationCallback(),
                            // getRetryConfiguration(), and getClass()
                            .filter(ignoreSomeGettersPredicate)
                            // find any for which these are NOT equal and short circuit
                            .map(
                                    m -> {
                                        String name =
                                                m.getName().substring(3); // strip "get" prefix
                                        name =
                                                Character.toLowerCase(name.charAt(0))
                                                        + name.substring(1);
                                        try {
                                            return name + "='" + m.invoke(request) + "'";
                                        } catch (InvocationTargetException
                                                | IllegalAccessException e) {
                                            return name
                                                    + "=<"
                                                    + e.getClass().getSimpleName()
                                                    + " while getting value>";
                                        }
                                    })
                            .collect(Collectors.joining(", "));
            return "GetObjectRequestCacheKey{"
                    + s
                    + "}";
        }
    }

    @Slf4j
    class GetObjectResponseCacheValue {
        private final Handler handler;
        @Getter private final GetObjectResponse originalResponse;
        private final PathHolder cachedContent;
        private final BufferedInputStreamMultiplexer mux;

        public GetObjectResponseCacheValue(
                Handler handler, GetObjectResponse response, Path cachedContentPath)
                throws IOException {
            this.handler = handler;
            this.originalResponse = response;
            this.cachedContent = new PathHolder(cachedContentPath);
            if (cachedContent != null) {
                File cachedContentFile = cachedContent.getPath().toFile();
                cachedContentFile.getParentFile().mkdirs();
                PathPhantomReference pathPhantomReference =
                        new PathPhantomReference(cachedContent, handler.referenceQueue, response.getContentLength());
                synchronized (handler.phantomReferences) {
                    handler.phantomReferences.add(pathPhantomReference);
                    LOG.trace("Phantom reference # {}: {}", handler.phantomReferences.size(), pathPhantomReference);
                }

                BufferedInputStreamMultiplexer.Buffer buf = new BufferedInputStreamMultiplexer.FileBuffer(cachedContentFile);
                mux = new BufferedInputStreamMultiplexer(response.getInputStream(), buf);

                // immediately start downloading
                final BufferedInputStreamMultiplexer.MultiplexerInputStream firstInputStream = mux.getInputStream();
                Future<?> future = handler.downloadExecutor.submit(() -> {
                    long length;
                    try {
                        LOG.debug("Starting to retrieve contents for cache file '{}' ({} bytes expected)",
                                  cachedContentFile,
                                  response.getContentLength());
                        IOUtils.copy(firstInputStream, NullOutputStream.NULL_OUTPUT_STREAM);
                        length = cachedContentFile.length();
                        LOG.debug("Retrieved contents for cache file '{}' ({} bytes)",
                                  cachedContentFile, length);
                    } catch (IOException ioe) {
                        length = cachedContentFile.length();
                        LOG.error("Failed to retrieve contents cache file '{}' ({} bytes)",
                                  cachedContentFile, length,
                                  ioe);
                    }
                    synchronized(handler) {
                        handler.diskSpaceUsedInBytes += length;
                        LOG.info("Cache size is now {} bytes", handler.diskSpaceUsedInBytes);
                    }
                });
            } else {
                mux = null;
            }
        }

        public GetObjectResponse getResponse() {
            if (mux != null) {
                try {
                    InputStream inputStream = new CachedInputStream(cachedContent, mux.getInputStream());

                    LOG.debug("Reusing cached content at " + cachedContent);
                    return GetObjectResponse.builder()
                            .copy(originalResponse)
                            .inputStream(inputStream)
                            .build();
                } catch (IOException ioe) {
                    throw new BmcException(
                            false,
                            "Failed to use cached content at " + cachedContent,
                            ioe,
                            originalResponse.getOpcClientRequestId());
                }
            } else {
                // we didn't get a body back, return the original response
                return originalResponse;
            }
        }

        @Override
        public String toString() {
            return "GetObjectResponseCacheValue{" + "cachedContent=" + cachedContent + '}';
        }
    }

    /**
     * Input stream using a cached file. When the stream is closed or the end is reached, it allows for
     * garbage collection of the cached file.
     */
    @Slf4j
    class CachedInputStream extends BufferedInputStream {
        @Getter private final Path cachedContentPath;
        private volatile PathHolder cachedContent; // set to null to allow garbage collection

        public CachedInputStream(PathHolder cachedContent, InputStream muxInputStream) {
            super(muxInputStream);
            this.cachedContent = Objects.requireNonNull(cachedContent, "cachedContent must be non-null");
            this.cachedContentPath = cachedContent.getPath();
        }

        @Override
        public void close() throws IOException {
            super.close();
            clearCachedContent();
        }

        private void clearCachedContent() {
            this.cachedContent = null;
            LOG.trace("Closed stream, clearing cachedContent for '{}' to allow garbage collection", cachedContentPath);
        }

        @Override
        public synchronized int read() throws IOException {
            if (this.cachedContent == null) {
                // was already closed
                return -1;
            }
            int read = super.read();
            if (read == -1) {
                close();
            }
            return read;
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) throws IOException {
            if (this.cachedContent == null) {
                // was already closed
                return -1;
            }
            int read = super.read(b, off, len);
            if (read == -1) {
                close();
            }
            return read;
        }

        @Override
        public int read(byte[] b) throws IOException {
            if (this.cachedContent == null) {
                // was already closed
                return -1;
            }
            int read = super.read(b);
            if (read == -1) {
                close();
            }
            return read;
        }
    }

    /**
     * This phantom reference is used to delete evicted cached files once they are not accessible anymore by the
     * Java application (because there are no more references held to it).
     */
    @Slf4j
    class PathPhantomReference extends PhantomReference<PathHolder> {
        @Getter final Path cachedContentPath;
        @Getter final long dataSizeInBytes;
        @Getter volatile boolean isCleared = false;

        public PathPhantomReference(PathHolder referent, ReferenceQueue<? super PathHolder> q, long dataSizeInBytes) {
            super(referent, q);
            this.cachedContentPath = referent.getPath();
            this.dataSizeInBytes = dataSizeInBytes;
        }

        @Override
        public void clear() {
            super.clear();
            isCleared = true;
        }

        @Override
        public String toString() {
            return "PathPhantomReference{" + "cachedContentPath='" + cachedContentPath + '\'' + ", dataSizeInBytes='" +
                    dataSizeInBytes + "', isCleared='" + isCleared + "', isEnqueued='" + isEnqueued() + "'}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PathPhantomReference that = (PathPhantomReference) o;
            return Objects.equals(cachedContentPath, that.cachedContentPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cachedContentPath);
        }
    }

    /**
     * It seems like the JDK is doing some kind of caching or re-use of Path instances, preventing PhantomReferences
     * from working properly. Wrapping a Path in another object, and then using PhantomReferences with the wrapper.
     */
    @Value
    class PathHolder {
        final Path path;

        public File toFile() {
            return path.toFile();
        }
    }

    @Slf4j
    class DeletionRunnable implements Runnable {
        private final Handler handler;

        public DeletionRunnable(Handler handler) {
            this.handler = handler;
        }

        @Override
        public void run() {
            Reference<? extends PathHolder> referenceFromQueue;

            while (!handler.isShutDown) {
                LOG.trace("Deletion loop is running");
                try {
                    referenceFromQueue = handler.referenceQueue.remove();
                    if (referenceFromQueue != null) {
                        PathPhantomReference reference = (PathPhantomReference) referenceFromQueue;
                        LOG.debug("Dequeued reference in deletion loop: " + reference.cachedContentPath);
                        long dataSizeInBytes = deleteCachedFile(reference);
                        synchronized (handler) {
                            handler.diskSpaceUsedInBytes -= dataSizeInBytes;
                            LOG.info("Freed {} bytes, cache size is now {} bytes", dataSizeInBytes, handler.diskSpaceUsedInBytes);
                        }
                        referenceFromQueue.clear();
                        synchronized (handler.phantomReferences) {
                            handler.phantomReferences.remove(referenceFromQueue);
                        }
                    }
                } catch (InterruptedException ie) {
                    // stopping the polling is controlled using isShutDown
                }
            }
            LOG.debug("Exiting deletion loop due to cache shutdown");
        }

        /**
         * Deletes the cached file and returns the amount of cache that was freed.
         * @param reference the phantom reference that is being processed in the reference queue
         * @return cache bytes freed
         */
        public long deleteCachedFile(PathPhantomReference reference) {
            Path cachedContentPath = reference.getCachedContentPath();
            if (cachedContentPath != null) {
                try {
                    Files.deleteIfExists(cachedContentPath);
                    synchronized (handler.evictedButNotDeleted) {
                        handler.evictedButNotDeleted.remove(cachedContentPath);
                    }
                    LOG.debug("Deleted cached file '{}'", cachedContentPath);
                    return reference.getDataSizeInBytes();
                } catch (IOException ioe) {
                    LOG.error("Failed to delete cached file '{}'", cachedContentPath, ioe);
                    return 0;
                }
            }
            return 0;
        }
    }

    @Slf4j
    class RemovalListener implements Cache.RemovalListener<GetObjectRequestCacheKey, GetObjectResponseCacheValue> {
        private final Handler handler;

        public RemovalListener(Handler handler) {
            this.handler = handler;
        }

        @Override
        public void onRemoval(Cache.RemovalNotification<GetObjectRequestCacheKey, GetObjectResponseCacheValue> removalNotification) {
            LOG.debug("Removed object content cache entry '{}' ('{}')",
                      removalNotification.getKey(),
                      removalNotification.toString());
            Path cachedContent = removalNotification.getValue().cachedContent.getPath();
            if (cachedContent != null) {
                synchronized (handler.evictedButNotDeleted) {
                    handler.evictedButNotDeleted.add(cachedContent);
                }
            }
            handler.cacheGarbageCollection.run();
        }
    }

    /**
     * Interface for deciding if a request cannot be cached.
     */
    interface UncacheablePredicate extends Predicate<GetObjectRequest> {

    }

    /**
     * By default, this implementation does not cache requests with if-none-match set.
     *
     * There also appears to be a problem with ranges.
     */
    class DefaultUncacheablePredicate implements UncacheablePredicate {
        @Override
        public boolean test(GetObjectRequest getObjectRequest) {
            return getObjectRequest.getIfNoneMatch() != null;
        }
    }

    @RequiredArgsConstructor
    class RowLock implements AutoCloseable {
        private final ReentrantLock lock;
        private final int lockIndex;

        @Override
        public void close() {
            lock.unlock();
        }

        @Override
        public String toString() {
            return "RowLock{" + "lockIndex=" + lockIndex + '}';
        }
    }

    interface RowLockProvider {
        RowLock lock(GetObjectRequestCacheKey request);
    }

    class DefaultRowLockProvider implements RowLockProvider {
        private ReentrantLock[] locks;

        public DefaultRowLockProvider(int size) {
            this.locks = new ReentrantLock[size];
            for(int i=0; i<size; ++i) {
                this.locks[i] = new ReentrantLock();
            }
        }

        @Override
        public RowLock lock(GetObjectRequestCacheKey key) {
            int lockIndex = Math.abs(key.hashCode() % locks.length);
            ReentrantLock lock = locks[lockIndex];
            lock.lock();
            return new RowLock(lock, lockIndex);
        }
    }
}
