/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Function;
import java.util.function.Supplier;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.oracle.bmc.hdfs.BmcConstants;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.caching.CachingObjectStorage;
import com.oracle.bmc.hdfs.caching.ConsistencyPolicy;
import com.oracle.bmc.hdfs.monitoring.*;
import com.oracle.bmc.hdfs.util.BiFunction;
import com.oracle.bmc.hdfs.util.BlockingRejectionHandler;
import com.oracle.bmc.hdfs.util.DirectExecutorService;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.BatchDeleteObjectsResult;
import com.oracle.bmc.objectstorage.model.ChecksumAlgorithm;
import com.oracle.bmc.objectstorage.model.CreateMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.DeletedObjectResult;
import com.oracle.bmc.objectstorage.model.FailedObjectResult;
import com.oracle.bmc.objectstorage.model.ObjectSummary;
import com.oracle.bmc.retrier.DefaultRetryCondition;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.DeleteObjectRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.HeadObjectRequest;
import com.oracle.bmc.objectstorage.requests.ListObjectsRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.DeleteObjectResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.HeadObjectResponse;
import com.oracle.bmc.objectstorage.responses.ListObjectsResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration.UploadConfigurationBuilder;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.oracle.bmc.util.internal.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 * BmcDataStore is a facade to Object Store that provides CRUD operations for objects by {@link Path} references.
 * <p>
 * Statistics are updated only on successful operations, and not on attempted operations.
 */
@Slf4j
public class BmcDataStore implements AutoCloseable{
    private static final int ERROR_CODE_FILE_EXISTS = 412;
    private static final int ERROR_CONCURRENT_UPDATE = 409;

    private static final int MiB = 1024 * 1024;

    // http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Data_Replication
    private static final int BLOCK_REPLICATION = 1;

    // TODO: need to get last modified date (creation date for objects) in some missing cases
    private static final long LAST_MODIFICATION_TIME = 0L;

    private static final long TIMEOUT_EXECUTOR_SHUTDOWN = 600L ;

    private static final TimeUnit THREAD_KEEP_ALIVE_TIME_UNIT = TimeUnit.SECONDS;

    private static final TimeUnit TIME_UNIT_EXECUTOR_SHUTDOWN = TimeUnit.SECONDS;

    private final ObjectStorage objectStorage;
    private final Statistics statistics;
    private final String bucket;
    private final String namespace;

    private final BmcPropertyAccessor propertyAccessor;
    private final UploadManager uploadManager;
    private final ExecutorService parallelUploadExecutor;
    private final ExecutorService parallelRenameExecutor;
    private final ExecutorService parallelDownloadExecutor;
    private final ExecutorService parallelDeleteExecutor;
    private final RequestBuilder requestBuilder;
    private final long blockSizeInBytes;
    private final boolean useInMemoryReadBuffer;
    private final boolean useInMemoryWriteBuffer;
    private final boolean useMultipartUploadWriteBuffer;

    private final LoadingCache<String, FileStatusInfo> objectMetadataCache;
    private final boolean useReadAhead;
    private final int readAheadSizeInBytes;
    private final int readAheadBlockCount;
    private final String parquetCacheString;
    private final String customReadStreamClass;
    private final String customWriteStreamClass;
    private final String additionalChecksumAlgorithm;

    private int recursiveDirListingFetchSize;

    private OCIMonitorPluginHandler ociMonitorPluginHandler;

    private final UploadConfiguration uploadConfiguration;

    private final long retryTimeoutInSeconds;
    private final long retryResetThresholdInSeconds;
    private final long requestCoalescingWaitTimeInmillis;
    private RequestCoalescer<String, HeadObjectResponse> headObjectRequestCoalescer;

    private final boolean firstReadOptimizationForTTFBEnabled;
    private final boolean parquetCacheEnabled;

    private final boolean batchDeleteEnabled;
    private final int batchDeleteSize;

    public BmcDataStore(
            final BmcPropertyAccessor propertyAccessor,
            final ObjectStorage objectStorage,
            final String namespace,
            final String bucket,
            final Statistics statistics,
            OCIMonitorPluginHandler ociMonitorPluginHandler) {
        this.propertyAccessor = propertyAccessor;
        this.objectStorage = configureObjectStorage(objectStorage, propertyAccessor);
        this.statistics = statistics;
        this.bucket = bucket;
        this.namespace = namespace;

        this.parallelDownloadExecutor = this.createParallelDownloadExecutor(propertyAccessor);
        this.parallelDeleteExecutor = this.createParallelDeleteExecutor(propertyAccessor);
        final UploadConfigurationBuilder uploadConfigurationBuilder =
                createUploadConfiguration(propertyAccessor);
        this.parallelUploadExecutor =
                this.createParallelUploadExecutor(propertyAccessor, uploadConfigurationBuilder);
        String checksumCombineMode = propertyAccessor.getHadoopProperty(BmcConstants.DFS_CHECKSUM_COMBINE_MODE_KEY,BmcConstants.DEFAULT_CHECKSUM_COMBINE_MODE);
        if (BmcConstants.CHECKSUM_COMBINE_MODE_CRC.equalsIgnoreCase(checksumCombineMode)) {
            this.additionalChecksumAlgorithm = ChecksumAlgorithm.Crc32C.getValue();
            uploadConfigurationBuilder.additionalChecksumAlgorithm(ChecksumAlgorithm.Crc32C);
        } else {
            this.additionalChecksumAlgorithm = null;
        }
        this.uploadConfiguration = uploadConfigurationBuilder.build();
        LOG.info("Using upload configuration: {}", uploadConfiguration);
        this.uploadManager =
                new UploadManager(
                        configureObjectStorage(objectStorage, propertyAccessor),
                        uploadConfiguration);
        this.requestBuilder = new RequestBuilder(namespace, bucket);
        this.blockSizeInBytes = propertyAccessor.asLong().get(BmcProperties.BLOCK_SIZE_IN_MB) * MiB;
        this.useInMemoryReadBuffer =
                propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_READ_BUFFER);
        this.useInMemoryWriteBuffer =
                propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_WRITE_BUFFER);
        this.useMultipartUploadWriteBuffer =
                propertyAccessor
                        .asBoolean()
                        .get(BmcProperties.MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED);
        boolean useHeadObjectRequestCoalescing =
                propertyAccessor
                        .asBoolean()
                        .get(BmcProperties.HEADOBJECT_REQUEST_COALESCING_ENABLED);
        this.requestCoalescingWaitTimeInmillis =
                propertyAccessor
                        .asLong()
                        .get(BmcProperties.REQUEST_COALESCING_WAIT_TIME_IN_MILLIS);
        if (useHeadObjectRequestCoalescing) {
            LOG.info("HeadObject request coalescing enabled");
            this.headObjectRequestCoalescer = new RequestCoalescer<>(Duration.ofMillis(requestCoalescingWaitTimeInmillis));
        }
        this.useReadAhead = propertyAccessor.asBoolean().get(BmcProperties.READ_AHEAD);
        this.readAheadSizeInBytes = getReadAheadSizeInBytes(propertyAccessor);
        this.readAheadBlockCount =
                propertyAccessor.asInteger().get(BmcProperties.READ_AHEAD_BLOCK_COUNT);
        this.customReadStreamClass =
                propertyAccessor.asString().get(BmcProperties.READ_STREAM_CLASS);
        this.customWriteStreamClass =
                propertyAccessor.asString().get(BmcProperties.WRITE_STREAM_CLASS);

        if (this.useInMemoryWriteBuffer && this.useMultipartUploadWriteBuffer) {
            throw new IllegalArgumentException(
                    BmcProperties.IN_MEMORY_WRITE_BUFFER.getPropertyName()
                            + " and "
                            + BmcProperties.MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED
                                    .getPropertyName()
                            + " are mutually exclusive");
        }

        if (this.useInMemoryReadBuffer && this.useReadAhead) {
            throw new IllegalArgumentException(
                    BmcProperties.IN_MEMORY_READ_BUFFER.getPropertyName()
                            + " and "
                            + BmcProperties.READ_AHEAD.getPropertyName()
                            + " are mutually exclusive");
        }

        this.objectMetadataCache = configureHeadObjectCache(propertyAccessor);
        this.parquetCacheString = configureParquetCacheString(propertyAccessor);
        this.parallelRenameExecutor = this.createParallelRenameExecutor(propertyAccessor);
        this.recursiveDirListingFetchSize =
                propertyAccessor.asInteger().get(BmcProperties.RECURSIVE_DIR_LISTING_FETCH_SIZE);
        this.ociMonitorPluginHandler = ociMonitorPluginHandler;
        this.retryTimeoutInSeconds = propertyAccessor.asLong().get(BmcProperties.RETRY_TIMEOUT_IN_SECONDS);
        this.retryResetThresholdInSeconds = propertyAccessor.asLong().get(BmcProperties.RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS);
        this.firstReadOptimizationForTTFBEnabled = propertyAccessor.asBoolean().get(BmcProperties.OCI_FIRST_READ_OPTIMIZATION_FOR_TTFB);

        this.parquetCacheEnabled = propertyAccessor.asBoolean().get(BmcProperties.OBJECT_PARQUET_CACHING_ENABLED);

        this.batchDeleteEnabled = propertyAccessor.asBoolean().get(BmcProperties.BATCH_DELETE_ENABLED);
        this.batchDeleteSize = propertyAccessor.asInteger().get(BmcProperties.BATCH_DELETE_SIZE);
    }

    public static int getReadAheadSizeInBytes(BmcPropertyAccessor propertyAccessor) {
        return propertyAccessor.asInteger().get(BmcProperties.READ_AHEAD_BLOCK_SIZE);
    }

    private ExecutorService createParallelDeleteExecutor(final BmcPropertyAccessor propertyAccessor) {
        Integer numThreadsForDelete =
                propertyAccessor.asInteger().get(BmcProperties.NUM_DELETE_THREADS);
        final long threadsTimeoutInSeconds = getThreadsTimeoutInSeconds(propertyAccessor);

        if (numThreadsForDelete <= 1) {
            numThreadsForDelete = 1;
        }

        final ExecutorService executorService =
                    newSwingFixedThreadPool(
                            numThreadsForDelete,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("bmcs-hdfs-delete-%d")
                                    .build(),
                            threadsTimeoutInSeconds);

        return executorService;
    }

    private ExecutorService createParallelDownloadExecutor(final BmcPropertyAccessor propertyAccessor) {
        final Integer numThreadsForReadaheadOperations =
                propertyAccessor.asInteger().get(BmcProperties.NUM_READ_AHEAD_THREADS);
        final long threadsTimeoutInSeconds = getThreadsTimeoutInSeconds(propertyAccessor);
        final ExecutorService executorService;
        if (numThreadsForReadaheadOperations == null
                || numThreadsForReadaheadOperations <= 1) {
            executorService =
                newSwingFixedThreadPool(
                            1,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("bmcs-hdfs-readahead-%d")
                                    .build(),threadsTimeoutInSeconds);
        } else {
            executorService =
                newSwingFixedThreadPool(
                            numThreadsForReadaheadOperations,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("bmcs-hdfs-readahead-%d")
                                    .build(),threadsTimeoutInSeconds);
        }
        return executorService;
    }


    private long getThreadsTimeoutInSeconds(BmcPropertyAccessor propertyAccessor) {
        Long threadsTimeoutInSeconds = propertyAccessor.asLong().get(BmcProperties.BMC_DATASTORE_IO_THREAD_TIMEOUT_IN_SECONDS);
        if(threadsTimeoutInSeconds == null || threadsTimeoutInSeconds <= 0) {
            // safety check to handle even if previous impl sent a non-positive value
            LOG.warn("Invalid value received {} for property {}. Using default value of {} seconds", threadsTimeoutInSeconds , BmcProperties.BMC_DATASTORE_IO_THREAD_TIMEOUT_IN_SECONDS.getPropertyName(), (Long) BmcProperties.BMC_DATASTORE_IO_THREAD_TIMEOUT_IN_SECONDS.getDefaultValue());
            threadsTimeoutInSeconds = (Long) BmcProperties.BMC_DATASTORE_IO_THREAD_TIMEOUT_IN_SECONDS.getDefaultValue();
        }
        return threadsTimeoutInSeconds;
    }


    /**
     * Creates a thread pool that will be able to swing open threads between 0 to nThreads
     * If any additional tasks come , they wait in the queue
     *
     * @param nThreads the number of threads in the pool
     * @param threadFactory the factory to use when creating new threads
     * @param threadsTimeoutInSeconds timeout Of threads in seconds
     * @return the newly created thread pool
     * @throws NullPointerException if threadFactory is null
     * @throws IllegalArgumentException if {@code nThreads <= 0}
     */
    public static ExecutorService newSwingFixedThreadPool(int nThreads, ThreadFactory threadFactory,long threadsTimeoutInSeconds) {
        ThreadPoolExecutor tp =  new ThreadPoolExecutor(nThreads, nThreads,
            threadsTimeoutInSeconds, THREAD_KEEP_ALIVE_TIME_UNIT,
            new LinkedBlockingQueue<Runnable>(),
            threadFactory);
        tp.allowCoreThreadTimeOut(true);
        return tp;
    }

    private ExecutorService createParallelRenameExecutor(BmcPropertyAccessor propertyAccessor) {
        final Integer numThreadsForRenameDirectoryOperation =
                propertyAccessor.asInteger().get(BmcProperties.RENAME_DIRECTORY_NUM_THREADS);
        final long threadsTimeoutInSeconds = getThreadsTimeoutInSeconds(propertyAccessor);
        final ExecutorService executorService;
        if (numThreadsForRenameDirectoryOperation == null
                || numThreadsForRenameDirectoryOperation <= 1) {
            executorService =
                newSwingFixedThreadPool(
                            1,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("bmcs-hdfs-rename-%d")
                                    .build(),threadsTimeoutInSeconds);
        } else {
            executorService =
                newSwingFixedThreadPool(
                            numThreadsForRenameDirectoryOperation,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("bmcs-hdfs-rename-%d")
                                    .build(),threadsTimeoutInSeconds);
        }
        return executorService;
    }

    private ObjectStorage configureObjectStorage(
            ObjectStorage originalObjectStorage, BmcPropertyAccessor propertyAccessor) {
        ObjectStorage objectStorage = originalObjectStorage;

        boolean usePayloadCaching =
                propertyAccessor.asBoolean().get(BmcProperties.OBJECT_PAYLOAD_CACHING_ENABLED);

        if (usePayloadCaching) {
            try {
                String cachingDirectoryProperty =
                        propertyAccessor
                                .asString()
                                .get(BmcProperties.OBJECT_PAYLOAD_CACHING_DIRECTORY);
                java.nio.file.Path directory =
                        (cachingDirectoryProperty != null)
                                ? Paths.get(cachingDirectoryProperty)
                                : Paths.get(System.getProperty("java.io.tmpdir"))
                                        .resolve("oci-hdfs-payload-cache");
                LOG.debug("Payload caching directory is '{}'", directory);

                Class<ConsistencyPolicy> consistencyPolicyClass =
                        (Class<ConsistencyPolicy>)
                                Class.forName(
                                        propertyAccessor
                                                .asString()
                                                .get(
                                                        BmcProperties
                                                                .OBJECT_PAYLOAD_CACHING_CONSISTENCY_POLICY_CLASS));
                ConsistencyPolicy consistencyPolicy = consistencyPolicyClass.newInstance();
                LOG.debug("Consistency policy is '{}'", consistencyPolicy.getClass().getName());

                boolean recordStatistics =
                        propertyAccessor
                                .asBoolean()
                                .get(BmcProperties.OBJECT_PAYLOAD_CACHING_RECORD_STATS_ENABLED);
                CachingObjectStorage.Configuration.ConfigurationBuilder configurationBuilder =
                        CachingObjectStorage.newConfiguration()
                                .client(objectStorage)
                                .cacheDirectory(directory)
                                .recordStats(recordStatistics)
                                .initialCapacity(
                                        propertyAccessor
                                                .asInteger()
                                                .get(
                                                        BmcProperties
                                                                .OBJECT_PAYLOAD_CACHING_INITIAL_CAPACITY))
                                .consistencyPolicy(consistencyPolicy);

                Integer maxSize =
                        propertyAccessor
                                .asInteger()
                                .get(BmcProperties.OBJECT_PAYLOAD_CACHING_MAXIMUM_SIZE);
                if (maxSize != null) {
                    configurationBuilder = configurationBuilder.maximumSize(maxSize);
                }
                Long maxWeight =
                        propertyAccessor
                                .asLong()
                                .get(BmcProperties.OBJECT_PAYLOAD_CACHING_MAXIMUM_WEIGHT_IN_BYTES);
                if (maxWeight != null) {
                    configurationBuilder = configurationBuilder.maximumWeight(maxWeight);
                }
                Integer expireAfterAccess =
                        propertyAccessor
                                .asInteger()
                                .get(
                                        BmcProperties
                                                .OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_ACCESS_SECONDS);
                if (expireAfterAccess != null) {
                    configurationBuilder =
                            configurationBuilder.expireAfterAccess(
                                    Duration.ofSeconds(expireAfterAccess));
                }
                Integer expireAfterWrite =
                        propertyAccessor
                                .asInteger()
                                .get(
                                        BmcProperties
                                                .OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_WRITE_SECONDS);
                if (expireAfterWrite != null) {
                    configurationBuilder =
                            configurationBuilder.expireAfterWrite(
                                    Duration.ofSeconds(expireAfterWrite));
                }

                CachingObjectStorage cachingObjectStorage =
                        CachingObjectStorage.build(configurationBuilder.build());
                objectStorage = cachingObjectStorage;
                long period =
                        propertyAccessor
                                .asLong()
                                .get(
                                        BmcProperties
                                                .OBJECT_PAYLOAD_CACHING_RECORD_STATS_TIME_INTERVAL_IN_SECONDS);
                if (recordStatistics) {
                    logCacheStatistics(period, cachingObjectStorage);
                }
            } catch (Exception e) {
                LOG.error(
                        "Failed to configure Object Storage payload caching; payload caching disabled",
                        e);
            }
        }
        return objectStorage;
    }

    /**
     * Logs the statistics for the getObject cache.
     * ScheduledExecutorService is set to log the statistics every minute (by default) with the initial delay of 30 seconds.
     * The interval can be changed by setting {@link BmcConstants#OBJECT_PAYLOAD_CACHING_RECORD_STATS_TIME_INTERVAL_IN_SECONDS_KEY} config key
     *
     * @param cachingObjectStorage
     */
    private void logCacheStatistics(long period, CachingObjectStorage cachingObjectStorage) {
        final ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactory() {
                            public Thread newThread(Runnable r) {
                                Thread t = Executors.defaultThreadFactory().newThread(r);
                                t.setDaemon(true);
                                return t;
                            }
                        });
        try {
            executorService.scheduleAtFixedRate(
                    () ->
                            LOG.info(
                                    "Cache statistics: {}",
                                    cachingObjectStorage.getCacheStatistics()),
                    30,
                    period,
                    TimeUnit.SECONDS);
        } finally {
            executorService.shutdown();
        }
    }

    private LoadingCache<String, FileStatusInfo> configureHeadObjectCache(
            BmcPropertyAccessor propertyAccessor) {
        boolean headObjectCachingEnabled =
                propertyAccessor.asBoolean().get(BmcProperties.OBJECT_METADATA_CACHING_ENABLED);
        String loadMessage =
                headObjectCachingEnabled
                        ? "Not in object metadata cache, getting actual metadata for key: '{}'"
                        : "Getting metadata for key: '{}'";

        CacheLoader<String, FileStatusInfo> loader =
                new CacheLoader<String, FileStatusInfo>() {
                    @Override
                    public FileStatusInfo load(String key) throws Exception {
                        LOG.info(loadMessage, key);
                        return getFileStatusUncached(key);
                    }
                };

        if (!headObjectCachingEnabled) {
            LOG.info("Object metadata caching disabled");
            return CacheBuilder.newBuilder().maximumSize(0).build(loader);
        }

        String headObjectCachingSpec =
                propertyAccessor.asString().get(BmcProperties.OBJECT_METADATA_CACHING_SPEC);

        CacheBuilderSpec cacheBuilderSpec = CacheBuilderSpec.parse(headObjectCachingSpec);

        LOG.info("Object metadata caching enabled with cache spec: '{}'", cacheBuilderSpec);

        return CacheBuilder.from(cacheBuilderSpec)
                .removalListener(
                        new RemovalListener<String, FileStatusInfo>() {
                            @Override
                            public void onRemoval(
                                    RemovalNotification<String, FileStatusInfo> removalNotification) {
                                LOG.info(
                                        "Object metadata cache entry '{}' removed (cause '{}', was evicted '{}')",
                                        removalNotification.getKey(),
                                        removalNotification.getCause(),
                                        removalNotification.wasEvicted());
                            }
                        })
                .build(loader);
    }

    public static String configureParquetCacheString(BmcPropertyAccessor propertyAccessor) {
        // this disables the cache by default
        String spec = "maximumSize=0";
        if (propertyAccessor.asBoolean().get(BmcProperties.OBJECT_PARQUET_CACHING_ENABLED)) {
            spec = propertyAccessor.asString().get(BmcProperties.OBJECT_PARQUET_CACHING_SPEC);
            LOG.info(
                    "{} is enabled, setting parquet cache spec to '{}'",
                    BmcProperties.OBJECT_PARQUET_CACHING_ENABLED.getPropertyName(),
                    spec);
        } else {
            LOG.info(
                    "{} is disabled, setting parquet cache spec to '{}', which disables the cache",
                    BmcProperties.OBJECT_PARQUET_CACHING_ENABLED.getPropertyName(),
                    spec);
        }
        return spec;
    }

    private UploadConfigurationBuilder createUploadConfiguration(
            final BmcPropertyAccessor propertyAccessor) {
        final UploadConfigurationBuilder uploadConfigurationBuilder = UploadConfiguration.builder();

        final boolean allowMultipartUploads =
                propertyAccessor.asBoolean().get(BmcProperties.MULTIPART_ALLOWED);
        uploadConfigurationBuilder.allowMultipartUploads(allowMultipartUploads);

        final Integer minimumLengthForMultipartUpload =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB);
        uploadConfigurationBuilder.minimumLengthForMultipartUpload(minimumLengthForMultipartUpload);

        final Integer deprecatedMinLengthPerUploadPart =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_MIN_PART_SIZE_IN_MB);
        final Integer lengthPerUploadPart =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_PART_SIZE_IN_MB);

        if (lengthPerUploadPart != null) {
            uploadConfigurationBuilder.lengthPerUploadPart(lengthPerUploadPart);
        } else if (deprecatedMinLengthPerUploadPart != null) {
            LOG.warn(
                    "Using deprecated configuration option to specify the length per upload part: [{}]"
                            + " Consider defining the value for {} instead",
                    deprecatedMinLengthPerUploadPart,
                    BmcProperties.MULTIPART_PART_SIZE_IN_MB.getPropertyName());
            uploadConfigurationBuilder.lengthPerUploadPart(deprecatedMinLengthPerUploadPart);
        }

        return uploadConfigurationBuilder;
    }

    private ExecutorService createParallelUploadExecutor(
            final BmcPropertyAccessor propertyAccessor,
            final UploadConfigurationBuilder uploadConfigurationBuilder) {
        final Integer numThreadsForParallelUpload =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_NUM_UPLOAD_THREADS);
        final long threadsTimeoutInSeconds = getThreadsTimeoutInSeconds(propertyAccessor);

        final boolean streamMultipartEnabled =
                propertyAccessor
                        .asBoolean()
                        .get(BmcProperties.MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED);

        if (!streamMultipartEnabled
                && (numThreadsForParallelUpload == null || numThreadsForParallelUpload <= 0)) {
            return null;
        }
        if (!streamMultipartEnabled && numThreadsForParallelUpload == 1) {
            uploadConfigurationBuilder.allowParallelUploads(false);
            return null;
        }

        if (numThreadsForParallelUpload == null) {
            // if !streamMultipartEnabled, then this would have returned null above, so the only case this can happoen
            // is if streamMultipartEnabled and numThreadsForParallelUpload == null
            throw new IllegalArgumentException(
                    BmcProperties.MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED.getPropertyName()
                            + " requires "
                            + BmcProperties.MULTIPART_NUM_UPLOAD_THREADS.getPropertyName()
                            + " to be set");
        }

        /*
           This case is handled differently. When streaming, if we didn't fix the amount of work that the threads can
           handle at one time, we would read all of the stream into memory while writing was in progress. This defeats
           the purpose having stream <-> stream uploads without holding the entire stream in memory. This executor
           will reject work after the queue becomes full and it will wait until a slot opens to re-enqueue that work.
        */
        if (streamMultipartEnabled) {
            final int taskTimeout =
                    propertyAccessor
                            .asInteger()
                            .get(BmcProperties.MULTIPART_IN_MEMORY_WRITE_TASK_TIMEOUT_SECONDS);
            final BlockingRejectionHandler rejectedExecutionHandler =
                    new BlockingRejectionHandler(taskTimeout);

            ThreadPoolExecutor tp = new ThreadPoolExecutor(
                numThreadsForParallelUpload,
                numThreadsForParallelUpload,
                threadsTimeoutInSeconds,
                THREAD_KEEP_ALIVE_TIME_UNIT,
                new LinkedBlockingQueue<Runnable>(numThreadsForParallelUpload),
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("bmcs-hdfs-blocking-upload-%d")
                    .build(),
                rejectedExecutionHandler);
            tp.allowCoreThreadTimeOut(true);
            return tp;
        }
        return newSwingFixedThreadPool(
                numThreadsForParallelUpload,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bmcs-hdfs-upload-%d")
                        .build(),threadsTimeoutInSeconds);
    }

    /**
     * Renames an object from one name to another. This is a multi-step operation that consists of finding all matching
     * objects, copying them to the destination, and then deleting the original objects.
     *
     * @param source      The source to rename, assumed to exist.
     * @param destination The destination, may not exist, will be overwritten
     * @throws IOException if the operation cannot be completed.
     */
    public void renameFile(final Path source, final Path destination) throws IOException {
        final String sourceObject = this.pathToObject(source);
        final String destinationObject = this.pathToObject(destination);
        LOG.debug(
                "Attempting to rename path {} to {} (object {} to {})",
                source,
                destination,
                sourceObject,
                destinationObject);

        this.rename(sourceObject, destinationObject);
    }

    /**
     * Renames a directory to have a new location or name. This will move all objects that were considered part of the
     * original directory to be in the new one (all objects that matched the path prefix). This is a multi-step
     * operation that consists of finding all matching objects, copying them to the destination, and then deleting the
     * original objects.
     * <p>
     * Note, source and destination should not be root TODO: destination could be?
     *
     * @param sourceDirectoryPath      The source directory to rename, assumed to exist.
     * @param destinationDirectoryPath The destination directory.
     * @throws IOException if the operation cannot be completed.
     */
    public void renameDirectory(final Path sourceDirectoryPath, final Path destinationDirectoryPath)
            throws IOException {
        final String sourceDirectory = this.pathToDirectory(sourceDirectoryPath);
        final String destinationDirectory = this.pathToDirectory(destinationDirectoryPath);
        LOG.debug(
                "Attempting to rename path {} to {} (object {} to {})",
                sourceDirectoryPath,
                destinationDirectoryPath,
                sourceDirectory,
                destinationDirectory);

        // find all objects to rename first to prevent any modification of the result set while iterating over it
        final ArrayList<String> objectsToRename = new ArrayList<>();
        try {
            ListObjectsRequest request;
            ListObjectsResponse response;
            String nextToken = null;
            do {
                LOG.debug("Making request with next token {}", nextToken);
                request = this.requestBuilder.listObjects(sourceDirectory, nextToken, null, 1000);

                response = getListObjectsResponse(request);
                this.statistics.incrementReadOps(1);

                final List<ObjectSummary> summaries = response.getListObjects().getObjects();
                for (final ObjectSummary summary : summaries) {
                    objectsToRename.add(summary.getName());
                }

                nextToken = response.getListObjects().getNextStartWith();
            } while (nextToken != null);
        } catch (final BmcException e) {
            LOG.debug("Failed to list objects for path {}", sourceDirectory, e);
            throw new IOException("Failed to rename directory", e);
        }

        renameOperationsUsingExecutor(objectsToRename, sourceDirectory, destinationDirectory);
    }

    private ListObjectsResponse getListObjectsResponse(ListObjectsRequest request) {
        Stopwatch sw = Stopwatch.createStarted();
        ListObjectsResponse response = null;
        RetryMetricsCollector collector = new RetryMetricsCollector(OCIMetricKeys.LIST, retryTimeoutInSeconds, retryResetThresholdInSeconds);

        try {
            request = ListObjectsRequest.builder()
                    .copy(request)
                    .retryConfiguration(collector.getRetryConfiguration())
                    .build();

            response = this.objectStorage.listObjects(request);

            sw.stop();
            recordOCIStats(OCIMetricKeys.LIST, sw.elapsed(TimeUnit.MILLISECONDS), null, collector.getAttemptCount(),
                    collector.getRetry503Count(), collector.getRetry429Count());
        } catch (Exception e) {
            sw.stop();
            recordOCIStats(OCIMetricKeys.LIST, sw.elapsed(TimeUnit.MILLISECONDS), e, collector.getAttemptCount(),
                    collector.getRetry503Count(), collector.getRetry429Count());
            throw e;
        } finally {
            collector.close();
        }

        return response;
    }

    private HeadObjectResponse getHeadObjectResponse(HeadObjectRequest request) {
        Stopwatch sw = Stopwatch.createStarted();
        HeadObjectResponse response;
        final AtomicBoolean isCoalesced = new AtomicBoolean(false);
        RetryMetricsCollector collector = new RetryMetricsCollector(OCIMetricKeys.HEAD, retryTimeoutInSeconds, retryResetThresholdInSeconds);

        try {
            final HeadObjectRequest requestWithCollector = HeadObjectRequest.builder()
                    .copy(request)
                    .retryConfiguration(collector.getRetryConfiguration())
                    .build();

            LOG.debug("headObject called {} ", requestWithCollector.getObjectName());

            if (headObjectRequestCoalescer != null) {
                StringBuilder keyBuilder = new StringBuilder(request.getObjectName());
                String ifMatch = request.getIfMatch();
                if (ifMatch != null) {
                    keyBuilder.append(ifMatch);
                }
                String ifNoneMatch = request.getIfNoneMatch();
                if (ifNoneMatch != null) {
                    keyBuilder.append(ifNoneMatch);
                }
                String key = keyBuilder.toString();
                LOG.debug("Starting head object coalescing process for key {}", key);
                response = this.headObjectRequestCoalescer.performRequest(key,
                        () -> this.objectStorage.headObject(requestWithCollector),
                        isCoalesced);
            } else {
                response = this.objectStorage.headObject(requestWithCollector);
            }
            sw.stop();
            recordOCIStats(OCIMetricKeys.HEAD, sw.elapsed(TimeUnit.MILLISECONDS), null, collector.getAttemptCount(),
                    collector.getRetry503Count(), collector.getRetry429Count(), isCoalesced.get());
        } catch (Exception e) {
            sw.stop();
            if (e instanceof BmcException) {
                int status = ((BmcException) e).getStatusCode();
                if (status == 404 || status == 412) {
                    // Don't record 404/412 status code as an error. But it should go as a success metric.
                    recordOCIStats(OCIMetricKeys.HEAD, sw.elapsed(TimeUnit.MILLISECONDS), null, collector.getAttemptCount(),
                            collector.getRetry503Count(), collector.getRetry429Count(), isCoalesced.get());
                } else {
                    recordOCIStats(OCIMetricKeys.HEAD, sw.elapsed(TimeUnit.MILLISECONDS), e, collector.getAttemptCount(),
                            collector.getRetry503Count(), collector.getRetry429Count(), isCoalesced.get());
                }
            }
            throw e;
        } finally {
            collector.close();
        }
        return response;
    }

    private DeleteObjectResponse getDeleteObjectResponse(DeleteObjectRequest request) {
        Stopwatch sw = Stopwatch.createStarted();
        DeleteObjectResponse response;
        RetryMetricsCollector collector = new RetryMetricsCollector(OCIMetricKeys.DELETE, retryTimeoutInSeconds, retryResetThresholdInSeconds);

        try {
            request = DeleteObjectRequest.builder()
                    .copy(request)
                    .retryConfiguration(collector.getRetryConfiguration())
                    .build();

            response = this.objectStorage.deleteObject(request);
            sw.stop();
            recordOCIStats(OCIMetricKeys.DELETE, sw.elapsed(TimeUnit.MILLISECONDS), null, collector.getAttemptCount(),
                    collector.getRetry503Count(), collector.getRetry429Count());
        } catch (Exception e) {
            sw.stop();
            recordOCIStats(OCIMetricKeys.DELETE, sw.elapsed(TimeUnit.MILLISECONDS), e, collector.getAttemptCount(),
                    collector.getRetry503Count(), collector.getRetry429Count());
            throw e;
        } finally {
            collector.close();
        }

        return response;
    }

    private void renameOperationsUsingExecutor(
            final ArrayList<String> objectsToRename,
            final String sourceDirectory,
            final String destinationDirectory)
            throws IOException {
        List<RenameResponse> renameResponses = new ArrayList<>();

        for (final String objectToRename : objectsToRename) {
            final String newObjectName =
                    objectToRename.replaceFirst(
                            Pattern.quote(sourceDirectory), Matcher.quoteReplacement(destinationDirectory));
            Future<String> futureResponse =
                    this.parallelRenameExecutor.submit(
                            new RenameOperation(
                                    this.objectStorage,
                                    this.requestBuilder.renameObject(
                                            objectToRename, newObjectName), ociMonitorPluginHandler, propertyAccessor));
            renameResponses.add(new RenameResponse(objectToRename, newObjectName, futureResponse));
        }
        awaitRenameOperationTermination(renameResponses);
    }

    @Override
    public void close() {
        /*
        To close the executor Services to avoid thread leaking causing OOM
         */
        closeExecutorService(this.parallelDownloadExecutor, TIMEOUT_EXECUTOR_SHUTDOWN, TIME_UNIT_EXECUTOR_SHUTDOWN);
        closeExecutorService(this.parallelUploadExecutor, TIMEOUT_EXECUTOR_SHUTDOWN, TIME_UNIT_EXECUTOR_SHUTDOWN);
        closeExecutorService(this.parallelRenameExecutor, TIMEOUT_EXECUTOR_SHUTDOWN, TIME_UNIT_EXECUTOR_SHUTDOWN);
        closeExecutorService(this.parallelDeleteExecutor, TIMEOUT_EXECUTOR_SHUTDOWN, TIME_UNIT_EXECUTOR_SHUTDOWN);

        // Shutdown OCI Monitor Plugin Executor
        if (ociMonitorPluginHandler != null) {
            closeExecutorService(
                    ociMonitorPluginHandler.getExecutorService(),
                    TIMEOUT_EXECUTOR_SHUTDOWN,
                    TIME_UNIT_EXECUTOR_SHUTDOWN
            );

            List<OCIMonitorConsumerPlugin> plugins = ociMonitorPluginHandler.getListOfPlugins();
            if (plugins != null) {
                for (OCIMonitorConsumerPlugin plugin : plugins) {
                    plugin.shutdown();
                }
            }
        }
    }

    private void closeExecutorService(ExecutorService executorService,long timeOut,TimeUnit timeUnitOfTimeout) {
        if (executorService == null) {
            LOG.debug("ExecutorService is null, skipping shutdown");
            return;
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(timeOut, timeUnitOfTimeout)) {
                LOG.warn("ExecutorService did not terminate within the specified timeout {} {}",timeOut,timeUnitOfTimeout);
            }
        } catch (InterruptedException e) {
            LOG.error("Current Thread was interrupted while awaiting termination of ExecutorService.", e);
            /* set back the interrupt status .
            Ref : https://docs.oracle.com/javase/tutorial/essential/concurrency/interrupt.html */
            Thread.currentThread().interrupt();
        } finally {
            // In both cases (timeout or interrupted), force shutdown
            if (!executorService.isTerminated()) {
                LOG.warn("Forcing shutdown of ExecutorService by sending interrupt to threads in Exec service ");
                executorService.shutdownNow();
            }
        }
    }

    @RequiredArgsConstructor
    private static class RenameResponse {
        @Getter private final String oldName;
        @Getter private final String newName;
        @Getter private final Future<String> renameOperationFuture;
    }

    private void awaitRenameOperationTermination(List<RenameResponse> renameResponses)
            throws IOException {
        LOG.debug("Attempting to rename objects in parallel");
        for (RenameResponse renameResponse : renameResponses) {
            try {
                LOG.debug(
                        "Attempting to rename {} to {}",
                        renameResponse.getOldName(),
                        renameResponse.getNewName());
                Future<String> renameFuture = renameResponse.getRenameOperationFuture();
                String newEntityTag = renameFuture.get();
                this.statistics.incrementWriteOps(1); // 1 put
                LOG.debug(
                        "{} renamed to {}",
                        renameResponse.getOldName(),
                        renameResponse.getNewName());
                LOG.debug("{} has eTag {}", renameResponse.getNewName(), newEntityTag);
            } catch (InterruptedException e) {
                LOG.debug("Thread interrupted while waiting for rename completion", e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof BmcException) {
                    BmcException bmcException = (BmcException) e.getCause();
                    // if running jobs in parallel, it's possible multiple threads try to run rename
                    // operation on same file at the same time, which might lead to 409 conflicts.

                    if (bmcException.getStatusCode() == ERROR_CODE_FILE_EXISTS
                            || bmcException.getStatusCode() == ERROR_CONCURRENT_UPDATE) {
                        LOG.debug(
                                "Failed to rename {} to {}",
                                renameResponse.getOldName(),
                                renameResponse.getNewName(),
                                e);
                        throw new FileAlreadyExistsException(
                                "Cannot rename file, destination file already exists : " + renameResponse.getNewName());
                    }
                }
                LOG.debug("Execution exception while waiting for rename completion", e);
            } catch (Exception e) {
                LOG.debug(
                        "Failed to rename {} to {}",
                        renameResponse.getOldName(),
                        renameResponse.getNewName(),
                        e);
                throw new IOException("Unable to perform rename", e);
            }
        }
    }

    private void rename(final String sourceObject, final String destinationObject)
            throws IOException {
        LOG.debug("Attempting to rename {} to {}", sourceObject, destinationObject);
        try {
            final String newEntityTag =
                    new RenameOperation(
                            this.objectStorage,
                            this.requestBuilder.renameObject(
                                    sourceObject, destinationObject), ociMonitorPluginHandler, propertyAccessor)
                            .call();
            this.statistics.incrementWriteOps(1); // 1 put
            LOG.debug("Newly renamed object has eTag {}", newEntityTag);
        } catch (final BmcException e) {
            LOG.debug("Failed to rename {} to {}", sourceObject, destinationObject, e);
            if (e.getStatusCode() == ERROR_CODE_FILE_EXISTS) {
                throw new FileAlreadyExistsException(
                        "Cannot rename file, destination file already exists : " + destinationObject);
            }
            throw new IOException("Unable to perform rename", e);
        } catch (final Exception e) {
            LOG.debug("Failed to rename {} to {}", sourceObject, destinationObject, e);
            throw new IOException("Unable to perform rename", e);
        }
    }

    /**
     * Deletes the object at the given path.
     *
     * @param path Path of object to delete.
     * @throws IOException if the operation cannot be completed.
     */
    public void delete(final Path path) throws IOException {
        final String object = this.pathToObject(path);
        LOG.debug("Attempting to delete object {} from path {}", object, path);

        try {
            getDeleteObjectResponse(this.requestBuilder.deleteObject(object));
            // Invalidate the cache after deleting the object to ensure data consistency when objectMetadataCache is enabled
            this.objectMetadataCache.invalidate(object);
            this.statistics.incrementWriteOps(1);
        } catch (final BmcException e) {
            // deleting an object that doesn't actually exist, nothing to do.
            if (e.getStatusCode() != 404) {
                LOG.debug("Failed to delete object {}", object, e);
                throw new IOException("Error attempting to delete object", e);
            }
        }
    }

    /**
     * Delete an object using the common delete threadpool and return a future.
     * @param path Path of the object to delete asynchronously.
     * @throws IOException
     */
    public Future<?> deleteFileAsync(final Path path) throws IOException {
        Runnable deleteRunnable = () -> {
            try {
                delete(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return this.parallelDeleteExecutor.submit(deleteRunnable);
    }

    /**
     * Delete an directory using the common delete threadpool and return a future.
     * @param dir Path of the directory to delete asynchronously.
     * @throws IOException
     */
    public Future<?> deleteDirAsync(final Path dir) throws IOException {
        Runnable deleteRunnable = () -> {
            try {
                deleteDirectory(dir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return this.parallelDeleteExecutor.submit(deleteRunnable);
    }

    public int getRecursiveDirListingFetchSize() {
        return recursiveDirListingFetchSize;
    }

    public boolean isBatchDeleteEnabled() {
        return batchDeleteEnabled;
    }

    public int getBatchDeleteSize() {
        return batchDeleteSize;
    }

    /**
     * Deletes the directory at the given path.
     *
     * @param path Path of object to delete.
     * @throws IOException if the operation cannot be completed.
     */
    public void deleteDirectory(final Path path) throws IOException {
        if (path.isRoot()) {
            LOG.debug("Deleting root directory is a no-op");
            return;
        }

        final String directory = this.pathToDirectory(path);
        LOG.debug("Attempting to delete directory {} from path {}", directory, path);

        try {
            getDeleteObjectResponse(this.requestBuilder.deleteObject(directory));
            // Invalidate the cache after deleting the directory to ensure data consistency when objectMetadataCache is enabled
            this.objectMetadataCache.invalidate(this.pathToObject(path));
            this.statistics.incrementWriteOps(1);
        } catch (final BmcException e) {
            // deleting an object that doesn't actually exist, nothing to do.
            if (e.getStatusCode() != 404) {
                LOG.debug("Failed to delete directory {}", directory, e);
                throw new IOException("Error attempting to delete directory", e);
            }
        }
    }

    /**
     * Deletes multiple files using the Batch Delete API with incremental retry strategy.
     * Uses DeletionEntry pattern for per-object retry tracking. Only retries failed objects.
     */
    public void deleteFilesInBatches(final List<Path> filePaths) throws IOException {
        if (filePaths == null || filePaths.isEmpty()) {
            LOG.debug("No files to delete");
            return;
        }

        LOG.info("Deleting {} files using batch delete API with incremental retry", filePaths.size());

        // Convert to DeletionEntry for per-object retry tracking
        final LinkedList<DeletionEntry> pendingDeletes = new LinkedList<>();
        for (Path path : filePaths) {
            pendingDeletes.add(new DeletionEntry(path));
        }

        final int maxRetries = propertyAccessor.asInteger().get(BmcProperties.WRITE_MAX_RETRIES);
        final DefaultRetryCondition retryCondition = new DefaultRetryCondition();

        // Process queue until empty
        while (!pendingDeletes.isEmpty()) {
            // Take next batch from queue (up to batchDeleteSize)
            final List<DeletionEntry> currentBatch = takeNextBatch(pendingDeletes, batchDeleteSize);

            final List<Path> batchPaths = currentBatch.stream()
                    .map(DeletionEntry::getPath)
                    .collect(Collectors.toList());

            LOG.debug("Processing batch of {} objects", batchPaths.size());

            final BatchDeleteObjectsResult result;
            try {
                result = executeBatchDeleteAndGetResult(batchPaths);
            } catch (IOException e) {
                // Handle 501
                if (e.getCause() instanceof BmcException) {
                    BmcException bmcEx = (BmcException) e.getCause();
                    if (bmcEx.getStatusCode() == 501) {
                        LOG.info("Batch delete not supported (501), switching to individual deletes");

                        // Collect all remaining paths (current batch + pending queue)
                        List<Path> allRemainingPaths = new ArrayList<>(batchPaths);
                        for (DeletionEntry pending : pendingDeletes) {
                            allRemainingPaths.add(pending.getPath());
                        }

                        // Fall back
                        LOG.debug("Deleting {} files using individual async deletes", allRemainingPaths.size());
                        final int batchSize = getRecursiveDirListingFetchSize();
                        final List<Future<?>> deleteFutures = new ArrayList<>();

                        for (Path path : allRemainingPaths) {
                            deleteFutures.add(deleteFileAsync(path));

                            // Wait for batch to complete
                            if (deleteFutures.size() >= batchSize) {
                                dealWithDeleteFutures(deleteFutures, path);
                            }
                        }

                        // Wait for remaining futures
                        if (!deleteFutures.isEmpty()) {
                            dealWithDeleteFutures(deleteFutures, allRemainingPaths.get(allRemainingPaths.size() - 1));
                        }

                        LOG.debug("Successfully deleted all {} files using individual deletes", allRemainingPaths.size());
                        return;
                    }
                }
                // Not a 501
                throw e;
            }
            
            final Map<String, DeletionEntry> entryIndex = buildEntryIndex(currentBatch);

            // Emit per-object DELETE metrics for successful deletions only
            // Failed objects will emit metrics only when we decide NOT to retry them
            emitSuccessMetrics(result, entryIndex);

            // Process failures - add retryable ones back to queue
            if (result.getFailed() != null) {
                for (FailedObjectResult failure : result.getFailed()) {
                    final DeletionEntry entry = entryIndex.get(failure.getObjectName());

                    if (entry == null) {
                        LOG.warn("Could not find DeletionEntry for failed object: {}", failure.getObjectName());
                        continue;
                    }

                    // Skip 404s - object already deleted
                    if (failure.getStatusCode() != null && failure.getStatusCode() == 404) {
                        LOG.debug("Skipping 404 for object: {} (already deleted)", entry.getPath());
                        continue;
                    }

                    // Check if retryable
                    if (!isRetryableFailure(failure, retryCondition)) {
                        // Non-retryable failure - emit failure metric
                        emitFailureMetric(entry, failure);

                        LOG.error("Non-retryable failure for object {}: status={}, message={}",
                                entry.getPath(),
                                failure.getStatusCode(),
                                failure.getErrorMessage());
                        throw new IOException(
                                "Non-retryable failure for object " + entry.getPath() +
                                ": " + failure.getErrorMessage());
                    }
                    entry.recordRetry(failure.getStatusCode() != null ? failure.getStatusCode() : 0);

                    // Check if within retry limit
                    if (entry.shouldRetry(maxRetries)) {
                        LOG.debug("Adding object {} back to queue for retry (attempt {})",
                                entry.getPath(), entry.getRetryAttempt());
                        pendingDeletes.addLast(entry); // Add to end of queue for retry
                    } else {
                        // Max retries exceeded - emit failure metric
                        emitFailureMetric(entry, failure);

                        LOG.error("Object {} exceeded max retries ({}) - status={}, message={}",
                                entry.getPath(),
                                maxRetries,
                                failure.getStatusCode(),
                                failure.getErrorMessage());
                        throw new IOException(
                                "Object " + entry.getPath() +
                                " failed after " + maxRetries +
                                " retries: " + failure.getErrorMessage());
                    }
                }
            }
        }

        LOG.info("Successfully deleted all {} files", filePaths.size());
    }

    public void dealWithDeleteFutures(List<Future<?>> deleteFutures, Path path) throws IOException {
        for (Future<?> future : deleteFutures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new IOException("Exception while deleting : " + path, e);
            }
        }
        deleteFutures.clear();
    }

    /**
     * Takes the next batch of DeletionEntry objects from the queue.
     */
    private List<DeletionEntry> takeNextBatch(final LinkedList<DeletionEntry> queue, final int batchSize) {
        final List<DeletionEntry> batch = new ArrayList<>();
        for (int i = 0; i < batchSize && !queue.isEmpty(); i++) {
            batch.add(queue.removeFirst());
        }
        return batch;
    }

    /**
     * Determines if a failed object should be retried using DefaultRetryCondition.
     */
    private boolean isRetryableFailure(final FailedObjectResult failure, final DefaultRetryCondition retryCondition) {
        final Integer statusCode = failure.getStatusCode();

        if (statusCode != null && statusCode == 404) {
            return false;
        }

        if (statusCode != null) {
            final BmcException syntheticException =
                    new BmcException(
                            statusCode,
                            null,
                            failure.getErrorMessage(),
                            null);
            return retryCondition.shouldBeRetried(syntheticException);
        }
        return true;
    }

    /**
     * Executes a single batch delete operation and returns the result.
     *
     * @param batch List of paths to delete
     * @return BatchDeleteObjectsResult containing deleted and failed objects
     * @throws IOException if the operation cannot be completed
     */
    private BatchDeleteObjectsResult executeBatchDeleteAndGetResult(final List<Path> batch) throws IOException {
        final BatchDeleteOperation operation =
                new BatchDeleteOperation(
                        objectStorage,
                        requestBuilder,
                        batch,
                        statistics,
                        ociMonitorPluginHandler,
                        propertyAccessor);

        try {
            return operation.call();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Failed to execute batch delete for " + batch.size() + " files", e);
        }
    }

    private Map<String, DeletionEntry> buildEntryIndex(final List<DeletionEntry> entries) {
        final Map<String, DeletionEntry> index = new HashMap<>(entries.size());
        for (final DeletionEntry entry : entries) {
            index.put(entry.pathToObject(), entry);
        }
        return index;
    }

    /**
     * Emits per-object DELETE success metrics after processing a batch delete result.
     * Only emits for successfully deleted objects. Failure metrics are emitted separately
     * when we determine the object should not be retried.
     */
    private void emitSuccessMetrics(
            final BatchDeleteObjectsResult result,
            final Map<String, DeletionEntry> entryIndex) {

        if (!ociMonitorPluginHandler.isEnabled()) {
            return;
        }

        if (entryIndex.isEmpty()) {
            return;
        }

        // Emit success metrics for deleted objects
        if (result.getDeleted() != null) {
            for (DeletedObjectResult deleted : result.getDeleted()) {
                if (deleted.getObjectName() == null) {
                    continue;
                }

                final DeletionEntry entry = entryIndex.get(deleted.getObjectName());
                if (entry != null) {
                    ociMonitorPluginHandler.recordStats(
                            OCIMetricKeys.DELETE,
                            0.0,
                            null,
                            entry.getRetryAttempt(),
                            entry.getRetry503Count(),
                            entry.getRetry429Count()
                    );
                } else {
                    LOG.warn("Could not find DeletionEntry for deleted object in metrics emission: {}",
                            deleted.getObjectName());
                }
            }
        }
    }

    /**
     * Emits a DELETE error metric for a single failed object.
     * Called when we determine the object should not be retried (either non-retryable error
     * or max retries exceeded).
     */
    private void emitFailureMetric(final DeletionEntry entry, final FailedObjectResult failure) {
        if (!ociMonitorPluginHandler.isEnabled()) {
            return;
        }

        final BmcException exception = new BmcException(
                failure.getStatusCode() != null ? failure.getStatusCode() : 0,
                null,
                failure.getErrorMessage(),
                null
        );

        ociMonitorPluginHandler.recordStats(
                OCIMetricKeys.DELETE,
                0.0,
                exception,
                entry.getRetryAttempt(),
                entry.getRetry503Count(),
                entry.getRetry429Count()
        );
    }

    /**
     * Creates a pseudo directory at the given path.
     *
     * @param path The path to create a directory object at.
     * @throws IOException if the operation cannot be completed.
     */
    public void createDirectory(final Path path) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        // nothing to do for the "root" directory
        if (this.isRootDirectory(path)) {
            LOG.debug("Root directory specified, nothing to create");
            return;
        }

        final String key = this.pathToDirectory(path);

        LOG.debug("Attempting to create directory {} with object {}", path, key);

        final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        final PutObjectResponse response;
        RetryMetricsCollector collector = new RetryMetricsCollector(OCIMetricKeys.WRITE, retryTimeoutInSeconds, retryResetThresholdInSeconds);

        try {
            response = this.objectStorage.putObject(this.requestBuilder.putObjectWithIfNoneMatch(key, bais, 0L, collector));
            sw.stop();
            recordOCIStats(OCIMetricKeys.WRITE, sw.elapsed(TimeUnit.MILLISECONDS), null, collector.getAttemptCount(),
                    collector.getRetry503Count(), collector.getRetry429Count());

            this.statistics.incrementWriteOps(1);
            LOG.debug("Created directory at {} with etag {}", path, response.getETag());
        } catch (final BmcException e) {
            sw.stop();
            recordOCIStats(OCIMetricKeys.WRITE, sw.elapsed(TimeUnit.MILLISECONDS), e, collector.getAttemptCount(),
                    collector.getRetry503Count(), collector.getRetry429Count());
            // if running jobs in parallel, it's possible multiple threads try to
            // create the directory at the same time, which might lead to 409 conflicts.
            // also allowing 412 (even though we don't set im/inm headers) as we
            // basically just want to ensure the placeholder directory object exists
            if (e.getStatusCode() != 409 && e.getStatusCode() != 412) {
                LOG.debug("Failed to create directory for {}", key, e);
                throw new IOException("Unable to put object", e);
            }
            LOG.debug(
                    "Exception while creating directory, ignoring {} {}",
                    e.getStatusCode(),
                    e.getMessage());
        } finally {
            collector.close();
        }
    }

    /**
     * Tests to see if the directory at the given path is considered empty or not.
     *
     * @param path The directory path.
     * @return true if the directory is empty, false if not.
     * @throws IOException if the operation could not be completed.
     */
    public boolean isEmptyDirectory(final Path path) throws IOException {
        final String key = this.pathToDirectory(path);
        LOG.debug("Checking to see if directory path {} is empty (object key {})", path, key);

        final ListObjectsRequest request = this.requestBuilder.listObjects(key, null, "/", 2);
        this.statistics.incrementReadOps(1);

        final ListObjectsResponse response;
        try {
            response = getListObjectsResponse(request);
        } catch (final BmcException e) {
            LOG.debug("Failed to list objects for {}", key, e);
            throw new IOException("Unable to determine if path is a directory", e);
        }

        final boolean hasSubDirectories = !response.getListObjects().getPrefixes().isEmpty();
        if (hasSubDirectories) {
            return false;
        }

        if (response.getListObjects().getObjects().isEmpty()) {
            return true;
        }
        if (response.getListObjects().getObjects().size() > 1) {
            return false;
        }
        return response.getListObjects().getObjects().get(0).getName().equals(key);
    }

    /**
     * Returns the status of each entry in the directory specified.
     *
     * @param path The directory path.
     * @return A list of file statuses, or empty if the directory was empty.
     * @throws IOException if the operation could not be completed
     */
    public List<FileStatus> listDirectory(final Path path) throws IOException {
        final String key = this.pathToDirectory(path);
        LOG.debug("Listing directory for path {}, object {}", path, key);

        final ArrayList<FileStatus> entries = new ArrayList<>();

        try {
            ListObjectsRequest request = null;
            ListObjectsResponse response = null;
            String nextToken = null;

            Set<String> prefixes = new HashSet<>();
            do {
                LOG.debug("Listing objects with next token {}", nextToken);
                request = this.requestBuilder.listObjects(key, nextToken, "/", 1000);
                response = getListObjectsResponse(request);

                nextToken =
                        calculateNextToken(
                                response.getListObjects().getNextStartWith(),
                                response.getListObjects().getPrefixes());

                this.statistics.incrementReadOps(1);

                final List<ObjectSummary> summaries = response.getListObjects().getObjects();
                for (final ObjectSummary summary : summaries) {
                    // skip over the placeholder directory object
                    if (summary.getName().equals(key)) {
                        continue;
                    }
                    entries.add(this.createFileStatus(path, summary));
                }
                for (final String prefix : response.getListObjects().getPrefixes()) {
                    // depending on how many items were paged over, it's possible the same prefix
                    // can be found on different pages
                    if (!prefixes.contains(prefix)) {
                        prefixes.add(prefix);
                        entries.add(this.createDirectoryFileStatus(path, prefix));
                    }
                }
            } while (nextToken != null);
        } catch (final BmcException e) {
            LOG.debug("Failed to list objects for {}", key, e);
            throw new IOException("Unable to determine if path is a directory", e);
        }

        return entries;
    }

    private void recordOCIStats(String key, long overallTime, Exception e, int retryAttempts, int retry503Count, int retry429Count) {
        ociMonitorPluginHandler.recordStats(key, overallTime, e, retryAttempts, retry503Count, retry429Count);
    }

    private void recordOCIStats(String key, long overallTime, Exception e, int retryAttempts, int retry503Count, int retry429Count, boolean isCoalesced) {
        ociMonitorPluginHandler.recordStats(key, overallTime, e, retryAttempts, retry503Count, retry429Count, isCoalesced);
    }
    /**
     * A method to list all files/dirs in a given directory in a flat manner. This is done without using any
     * delimiters in the OSS list objects API.
     * @param path The path to the directory for which the listing needs to be done.
     * @param nextToken This is the token string in order to continue with a next page of results. It should be
     *                  passed null in the first call to start fresh.
     * @return A pair containing list of FileStatus objects and a possible token to the next page.
     * @throws IOException
     */
    public Pair<List<FileStatus>, String> flatListDirectoryRecursive(final Path path,
                                                                     String nextToken) throws IOException {
        ArrayList<FileStatus> entries = new ArrayList<>();
        String key = this.pathToDirectory(path);
        String freshToken = null;

        try {

            ListObjectsRequest request = this.requestBuilder.listObjects(
                    key, nextToken, null, recursiveDirListingFetchSize);
            ListObjectsResponse response = getListObjectsResponse(request);

            List<ObjectSummary> summaries = response.getListObjects().getObjects();

            freshToken = response.getListObjects().getNextStartWith();
            for (ObjectSummary summary : summaries) {
                entries.add(createFileStatus(path, summary));
            }

        } catch (final BmcException e) {
            LOG.debug("Failed to list objects for {}", key, e);
            throw new IOException("Failed to list path for "+key, e);
        }
        return new ImmutablePair<>(entries, freshToken);
    }

    private static String calculateNextToken(String token, List<String> prefixes) {
        if (token == null) {
            return null;
        }
        if (prefixes.isEmpty()) {
            return token;
        }
        // last prefix (they're alpha ordered)
        String lastPrefix = prefixes.get(prefixes.size() - 1);
        // the next page token is the start object of the next page.
        // if the next token starts with the last prefix (in directory form), then
        // we can skip over all objects and directories contained within the last
        // prefix rather than having to iterate over them
        // ex, if next token is 'foo/bar/baz.txt', and the prefixes list is ['foo/abc/, 'foo/bar/'],
        // then we can change the next page token to 'foo/bar0' instead to skip over all
        // files that match 'foo/bar/*'
        if (token.startsWith(lastPrefix)) {
            token = lastPrefix.substring(0, lastPrefix.length() - 1) + "0";
        }
        return token;
    }

    private FileStatus createFileStatus(final Path parentPath, final ObjectSummary summary)
            throws IOException {
        return new FileStatus(
                summary.getSize(),
                this.isDirectory(summary),
                BLOCK_REPLICATION,
                this.blockSizeInBytes,
                summary.getTimeModified().getTime(),
                this.objectToPath(parentPath, summary.getName()));
    }

    private FileStatus createDirectoryFileStatus(final Path parentPath, final String prefix)
            throws IOException {
        // TODO: cannot get modification/creation time from list prefixes
        return new FileStatus(
                0,
                true,
                BLOCK_REPLICATION,
                this.blockSizeInBytes,
                LAST_MODIFICATION_TIME,
                this.objectToPath(parentPath, prefix));
    }

    public ContentSummary getContentSummary(final Path path) throws IOException {
        final String objKey = this.pathToObject(path);
        LOG.debug("Getting content summary for path {}, object {}", path, objKey);

        try {
            HeadObjectResponse response = getHeadObjectResponse(this.requestBuilder.headObject(objKey));
            this.statistics.incrementReadOps(1);
            return new ContentSummary.Builder()
                    .length(response.getContentLength())
                    .fileCount(1)
                    .directoryCount(0)
                    .spaceConsumed(response.getContentLength())
                    .build();
        } catch (final BmcException e) {
            if (e.getStatusCode() != 404) {
                throw new IOException("Unable to get content summary for " + path, e);
            }
        }

        final String key = this.pathToDirectory(path);
        long sumLength = 0;
        long sumFileCount = 0;
        long sumDirectoryCount = 0;

        try {
            ListObjectsRequest request = null;
            ListObjectsResponse response = null;
            String nextToken = null;
            String lastDirectoryVisited = null;

            do {
                LOG.debug("Listing objects with next token {}", nextToken);
                request = this.requestBuilder.listObjects(key, nextToken, null, 1000);
                response = getListObjectsResponse(request);
                nextToken = response.getListObjects().getNextStartWith();

                this.statistics.incrementReadOps(1);

                final List<ObjectSummary> summaries = response.getListObjects().getObjects();
                for (final ObjectSummary summary : summaries) {
                    String objectName = summary.getName();
                    // We need to count any implicit directories for which the marker object is
                    // missing. Start by finding the first ancestor of objectName that is not
                    // also an ancestor of lastDirectoryVisited.
                    int parentLength = key.length();
                    if (lastDirectoryVisited != null) {
                        while (parentLength < lastDirectoryVisited.length()
                                && parentLength < objectName.length()
                                && lastDirectoryVisited.charAt(parentLength) == objectName.charAt(parentLength)) {
                            parentLength++;
                        }
                        parentLength = objectName.indexOf('/', parentLength);
                        if (parentLength != -1) {
                            // Include the terminating '/'
                            parentLength += 1;
                        }
                    }
                    // Count the missing parent directories
                    while (parentLength != -1 && parentLength < objectName.length()) {
                        sumDirectoryCount++;
                        lastDirectoryVisited = objectName.substring(0, parentLength);

                        parentLength = objectName.indexOf('/', parentLength);
                        if (parentLength != -1) {
                            // Include the terminating '/'
                            parentLength += 1;
                        }
                    }

                    // Count the object returned by the listing
                    if (this.isDirectory(summary)) {
                        sumDirectoryCount++;
                        lastDirectoryVisited = summary.getName();
                    } else {
                        sumFileCount++;
                        sumLength += summary.getSize();
                    }
                }
            } while (nextToken != null);
        } catch (final BmcException e) {
            LOG.debug("Failed to list objects for {}", key, e);
            throw new IOException("Unable to determine if path is a directory", e);
        }

        return new ContentSummary.Builder()
                .length(sumLength)
                .fileCount(sumFileCount)
                .directoryCount(sumDirectoryCount)
                .spaceConsumed(sumLength)
                .build();
    }

    /**
     * Returns the {@link FileStatus} for the object at the given path.
     *
     * @param path The path to query.
     * @return The file status, null if there was no file at this location.
     * @throws IOException if the operation could not be completed.
     */
    public FileStatus getFileStatus(final Path path) throws IOException {
        return getFileStatus(path, false);
    }

    public FileStatus getFileStatus(final Path path, boolean checkOnlyExists) throws IOException {
        // base case, root directory always exists, nothing to create
        if (this.isRootDirectory(path)) {
            LOG.debug("Requested file status for root directory");
            return new FileStatus(
                    0,
                    true,
                    BLOCK_REPLICATION,
                    this.blockSizeInBytes,
                    LAST_MODIFICATION_TIME,
                    path);
        }

        final String key = this.pathToObject(path);
        LOG.debug("Getting file status for path {}, object {}", path, key);

        try {
            FileStatusInfo fileStatusInfo = checkOnlyExists
                    ? this.getFileStatusUncached(key, true)
                    : objectMetadataCache.getUnchecked(key);

            if (fileStatusInfo == null && checkOnlyExists) {
                return null;
            }
            return new FileStatus(fileStatusInfo.contentLength,
                    fileStatusInfo.isDirectory,
                    BLOCK_REPLICATION,
                    this.blockSizeInBytes,
                    fileStatusInfo.modificationTime,
                    path);
        } catch (UncheckedExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
                throw (IOException) ee.getCause();
            } else if (ee.getCause() instanceof ObjectMetadataNotFoundException) {
                return null;
            } else {
                throw ee;
            }
        }
    }

    /**
     * Retrieves the FileStatusInfo for a given key, bypassing the object metadata cache.
     * If the key ends with "/", it's treated as a directory. If not, it's treated as a file.
     * For a directory:
     *     - Returns its status if exists, otherwise throws ObjectMetadataNotFoundException.
     * For a file:
     *     - Returns a new FileStatusInfo with metadata, or, if file doesn't exist, tries to retrieve it as a directory.
     *     - If no file or directory found, throws an ObjectMetadataNotFoundException.
     *
     * @param key The object key to retrieve the FileStatusInfo for.
     * @return The FileStatusInfo for the given key.
     * @throws IOException if the operation could not be completed.
     * @throws ObjectMetadataNotFoundException if neither the object metadata nor directory are found.
     */
    private FileStatusInfo getFileStatusUncached(final String key) throws IOException {
        return getFileStatusUncached(key, false);
    }

    private FileStatusInfo getFileStatusUncached(final String key, final boolean checkOnlyExists) throws IOException {
        if (key.endsWith("/")) {
            FileStatusInfo directoryStatus = getDirectoryStatus(key, checkOnlyExists);
            if (directoryStatus == null) {
                throw new ObjectMetadataNotFoundException(key);
            }
            return directoryStatus;
        }

        try {
            HeadObjectResponse response = checkOnlyExists
                    ? getHeadObjectResponse(this.requestBuilder.headObjectWithNonMatchingIfMatch(key))
                    : getHeadObjectResponse(this.requestBuilder.headObject(key));
            return new FileStatusInfo(response.getContentLength(),
                    false,
                    response.getLastModified().getTime());
        } catch (final BmcException e) {
            // also try to query for a directory with this key name
            if (e.getStatusCode() == 404) {
                FileStatusInfo directoryStatus = getDirectoryStatus(key + "/", checkOnlyExists);
                if (directoryStatus != null) {
                    return directoryStatus;
                }
            } else if (e.getStatusCode() == 412) {
                // Optimization: file exists but ETag didn't match : treat as valid case
                // Passing contentLength = -1L for file
                return new FileStatusInfo(-1L, false, System.currentTimeMillis());
            } else {
                LOG.debug("Failed to get object metadata for {}", key, e);
                throw new IOException("Unable to fetch file status for: " + key, e);
            }
        } finally {
            this.statistics.incrementReadOps(1);
        }

        // nothing left, return null
        if (checkOnlyExists) {
            return null;
        }
        throw new ObjectMetadataNotFoundException(key);
    }

    /**
     * Checks if the specified path is a directory.
     *
     * @param path
     * @return true if path exists and is a directory, false otherwise.
     */
    public boolean isDirectory(Path path, boolean checkOnlyExists) throws IOException {

        if (this.isRootDirectory(path)) {
            return true;
        }

        final String dirKey = this.pathToDirectory(path);

        FileStatusInfo fileStatus = null;
        if (!checkOnlyExists) {
            fileStatus = this.objectMetadataCache.getIfPresent(dirKey);
            if (fileStatus != null) {
                return fileStatus.isDirectory;
            }
        }
        fileStatus = this.getDirectoryStatus(dirKey, true);
        return fileStatus != null && fileStatus.isDirectory;
    }

    /**
     * Retrieves the FileStatusInfo for a directory with the given key. The method sends a ListObjectsRequest to
     * the object storage to fetch a list of objects with a maximum size of 1, using the provided key as a prefix.
     * Although there can be multiple objects with the provided key as a prefix, we're only interested in the
     * directory represented by the key itself. Hence, we only fetch one object with the maximum size of 1.
     * If the list is empty (no objects found with the key as a prefix), the method returns null.
     * If the list has an object with the same key and its size is not 0 (indicating it is a file),
     * it returns a new FileStatusInfo instance with the object's size, false for isDirectory, and the
     * object's modification time. If the object size is 0 (indicating it is a directory), it returns a
     * new FileStatusInfo instance with size 0, true for isDirectory, and the object's creation time.
     *
     * @param key The object key for which to retrieve the directory status.
     * @return The FileStatusInfo for the directory, or null if the object list is empty.
     * @throws IOException if the operation could not be completed.
     */
    private FileStatusInfo getDirectoryStatus(String key) throws IOException {
        return getDirectoryStatus(key, false);
    }

    private FileStatusInfo getDirectoryStatus(String key, boolean checkOnlyExists) throws IOException {
        LOG.debug("Getting directory status for object key {}", key);
        if (propertyAccessor.asBoolean().get(BmcProperties.REQUIRE_DIRECTORY_MARKER)) {
            // The property is true, so we assume if the directory exists it has a placeholder object.
            try {
                HeadObjectResponse response = checkOnlyExists
                        ? getHeadObjectResponse(this.requestBuilder.headObjectWithNonMatchingIfMatch(key))
                        : getHeadObjectResponse(this.requestBuilder.headObject(key));
                return new FileStatusInfo(0L,
                        true,
                        response.getLastModified().getTime());
            } catch (final BmcException e) {
                // If the marker object is missing, assume the directory does not exist
                if (e.getStatusCode() == 404) {
                    return null;
                } else if (e.getStatusCode() == 412){
                    // Optimization: directory exists but ETag didn't match : treat as valid case
                    // Passing contentLength = 0L for directory
                    return new FileStatusInfo(0L, true, System.currentTimeMillis());
                } else {
                    LOG.debug("Failed to get object metadata for {}", key, e);
                    throw new IOException("Unable to fetch file status for: " + key, e);
                }
            } finally {
                this.statistics.incrementReadOps(1);
            }
        }

        final ListObjectsRequest request = this.requestBuilder.listObjects(key, null, null, 1);
        this.statistics.incrementReadOps(1);

        final ListObjectsResponse response;
        try {
            response = getListObjectsResponse(request);
        } catch (final BmcException e) {
            LOG.debug("Failed to list objects for {}", key, e);
            throw new IOException("Unable to determine if path is a directory", e);
        } finally {
            this.statistics.incrementReadOps(1);
        }

        if (response.getListObjects().getObjects().isEmpty()) {
            return null;
        }

        ObjectSummary objSummary = response.getListObjects().getObjects().get(0);
        long modifiedTime = LAST_MODIFICATION_TIME;
        if (objSummary.getName().equals(key)) {
            if (objSummary.getSize() != 0L) {
                return new FileStatusInfo(objSummary.getSize(),
                        false,
                        objSummary.getTimeModified().getTime());
            }
            modifiedTime = objSummary.getTimeCreated().getTime();
        }

        return new FileStatusInfo(0L,
                true,
                modifiedTime);
    }

    private static class ObjectMetadataNotFoundException extends RuntimeException {
        @Getter private final String key;

        public ObjectMetadataNotFoundException(String key) {
            super("Object metadata not found for key: " + key);
            this.key = key;
        }
    }

    /**
     * Creates a new {@link FSInputStream} that can be used to read the object at the given path.
     *
     * @param status            The file status for this file.
     * @param path              The path to open.
     * @param bufferSizeInBytes The buffer size in bytes (may not be used).
     * @param statistics        The {@link Statistics} instance to publish metrics into.
     * @return A new input stream to read from.
     */
    public FSInputStream openReadStream(
            final FileStatus status,
            final Path path,
            final int bufferSizeInBytes,
            final Statistics statistics) {
        LOG.debug("Opening read stream for {}", path);
        RetryMetricsCollector collector = new RetryMetricsCollector(OCIMetricKeys.READ, retryTimeoutInSeconds, retryResetThresholdInSeconds);
        final Supplier<GetObjectRequest.Builder> requestBuilder =
                new GetObjectRequestFunction(path, collector);

        if (!StringUtils.isBlank(this.customReadStreamClass)) {
            FSInputStream readStreamInstance =
                    createCustomReadStreamClass(
                            this.customReadStreamClass,
                            this.objectStorage,
                            status,
                            requestBuilder,
                            this.statistics);
            return readStreamInstance;
        }
        if (this.useInMemoryReadBuffer) {
            return new StatsMonitorInputStream(new BmcInMemoryFSInputStream(
                    this.objectStorage,
                    status,
                    requestBuilder,
                    this.propertyAccessor.asInteger().get(BmcProperties.READ_MAX_RETRIES),
                    this.statistics,
                    collector),
                    ociMonitorPluginHandler, collector);
        }
        if (this.useReadAhead) {
            if (this.readAheadBlockCount > 1) {
                return new StatsMonitorInputStream(
                        new BmcParallelReadAheadFSInputStream(
                                this.objectStorage,
                                status,
                                requestBuilder,
                                this.propertyAccessor.asInteger().get(BmcProperties.READ_MAX_RETRIES),
                                this.statistics,
                                this.parallelDownloadExecutor,
                                this.readAheadSizeInBytes,
                                this.readAheadBlockCount,
                                collector, firstReadOptimizationForTTFBEnabled), ociMonitorPluginHandler, collector);
            }
            return new StatsMonitorInputStream(
                    new BmcReadAheadFSInputStream(
                            this.objectStorage,
                            status,
                            requestBuilder,
                            this.propertyAccessor.asInteger().get(BmcProperties.READ_MAX_RETRIES),
                            this.statistics,
                            this.readAheadSizeInBytes,
                            this.parquetCacheString, collector, firstReadOptimizationForTTFBEnabled, parquetCacheEnabled), ociMonitorPluginHandler, collector);
        } else {
            return new StatsMonitorInputStream(
                    new BmcDirectFSInputStream(
                            this.objectStorage,
                            status,
                            requestBuilder,
                            this.propertyAccessor.asInteger().get(BmcProperties.READ_MAX_RETRIES),
                            this.statistics, collector),ociMonitorPluginHandler, collector);
        }
    }

    /**
     * Creates a new {@link OutputStream} that can be written to in order to create a new file.
     *
     * @param path              The path for the new file.
     * @param bufferSizeInBytes The buffer size in bytes can be configured by setting the config key {@link BmcConstants#MULTIPART_PART_SIZE_IN_MB_KEY}.
     *                          Default value is 128MiB which comes from OCI Java SDK {@link UploadConfiguration}
     * @param progress          {@link Progressable} instance to report progress updates to.
     * @return A new output stream to write to.
     */
    public OutputStream openWriteStream(
            final Path path, int bufferSizeInBytes, final Progressable progress) {
        LOG.debug("Opening write stream to {}", path);
        final boolean allowOverwrite =
                this.propertyAccessor.asBoolean().get(BmcProperties.OBJECT_ALLOW_OVERWRITE);
        LOG.debug("Allowing overwrites during object upload");

        // The value set for MULTIPART_PART_SIZE_IN_MB is in megabytes and needs to be converted to bytes
        final Integer lengthPerUploadPart =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_PART_SIZE_IN_MB);
        if (lengthPerUploadPart != null) {
            bufferSizeInBytes = lengthPerUploadPart * MiB;
            LOG.debug("Buffer size in bytes: {}", bufferSizeInBytes);
        }
        RetryMetricsCollector collector = new RetryMetricsCollector(OCIMetricKeys.WRITE_PART, retryTimeoutInSeconds, retryResetThresholdInSeconds);

        final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn =
                new UploadDetailsFunction(this.pathToObject(path), allowOverwrite, progress, collector);

        if (!StringUtils.isBlank(this.customWriteStreamClass)) {
            LOG.debug("Using custom write stream class: {}", customWriteStreamClass);
            OutputStream writeStreamInstance =
                    createCustomWriteStreamClass(
                            this.customWriteStreamClass,
                            this.propertyAccessor,
                            this.uploadManager,
                            bufferSizeInBytes,
                            requestBuilderFn);
            return writeStreamInstance;
        }
        // takes precedence
        if (this.useMultipartUploadWriteBuffer) {
            final String objectName = this.pathToObject(path);
            final CreateMultipartUploadDetails details =
                    CreateMultipartUploadDetails.builder().object(objectName).build();
            final CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder =
                    CreateMultipartUploadRequest.builder()
                            .bucketName(this.bucket)
                            .namespaceName(this.namespace)
                            .createMultipartUploadDetails(details);
            if (additionalChecksumAlgorithm != null &&
                    additionalChecksumAlgorithm.equalsIgnoreCase(ChecksumAlgorithm.Crc32C.getValue()) ) {
                createMultipartUploadRequestBuilder.opcChecksumAlgorithm(ChecksumAlgorithm.Crc32C);
            }
            final CreateMultipartUploadRequest createMultipartUploadRequest =
                    createMultipartUploadRequestBuilder.buildWithoutInvocationCallback();
            final MultipartUploadRequest multipartUploadRequest =
                    MultipartUploadRequest.builder()
                            .objectStorage(this.objectStorage)
                            .multipartUploadRequest(createMultipartUploadRequest)
                            .allowOverwrite(allowOverwrite)
                            .build();
            return new StatsMonitorOutputStream(new BmcMultipartOutputStream(
                    this.propertyAccessor,
                    multipartUploadRequest,
                    bufferSizeInBytes,
                    parallelUploadExecutor,
                    this.propertyAccessor.asInteger().get(BmcProperties.WRITE_MAX_RETRIES),
                    new RetryMetricsCollector(OCIMetricKeys.WRITE, retryTimeoutInSeconds, retryResetThresholdInSeconds),
                    true, uploadConfiguration),
                    ociMonitorPluginHandler,
                    collector);
        } else if (this.useInMemoryWriteBuffer) {
            return new StatsMonitorOutputStream(new BmcInMemoryOutputStream(
                    this.uploadManager, bufferSizeInBytes, requestBuilderFn,
                    this.propertyAccessor.asInteger().get(BmcProperties.WRITE_MAX_RETRIES), false, uploadConfiguration),
                    ociMonitorPluginHandler, collector);
        } else {
            return new StatsMonitorOutputStream(new BmcFileBackedOutputStream(
                    this.propertyAccessor, this.uploadManager, requestBuilderFn,
                    this.propertyAccessor.asInteger().get(BmcProperties.WRITE_MAX_RETRIES), false, uploadConfiguration),
                    ociMonitorPluginHandler, collector);
        }
    }

    public FileChecksum getFileChecksum(Path path) throws IOException {
        if (additionalChecksumAlgorithm == null) {
            return null;
        }

        if (!ChecksumAlgorithm.Crc32C.getValue().equalsIgnoreCase(additionalChecksumAlgorithm)) {
            LOG.warn("Unsupported checksum algorithm: {}", additionalChecksumAlgorithm);
            return null;
        }

        String objectName = pathToObject(path);
        LOG.debug("Get Checksum for objectName : {}", objectName);

        HeadObjectResponse headObjectResponse;
        try (RetryMetricsCollector collector = new RetryMetricsCollector(OCIMetricKeys.HEAD, retryTimeoutInSeconds, retryResetThresholdInSeconds)) {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucketName(this.bucket)
                    .namespaceName(this.namespace)
                    .objectName(objectName)
                    .retryConfiguration(collector.getRetryConfiguration())
                    .build();
            headObjectResponse = getHeadObjectResponse(headObjectRequest);
            this.statistics.incrementReadOps(1);
        } catch (Exception e) {
            throw new IOException("Failed to get object metadata", e);
        }

        Optional<String> checksum = Optional.ofNullable(headObjectResponse.getOpcContentCrc32c());

        if (!checksum.isPresent()) {
            LOG.warn("Checksum not found for algorithm: {}", additionalChecksumAlgorithm);
            return null;
        }

        return new CRC32CFileChecksum(checksum.get());
    }

    private static class CRC32CFileChecksum extends FileChecksum {
        private String checksum;

        public CRC32CFileChecksum(String checksum) {
            this.checksum = checksum;
        }

        public String getBase64Encode() { return checksum; }

        @Override
        public String getAlgorithmName() {
            return "COMPOSITE-CRC32C";
        }

        @Override
        public int getLength() {
            return Base64.getDecoder().decode(checksum).length;
        }

        @Override
        public byte[] getBytes() {
            return Base64.getDecoder().decode(checksum);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(checksum);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            checksum = in.readUTF();
        }
    }

    /**
     * Gets the configured block size in bytes.
     *
     * @return Block size in bytes.
     */
    public long getBlockSizeInBytes() {
        return this.blockSizeInBytes;
    }

    private boolean isRootDirectory(final Path path) {
        return path.isRoot();
    }

    private boolean isDirectory(final ObjectSummary summary) {
        return (summary.getSize() == 0L) && summary.getName().endsWith("/");
    }

    private String pathToDirectory(final Path path) {
        final String objectKey = this.pathToObject(path);

        // root is special, do not use '/'
        if (objectKey.isEmpty()) {
            return objectKey;
        }

        if (objectKey.endsWith("/")) {
            return objectKey;
        }
        return objectKey + "/";
    }

    private String pathToObject(final Path path) {
        // strip leading '/', everything else is the object name
        return path.toUri().getPath().substring(1);
    }

    private Path objectToPath(final Path parentPath, final String object) {
        return new Path(parentPath, "/" + object);
    }

    @RequiredArgsConstructor
    private final class GetObjectRequestFunction implements Supplier<GetObjectRequest.Builder> {
        private final Path path;
        private final RetryMetricsCollector retryMetricsCollector;

        @Override
        public GetObjectRequest.Builder get() {
            return BmcDataStore.this.requestBuilder.getObjectBuilder(
                    BmcDataStore.this.pathToObject(path), retryMetricsCollector);
        }
    }

    @RequiredArgsConstructor
    private final class PutObjectFromGetRequestFunction
            implements Function<GetObjectResponse, PutObjectRequest> {
        private final String objectName;

        private final RetryMetricsCollector collector;

        @Override
        public PutObjectRequest apply(GetObjectResponse getResponse) {
            // always pass MD5 when we start with a GetObjectResponse
            return BmcDataStore.this.requestBuilder.putObject(
                    objectName,
                    getResponse.getInputStream(),
                    getResponse.getContentLength(),
                    getResponse.getContentMd5(),
                    collector);
        }
    }

    @RequiredArgsConstructor
    private final class UploadDetailsFunction
            implements BiFunction<Long, InputStream, UploadRequest> {
        private final String objectName;
        private final boolean allowOverwrite;
        private final Progressable progressable;
        private final RetryMetricsCollector collector;

        @Override
        public UploadRequest apply(Long contentLengthInBytes, InputStream inputStream) {
            return BmcDataStore.this.requestBuilder.uploadRequest(
                    objectName,
                    inputStream,
                    contentLengthInBytes,
                    progressable,
                    allowOverwrite,
                    BmcDataStore.this.parallelUploadExecutor,
                    collector);
        }
    }

    @RequiredArgsConstructor
    private static final class FileStatusInfo {
        private final long contentLength;
        private final boolean isDirectory;
        private final long modificationTime;

        public FileStatus toFileStatus(long blockSizeInBytes, Path path) {
            return new FileStatus(
                    contentLength,
                    isDirectory,
                    BLOCK_REPLICATION,
                    blockSizeInBytes,
                    modificationTime,
                    path);
        }
    }

    private <T> T createCustomReadStreamClass(
            final String className,
            final ObjectStorage objectStorage,
            final FileStatus status,
            final Supplier<GetObjectRequest.Builder> requestBuilder,
            final Statistics statistics) {
        try {
            final Class<?> customClass = Class.forName(className);
            final Constructor<?> customClassConstructor =
                    customClass.getConstructor(
                            BmcPropertyAccessor.class,
                            ObjectStorage.class,
                            FileStatus.class,
                            Supplier.class,
                            Statistics.class);
            try {
                return (T)
                        customClassConstructor.newInstance(
                                this.propertyAccessor,
                                objectStorage,
                                status,
                                requestBuilder,
                                statistics);
            } catch (InstantiationException
                    | IllegalAccessException
                    | IllegalArgumentException
                    | InvocationTargetException e) {
                throw new IllegalStateException("Unable to create new custom client instance", e);
            }
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Configured to create custom class '" + className + "', but none exists");
        } catch (final NoSuchMethodException e) {
            throw new IllegalStateException(
                    "Custom client class does not have the required constructor", e);
        } catch (final SecurityException e) {
            throw new IllegalStateException("Unable to create new custom client instance", e);
        }
    }

    private <T> T createCustomWriteStreamClass(
            final String className,
            final BmcPropertyAccessor propertyAccessor,
            final UploadManager uploadManager,
            final int bufferSizeInBytes,
            final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn) {

        try {
            final Class<?> customClass = Class.forName(className);
            final Constructor<?> customClassConstructor =
                    customClass.getConstructor(
                            BmcPropertyAccessor.class,
                            UploadManager.class,
                            int.class,
                            BiFunction.class);
            try {
                return (T)
                        customClassConstructor.newInstance(
                                propertyAccessor,
                                uploadManager,
                                bufferSizeInBytes,
                                requestBuilderFn);
            } catch (InstantiationException
                    | IllegalAccessException
                    | IllegalArgumentException
                    | InvocationTargetException e) {
                throw new IllegalStateException("Unable to create new custom client instance", e);
            }
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Configured to create custom class '" + className + "', but none exists");
        } catch (final NoSuchMethodException e) {
            throw new IllegalStateException(
                    "Custom client class does not have the required constructor", e);
        } catch (final SecurityException e) {
            throw new IllegalStateException("Unable to create new custom client instance", e);
        }
    }
}
