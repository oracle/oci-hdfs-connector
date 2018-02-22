/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.oracle.bmc.objectstorage.model.RenameObjectDetails;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.util.BiFunction;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.ObjectSummary;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.ListObjectsRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.HeadObjectResponse;
import com.oracle.bmc.objectstorage.responses.ListObjectsResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration.UploadConfigurationBuilder;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * BmcDataStore is a facade to Object Store that provides CRUD operations for objects by {@link Path} references.
 * <p>
 * Statistics are updated only on successful operations, and not on attempted operations.
 */
@Slf4j
public class BmcDataStore {
    private static final int MiB = 1024 * 1024;

    // http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Data_Replication
    private static final int BLOCK_REPLICATION = 1;

    // TODO: need to get last modified date (creation date for objects) in some missing cases
    private static final long LAST_MODIFICATION_TIME = 0L;

    private final ObjectStorage objectStorage;
    private final Statistics statistics;

    private final BmcPropertyAccessor propertyAccessor;
    private final UploadManager uploadManager;
    private final ExecutorService parallelUploadExecutor;
    private final RequestBuilder requestBuilder;
    private final long blockSizeInBytes;
    private final boolean useInMemoryReadBuffer;
    private final boolean useInMemoryWriteBuffer;

    public BmcDataStore(
            final BmcPropertyAccessor propertyAccessor,
            final ObjectStorage objectStorage,
            final String namespace,
            final String bucket,
            final Statistics statistics) {
        this.propertyAccessor = propertyAccessor;
        this.objectStorage = objectStorage;
        this.statistics = statistics;

        final UploadConfigurationBuilder uploadConfigurationBuilder =
                createUploadConfiguration(propertyAccessor);
        this.parallelUploadExecutor =
                this.createExecutor(propertyAccessor, uploadConfigurationBuilder);
        final UploadConfiguration uploadConfiguration = uploadConfigurationBuilder.build();
        LOG.info("Using upload configuration: %s", uploadConfiguration);
        this.uploadManager = new UploadManager(objectStorage, uploadConfiguration);
        this.requestBuilder = new RequestBuilder(namespace, bucket);
        this.blockSizeInBytes = propertyAccessor.asLong().get(BmcProperties.BLOCK_SIZE_IN_MB) * MiB;
        this.useInMemoryReadBuffer =
                propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_READ_BUFFER);
        this.useInMemoryWriteBuffer =
                propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_WRITE_BUFFER);
    }

    private UploadConfigurationBuilder createUploadConfiguration(
            final BmcPropertyAccessor propertyAccessor) {
        UploadConfigurationBuilder uploadConfigurationBuilder = UploadConfiguration.builder();

        boolean allowMultipartUploads =
                propertyAccessor.asBoolean().get(BmcProperties.MULTIPART_ALLOWED);
        uploadConfigurationBuilder.allowMultipartUploads(allowMultipartUploads);

        Integer minimumLengthForMultipartUpload =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB);
        if (minimumLengthForMultipartUpload != null && minimumLengthForMultipartUpload > 0) {
            uploadConfigurationBuilder.minimumLengthForMultipartUpload(
                    minimumLengthForMultipartUpload);
        }

        Integer minimumLengthPerUploadPart =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_MIN_PART_SIZE_IN_MB);
        if (minimumLengthPerUploadPart != null && minimumLengthPerUploadPart > 0) {
            uploadConfigurationBuilder.minimumLengthPerUploadPart(minimumLengthPerUploadPart);
        }

        Integer maxPartsForMultipartUpload =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_MAX_PARTS);
        if (maxPartsForMultipartUpload != null && maxPartsForMultipartUpload > 0) {
            uploadConfigurationBuilder.maxPartsForMultipartUpload(maxPartsForMultipartUpload);
        }

        return uploadConfigurationBuilder;
    }

    private ExecutorService createExecutor(
            final BmcPropertyAccessor propertyAccessor,
            final UploadConfigurationBuilder uploadConfigurationBuilder) {
        final Integer numThreadsForParallelUpload =
                propertyAccessor.asInteger().get(BmcProperties.MULTIPART_NUM_UPLOAD_THREADS);
        if (numThreadsForParallelUpload == null || numThreadsForParallelUpload <= 0) {
            return null;
        }
        if (numThreadsForParallelUpload == 1) {
            uploadConfigurationBuilder.allowParallelUploads(false);
            return null;
        }
        return Executors.newFixedThreadPool(
                numThreadsForParallelUpload,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bmcs-hdfs-upload-%d")
                        .build());
    }

    /**
     * Renames an object from one name to another. This is a multi-step operation that consists of finding all matching
     * objects, copying them to the destination, and then deleting the original objects.
     *
     * @param source
     *            The source to rename, assumed to exist.
     * @param destination
     *            The destination, may not exist, will be overwritten
     * @throws IOException
     *             if the operation cannot be completed.
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
     * @param sourceDirectoryPath
     *            The source directory to rename, assumed to exist.
     * @param destinationDirectoryPath
     *            The destination directory.
     * @throws IOException
     *             if the operation cannot be completed.
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

        // find all objects to rename first to prevent any modifcation of the result set while iterating over it
        final ArrayList<String> objectsToRename = new ArrayList<>();
        try {
            ListObjectsRequest request;
            ListObjectsResponse response;
            String nextToken = null;
            do {
                LOG.debug("Making request with next token {}", nextToken);
                request = this.requestBuilder.listObjects(sourceDirectory, nextToken, null, 1000);

                response = this.objectStorage.listObjects(request);
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

        for (final String objectToRename : objectsToRename) {
            final String newObjectName =
                    objectToRename.replaceFirst(sourceDirectory, destinationDirectory);
            this.rename(objectToRename, newObjectName);
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
                                            sourceObject, destinationObject))
                            .call();
            this.statistics.incrementWriteOps(1); // 1 put
            LOG.debug("Newly renamed object has eTag {}", newEntityTag);
        } catch (final Exception e) {
            LOG.debug("Failed to rename {} to {}", sourceObject, destinationObject, e);
            throw new IOException("Unable to perform rename", e);
        }
    }

    /**
     * Deletes the object at the given path.
     *
     * @param path
     *            Path of object to delete.
     * @throws IOException
     *             if the operation cannot be completed.
     */
    public void delete(final Path path) throws IOException {
        final String object = this.pathToObject(path);
        LOG.debug("Attempting to delete object {} from path {}", object, path);

        try {
            this.objectStorage.deleteObject(this.requestBuilder.deleteObject(object));
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
     * Deletes the directory at the given path.
     *
     * @param path
     *            Path of object to delete.
     * @throws IOException
     *             if the operation cannot be completed.
     */
    public void deleteDirectory(final Path path) throws IOException {
        if (path.isRoot()) {
            LOG.debug("Deleting root directory is a no-op");
            return;
        }

        final String directory = this.pathToDirectory(path);
        LOG.debug("Attempting to delete directory {} from path {}", directory, path);

        try {
            this.objectStorage.deleteObject(this.requestBuilder.deleteObject(directory));
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
     * Creates a pseudo directory at the given path.
     *
     * @param path
     *            The path to create a directory object at.
     * @throws IOException
     *             if the operation cannot be completed.
     */
    public void createDirectory(final Path path) throws IOException {
        // nothing to do for the "root" directory
        if (this.isRootDirectory(path)) {
            LOG.debug("Root directory specified, nothing to create");
            return;
        }

        final String key = this.pathToDirectory(path);

        LOG.debug("Attempting to create directory {} with object {}", path, key);

        final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        final PutObjectResponse response;
        try {
            response = this.objectStorage.putObject(this.requestBuilder.putObject(key, bais, 0L));
            this.statistics.incrementWriteOps(1);
            LOG.debug("Created directory at {} with etag {}", path, response.getETag());
        } catch (final BmcException e) {
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
        }
    }

    /**
     * Tests to see if the directory at the given path is considered empty or not.
     *
     * @param path
     *            The directory path.
     * @return true if the directory is empty, false if not.
     * @throws IOException
     *             if the operation could not be completed.
     */
    public boolean isEmptyDirectory(final Path path) throws IOException {
        final String key = this.pathToDirectory(path);
        LOG.debug("Checking to see if directory path {} is empty (object key {})", path, key);

        final ListObjectsRequest request = this.requestBuilder.listObjects(key, null, "/", 2);
        this.statistics.incrementReadOps(1);

        final ListObjectsResponse response;
        try {
            response = this.objectStorage.listObjects(request);
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
     * @param path
     *            The directory path.
     * @return A list of file statuses, or empty if the directory was empty.
     * @throws IOException
     *             if the operation could not be completed
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
                response = this.objectStorage.listObjects(request);
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
                summary.getTimeCreated().getTime(),
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

    /**
     * Returns the {@link FileStatus} for the object at the given path.
     *
     * @param path
     *            The path to query.
     * @return The file status, null if there was no file at this location.
     * @throws IOException
     *             if the operation could not be completed.
     */
    public FileStatus getFileStatus(final Path path) throws IOException {
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

        // will get metadata for either the actual object or the placeholder folder object
        final HeadPair headData = this.getObjectMetadata(key);
        if (headData != null) {
            return new FileStatus(
                    headData.response.getContentLength(),
                    this.isDirectory(headData),
                    BLOCK_REPLICATION,
                    this.blockSizeInBytes,
                    headData.response.getLastModified().getTime(),
                    path);
        }

        // try last attempt to scan for files even though the placeholder folder doesn't exist
        if (!this.isEmptyDirectory(path)) {
            LOG.debug("No placeholder file, but found non-empty directory anyway");
            return new FileStatus(
                    0,
                    true,
                    BLOCK_REPLICATION,
                    this.blockSizeInBytes,
                    LAST_MODIFICATION_TIME,
                    path);
        }

        // nothing left, return null
        return null;
    }

    /**
     * This method attempts to get the metadata for the given object key.
     * <p>
     * Note: Since "directories" in Object Store are just objects whose names have a trailing '/', this method will make
     * two attempts to get the metadata, once for the actual key given, and once for the (key + '/') iff no object for
     * the given key exists. This is necessary as its not immediately possible to know from the Path instances provided
     * if they were meant to be a file or a directory.
     *
     * @param key
     *            The object key.
     * @return The metadata
     * @throws IOException
     *             if no object (or directory) could be found.
     */
    private HeadPair getObjectMetadata(final String key) throws IOException {
        HeadObjectResponse response = null;
        String keyUsed = key;
        try {
            response = this.objectStorage.headObject(this.requestBuilder.headObject(key));
            this.statistics.incrementReadOps(1);
        } catch (final BmcException e) {
            boolean throwEx = true;
            // also try to query for a directory with this key name
            if (e.getStatusCode() == 404) {
                if (!key.endsWith("/")) {
                    try {
                        keyUsed = key + "/";
                        response =
                                this.objectStorage.headObject(
                                        this.requestBuilder.headObject(keyUsed));
                        throwEx = false;
                    } catch (final BmcException e1) {
                        if (e1.getStatusCode() == 404) {
                            return null;
                        }
                    } finally {
                        // in either case, it took 2 read operations to figure out this object either did or did not
                        // exist
                        this.statistics.incrementReadOps(2);
                    }
                } else {
                    this.statistics.incrementReadOps(1);
                    return null;
                }
            }

            if (throwEx) {
                LOG.debug("Failed to get object metadata for {}", key, e);
                throw new IOException("Unable to fetch file status for: " + key, e);
            }
        }

        return new HeadPair(response, keyUsed);
    }

    /**
     * Creates a new {@link FSInputStream} that can be used to read the object at the given path.
     *
     * @param status
     *            The file status for this file.
     * @param path
     *            The path to open.
     * @param bufferSizeInBytes
     *            The buffer size in bytes (may not be used).
     * @param statistics
     *            The {@link Statistics} instance to publish metrics into.
     * @return A new input stream to read from.
     */
    public FSInputStream openReadStream(
            final FileStatus status,
            final Path path,
            final int bufferSizeInBytes,
            final Statistics statistics) {
        LOG.debug("Opening read stream for {}", path);
        final Supplier<GetObjectRequest.Builder> requestBuilder =
                new GetObjectRequestFunction(path);

        if (this.useInMemoryReadBuffer) {
            return new BmcInMemoryFSInputStream(
                    this.objectStorage, status, requestBuilder, this.statistics);
        } else {
            return new BmcDirectFSInputStream(
                    this.objectStorage, status, requestBuilder, this.statistics);
        }
    }

    /**
     * Creates a new {@link OutputStream} that can be written to in order to create a new file.
     *
     * @param path
     *            The path for the new file.
     * @param bufferSizeInBytes
     *            The buffer size in bytes (may not be used).
     * @param progress
     *            {@link Progressable} instance to report progress updates to.
     * @return A new output stream to write to.
     */
    public OutputStream openWriteStream(
            final Path path, final int bufferSizeInBytes, final Progressable progress) {
        LOG.debug("Opening write stream to {}", path);
        final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn =
                new UploadDetailsFunction(this.pathToObject(path));

        if (this.useInMemoryWriteBuffer) {
            return new BmcInMemoryOutputStream(
                    this.uploadManager, progress, bufferSizeInBytes, requestBuilderFn);
        } else {
            return new BmcFileBackedOutputStream(
                    this.propertyAccessor, this.uploadManager, progress, requestBuilderFn);
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

    private boolean isDirectory(final HeadPair headData) {
        return (headData.response.getContentLength() == 0L) && headData.objectKey.endsWith("/");
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

        @Override
        public GetObjectRequest.Builder get() {
            return BmcDataStore.this.requestBuilder.getObjectBuilder(
                    BmcDataStore.this.pathToObject(path));
        }
    }

    @RequiredArgsConstructor
    private final class PutObjectFromGetRequestFunction
            implements Function<GetObjectResponse, PutObjectRequest> {
        private final String objectName;

        @Override
        public PutObjectRequest apply(GetObjectResponse getResponse) {
            // always pass MD5 when we start with a GetObjectResponse
            return BmcDataStore.this.requestBuilder.putObject(
                    objectName,
                    getResponse.getInputStream(),
                    getResponse.getContentLength(),
                    getResponse.getContentMd5());
        }
    }

    @RequiredArgsConstructor
    private final class UploadDetailsFunction
            implements BiFunction<Long, InputStream, UploadRequest> {
        private final String objectName;

        @Override
        public UploadRequest apply(Long contentLengthInBytes, InputStream inputStream) {
            return BmcDataStore.this.requestBuilder.uploadRequest(
                    objectName,
                    inputStream,
                    contentLengthInBytes,
                    BmcDataStore.this.parallelUploadExecutor);
        }
    }

    @RequiredArgsConstructor
    private static final class HeadPair {
        private final HeadObjectResponse response;
        private final String objectKey;
    }
}
