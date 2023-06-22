/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.oracle.bmc.hdfs.store.BmcDataStore;
import com.oracle.bmc.hdfs.store.BmcDataStoreFactory;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of a HDFS {@link FileSystem} that is backed by the BMC Object Store.
 * <p>
 * Filesystems using this store take the URI form: <i>oci://bucket@namespace</i>. The bucket must be pre-created.
 * <p>
 * Unless otherwise noted, APIs try to follow the specification as defined by:
 * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/filesystem.html
 *
 * This is the proxy for the actual implementation, {@link BmcFilesystemImpl}, which may be cached.
 */
@Slf4j
public class BmcFilesystem extends FileSystem {

    @VisibleForTesting
    static class UriParser extends BmcFilesystemImpl.UriParser {
        UriParser(final URI uri) {
            super(uri);
        }
    }

    private volatile BmcFilesystemImpl delegate;

    private static class FSKey {
        private final URI uri;
        private final Configuration configuration;

        public FSKey(URI uri, Configuration configuration) {
            this.uri = uri;
            this.configuration = configuration;
        }

        public int hashCode() {
            return Objects.hash(uri, configuration);
        }

        public boolean equals(Object o) {
            try {
                FSKey that = (FSKey) o;
                return this.uri.equals(that.uri) && this.configuration.equals(that.configuration);
            } catch (Exception e) {
                return false;
            }
        }
    }

    private static volatile LoadingCache<FSKey, BmcFilesystemImpl> fsCache = null;

    private static synchronized void setupFilesystemCache(Configuration configuration) {
        if (fsCache != null) {
            return;
        }

        // cannot be overridden per namespace and bucket, hence propertyOverrideSuffix is ""
        final BmcPropertyAccessor propertyAccessor = new BmcPropertyAccessor(configuration, "");

        CacheLoader<FSKey, BmcFilesystemImpl> loader =
                new CacheLoader<FSKey, BmcFilesystemImpl>() {
                    @Override
                    public BmcFilesystemImpl load(FSKey key) throws Exception {
                        LOG.info("Creating new BmcFilesystemImpl delegate for " + key.uri);
                        BmcFilesystemImpl impl = new BmcFilesystemImpl();
                        impl.initialize(key.uri, key.configuration);
                        return impl;
                    }
                };

        CacheBuilder<FSKey, BmcFilesystemImpl> cacheBuilder =
                CacheBuilder.newBuilder()
                        .removalListener(
                                rn -> {
                                    LOG.info("Physically closing delegate for " + rn.getKey().uri);
                                    try {
                                        rn.getValue().close();
                                    } catch (IOException ioe) {
                                        LOG.warn(
                                                "IOException "
                                                        + ioe
                                                        + " while physically closing "
                                                        + rn.getKey().uri);
                                    }
                                });

        if (!propertyAccessor.asBoolean().get(BmcProperties.FILESYSTEM_CACHING_ENABLED)) {
            LOG.info("BmcFilesystem caching disabled");
            fsCache = cacheBuilder.maximumSize(0).build(loader);
            return;
        }

        propertyAccessor
                .asInteger()
                .forNonNull(
                        BmcProperties.FILESYSTEM_CACHING_MAXIMUM_SIZE,
                        i -> cacheBuilder.maximumSize(i));
        propertyAccessor
                .asInteger()
                .forNonNull(
                        BmcProperties.FILESYSTEM_CACHING_INITIAL_CAPACITY,
                        i -> cacheBuilder.initialCapacity(i));
        propertyAccessor
                .asInteger()
                .forNonNull(
                        BmcProperties.FILESYSTEM_CACHING_EXPIRE_AFTER_ACCESS_SECONDS,
                        i -> cacheBuilder.expireAfterAccess(i, TimeUnit.SECONDS));
        propertyAccessor
                .asInteger()
                .forNonNull(
                        BmcProperties.FILESYSTEM_CACHING_EXPIRE_AFTER_WRITE_SECONDS,
                        i -> cacheBuilder.expireAfterWrite(i, TimeUnit.SECONDS));

        fsCache = cacheBuilder.build(loader);

        LOG.info("BmcFilesystem caching enabled, settings " + cacheBuilder);
    }

    public BmcFilesystem() {}

    @VisibleForTesting
    protected BmcFilesystemImpl getDelegate() {
        return this.delegate;
    }

    public void initialize(URI uri, final Configuration configuration) throws IOException {
        if (delegate != null) {
            return;
        }
        setupFilesystemCache(configuration);
        delegate = fsCache.getUnchecked(new FSKey(uri, configuration));
        delegate.addOwner(this);
    }

    @Override
    public String getScheme() {
        // Can't use delegate here since this is used before initialization
        return BmcConstants.OCI_SCHEME;
    }

    @Override
    public FSDataOutputStream append(
            final Path path, final int bufferSize, final Progressable progress) throws IOException {
        return delegate.append(path, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(
            final Path path,
            final FsPermission permission,
            final boolean overwrite,
            final int bufferSize,
            final short replication,
            final long blockSize,
            final Progressable progress)
            throws IOException {
        return delegate.create(
                path, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream createNonRecursive(
            Path f,
            FsPermission permission,
            EnumSet<CreateFlag> flags,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        return delegate.createNonRecursive(
                f, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean delete(final Path path, final boolean recursive) throws IOException {
        return delegate == null ? false : delegate.delete(path, recursive);
    }

    @Override
    public ContentSummary getContentSummary(final Path path) throws IOException {
        return delegate == null ? null : delegate.getContentSummary(path);
    }

    @Override
    public FileStatus getFileStatus(final Path path) throws IOException {
        return delegate == null ? null : delegate.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(final Path path) throws IOException {
        return delegate == null ? null : delegate.listStatus(path);
    }

    @Override
    public boolean mkdirs(final Path path, final FsPermission permission) throws IOException {
        return delegate == null ? false : delegate.mkdirs(path, permission);
    }

    @Override
    public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
        return delegate == null ? null : delegate.open(path, bufferSize);
    }

    @Override
    public boolean rename(final Path source, final Path destination) throws IOException {
        return delegate == null ? false : delegate.rename(source, destination);
    }

    @Override
    public long getDefaultBlockSize() {
        return delegate == null ? 0 : delegate.getDefaultBlockSize();
    }

    @Override
    public int getDefaultPort() {
        return delegate == null ? BmcConstants.DEFAULT_PORT : delegate.getDefaultPort();
    }

    @Override
    public String getCanonicalServiceName() {
        return delegate == null ? null : delegate.getCanonicalServiceName();
    }

    // This will only close if all owners have been closed to avoid memory leaks.
    public void close() throws IOException {
        if (delegate != null && delegate.isClosed()) {
            super.close();
        }
    }

    @Override
    public Path getWorkingDirectory() {
        return delegate == null ? null : delegate.getWorkingDirectory();
    }

    @Override
    public void setWorkingDirectory(final Path workingDirectory) {
        if (delegate == null) {
            return;
        }
        delegate.setWorkingDirectory(workingDirectory);
    }

    @Override
    public URI getUri() {
        return delegate == null ? null : delegate.getUri();
    }

    public BmcDataStore getDataStore() {
        return delegate == null ? null : delegate.getDataStore();
    }

    @Override
    public Configuration getConf() {
        return delegate == null ? null : delegate.getConf();
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(final Path f,
                final boolean recursive) throws FileNotFoundException, IOException {
        if (recursive) {
            return delegate == null ? null : delegate.listFiles(f, true);
        } else {
            return super.listFiles(f, false);
        }
    }
}

/**
 * Implementation of a HDFS {@link FileSystem} that is backed by the BMC Object Store.
 * <p>
 * Filesystems using this store take the URI form: <i>oci://bucket@namespace</i>. The bucket must be pre-created.
 * <p>
 * Unless otherwise noted, APIs try to follow the specification as defined by:
 * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/filesystem.html
 *
 * This is the actual implementation that {@link BmcFilesystem} delegates to.
 */
@Slf4j
class BmcFilesystemImpl extends FileSystem {
    private static final PathLengthComparator PATH_LENGTH_COMPARATOR = new PathLengthComparator();

    @Getter(onMethod = @__({@Override}))
    @Setter(onMethod = @__({@Override}))
    private Path workingDirectory;

    @Getter(value = AccessLevel.PACKAGE)
    private BmcDataStore dataStore;

    @Getter(onMethod = @__({@Override}))
    private URI uri;

    @Getter private volatile boolean isClosed;

    // This field keeps track of filesystems to be able to close them all
    private final HashSet<BmcFilesystem> owners = new HashSet<>();

    private volatile boolean isInitialized;

    @VisibleForTesting
    static class UriParser {

        // This pattern parses filesystem uris and matches groups for the bucket and namespace.
        // The uris follow the format oci://{bucket}@{namespace}[:port]/{path} where
        // {bucket} should not contain '/' or ':' characters
        // {namespace} should not contain '/' or ':' characters
        private final static Pattern URI_PATTERN =
                Pattern.compile("^(?:oci|oraclebmc):\\/\\/([^:\\/]+)@([^:\\/]+)");

        private final URI uri;
        private final Matcher uriMatcher;

        UriParser(final URI uri) {
            this.uri = uri;
            uriMatcher = URI_PATTERN.matcher(uri.toString());
            if (!(uriMatcher.find() && uriMatcher.groupCount() == 2)) {
                throw new IllegalArgumentException("Unknown uri pattern: " + uri.toString());
            }
        }

        String getScheme() {
            return uri.getScheme();
        }

        String getAuthority() {
            return uri.getAuthority();
        }

        String getNamespace() {
            final String namespace = uri.getHost();
            if (namespace != null) {
                return namespace.trim();
            }

            // The above would fail if the namespace contains underscores,
            // fallback to regex matching
            return uriMatcher.group(2).trim();
        }

        String getBucket() {
            final String bucket = uri.getUserInfo();
            if (bucket != null) {
                return bucket.trim();
            }

            // The above would fail if the namespace contains underscores,
            // fallback to regex matching
            return uriMatcher.group(1).trim();
        }
    }

    @Override
    public void initialize(URI uri, final Configuration configuration) throws IOException {
        if (isInitialized) {
            return;
        }
        LOG.info("Attempting to initialize filesystem with URI {}", uri);
        final UriParser uriParser = new UriParser(uri);
        final String scheme = uriParser.getScheme();
        if (scheme.equals(BmcConstants.Deprecated.BMC_SCHEME)) {
            LOG.warn("Using deprecated scheme {}", uri.getScheme());
        }

        super.initialize(uri, configuration);
        super.setConf(configuration);

        // URI should be oci://bucket@namesapce
        // HDFS only allows the scheme and authority to be used, so we need to fit both variables in there
        final String namespace = uriParser.getNamespace();
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace cannot be empty");
        }
        final String bucket = uriParser.getBucket();
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket cannot be empty");
        }
        LOG.info("Initialized filesystem for namespace {} and bucket {}", namespace, bucket);

        // only scheme and authority define this filesystem
        this.uri = URI.create(scheme + "://" + uriParser.getAuthority());

        this.dataStore =
                new BmcDataStoreFactory(configuration)
                        .createDataStore(namespace, bucket, super.statistics);

        // NOTE: working dir is what all relative Paths will be resolved against
        final String username = System.getProperty("user.name");
        this.workingDirectory = super.makeQualified(new Path("/user", username));
        this.isInitialized = true;
        LOG.info(
                "Setting working directory to {}, and initialized uri to {}",
                this.workingDirectory,
                this.uri);
    }

    /**
     * Returns the scheme for Oracle BMC.
     */
    @Override
    public String getScheme() {
        return BmcConstants.OCI_SCHEME;
    }

    /**
     * Append is not supported.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public FSDataOutputStream append(
            final Path path, final int bufferSize, final Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Appending is not supported with BMC Object Store");
    }

    /**
     * Creates a new output stream. Permissions are not used.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public FSDataOutputStream create(
            final Path path,
            final FsPermission permission,
            final boolean overwrite,
            final int bufferSize,
            final short replication,
            final long blockSize,
            final Progressable progress)
            throws IOException {
        return create(
                path, permission, overwrite, bufferSize, replication, blockSize, progress, true);
    }

    /**
     * Creates a new output stream. Permissions are not used.
     * <p>
     * {@inheritDoc}
     */
    protected FSDataOutputStream create(
            final Path path,
            final FsPermission permission,
            final boolean overwrite,
            final int bufferSize,
            final short replication,
            final long blockSize,
            final Progressable progress,
            final boolean isRecursive)
            throws IOException {
        LOG.debug(
                "Attempting to create path {}, overwrite {}, bufferSize {}",
                path,
                overwrite,
                bufferSize);
        final FileStatus existingFile = this.getNullableFileStatus(path);
        if (existingFile != null) {
            // if there is an existing file, assuming all of the parent
            // directories correctly exist

            if (existingFile.isDirectory()) {
                throw new FileAlreadyExistsException(
                        "Cannot create file, path already exists as a directory: " + path);
            }
            if (!overwrite) {
                throw new FileAlreadyExistsException(
                        "Path already exists, and no overwrite allowed: " + path);
            }

            LOG.debug("Found existing file at path, deleting");
            this.dataStore.delete(path);
        } else {
            if (isRecursive) {
                LOG.debug(
                        "No existing file at path {}, verifying all directories exist with mkdirs",
                        path);
                // no existing file, so make sure all of the parent "directories" are created
                this.mkdirs(path.getParent(), permission);
            } else {
                if (this.getNullableFileStatus(path.getParent()) == null) {
                    throw new FileNotFoundException(
                            "Cannot create file " + path + ", the parent directory does not exist");
                }
            }
        }

        return new FSDataOutputStream(
                this.dataStore.openWriteStream(path, bufferSize, progress), super.statistics);
    }

    @Override
    public FSDataOutputStream createNonRecursive(
            Path f,
            FsPermission permission,
            EnumSet<CreateFlag> flags,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        return create(
                f,
                permission,
                flags.contains(CreateFlag.OVERWRITE),
                bufferSize,
                replication,
                blockSize,
                progress,
                false);
    }

    @Override
    public boolean delete(final Path path, final boolean recursive) throws IOException {
        LOG.debug("Requested to delete {}, recursive {}", path, recursive);
        final FileStatus status = this.getNullableFileStatus(path);
        if (status == null) {
            LOG.debug("No file at path {} found, nothing to delete", path);
            return false;
        }

        // if it's a file, just delete, nothing to do with recursive
        if (status.isFile()) {
            LOG.info("Deleting file");
            this.dataStore.delete(path);
            return true;
        }

        if (!recursive) {
            // Try to find if it's an empty dir. If yes, delete the empty dir.
            // Find one candidate file or dir inside the given Path to determine emptiness of the dir.
            RemoteIterator<LocatedFileStatus> iter = listFilesAndZeroByteDirs(path);
            boolean empty = true;
            while (iter.hasNext()) {
                LocatedFileStatus lfs = iter.next();
                if (lfs.isDirectory()) {
                    Path nPath = ensureAbsolutePath(lfs.getPath());
                    // Do not consider the Path itself in empty determination.
                    if (!nPath.equals(path)) {
                        empty = false;
                        break;
                    }
                } else {
                    empty = false;
                    break;
                }
            }

            if (!empty) {
                throw new IOException(
                        "Attempting to delete a directory that is not empty, and recursive delete not specified: "
                                + path);
            } else {
                // If the path is root, it won't be deleted by this deleteDirectory method.
                // Delete the empty dir since it's empty.
                dataStore.deleteDirectory(path);
                return true;
            }
        }

        List<Path> zeroByteDirsToDelete = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iter = listFilesAndZeroByteDirs(path);
        while (iter.hasNext()) {
            LocatedFileStatus lfs = iter.next();
            Path absPath = ensureAbsolutePath(lfs.getPath());

            if (lfs.isDirectory()) {
                // This path is traversed only for zero byte dir markers.
                // Store the zero byte dir paths to a list and only delete once all the file paths
                // are done with deletion. This is to preserve the directory structure in case the process
                // of this delete is interrupted somehow.
                zeroByteDirsToDelete.add(absPath);
            } else {
                dataStore.delete(absPath);
            }
        }

        // Delete the longest paths first to retain structure in case of interruptions.
        Collections.sort(zeroByteDirsToDelete, PATH_LENGTH_COMPARATOR);
        for (Path dir : zeroByteDirsToDelete) {
            dataStore.deleteDirectory(dir);
        }

        return true;
    }

    @Override
    public ContentSummary getContentSummary(Path path) throws IOException {
        final Path absolutePath = this.ensureAbsolutePath(path);
        return this.dataStore.getContentSummary(absolutePath);
    }

    @Override
    public FileStatus getFileStatus(final Path path) throws IOException {
        LOG.debug("Requested file status for {}", path);
        final Path absolutePath = this.ensureAbsolutePath(path);
        final FileStatus fileStatus = this.dataStore.getFileStatus(absolutePath);
        if (fileStatus == null) {
            throw new FileNotFoundException("No file found at path: " + path);
        }
        return fileStatus;
    }

    // helper method that returns null when a file doesn't exist
    private FileStatus getNullableFileStatus(final Path path) throws IOException {
        try {
            return this.getFileStatus(path);
        } catch (final FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public FileStatus[] listStatus(final Path path) throws FileNotFoundException, IOException {
        LOG.debug("Requested listStatus for {}", path);
        final FileStatus status = this.getFileStatus(path);

        if (status.isFile()) {
            return new FileStatus[] {status};
        }

        return this.dataStore.listDirectory(path).toArray(new FileStatus[0]);
    }

    /**
     * Permissions are not used.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public boolean mkdirs(final Path path, final FsPermission permission) throws IOException {
        LOG.debug("Requested mkdirs on path {}", path);
        Path currentPath = path;

        FileStatus status = this.getNullableFileStatus(currentPath);

        if (status != null) {
            // path exists, and is not a directory, throw exception. else, it
            // exists, nothing to do
            if (!status.isDirectory()) {
                throw new FileAlreadyExistsException(
                        "Cannot mkdir, file at path already exists: " + path);
            } else {
                LOG.debug("Path already exists, nothing to create");
                return true;
            }
        }

        final ArrayList<Path> directoriesToCreate = new ArrayList<>();
        // eventually we'll get to the root (or a file)
        while (status == null) {
            directoriesToCreate.add(currentPath);
            currentPath = currentPath.getParent();

            status = this.getNullableFileStatus(currentPath);
        }

        if (!status.isDirectory()) {
            throw new ParentNotDirectoryException(
                    "Found a parent path that is not a directory: " + status.getPath());
        }

        LOG.debug("Attempting to create directories: {}", directoriesToCreate);
        for (final Path directoryToCreate : directoriesToCreate) {
            this.dataStore.createDirectory(directoryToCreate);
        }

        // always return true
        return true;
    }

    @Override
    public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
        LOG.debug("Opening path {}, bufferSize {}", path, bufferSize);
        final FileStatus status = this.getFileStatus(path);
        if (status.isDirectory()) {
            throw new FileNotFoundException("File at path location is a directory: " + path);
        }

        return new FSDataInputStream(
                this.dataStore.openReadStream(status, path, bufferSize, super.statistics));
    }

    /**
     * This is not an atomic operation and can be very lengthy, especially if renaming directories.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public boolean rename(final Path source, final Path destination) throws IOException {
        LOG.debug("Renaming {} to {}", source, destination);
        final Path absoluteSource = this.ensureAbsolutePath(source);
        final Path absoluteDestination = this.ensureAbsolutePath(destination);

        final FileStatus sourceStatus;
        try {
            sourceStatus = this.getFileStatus(absoluteSource);
        } catch (final FileNotFoundException e) {
            LOG.debug("Source file not found");
            // spec says to throw FileNotFoundException, but other cloud providers return false,
            // so we'll do the same.
            return false;
        }

        // cannot rename root
        if (sourceStatus.getPath().isRoot()) {
            LOG.debug("Cannot rename root");
            return false;
        }

        // trivial check, need to check resolved path later still
        if (absoluteSource.equals(absoluteDestination)) {
            if (sourceStatus.isDirectory()) {
                LOG.debug(
                        "Destination is the same as source, renaming directory to itself not allowed");
                return false;
            } else {
                LOG.debug("Destination is the same as source, renaming file to itself is allowed");
                return true;
            }
        }

        FileStatus destinationStatus = this.getNullableFileStatus(absoluteDestination);
        Path destinationPathToUse = absoluteDestination;
        if (destinationStatus == null) {
            final FileStatus destinationParentStatus =
                    this.getNullableFileStatus(absoluteDestination.getParent());

            // parent directory doesn't exist or is a file, return false to be in sync with other cloud connectors
            if ((destinationParentStatus == null) || destinationParentStatus.isFile()) {
                LOG.debug("Destination parent directory does not exist, or is a file");
                return false;
            }
        } else if (destinationStatus.isDirectory()) {
            // destination is a directory, need down one level.
            // copy file/dir name of source, we have to check if the source name does exist in this directory
            destinationPathToUse = new Path(absoluteDestination, absoluteSource.getName());
            destinationStatus = this.getNullableFileStatus(destinationPathToUse);
        }

        // test again now that it's resolved
        // ex, moving /foo/bar.json to /foo/, or /foo/bar/ to /foo/
        if (absoluteSource.equals(destinationPathToUse)) {
            if (sourceStatus.isDirectory()) {
                LOG.debug(
                        "Resolved destination is the same as source, renaming directory to itself not allowed");
                return false;
            } else {
                LOG.debug(
                        "Resolved destination is the same as source, renaming file to itself is allowed");
                return true;
            }
        }

        // cannot rename something to be a descendant of itself
        // ex, moving /foo/bar.json to /foo/bar.json/bar.json, or /foo/bar/ to /foo/bar/bar/
        if (this.isDescendant(absoluteSource, absoluteDestination)) {
            LOG.debug("Destination cannot be a child of src");
            return false;
        }


        // destination should not exist, no matter it is a file or directory
        if (destinationStatus != null ) {
            // spec says to throw FileAlreadyExistsException or IOException, but most cloud providers
            // return false instead, staying consistent here too
            LOG.debug("Destination {} {} already exists", (destinationStatus.isFile() ? "file" : "directory"), destinationPathToUse.toString() );
            return false;
        }

        try {
            if (sourceStatus.isFile()) {
                // file rename
                LOG.debug("Renaming file {} to {}", absoluteSource, destinationPathToUse);
                this.dataStore.renameFile(absoluteSource, destinationPathToUse);
            } else {
                // directory rename
                LOG.debug("Renaming directory {} to {}", absoluteSource, destinationPathToUse);
                this.dataStore.renameDirectory(absoluteSource, destinationPathToUse);
            }
        } catch (final FileAlreadyExistsException e) {
            return false;
        }

        return true;
    }

    private boolean isDescendant(final Path source, final Path destination) {
        String sourcePath = source.toUri().getPath();
        if (!sourcePath.endsWith("/")) {
            sourcePath += "/";
        }

        final String destinationPath = destination.toUri().getPath();
        return sourcePath.equals(destinationPath) || destinationPath.startsWith(sourcePath);
    }

    /**
     * Block size determined by property value (else goes to default value).
     * <p>
     * {@inheritDoc}
     */
    @Override
    public long getDefaultBlockSize() {
        return this.dataStore.getBlockSizeInBytes();
    }

    @Override
    public int getDefaultPort() {
        return BmcConstants.DEFAULT_PORT;
    }

    @Override
    public String getCanonicalServiceName() {
        return null;
    }

    private Path ensureAbsolutePath(final Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(this.workingDirectory, path);
    }

    private static final class PathLengthComparator implements Comparator<Path> {
        @Override
        public int compare(Path path1, Path path2) {
            return Integer.compare(
                    path2.toUri().toString().length(), path1.toUri().toString().length());
        }
    }

    /**
     * Closing filesystem(s) needs to be synchronized to avoid race conditions with owners in multi-threaded environments.
     */
    @Override
    public synchronized void close() throws IOException {
        if (isClosed) {
            return;
        }
        try {
            super.close();
            isClosed = true;
        } catch (Exception e) {
            LOG.warn("Caught exception while closing filesystem", e);
        } finally {
            for (BmcFilesystem fs : owners) {
                fs.close();
            }
        }
    }

    /**
     * addOwner() needs to be synchronized to avoid race conditions with owners in multi-threaded environments.
     */
    public synchronized void addOwner(BmcFilesystem fs) {
        owners.add(fs);
    }

    private RemoteIterator<LocatedFileStatus> listFilesAndZeroByteDirs(Path path) throws IOException {
        return new FlatListingRemoteIterator(path, true);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(final Path f,
                   final boolean recursive) throws FileNotFoundException, IOException {

        if (!recursive) {
            return super.listFiles(f, false);
        }

        return new FlatListingRemoteIterator(f, false);
    }

    /**
     * This class provides an implementation of remote iterator that does a non-recursive, flat listing
     * using OSS API. Given a path, the listing is done without prefix so that all zero byte dirs and objects
     * inside the given prefix are listed. includeZeroByteDirs is an option to the constructor.
     * For flat listing of just files, the includeZeroByteDirs should be false.
     * In the recursive delete implementation the includeZeroByteDirs is supplied as true so that we can
     * cleanup even the zero byte marker directories.
     */
    class FlatListingRemoteIterator implements RemoteIterator<LocatedFileStatus> {
        private List<LocatedFileStatus> resultList = new LinkedList<>();
        private boolean fetchComplete = false;
        private String nextEleCursor = null;
        private Path path;
        private boolean includeZeroByteDirs;

        public FlatListingRemoteIterator(Path path, boolean includeZeroByteDirs) {
            this.path = path;
            this.includeZeroByteDirs = includeZeroByteDirs;
        }

        public boolean hasNext() throws IOException {

            if (resultList.size() > 0) {
                return true;
            }

            if (fetchComplete) {
                return false;
            } else {
                while (true) {
                    BmcDataStore ds = BmcFilesystemImpl.this.getDataStore();
                    Pair<List<FileStatus>, String> resultPair = ds.flatListDirectoryRecursive(path, nextEleCursor);

                    List<FileStatus> fsResultList = resultPair.getLeft();

                    if (fsResultList.size() > 0) {
                        for (FileStatus result : fsResultList) {

                            if (result.isFile()) {
                                BlockLocation[] locs =
                                        BmcFilesystemImpl.this.getFileBlockLocations(result, 0L, result.getLen());
                                resultList.add(new LocatedFileStatus(result, locs));
                            } else if (includeZeroByteDirs) {
                                resultList.add(new LocatedFileStatus(result, null));
                            }
                        }
                        nextEleCursor = resultPair.getRight();
                        if (nextEleCursor == null) {
                            fetchComplete = true;
                        }
                    } else {
                        fetchComplete = true;
                        return false;
                    }

                    if (resultList.size() > 0) {
                        return true;
                    } else if (fetchComplete && resultList.size() == 0) {
                        // This check is needed because sometimes the resultList can be empty
                        // due to it having only directories. So we still need to continue with
                        // the next token if the fetch has not been completed yet.
                        return false;
                    }
                }
            }
        }

        public LocatedFileStatus next() throws IOException {
            if (hasNext()) {
                return resultList.remove(0);
            } else {
                throw new NoSuchElementException("No more elements exist.");
            }
        }
    }
}
