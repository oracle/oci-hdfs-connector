/**
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.  All rights reserved.
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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

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
 */
@Slf4j
public class BmcFilesystem extends FileSystem {
    private static final PathLengthComparator PATH_LENGTH_COMPARATOR = new PathLengthComparator();

    @Getter(onMethod = @__({@Override}))
    @Setter(onMethod = @__({@Override}))
    private Path workingDirectory;

    @Getter(value = AccessLevel.PACKAGE)
    private BmcDataStore dataStore;

    @Getter(onMethod = @__({@Override}))
    private URI uri;

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
            LOG.debug(
                    "No existing file at path {}, verifying all directories exist with mkdirs",
                    path);
            // no existing file, so make sure all of the parent "directories"
            // are created
            this.mkdirs(path.getParent(), permission);
        }

        return new FSDataOutputStream(
                this.dataStore.openWriteStream(path, bufferSize, progress), super.statistics);
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

        // else, it must be a directory

        final boolean isEmptyDirectory = this.dataStore.isEmptyDirectory(path);
        // handle empty directories first
        if (isEmptyDirectory) {
            // removing empty root directory means nothing, can return true or
            // false per spec
            if (status.getPath().isRoot()) {
                LOG.info("Empty root directory, nothing to delete");
                return true;
            }
            LOG.info("Deleting empty directory");
            // else remove the placeholder file
            this.dataStore.deleteDirectory(path);
            return true;
        }

        // everything else is a non-empty directory

        // non-empty and !recursive, cannot continue
        if (!recursive) {
            throw new IOException(
                    "Attempting to delete a directory that is not empty, and recursive delete not specified: "
                            + path);
        }

        final List<FileStatus> directories = new ArrayList<>();
        directories.add(status);

        final List<Path> directoriesToDelete = new ArrayList<>();

        LOG.debug("Recursively deleting directory");
        // breadth-first recursive delete everything except for directory placeholders.
        // leave those until the end to try to maintain some sort of directory
        // structure if sub files fail to delete
        while (!directories.isEmpty()) {
            final FileStatus directory = directories.remove(0);
            final Path directoryPath = this.ensureAbsolutePath(directory.getPath());
            final List<FileStatus> entries = this.dataStore.listDirectory(directoryPath);
            for (final FileStatus entry : entries) {
                if (entry.isDirectory()) {
                    directories.add(entry);
                } else {
                    this.dataStore.delete(this.ensureAbsolutePath(entry.getPath()));
                }
            }
            // track this to delete later
            directoriesToDelete.add(directoryPath);
        }

        // now that all objects under this directory have been deleted, delete
        // all of the individual directory objects we found

        // sort by length, effectively to delete child directories before parent directories. doing this
        // in case a delete fails midway, then we done our best not to create unreachable directories
        Collections.sort(directoriesToDelete, PATH_LENGTH_COMPARATOR);

        for (final Path directoryToDelete : directoriesToDelete) {
            this.dataStore.deleteDirectory(directoryToDelete);
        }

        return true;
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
            LOG.debug("Destination is the same as source");
            return true;
        }

        final FileStatus destinationStatus = this.getNullableFileStatus(absoluteDestination);
        final Path destinationPathToUse;
        if (destinationStatus == null) {
            final FileStatus destinationParentStatus =
                    this.getNullableFileStatus(absoluteDestination.getParent());

            // parent directory doesn't exist or is a file, return false to be in sync with other cloud connectors
            if ((destinationParentStatus == null) || destinationParentStatus.isFile()) {
                LOG.debug("Destination parent directory does not exist, or is a file");
                return false;
            }

            // destination at this point must be a filename, so this is a move + rename operation
            destinationPathToUse = absoluteDestination;
        } else if (destinationStatus.isFile()) {
            // spec says to throw FileAlreadyExistsException or IOException, but most cloud providers
            // return false instead, staying consistent here too
            LOG.debug("Destination exists and is a file");
            return false;
        } else {
            // destination is a directory, copy file name of source
            destinationPathToUse = new Path(absoluteDestination, absoluteSource.getName());
        }

        // test again now that it's resolved
        // ex, moving /foo/bar.json to /foo/, or /foo/bar/ to /foo/
        if (absoluteSource.equals(destinationPathToUse)) {
            LOG.debug("Resolved destination is the same as source");
            return true;
        }

        // cannot rename something to be a descendant of itself
        // ex, moving /foo/bar.json to /foo/bar.json/bar.json, or /foo/bar/ to /foo/bar/bar/
        if (this.isDescendant(absoluteSource, absoluteDestination)) {
            throw new IOException("destination cannot be a child of src");
        }

        if (sourceStatus.isFile()) {
            // file rename
            LOG.debug("Renaming file {} to {}", absoluteSource, destinationPathToUse);
            this.dataStore.renameFile(absoluteSource, destinationPathToUse);
        } else {
            // directory rename
            LOG.debug("Renaming directory {} to {}", absoluteSource, destinationPathToUse);
            this.dataStore.renameDirectory(absoluteSource, destinationPathToUse);
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
}
