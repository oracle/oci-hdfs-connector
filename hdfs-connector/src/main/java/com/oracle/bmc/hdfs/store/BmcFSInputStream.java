/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;

import com.google.common.base.Supplier;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Common implementation of {@link FSInputStream} that has basic read support, along with state validation.
 * Implementations should inherit from this class when there is not too much custom logic required to implement seek
 * behavior.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
abstract class BmcFSInputStream extends FSInputStream {
    private static final int EOF = -1;

    private final ObjectStorage objectStorage;
    private final FileStatus status;
    private final Supplier<GetObjectRequest.Builder> requestBuilder;

    @Getter(value = AccessLevel.PROTECTED)
    private final Statistics statistics;

    @Setter(value = AccessLevel.PROTECTED)
    @Getter(value = AccessLevel.PROTECTED)
    private InputStream sourceInputStream;

    private long currentPosition = 0;
    private boolean closed = false;

    @Override
    public long getPos() throws IOException {
        return this.currentPosition;
    }

    @Override
    public void seek(final long position) throws IOException {
        this.checkNotClosed();

        if (this.currentPosition == position) {
            LOG.debug("Already at desired position, nothing to seek");
            return;
        }

        if (position < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }

        if (position >= this.status.getLen()) {
            throw new EOFException(
                    "Cannot seek to " + position + " (file size : " + this.status.getLen() + ")");
        }

        this.currentPosition = this.doSeek(position);
        LOG.debug("Requested seek to {}, ended at position {}", position, this.currentPosition);
    }

    /**
     * Perform the requested seek operation. Note, if the subclass changes the input stream or closes it, a new one must
     * be provided and set using {@link #setSourceInputStream(InputStream)} before returning. The input stream that was
     * originally created (and wrapped by {@link #wrap(InputStream)} can be obtained from
     * {@link #getSourceInputStream()}.
     *
     * @param position
     *            The position to seek to.
     * @return The new position after seeking.
     * @throws IOException
     *             if the operation could not be completed
     */
    abstract protected long doSeek(long position) throws IOException;

    /**
     * There are no new sources, this method always returns false.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public boolean seekToNewSource(final long arg0) throws IOException {
        // no new sources
        return false;
    }

    @Override
    public int read() throws IOException {
        this.validateState(this.currentPosition);

        final int byteRead = this.sourceInputStream.read();
        if (byteRead != EOF) {
            this.currentPosition++;
            this.statistics.incrementBytesRead(1L);
        }
        return byteRead;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        this.validateState(this.currentPosition);

        final int bytesRead = this.sourceInputStream.read(b, off, len);
        if (bytesRead != EOF) {
            this.currentPosition += bytesRead;
            this.statistics.incrementBytesRead(bytesRead);
        }
        return bytesRead;
    }

    @Override
    public int available() throws IOException {
        this.validateState(this.currentPosition);

        final long bytesRemaining = this.status.getLen() - this.currentPosition;
        return bytesRemaining <= Integer.MAX_VALUE ? (int) bytesRemaining : Integer.MAX_VALUE;
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.closed = true;
        if (this.sourceInputStream != null) {
            // specifications says close should not throw any IOExceptions
            IOUtils.closeQuietly(this.sourceInputStream);
            this.sourceInputStream = null;
        }
    }

    /**
     * Allows the subclass to wrap the raw input stream from Casper in another one if desired.
     *
     * @param rawInputStream
     *            The raw input stream.
     * @return An input stream to set as the source.
     * @throws IOException
     *             if the operation could not be completed.
     */
    protected InputStream wrap(final InputStream rawInputStream) throws IOException {
        return rawInputStream;
    }

    /**
     * Allows subclasses to validate the state of this stream. Involves:
     * <ol>
     * <li>Verifying the stream is not closed.</li>
     * <li>Creating a new input stream (and wrapping it with {@link #wrap(InputStream)})</li>
     * </ol>
     *
     * @param startPosition
     *            The starting byte offset.
     * @throws IOException
     *             if the filesystem could not be initialized
     */
    protected void validateState(final long startPosition) throws IOException {
        this.checkNotClosed();
        try {
            this.verifyInitialized(startPosition);
        } catch (final BmcException e) {
            throw new IOException("Unable to initialize data", e);
        }
    }

    private void checkNotClosed() throws IOException {
        if (this.closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    private void verifyInitialized(final long startPosition) throws IOException, BmcException {
        if (this.sourceInputStream != null) {
            return;
        }

        Range range = null;
        if (startPosition > 0) {
            LOG.debug("Initializing with start position {}", startPosition);
            // end is null as we want until the end of object
            range = new Range(startPosition, null);
        }

        GetObjectRequest request = this.requestBuilder.get().range(range).build();

        final GetObjectResponse response = this.objectStorage.getObject(request);
        LOG.debug(
                "Opened object with etag {} and size {}",
                response.getETag(),
                response.getContentLength());
        final InputStream dataStream = response.getInputStream();
        this.sourceInputStream = this.wrap(dataStream);
        // if range request, use the first byte returned, else it's just 0 (startPosition)
        if (range != null) {
            this.currentPosition = response.getContentRange().getStartByte();
        } else {
            this.currentPosition = startPosition;
        }
    }
}
