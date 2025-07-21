/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.oracle.bmc.hdfs.util.FSStreamUtils;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public abstract class BmcFSInputStream extends FSInputStream {
    private static final int EOF = -1;
    protected volatile RetryPolicy<Object> retryPolicy;
    protected final ObjectStorage objectStorage;
    protected final FileStatus status;
    protected final Supplier<GetObjectRequest.Builder> requestBuilder;
    protected  final int readMaxRetries;

    @Getter(value = AccessLevel.PROTECTED)
    protected final Statistics statistics;

    protected final RetryMetricsCollector retryMetricsCollector;

    @Setter(value = AccessLevel.PROTECTED)
    @Getter(value = AccessLevel.PROTECTED)
    protected InputStream sourceInputStream;

    protected long currentPosition = 0;
    protected boolean closed = false;

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
        LOG.debug("{}: Reading single byte from position {}", this, this.currentPosition);
        final AtomicInteger byteReadAtomic = new AtomicInteger();
        try {
            Failsafe.with(retryPolicy()).run(() -> {
                this.validateState(this.currentPosition);
                byteReadAtomic.set(this.sourceInputStream.read());
            });
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw e;
            }
        }

        final int byteRead = byteReadAtomic.get();
        if (byteRead != EOF) {
            this.currentPosition++;
            this.statistics.incrementBytesRead(1L);
        }
        return byteRead;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        LOG.debug("{}: Attempting to read offset {} length {} from position {}", this, off, len, this.currentPosition);
        // see https://issues.apache.org/jira/browse/HDFS-10277
        if (len == 0) {
            return 0;
        }
        final AtomicInteger bytesReadAtomic = new AtomicInteger();
        try {
            Failsafe.with(retryPolicy()).run(() -> {
                this.validateState(this.currentPosition);
                bytesReadAtomic.set(this.sourceInputStream.read(b, off, len));
            });
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw e;
            }
        }

        final int bytesRead = bytesReadAtomic.get();
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
            FSStreamUtils.closeQuietly(this.sourceInputStream);
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

    protected void checkNotClosed() throws IOException {
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

        try (RetryMetricsCollector collector = this.retryMetricsCollector) {
            final GetObjectResponse response = this.objectStorage.getObject(request.toBuilder()
                    .retryConfiguration(collector.getRetryConfiguration()).build());
            LOG.debug(
                    "New stream, opened object with etag {} and size {}",
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

    public RetryPolicy retryPolicy() {
        if (retryPolicy != null) {
            return retryPolicy;
        }
        synchronized (this) {
            if (this.retryPolicy == null) {
                LOG.info("Read retry policy, maximum retries: {}", readMaxRetries);
                this.retryPolicy = new RetryPolicy<>()
                        .handleIf(e -> !this.closed)
                        .withMaxRetries(readMaxRetries)
                        .withDelay(Duration.ofSeconds(3))
                        .withJitter(Duration.ofMillis(200))
                        .onRetry(e -> {
                            LOG.info("Read failed, possibly a stale connection. Will close connection and " +
                                            "re-attempt. Message: {}, Retry count {}",
                                    e.getLastFailure().getMessage(), e.getAttemptCount());
                            // reset sourceInputStream, call verifyInitialized to rebuild if needed.
                            FSStreamUtils.closeQuietly(sourceInputStream);
                            sourceInputStream = null;
                        })
                        .onRetriesExceeded(e -> {
                            LOG.error("Retries exhausted. Last failure: ", e.getFailure());
                            FSStreamUtils.closeQuietly(sourceInputStream);
                            sourceInputStream = null;
                        });
            }
        }
        return this.retryPolicy;
    }

    protected void readAllBytes(Range range, byte[] b) throws IOException {
        try (RetryMetricsCollector collector = this.retryMetricsCollector){
            Failsafe.with(retryPolicy()).run(() -> {
                GetObjectRequest request = this.requestBuilder.get().range(range).build();
                final GetObjectResponse response = this.objectStorage.getObject(request.toBuilder()
                        .retryConfiguration(collector.getRetryConfiguration()).build());
                try (final InputStream is = response.getInputStream()) {
                    readAllBytes(is, b);
                }
            });
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    protected void readAllBytes(InputStream is, byte[] b) throws IOException {
        int offset = 0;
        int n = b.length;
        while (n > 0) {
            int i = is.read(b, offset, n);
            if (i <= 0) throw new IOException("Unexpected EOF");
            offset += i;
            n -= i;
        }
    }
}
