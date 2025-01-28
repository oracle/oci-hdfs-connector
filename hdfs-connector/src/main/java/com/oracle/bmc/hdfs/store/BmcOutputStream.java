/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.hadoop.fs.FSExceptionMessages;

import com.oracle.bmc.hdfs.util.BiFunction;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadResponse;

import lombok.extern.slf4j.Slf4j;

/**
 * Shared {@link OutputStream} class for Object Store that handles all of the 'write' logic.
 * <p>
 * Note, this class only publishes to Object Store when the stream is closed, subclasses may choose to buffer the
 * contents to a location of its choice.
 */
@Slf4j
abstract class BmcOutputStream extends OutputStream {
    private static final int ERROR_CODE_FILE_EXISTS = 412;
    private final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn;
    private final UploadManager uploadManager;
    protected volatile RetryPolicy<Object> retryPolicy;
    protected  final int writeMaxRetries;

    private OutputStream outputBufferStream;
    private boolean closed = false;

    public BmcOutputStream(
            final UploadManager uploadManager,
            final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn, int writeMaxRetries) {
        this.uploadManager = uploadManager;
        this.requestBuilderFn = requestBuilderFn;
        this.writeMaxRetries = writeMaxRetries;
    }

    @Override
    public void write(final int b) throws IOException {
        this.validateState();
        this.outputBufferStream.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        this.validateState();
        this.outputBufferStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        this.validateState();
        this.outputBufferStream.flush();
    }

    /**
     * On close, this class will attempt to submit a PUT request to Object Store using the data that has been
     * accumulated in the buffer. If the PUT fails, an IOException will be raised.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (this.closed) {
            LOG.debug("Output stream already closed");
            return;
        }
        // just to catch the case where somebody opened the file to write, but never actually called any methods on it
        // (like write, or flush)
        this.validateState();
        // only attempting once
        this.closed = true;

        this.outputBufferStream.flush();
        this.outputBufferStream.close();
        this.outputBufferStream = null;

        try {
            Failsafe.with(retryPolicy()).run(() -> {
                try (InputStream fromBufferedStream =  this.getInputStreamFromBufferedStream()) {
                    final UploadRequest request =
                            this.requestBuilderFn.apply(
                                    this.getInputStreamLengthInBytes(), fromBufferedStream);
                    final UploadResponse response = this.uploadManager.upload(request);
                    LOG.debug("Put new file with etag {}", response.getETag());
                }
            });
        } catch (final BmcException e) {
            throw new IOException("Unable to put object", e);
        } finally {
            super.close();
        }
    }

    /**
     * Creates an input stream that will be used to fetch the data to send to Object Store. At this point, the original
     * output stream that was created by {@link #createOutputBufferStream()} has been closed.
     *
     * @return An input stream to use to read the data that had been accumulated.
     * @throws IOException
     *             if the operation could not be completed.
     */
    protected abstract InputStream getInputStreamFromBufferedStream() throws IOException;

    /**
     * Fetches the length of the input stream that needs to be sent.
     *
     * @return The length, in bytes.
     * @throws IOException
     *             if the length could not be determined.
     */
    protected abstract long getInputStreamLengthInBytes() throws IOException;

    /**
     * Creates the output stream that should be used to buffer all bytes to.
     *
     * @return A new output stream.
     * @throws IOException
     *             if the operation could not be completed.
     */
    protected abstract OutputStream createOutputBufferStream() throws IOException;

    private void validateState() throws IOException {
        this.checkNotClosed();
        this.verifyInitialized();
    }

    private void verifyInitialized() throws IOException {
        if (this.outputBufferStream == null) {
            this.outputBufferStream = this.createOutputBufferStream();
        }
    }

    private void checkNotClosed() throws IOException {
        if (this.closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    /**
     * Creates a retrier that retries on all exceptions except for 412 File Exists exception.
     */
    public RetryPolicy retryPolicy() {
        if (retryPolicy != null) {
            return retryPolicy;
        }
        synchronized (this) {
            if (this.retryPolicy == null) {
                LOG.info("Initializing write retry policy, maximum retries: {}", writeMaxRetries);
                this.retryPolicy = new RetryPolicy<>()
                        .handle(Exception.class)
                        .handleIf(e -> !this.closed &&
                                (!(e instanceof BmcException) ||
                                        ((BmcException) e).getStatusCode() != ERROR_CODE_FILE_EXISTS))
                        .withMaxRetries(writeMaxRetries)
                        .withDelay(Duration.ofSeconds(3))
                        .withJitter(Duration.ofMillis(200))
                        .onRetry(e -> LOG.info("Write failed, retrying. Message: {}, Retry count {}",
                                    e.getLastFailure().getMessage(), e.getAttemptCount()))
                        .onRetriesExceeded(e -> LOG.error("Write retries exhausted. Last failure: ", e.getFailure()));
            }
        }
        return this.retryPolicy;
    }
}
