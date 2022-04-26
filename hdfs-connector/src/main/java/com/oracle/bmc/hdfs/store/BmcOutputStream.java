/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSExceptionMessages;

import com.oracle.bmc.hdfs.util.BiFunction;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadResponse;
import com.oracle.bmc.util.StreamUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Shared {@link OutputStream} class for Object Store that handles all of the 'write' logic.
 * <p>
 * Note, this class only publishes to Object Store when the stream is closed, subclasses may choose to buffer the
 * contents to a location of its choice.
 */
@Slf4j
abstract class BmcOutputStream extends OutputStream {
    private final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn;
    private final UploadManager uploadManager;

    private OutputStream outputBufferStream;
    private boolean closed = false;

    public BmcOutputStream(
            final UploadManager uploadManager,
            final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn) {
        this.uploadManager = uploadManager;
        this.requestBuilderFn = requestBuilderFn;
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

        InputStream fromBufferedStream = null;
        try {
            fromBufferedStream = this.getInputStreamFromBufferedStream();
            final UploadRequest request =
                    this.requestBuilderFn.apply(
                            this.getInputStreamLengthInBytes(), fromBufferedStream);
            final UploadResponse response = this.uploadManager.upload(request);
            LOG.debug("Put new file with etag {}", response.getETag());
        } catch (final BmcException e) {
            throw new IOException("Unable to put object", e);
        } finally {
            StreamUtils.closeQuietly(fromBufferedStream);
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
}
