/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.util.BlockingRejectionHandler;
import com.oracle.bmc.http.client.io.DuplicatableInputStream;
import com.oracle.bmc.objectstorage.model.CreateMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.CommitMultipartUploadResponse;
import com.oracle.bmc.objectstorage.transfer.MultipartManifest;
import com.oracle.bmc.objectstorage.transfer.MultipartObjectAssembler;
import com.oracle.bmc.objectstorage.transfer.internal.StreamHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BmcMultipartOutputStream extends BmcOutputStream {
    private final int bufferSizeInBytes;
    private final MultipartUploadRequest request;
    private ByteBufferOutputStream bbos;
    private MultipartObjectAssembler assembler;
    private AtomicInteger nextPartNumber;
    private Phaser partUploadPhaser;
    private volatile boolean closed = false;
    private MultipartManifest manifest;
    private ExecutorService executor;
    private ExecutorService parallelMd5executor;
    private boolean shutdownExecutor;
    private final BmcPropertyAccessor propertyAccessor;

    /**
     * Inner class which handles circular buffer writes to OCI via a ByteBuffer.
     * Once the buffer is filled it is spilled to OCI using the Multipart Upload API.
     * Once this OutputStream is closed, the Multipart upload is completed or aborted.
     *
     * Not thread-safe. But only one instance per {@link BmcMultipartOutputStream} is ever created.
     */
    private class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer buffer;
        private final int size;

        private ByteBufferOutputStream(int size) {
            this.size = size;
            this.buffer = ByteBuffer.allocate(this.size);
        }

        /**
         * Writes a byte to a fixed ByteBuffer, this will create a new Multipart Upload once full
         * @param b The byte to write
         * @throws IOException if any upload error happens
         * {@inheritDoc}
         */
        @Override
        public void write(final int b) throws IOException {
            buffer.put((byte) b);

            if (!buffer.hasRemaining()) {
                doUpload();
                buffer.clear();
            }
        }

        /**
         * * Writes an array of bytes to a fixed ByteBuffer, this will create a new Multipart upload once full
         * @param b The byte array to write
         * @param off the offset to start at in the byte array
         * @param len the length of bytes to copy from the offset
         * @throws IOException if we fail creating the multipart upload
         * @throws NullPointerException if the buffer is null
         * @throws IndexOutOfBoundsException if the offset+index is somehow out of bounds of the buffer
         * {@inheritDoc}
         */
        @Override
        public void write(byte b[], int off, int len)
                throws IOException, NullPointerException, IndexOutOfBoundsException {
            if (b == null) {
                throw new NullPointerException();
            } else if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
                throw new IndexOutOfBoundsException();
            }

            // only copy to the buffer if we have something
            if (len > 0) {
                if (buffer.remaining() <= len) {
                    int first = buffer.remaining();
                    buffer.put(b, off, first);
                    doUpload();
                    buffer.clear();
                    // this handles writing any left over
                    write(b, off + first, len - first);
                } else {
                    buffer.put(b, off, len);
                }
            }
        }

        private boolean outOfRange(int off, int len) {
            return off < 0 || off > len;
        }

        private void clear() {
            buffer.clear();
        }

        private int length() {
            return (buffer.hasRemaining()) ? (this.size - buffer.remaining()) : this.size;
        }

        private boolean isEmpty() {
            return buffer.remaining() == this.size;
        }

        private byte[] toByteArray() {
            return ArrayUtils.subarray(this.buffer.array(), 0, this.length());
        }

        @Override
        public void close() throws IOException {
            BmcMultipartOutputStream.this.close();
        }
    }

    public BmcMultipartOutputStream(
            final BmcPropertyAccessor propertyAccessor,
            final MultipartUploadRequest request,
            final int bufferSizeInBytes,
            final ExecutorService parallelMd5executor) {
        super(null, null);

        // delay creation until called in createOutputBufferStream
        this.propertyAccessor = propertyAccessor;
        this.bufferSizeInBytes = bufferSizeInBytes;
        this.request = request;
        this.shutdownExecutor = false;
        this.parallelMd5executor = parallelMd5executor;
    }

    /**
     * On close, this class will attempt to commit a multipart upload to Object Store using the data written to this
     * output stream so far. If any parts have failed, an IOException will be raised.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (this.closed) {
            LOG.debug("Output stream already closed");
            return;
        } else if (this.manifest == null) {
            LOG.debug("Nothing written to stream, creating empty object");
            PutObjectRequest putEmptyFileRequest =
                    PutObjectRequest.builder()
                            .putObjectBody(new ByteArrayInputStream(new byte[0]))
                            .namespaceName(request.getMultipartUploadRequest().getNamespaceName())
                            .bucketName(request.getMultipartUploadRequest().getBucketName())
                            .objectName(
                                    request.getMultipartUploadRequest()
                                            .getCreateMultipartUploadDetails()
                                            .getObject())
                            .buildWithoutInvocationCallback();
            request.getObjectStorage().putObject(putEmptyFileRequest);
            return;
        }
        // only attempting once
        this.closed = true;

        try {
            // upload any remaining data
            doUpload();

            // commit the multipart to OCI
            LOG.info(
                    String.format(
                            "Committing multipart upload id=%s, this awaits all transfers.",
                            manifest.getUploadId()));

            // wait for all parts to be uploaded
            partUploadPhaser.arriveAndAwaitAdvance();
            // this will block until all transfers are complete
            CommitMultipartUploadResponse r = this.assembler.commit();

            LOG.info(
                    String.format(
                            "Committed all parts for uploadId=%s, etag: %s",
                            manifest.getUploadId(),
                            r.getETag()));
        } catch (final Exception e) {
            String errorMsg =
                    String.format(
                            "Multipart upload id=%s has failed parts=%d, aborting...",
                            manifest.getUploadId(),
                            manifest.listFailedParts().size());
            LOG.warn(errorMsg);
            this.assembler.abort(); // TODO: this could throw, figure it out
            throw new IOException("Unable to put object via multipart upload", e);
        } finally {
            this.bbos.close();
            this.bbos = null;

            // clean up our own resources
            if (!this.executor.isShutdown() && this.shutdownExecutor) {
                this.executor.shutdown();
            }
            this.executor = null;
        }
    }

    private String computeMd5(byte[] bytes, int length) {
        OutputStream os = new StreamHelper.NullOutputStream();
        String md5Base64 = null;
        try (DigestOutputStream dos =
                new DigestOutputStream(os, MessageDigest.getInstance("MD5"))) {
            dos.write(bytes, 0, length);
            md5Base64 = StreamHelper.base64Encode(dos.getMessageDigest());
        } catch (NoSuchAlgorithmException | IOException ex) {
            LOG.warn("Failed to compute MD5", ex);
        }

        return md5Base64;
    }

    private synchronized void doUpload() {
        if (this.bbos.isEmpty()) {
            return;
        }

        byte[] bytesToWrite = this.bbos.toByteArray();
        this.bbos.clear();
        int writeLength = bytesToWrite.length;
        int partNumber = nextPartNumber.getAndIncrement();

        partUploadPhaser.register();
        parallelMd5executor.submit(() -> {
            try (InputStream is =
                         new WrappedFixedLengthByteArrayInputStream(bytesToWrite, 0, writeLength)) {
                this.assembler.setPart(is, writeLength, computeMd5(bytesToWrite, writeLength), partNumber);
            } catch (final IOException ioe) {
                LOG.error("Failed to create InputStream from byte array.");
            } finally {
                partUploadPhaser.arriveAndDeregister();
            }
        });
    }

    /**
     * Helper method to build a fixed executor and specify that we need to shut it down
     */
    private synchronized void initializeExecutorService() {
        if (this.executor == null) {
            final int taskTimeout =
                    propertyAccessor
                            .asInteger()
                            .get(BmcProperties.MULTIPART_IN_MEMORY_WRITE_TASK_TIMEOUT_SECONDS);
            final int numThreadsForParallelUpload =
                    propertyAccessor.asInteger().get(BmcProperties.MULTIPART_NUM_UPLOAD_THREADS);
            final BlockingRejectionHandler rejectedExecutionHandler =
                    new BlockingRejectionHandler(taskTimeout);
            this.executor =
                    new ThreadPoolExecutor(
                            numThreadsForParallelUpload,
                            numThreadsForParallelUpload,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<Runnable>(numThreadsForParallelUpload),
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("bmcs-hdfs-multipart-upload-%d")
                                    .build(),
                            rejectedExecutionHandler);
            this.shutdownExecutor = true;
        }
    }

    /**
     * This method delays initialization of resources included any thread pools used by the multipart upload API.
     * This will also create the underlying ByteBuffer OutputStream
     * {@inheritDoc}
     */
    @Override
    protected OutputStream createOutputBufferStream() {
        // this is delayed creation of our objects based on OCI semantics
        initializeExecutorService();
        // perhaps enhance this to name certain things with defaults
        final CreateMultipartUploadDetails details =
                this.request.getMultipartUploadRequest().getCreateMultipartUploadDetails();

        this.assembler =
                MultipartObjectAssembler.builder()
                        .allowOverwrite(this.request.isAllowOverwrite())
                        .bucketName(this.request.getMultipartUploadRequest().getBucketName())
                        .namespaceName(this.request.getMultipartUploadRequest().getNamespaceName())
                        .objectName(details.getObject())
                        .cacheControl(details.getCacheControl())
                        .storageTier(
                                details.getStorageTier() == null
                                        ? StorageTier.Standard
                                        : details.getStorageTier())
                        .contentDisposition(details.getContentDisposition())
                        .executorService(this.executor)
                        .opcClientRequestId(
                                this.request.getMultipartUploadRequest().getOpcClientRequestId())
                        .retryConfiguration(this.request.getRetryConfiguration())
                        .service(this.request.getObjectStorage())
                        .build();
        this.nextPartNumber = new AtomicInteger(1);
        this.partUploadPhaser = new Phaser(1);
        this.bbos = new ByteBufferOutputStream(this.bufferSizeInBytes);
        // TODO: do we need these params?
        this.manifest = this.assembler.newRequest(null, null, null, null);
        return this.bbos;
    }

    @Override
    protected long getInputStreamLengthInBytes() {
        return this.bbos.length();
    }

    private class WrappedFixedLengthByteArrayInputStream extends ByteArrayInputStream
            implements DuplicatableInputStream {
        private final int length;
        private final int offset;

        /**
         * Create a new stream from the given buffer.
         * @param buf The byte buffer.
         */
        private WrappedFixedLengthByteArrayInputStream(byte[] buf) {
            super(buf);
            this.length = buf.length;
            this.offset = 0;
        }

        private WrappedFixedLengthByteArrayInputStream(byte[] buf, int off, int length) {
            super(buf, off, length);
            this.length = length;
            this.offset = off;
        }

        /**
         * Returns the length of the underlying buffer (ie, the length of this stream).
         *
         * @return The length of the underlying buffer.
         */
        public long length() {
            return this.length;
        }

        @Override
        public InputStream duplicate() {
            return new WrappedFixedLengthByteArrayInputStream(this.buf, this.offset, this.length);
        }
    }

    @Override
    protected InputStream getInputStreamFromBufferedStream() {
        byte[] bytes = this.bbos.toByteArray();
        return new WrappedFixedLengthByteArrayInputStream(bytes, 0, bytes.length);
    }
}
