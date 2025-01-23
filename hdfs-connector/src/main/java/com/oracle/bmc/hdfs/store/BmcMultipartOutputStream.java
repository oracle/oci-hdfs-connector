/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.util.BlockingRejectionHandler;
import com.oracle.bmc.http.client.io.DuplicatableInputStream;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.ChecksumAlgorithm;
import com.oracle.bmc.objectstorage.model.CommitMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.CommitMultipartUploadPartDetails;
import com.oracle.bmc.objectstorage.requests.AbortMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CommitMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.requests.UploadPartRequest;
import com.oracle.bmc.objectstorage.responses.CommitMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.CreateMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;
import com.oracle.bmc.objectstorage.transfer.internal.StreamHelper;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oracle.bmc.hdfs.BmcConstants.CHECKSUM_COMBINE_MODE_CRC;
import static com.oracle.bmc.hdfs.BmcConstants.DEFAULT_CHECKSUM_COMBINE_MODE;
import static com.oracle.bmc.hdfs.BmcConstants.DFS_CHECKSUM_COMBINE_MODE_KEY;

@Slf4j
public class BmcMultipartOutputStream extends BmcOutputStream {
    private final int bufferSizeInBytes;
    private final MultipartUploadRequest request;
    private ByteBufferOutputStream bbos;
    final ObjectStorage objectStorage;
    final String namespaceName;
    final String bucketName;
    final String objectName;
    private volatile String uploadId;
    private AtomicInteger nextPartNumber;
    private Phaser partUploadPhaser;
    private volatile boolean closed = false;
    private ExecutorService executor;
    private ExecutorService parallelMd5executor;
    private boolean shutdownExecutor;
    private final BmcPropertyAccessor propertyAccessor;
    private final String additionalChecksumAlgorithm;

    Map<Integer, CommitMultipartUploadPartDetails> partDetails = Collections.synchronizedMap(new HashMap<>());

    // Thread-safe list to hold exceptions
    List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());

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
            throwOnPreviousFailedPartUpload();

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
            throwOnPreviousFailedPartUpload();

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
            final ExecutorService parallelMd5executor,
            int writeMaxRetires) {
        super(null, null, writeMaxRetires);

        // delay creation until called in createOutputBufferStream
        this.propertyAccessor = propertyAccessor;
        this.bufferSizeInBytes = bufferSizeInBytes;
        this.request = request;
        this.shutdownExecutor = false;
        this.parallelMd5executor = parallelMd5executor;
        this.objectStorage = request.getObjectStorage();
        this.namespaceName = request.getMultipartUploadRequest().getNamespaceName();
        this.bucketName = request.getMultipartUploadRequest().getBucketName();
        this.objectName = request.getMultipartUploadRequest().getCreateMultipartUploadDetails().getObject();
        String checksumCombineMode = propertyAccessor.getHadoopProperty(DFS_CHECKSUM_COMBINE_MODE_KEY,DEFAULT_CHECKSUM_COMBINE_MODE);
        this.additionalChecksumAlgorithm = CHECKSUM_COMBINE_MODE_CRC.equals(checksumCombineMode) ? ChecksumAlgorithm.Crc32C.getValue() : null;
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
        }
        // only attempting once
        this.closed = true;

        if (exceptions.size() > 0) {
            LOG.error("Fail-fast for {} ", uploadId);
            abort();
            cleanup();
            return;
        }
        if (this.uploadId == null) {
            LOG.debug("Not enough data for a full part, uploading as regular PUT");
            final byte[] bytesToWrite = bbos == null ?  new byte[0] : bbos.toByteArray();

            try {
                Failsafe.with(retryPolicy()).run(() -> {
                    try (InputStream bbis = new ByteArrayInputStream(bytesToWrite)) {
                        PutObjectRequest.Builder putFileRequestBuilder = PutObjectRequest.builder()
                                .putObjectBody(bbis)
                                .contentLength((long) bytesToWrite.length)
                                .namespaceName(namespaceName)
                                .bucketName(bucketName)
                                .objectName(objectName);

                        if (ChecksumAlgorithm.Crc32C.getValue().equalsIgnoreCase(additionalChecksumAlgorithm)) {
                            putFileRequestBuilder.opcChecksumAlgorithm(ChecksumAlgorithm.Crc32C);
                        } else {
                            putFileRequestBuilder.contentMD5(computeMd5(bytesToWrite, bytesToWrite.length));
                        }
                        PutObjectRequest putFileRequest = putFileRequestBuilder.buildWithoutInvocationCallback();
                        PutObjectResponse response = request.getObjectStorage().putObject(putFileRequest);
                        LOG.debug("Put new file with etag {}", response.getETag());
                    }
                });
                return;
            } catch (final BmcException e) {
                throw new IOException("Unable to put object", e);
            } finally {
                if (this.bbos != null) {
                    this.bbos.close();
                    this.bbos = null;
                }
            }
        }

        try {
            // upload any remaining data
            doUpload();

            // commit the multipart to OCI
            LOG.info(
                    String.format(
                            "Committing multipart upload id=%s, this awaits all transfers.",
                            uploadId));

            // wait for all parts to be uploaded
            partUploadPhaser.arriveAndAwaitAdvance();
            // this will block until all transfers are complete
            CommitMultipartUploadResponse r = commitMultipartUpload();
            LOG.info(
                    String.format(
                            "Committed all parts for uploadId=%s, etag: %s",
                            uploadId,
                            r.getETag()));
        } catch (final Exception e) {
            abort();
            throw new IOException("Unable to put object via multipart upload", e);
        } finally {
            cleanup();
        }
    }

    private void abort() {
        String errorMsg =
                String.format(
                        "Multipart upload id=%s has failed parts=%d, aborting...",
                        uploadId,
                        exceptions.size());
        LOG.warn(errorMsg);
        final AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                .namespaceName(namespaceName)
                .bucketName(bucketName)
                .objectName(objectName)
                .uploadId(uploadId)
                .build();
        try {
            objectStorage.abortMultipartUpload(abortMultipartUploadRequest);
        }  catch (Exception e) {
            // Abort operation is critical to clean up the uncommitted parts. Add one more attempt for this operation.
            if (!(e instanceof BmcException) || ((BmcException) e).getStatusCode() != 404) {
                try {
                    objectStorage.abortMultipartUpload(abortMultipartUploadRequest);
                } catch (Exception ex) {
                    LOG.error("Retry failed to abort multipart upload with id {}", uploadId, ex);
                }
            }
            LOG.warn("Multipart Upload abort not retried, upload id {} not found", uploadId);
        }
    }

    private void cleanup() throws IOException {
        this.bbos.close();
        this.bbos = null;

        // clean up our own resources
        if (!this.executor.isShutdown() && this.shutdownExecutor) {
            this.executor.shutdown();
        }
        this.executor = null;
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
        if (this.uploadId == null) {
            this.createMultipartUpload();
        }

        byte[] bytesToWrite = this.bbos.toByteArray();
        this.bbos.clear();
        int writeLength = bytesToWrite.length;
        int partNumber = nextPartNumber.getAndIncrement();

        Random random = new Random();
        partUploadPhaser.register();
        parallelMd5executor.submit(() -> {
            try {
                Failsafe.with(retryPolicy()).run(() -> {
                    try (InputStream is =
                                 new WrappedFixedLengthByteArrayInputStream(bytesToWrite, 0, writeLength)) {

                        UploadPartRequest.Builder uploadPartRequestBuilder = UploadPartRequest.builder()
                                .namespaceName(namespaceName)
                                .bucketName(bucketName)
                                .objectName(objectName)
                                .uploadId(uploadId)
                                .uploadPartNum(partNumber)
                                .uploadPartBody(is);

                        if (ChecksumAlgorithm.Crc32C.getValue().equalsIgnoreCase(additionalChecksumAlgorithm)) {
                            uploadPartRequestBuilder.opcChecksumAlgorithm(ChecksumAlgorithm.Crc32C);
                        } else {
                            final String md5 = computeMd5(bytesToWrite, writeLength);
                            uploadPartRequestBuilder.contentMD5(md5);
                        }

                        final UploadPartRequest uploadPartRequest = uploadPartRequestBuilder.build();
                        final UploadPartResponse uploadPartResponse = objectStorage.uploadPart(uploadPartRequest);
                        partDetails.put(partNumber, new CommitMultipartUploadPartDetails(partNumber,
                                uploadPartResponse.getETag()));
                    }
                });
            } catch (Exception e) {
                LOG.error("Failed to upload part {} for multipart upload {}", partNumber, uploadId);
                // This part failed, the entire stream should be failed.
                exceptions.add(e);
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
                            new SynchronousQueue<>(),
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
        this.bbos = new ByteBufferOutputStream(this.bufferSizeInBytes);
        return this.bbos;
    }

    @Override
    protected long getInputStreamLengthInBytes() {
        return null == this.bbos ? 0: this.bbos.length();
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
        byte[] bytes = (bbos == null ? new byte[0] : bbos.toByteArray());
        return new WrappedFixedLengthByteArrayInputStream(bytes, 0, bytes.length);
    }

    /**
     * If previous parts upload failed, calling this method will throw an IOException
     */
    private void throwOnPreviousFailedPartUpload() throws IOException {
        if (exceptions.size() > 0) {
            StringBuilder aggregatedMessage = new StringBuilder("Exceptions occurred:\n");
            for (Throwable exception : exceptions) {
                aggregatedMessage.append("- ").append(exception.getClass().getSimpleName())
                        .append(": ").append(exception.getMessage()).append("\n");
            }

            // Create the IOException with the aggregated message
            IOException aggregatedIOException = new IOException(aggregatedMessage.toString());

            // Optionally add each exception as a suppressed exception
            for (Throwable exception : exceptions) {
                aggregatedIOException.addSuppressed(exception);
            }
            throw  aggregatedIOException;
        }
    }

    /**
     * Create multipart uplaod
     */
    private void createMultipartUpload() {
        // this is delayed creation of our objects based on OCI semantics
        initializeExecutorService();
        // perhaps enhance this to name certain things with defaults
        LOG.info("Creating multipart upload for object {}/{}", bucketName, objectName);

        CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder =
                request.getMultipartUploadRequest().toBuilder();

        if (ChecksumAlgorithm.Crc32C.getValue().equalsIgnoreCase(additionalChecksumAlgorithm)) {
            createMultipartUploadRequestBuilder.opcChecksumAlgorithm(ChecksumAlgorithm.Crc32C);
        }

        final CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
        final CreateMultipartUploadResponse createMultipartUploadResponse =
                request.getObjectStorage().createMultipartUpload(createMultipartUploadRequest);

        this.uploadId = createMultipartUploadResponse.getMultipartUpload().getUploadId();
        this.nextPartNumber = new AtomicInteger(1);
        this.partUploadPhaser = new Phaser(1);
    }

    /**
     * Commit multipart upload
     */
    private CommitMultipartUploadResponse commitMultipartUpload() throws IOException {
        LOG.info("Finishing upload {} on object {}/{}", uploadId, bucketName, objectName);
        final CommitMultipartUploadDetails commitMultipartUploadDetails = CommitMultipartUploadDetails.builder()
                .partsToExclude(ImmutableList.of())
                .partsToCommit(ImmutableList.copyOf(partDetails.values()))
                .build();
        final CommitMultipartUploadRequest commitMultipartUploadRequest = CommitMultipartUploadRequest.builder()
                .namespaceName(namespaceName)
                .bucketName(bucketName)
                .objectName(objectName)
                .uploadId(uploadId)
                .commitMultipartUploadDetails(commitMultipartUploadDetails)
                .build();
        return objectStorage.commitMultipartUpload(commitMultipartUploadRequest);
    }
}
