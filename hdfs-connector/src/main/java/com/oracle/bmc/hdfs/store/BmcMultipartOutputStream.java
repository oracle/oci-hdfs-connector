package com.oracle.bmc.hdfs.store;


import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.util.BlockingRejectionHandler;
import com.oracle.bmc.io.DuplicatableInputStream;
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

@Slf4j
public class BmcMultipartOutputStream extends BmcOutputStream {
    private final int bufferSizeInBytes;
    private final MultipartUploadRequest request;
    private ByteBufferOutputStream bbos;
    private MultipartObjectAssembler assembler;
    private boolean closed = false;
    private MultipartManifest manifest;
    private ExecutorService executor;
    private boolean shutdownExecutor;
    private final BmcPropertyAccessor propertyAccessor;

    /**
     * Inner class which handles circular buffer writes to OCI via a ByteBuffer.
     * Once the buffer is filled it is spilled to OCI using the Multipart Upload API.
     * Once this OutputStream is closed, the Multipart upload is completed or aborted.
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
            buffer.put((byte)b);

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
        public void write(byte b[], int off, int len) throws IOException,
                NullPointerException, IndexOutOfBoundsException {
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

        private boolean nonEmpty() {
            return !isEmpty();
        }

        private void copyToByteArray(byte[] dst) {
            if (dst == null) {
                throw new NullPointerException("Destination cannot be null");
            } else if (dst.length < this.length()) {
                throw new IndexOutOfBoundsException("Destination length is too small");
            }
            System.arraycopy(this.buffer.array(), 0, dst, 0, this.length());
        }

        private byte[] toByteArray() {
            return ArrayUtils.subarray(this.buffer.array(), 0, this.length());
        }
    }

    public BmcMultipartOutputStream(
            final BmcPropertyAccessor propertyAccessor,
            final MultipartUploadRequest request,
            final int bufferSizeInBytes) {
        super(null, null);

        // delay creation until called in createOutputBufferStream
        this.propertyAccessor = propertyAccessor;
        this.bufferSizeInBytes = bufferSizeInBytes;
        this.request = request;
        this.shutdownExecutor = false;
    }

    /**
     * On close, this class will attempt to commit a multipart upload to Object Store using the data written to this
     * output stream so far. If any parts have failed, an IOException will be raised.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (this.closed || this.manifest == null) {
            LOG.debug("Output stream already closed");
            return;
        }
        // only attempting once
        this.closed = true;

        try {
            // upload any remaining data
            doUpload();

            // commit the multipart to OCI
            LOG.info("Committing multipart upload id=" + manifest.getUploadId());

            // this will block until all transfers are complete
            CommitMultipartUploadResponse r = this.assembler.commit();

            System.out.println("Got etag: " + r.getETag());
        } catch (final Exception e) {
            String errorMsg = String.format("Multipart upload id=%s has failed parts=%d, aborting...",
                    manifest.getUploadId(), manifest.listFailedParts().size());
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

    private String computeMD5(byte[] bytes, int length) {
        OutputStream os = new StreamHelper.NullOutputStream();
        String md5Base64 = null;
        try (DigestOutputStream dos = new DigestOutputStream(os, MessageDigest.getInstance("MD5"))) {
            dos.write(bytes, 0, length);
            md5Base64 = StreamHelper.base64Encode(dos.getMessageDigest());
        } catch (NoSuchAlgorithmException | IOException ex) {
            LOG.warn("Failed to compute MD5", ex);
        }

        return md5Base64;
    }

    private synchronized void doUpload() {
        byte[] bytesToWrite = this.bbos.toByteArray();
        int writeLength = bytesToWrite.length;

        try (InputStream is = new WrappedFixedLengthByteArrayInputStream(bytesToWrite, 0, writeLength)) {
            this.assembler.addPart(is, writeLength, computeMD5(bytesToWrite, writeLength));
        } catch (final IOException ioe) {
            LOG.error("Failed to create InputStream from byte array.");
        } finally {
            this.bbos.clear();
        }
    }

    /**
     * Helper method to build a fixed executor and specify that we need to shut it down
     */
    private synchronized void initializeExecutorService() {
        if (this.executor == null) {
            int numThreads = propertyAccessor.asInteger().get(BmcProperties.MULTIPART_IN_MEMORY_NUM_UPLOAD_THREADS);
            int maxConcurrent = propertyAccessor.asInteger().get(BmcProperties.MULTIPART_IN_MEMORY_WRITE_MAX_INFLIGHT);
            RejectedExecutionHandler rejectedExecutionHandler = new BlockingRejectionHandler();
            this.executor = new ThreadPoolExecutor(numThreads, numThreads,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(maxConcurrent), rejectedExecutionHandler);
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
        this.assembler = MultipartObjectAssembler.builder()
                .allowOverwrite(this.request.isAllowOverwrite())
                .bucketName(this.request.getBucketName())
                .namespaceName(this.request.getNamespaceName())
                .objectName(this.request.getObjectName())
                .cacheControl(this.request.getCacheControl())
                .storageTier(this.request.getStorageTier())
                .contentDisposition(this.request.getContentDisposition())
                .executorService(this.executor)
                .invocationCallback(this.request.getInvocationCallback())
                .opcClientRequestId(this.request.getOpcClientRequestId())
                .retryConfiguration(this.request.getRetryConfiguration())
                .service(this.request.getObjectStorage()).build();
        this.bbos = new ByteBufferOutputStream(this.bufferSizeInBytes);
        // TODO: do we need these params?
        this.manifest =
                this.assembler.newRequest(null, null, null, null);
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
