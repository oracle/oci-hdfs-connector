package com.oracle.bmc.hdfs.store;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.oracle.bmc.objectstorage.transfer.MultipartManifest;
import com.oracle.bmc.objectstorage.transfer.MultipartObjectAssembler;
import com.oracle.bmc.objectstorage.transfer.internal.StreamHelper;
import com.oracle.bmc.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BmcMultipartOutputStream extends BmcOutputStream {
    private final int bufferSizeInBytes;
    private final int maxInflight;
    private final MultipartUploadRequest request;
    private ByteBufferOutputStream bbos;
    private MultipartObjectAssembler assembler;
    private boolean closed = false;
    private MultipartManifest manifest;
    private ExecutorService executor;
    private boolean shutdownExecutor;
    private volatile AtomicInteger inFlightParts;

    /**
     * Inner class which handles circular buffer writes to OCI via a ByteBuffer.
     * Once the buffer is filled it is spilled to OCI using the Multipart Upload API.
     * Once this OutputStream is closed, the Multipart upload is completed or aborted.
     */
    private class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer buffer;
        private final int size;
        private final HashFunction md5Hasher = Hashing.md5();

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

        private byte[] toByteArray() {
            return ArrayUtils.subarray(this.buffer.array(), 0, this.length());
        }
    }

    public BmcMultipartOutputStream(
            final MultipartUploadRequest request,
            final Progressable progress,
            final int bufferSizeInBytes,
            final int maxInflight) {
        super(null, progress, null);

        // delay creation until called in createOutputBufferStream
        this.bufferSizeInBytes = bufferSizeInBytes;
        this.request = request;
        this.maxInflight = maxInflight;
        this.inFlightParts = new AtomicInteger(0);
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
            LOG.info("Committing multipart upload id="+manifest.getUploadId());
            this.assembler.commit();
        } catch (final Exception e) {
            String errorMsg = String.format("Multipart upload id=%s has failed parts=%d, aborting...",
                    manifest.getUploadId(), manifest.listFailedParts().size());
            LOG.warn(errorMsg);
            this.assembler.abort();
            throw new IOException("Unable to put object via multipart upload", e);
        } finally {
            this.bbos = null;

            if (this.shutdownExecutor) {
                this.executor.shutdownNow();
            }
        }
    }

    private synchronized void doUpload() throws IOException {
        // here we need to back-pressure on if we have too much inflight to avoid copying too many buffers
        this.inFlightParts.incrementAndGet();
        if (this.inFlightParts.get() >= this.maxInflight) {
            LOG.info("Performing backpressure on streaming Multipart");
            System.out.println("Performing backpressure on streaming Multipart");
            while (this.manifest.listInProgressParts().size() >= this.maxInflight) { }
        } else {
            System.out.println("Starting new upload...");
            LOG.info("Starting new upload");
        }

        InputStream is = null;
        try {
            if (this.bbos.nonEmpty()) {
                byte[] bytesToWrite = this.bbos.toByteArray();
                is = this.internalGetInputStreamFromBufferedStream(bytesToWrite);
                this.assembler.addPart(is, this.bbos.length(), StreamHelper.base64EncodeMd5Digest(bytesToWrite));
            }
        } catch (final Exception e) {
            throw new IOException("Unable to put object via multipart upload", e);
        } finally {
            StreamUtils.closeQuietly(is);
            this.bbos.clear();
        }
    }

    private void initializeExecutorService() {
        if (this.request.getUploadConfiguration().isAllowParallelUploads()) {
            if (this.request.getExecutorService() != null) {
                this.executor = this.request.getExecutorService();
                this.shutdownExecutor = false;
            } else {
                this.executor = buildDefaultParallelExecutor();
                this.shutdownExecutor = true;
            }
        } else {
            this.executor = Executors.newSingleThreadExecutor();
            this.shutdownExecutor = true;
        }
    }

    private static ExecutorService buildDefaultParallelExecutor() {
        return Executors.newFixedThreadPool(3, (new ThreadFactoryBuilder()).setNameFormat("multipart-upload-" + System.currentTimeMillis() + "-%d").setDaemon(true).build());
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
        this.assembler = new MultipartObjectAssembler(this.request.getObjectStorage(),
                this.request.getNamespaceName(),
                this.request.getBucketName(),
                this.request.getObjectName(),
                true,
                this.executor);
        this.bbos = new ByteBufferOutputStream(this.bufferSizeInBytes);
        // do we need these params?
        this.manifest =
                this.assembler.newRequest(null, null, "UTF8", null);
        return this.bbos;
    }

    @Override
    protected long getInputStreamLengthInBytes() {
        return this.bbos.length();
    }

    private InputStream internalGetInputStreamFromBufferedStream(byte[] bytes) {
        return StreamUtils.createByteArrayInputStream(bytes);
    }

    @Override
    protected InputStream getInputStreamFromBufferedStream() {
//        byte[] theBytes =
//                ArrayUtils.subarray(this.bbos.buffer.array(), 0, this.bbos.length());
        return internalGetInputStreamFromBufferedStream(this.bbos.toByteArray());
    }
}
