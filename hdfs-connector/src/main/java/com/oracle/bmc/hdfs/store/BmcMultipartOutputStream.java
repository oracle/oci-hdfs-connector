package com.oracle.bmc.hdfs.store;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.transfer.MultipartManifest;
import com.oracle.bmc.objectstorage.transfer.MultipartObjectAssembler;
import com.oracle.bmc.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer buffer;
        private final int size;
        private final HashFunction md5Hasher = Hashing.md5();

        private ByteBufferOutputStream(int size) {
            this.size = size;
            this.buffer = ByteBuffer.allocate(this.size);
        }

        @Override
        public void write(final int b) throws IOException {
            buffer.put((byte)b);

            if (!buffer.hasRemaining()) {
                doUpload();
                buffer.clear();
            }
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
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

        private int size() {
            return (buffer.hasRemaining()) ? (this.size - buffer.remaining()) : this.size;
        }

        private boolean isEmpty() {
            return buffer.remaining() == this.size;
        }

        private boolean nonEmpty() {
            return !isEmpty();
        }

        private String getMD5() {
            return md5Hasher.newHasher().putBytes(buffer.array()).hash().toString();
        }
    }

    public BmcMultipartOutputStream(
            final MultipartUploadRequest request,
            final Progressable progress,
            final int bufferSizeInBytes) {
        super(null, progress, null);

        // delay creation until called in createOutputBufferStream
        this.bufferSizeInBytes = bufferSizeInBytes;
        this.request = request;
    }

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
        try {
            if (this.bbos.nonEmpty()) {
                InputStream is = this.getInputStreamFromBufferedStream();
                assembler.addPart(is, this.bbos.size(), this.bbos.getMD5());
            }
        } catch (final BmcException e) {
            throw new IOException("Unable to put object via multipart upload", e);
        } finally {
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
                this.assembler.newRequest(null, null, null, null);
        return this.bbos;
    }

    @Override
    protected long getInputStreamLengthInBytes() {
        return this.bbos.size();
    }

    @Override
    protected InputStream getInputStreamFromBufferedStream() {
        return StreamUtils.createByteArrayInputStream(this.bbos.buffer.array());
    }
}
